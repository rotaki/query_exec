use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::Arc,
};

use fbtree::{
    access_method::sorted_run_store::SortedRunStore,
    bp::MemPool,
    prelude::AppendOnlyStore,
};
use crate::error::ExecError;

// --------------------------------------------------------------------------------------------
//  DATA STRUCTURES FOR JSON
// --------------------------------------------------------------------------------------------

/// Represents the "gold standard" quantiles after a full sort for (data_source, query).
#[derive(Debug, Serialize, Deserialize)]
pub struct QuantileRecord {
    pub data_source: String,
    pub query: String,
    pub quantiles: Vec<Vec<u8>>,
}

/// Represents run-level quantiles for each run, possibly storing partial or intermediate values.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunQuantileRecord {
    pub run_id: usize,
    pub quantiles: Vec<Vec<u8>>,
}

/// Represents a naive merging of run-level quantiles into a global set, for comparison.
#[derive(Debug, Serialize, Deserialize)]
pub struct MergedQuantileRecord {
    pub data_source: String,
    pub query: String,
    pub merged_quantiles: Vec<Vec<u8>>,
}

/// Represents an evaluation of merged vs. "true" quantiles (e.g., MSE, max error, etc.).
#[derive(Debug, Serialize, Deserialize)]
pub struct QuantileEvaluation {
    pub data_source: String,
    pub query: String,
    pub mse: f64,
    pub max_abs_error: f64,
}

// --------------------------------------------------------------------------------------------
//  Helper Functions
// --------------------------------------------------------------------------------------------

/// A helper that computes actual quantiles from an already-sorted `AppendOnlyStore`.
/// If the store is truly sorted, these represent the "gold-standard" quantiles.
pub fn compute_actual_quantiles_helper<M: MemPool>(
    final_store: &Arc<AppendOnlyStore<M>>,
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    // Count total tuples and check if empty
    let total_tuples = final_store.scan().count();
    if total_tuples == 0 {
        println!("No tuples to compute quantiles from.");
        return Vec::new();
    }

    // Calculate indices for each quantile
    let mut quantile_indices = Vec::new();
    for i in 1..=num_quantiles {
        let idx = (i * total_tuples) / (num_quantiles + 1);
        quantile_indices.push(idx);
    }

    // Initialize result vector
    let mut actual_quantiles = Vec::with_capacity(quantile_indices.len());
    let mut current_index = 0;
    let mut q = 0;

    // Collect quantiles directly from scan
    for (key, _) in final_store.scan() {
        if q >= quantile_indices.len() {
            break;
        }
        if current_index == quantile_indices[q] {
            actual_quantiles.push(key.clone());
            q += 1;
        }
        current_index += 1;
    }

    actual_quantiles
}
// --------------------------------------------------------------------------------------------
//  (1) Compute & store the “true” quantiles (gold standard) after a full sort
// --------------------------------------------------------------------------------------------

pub fn compute_and_store_true_quantiles<M: 'static + MemPool>(
    data_source: &str,
    query: &str,
    num_quantiles: usize,
    final_sorted_store: &Arc<AppendOnlyStore<M>>,
    output_path: &str,
) -> Result<(), ExecError> {
    let actual_quantiles = compute_actual_quantiles_helper(final_sorted_store, num_quantiles);

    let record = QuantileRecord {
        data_source: data_source.to_string(),
        query: query.to_string(),
        quantiles: actual_quantiles,
    };

    let serialized = serde_json::to_string_pretty(&record)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    let mut file = File::create(output_path)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;
    file.write_all(serialized.as_bytes())
        .map_err(|e| ExecError::Storage(format!("Write error: {:?}", e)))?;

    println!("Wrote true quantiles to: {}", output_path);
    Ok(())
}

// --------------------------------------------------------------------------------------------
//  (2) Compute  run-level quantiles
// --------------------------------------------------------------------------------------------

/// Compute partial quantiles for a single run (sorted or semi-sorted run).
pub fn compute_run_quantiles<M: 'static + MemPool>(
    run: &Arc<SortedRunStore<M>>,
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    let total_tuples = run.scan().count();
    if total_tuples == 0 {
        return Vec::new();
    }

    let mut quantile_indices = Vec::new();
    for i in 1..=num_quantiles {
        let idx = (i * total_tuples) / (num_quantiles + 1);
        quantile_indices.push(idx);
    }

    let mut run_quantiles = Vec::with_capacity(num_quantiles);
    let mut current_index = 0;
    let mut q = 0;

    for (key, _) in run.scan() {
        if q >= quantile_indices.len() {
            break;
        }
        if current_index == quantile_indices[q] {
            run_quantiles.push(key.clone());
            q += 1;
        }
        current_index += 1;
    }

    run_quantiles
}

// --------------------------------------------------------------------------------------------
//  (3) Merge run-level quantiles into a global set, store to JSON
// --------------------------------------------------------------------------------------------

/// A naive strategy that collects all run-level quantiles, sorts them,
/// picks out `desired_global_quantiles`, and then writes them to JSON.
pub fn merge_run_level_quantiles_naive(
    data_source: &str,
    query: &str,
    all_run_quantiles: &[RunQuantileRecord],
    desired_global_quantiles: usize,
    output_path: &str,
) -> Result<(), ExecError> {
    let mut all_keys = Vec::new();
    for r in all_run_quantiles {
        for q in &r.quantiles {
            all_keys.push(q.clone());
        }
    }

    all_keys.sort();
    if all_keys.is_empty() {
        let record = MergedQuantileRecord {
            data_source: data_source.to_string(),
            query: query.to_string(),
            merged_quantiles: Vec::new(),
        };
        let serialized = serde_json::to_string_pretty(&record)
            .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
        std::fs::write(output_path, serialized)
            .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;
        println!("Merged quantiles JSON created (empty).");
        return Ok(());
    }

    let total = all_keys.len();
    let mut merged = Vec::with_capacity(desired_global_quantiles);

    for i in 1..=desired_global_quantiles {
        let idx = (i * total) / (desired_global_quantiles + 1);
        merged.push(all_keys[idx].clone());
    }

    let record = MergedQuantileRecord {
        data_source: data_source.to_string(),
        query: query.to_string(),
        merged_quantiles: merged,
    };

    let serialized = serde_json::to_string_pretty(&record)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(output_path, serialized)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;

    println!("Wrote merged quantiles to: {}", output_path);
    Ok(())
}

// --------------------------------------------------------------------------------------------
//  (4) Compare merged quantiles to “true” quantiles with error metrics
// --------------------------------------------------------------------------------------------

/// Reads the gold-standard quantiles and some merged quantiles from JSON,
/// computes error metrics, and writes them to a third JSON file.
pub fn evaluate_and_store_quantiles(
    gold_path: &str,
    merged_path: &str,
    evaluation_output: &str,
) -> Result<(), ExecError> {
    // 1) Load gold
    let gold_file = std::fs::read_to_string(gold_path)
        .map_err(|e| ExecError::Storage(format!("Gold read error: {:?}", e)))?;
    let gold_record: QuantileRecord = serde_json::from_str(&gold_file)
        .map_err(|e| ExecError::Storage(format!("Gold parse error: {:?}", e)))?;

    // 2) Load merged
    let merged_file = std::fs::read_to_string(merged_path)
        .map_err(|e| ExecError::Conversion(format!("Merged read error: {:?}", e)))?;
    let merged_record: MergedQuantileRecord = serde_json::from_str(&merged_file)
        .map_err(|e| ExecError::Storage(format!("Merged parse error: {:?}", e)))?;

    // 3) Compute error
    let (mse, max_abs) = compute_error_metrics(&gold_record.quantiles, &merged_record.merged_quantiles);

    // 4) Build final evaluation struct
    let evaluation = QuantileEvaluation {
        data_source: gold_record.data_source.clone(),
        query: gold_record.query.clone(),
        mse,
        max_abs_error: max_abs,
    };

    // 5) Serialize & store
    let serialized = serde_json::to_string_pretty(&evaluation)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    let mut file = File::create(evaluation_output)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;
    file.write_all(serialized.as_bytes())
        .map_err(|e| ExecError::Storage(format!("Write error: {:?}", e)))?;

    println!("Wrote quantile evaluation to: {}", evaluation_output);
    Ok(())
}

/// Interprets each `Vec<u8>` as a big-endian u64 for numeric comparison. Adjust as needed.
fn compute_error_metrics(gold: &[Vec<u8>], merged: &[Vec<u8>]) -> (f64, f64) {
    let length = gold.len().min(merged.len());
    if length == 0 {
        return (0.0, 0.0);
    }

    let mut squared_diff_sum = 0.0;
    let mut max_abs = 0.0;
    for i in 0..length {
        let g_val = to_big_endian_u64(&gold[i]);
        let m_val = to_big_endian_u64(&merged[i]);
        let diff = (g_val as f64 - m_val as f64).abs();
        squared_diff_sum += diff * diff;
        if diff > max_abs {
            max_abs = diff;
        }
    }
    let mse = squared_diff_sum / length as f64;
    (mse, max_abs)
}

fn to_big_endian_u64(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    for (i, b) in bytes.iter().rev().enumerate().take(8) {
        array[7 - i] = *b;
    }
    u64::from_be_bytes(array)
}

// --------------------------------------------------------------------------------------------
//  CUSTOM EXTENSIONS FOR "ESTIMATED vs ACTUAL" LOGIC
// --------------------------------------------------------------------------------------------


/// Computes estimated quantiles from the runs without JSON handling
// In quantile_lib.rs
pub fn estimate_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles_per_run: usize,
) -> Vec<Vec<u8>> {
    // 1) For each run, compute partial quantiles:
    let mut all_partial_keys = Vec::new();
    for run in runs.iter() {
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }

        // Calculate indices for this run
        let mut quantile_indices = Vec::new();
        for i in 1..=num_quantiles_per_run {
            let idx = (i * total_tuples) / (num_quantiles_per_run + 1);
            quantile_indices.push(idx);
        }

        // Collect quantiles for this run
        let mut current_index = 0;
        let mut q = 0;

        for (key, _) in run.scan() {
            if q >= quantile_indices.len() {
                break;
            }
            if current_index == quantile_indices[q] {
                all_partial_keys.push(key.clone());
                q += 1;
            }
            current_index += 1;
        }
    }

    // 2) Sort them and pick out "estimated" quantiles:
    all_partial_keys.sort();
    let total_keys = all_partial_keys.len();
    if total_keys == 0 {
        return Vec::new();
    }

    // Calculate final estimated quantiles
    let mut estimated = Vec::with_capacity(num_quantiles_per_run);
    for i in 1..=num_quantiles_per_run {
        let idx = (i * total_keys) / (num_quantiles_per_run + 1);
        estimated.push(all_partial_keys[idx].clone());
    }

    estimated
}
/// Writes quantiles to JSON
pub fn write_quantiles_to_json(
    quantiles: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    output_path: &str,
) -> Result<(), ExecError> {
    let record = MergedQuantileRecord {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        merged_quantiles: quantiles.to_vec(),
    };
    
    let serialized = serde_json::to_string_pretty(&record)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(output_path, serialized)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;

    println!("Quantiles written to: {}", output_path);
    Ok(())
}


/// Check if "actual" quantiles exist in the given JSON for `(data_source, query_id, num_quantiles)`.
/// Return true if found, false otherwise.
pub fn check_actual_quantiles_exist(
    data_source: &str,
    query_id: u8,
    num_quantiles: usize,
    actual_store_json: &str,
) -> Result<bool, ExecError> {
    // Pseudocode:
    // 1) Attempt to open and parse the JSON file
    // 2) Check if there's an entry for this data_source+query_id that matches num_quantiles
    // 3) Return Ok(true) if found, else Ok(false)

    match std::fs::read_to_string(actual_store_json) {
        Ok(json_str) => {
            let maybe_existing: serde_json::Result<MergedQuantileRecord> = serde_json::from_str(&json_str);
            if let Ok(rec) = maybe_existing {
                if rec.data_source == data_source 
                    && rec.query == format!("q{}", query_id)
                    && rec.merged_quantiles.len() == num_quantiles 
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(_) => {
            // Means no file or parse error, so likely not exist
            Ok(false)
        }
    }
}

/// Compute & store "true" quantiles from the fully sorted store
pub fn compute_and_store_true_quantiles_custom<M: 'static + MemPool>(
    data_source: &str,
    query_id: u8,
    num_quantiles: usize,
    final_store: &Arc<AppendOnlyStore<M>>,
    output_path: &str,
) -> Result<Vec<Vec<u8>>, ExecError> {
    let actual = compute_actual_quantiles_helper(final_store, num_quantiles);

    // Build a record that fits your expected JSON structure
    let record = MergedQuantileRecord {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        merged_quantiles: actual.clone(),
    };

    let serialized = serde_json::to_string_pretty(&record)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(output_path, serialized)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;

    println!("Wrote actual quantiles to: {}", output_path);
    Ok(actual)
}

/// Load quantiles for `(data_source, query_id, num_quantiles)` from a JSON file you wrote earlier.
/// Return them in-memory.
pub fn load_quantiles_from_json(
    data_source: &str,
    query_id: u8,
    num_quantiles: usize,
    actual_store_json: &str,
) -> Result<Vec<Vec<u8>>, ExecError> {
    let file_str = std::fs::read_to_string(actual_store_json)
        .map_err(|e| ExecError::Storage(format!("Reading file error: {:?}", e)))?;
    let record: MergedQuantileRecord = serde_json::from_str(&file_str)
        .map_err(|e| ExecError::Conversion(format!("Deserialization error: {:?}", e)))?;

    // Optionally, verify record matches your data_source, query_id, etc.
    if record.data_source != data_source || record.query != format!("q{}", query_id) {
        return Err(ExecError::Conversion(
            "Mismatch in data_source/query_id while loading actual quantiles.".to_string(),
        ));
    }
    if record.merged_quantiles.len() < num_quantiles {
        println!("Warning: loaded fewer quantiles than expected!");
    }

    Ok(record.merged_quantiles)
}

/// Evaluate the difference between `estimated` vs. `actual` sets of quantiles, 
/// store results in `evaluation_output` as JSON.
pub fn evaluate_and_store_quantiles_custom(
    estimated: &[Vec<u8>],
    actual: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    evaluation_output: &str,
) -> Result<(), ExecError> {
    // Example error metric: use the same approach as `compute_error_metrics` above
    let (mse, max_abs_error) = compute_error_metrics(estimated, actual);

    // Build the final record
    let evaluation = QuantileEvaluation {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        mse,
        max_abs_error,
    };

    // Write to JSON
    let serialized = serde_json::to_string_pretty(&evaluation)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(evaluation_output, serialized)
        .map_err(|e| ExecError::Storage(format!("Write error: {:?}", e)))?;

    println!("Evaluation saved to: {}", evaluation_output);
    Ok(())
}
