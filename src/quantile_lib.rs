use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Write, sync::Arc};
use fbtree::{access_method::sorted_run_store::SortedRunStore, bp::MemPool, prelude::AppendOnlyStore};

use crate::error::ExecError;

// --------------------------------------------------------------------------------------------
//  TYPE DEFINITIONS AND STRUCTS
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum QuantileMethod {
    Actual,
    Mean,
    Median,
    Sampling,
    Histograms,
}

impl QuantileMethod {
    pub fn to_string(&self) -> String {
        match self {
            QuantileMethod::Actual => "actual".to_string(),
            QuantileMethod::Mean => "mean".to_string(),
            QuantileMethod::Median => "median".to_string(),
            QuantileMethod::Sampling => "sampling".to_string(),
            QuantileMethod::Histograms => "histograms".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuantileRecord {
    pub data_source: String,
    pub query: String,
    pub method: String,  // Only used for estimated quantiles, empty for actual
    pub quantiles: Vec<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuantileEvaluation {
    pub data_source: String,
    pub query: String,
    pub method: String,
    pub mse: f64,
    pub max_abs_error: f64,
}

// --------------------------------------------------------------------------------------------
//  COMPUTING ACTUAL QUANTILES (No Big-Endian)
// --------------------------------------------------------------------------------------------

/// A helper that computes actual quantiles from an already-sorted `AppendOnlyStore`.
/// We assume the `AppendOnlyStore` is sorted lexicographically by `key` (which is `Vec<u8>`).
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

    let mut actual_quantiles = Vec::with_capacity(num_quantiles);
    let mut current_index = 0;
    let mut q = 0;

    // Collect lexicographic quantiles by index
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
//  ESTIMATING QUANTILES (No Big-Endian Conversions)
// --------------------------------------------------------------------------------------------

/// Master function for estimating quantiles, dispatches to sub-methods
pub fn estimate_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles_per_run: usize,
    method: QuantileMethod,
) -> Vec<Vec<u8>> {
    match method {
        QuantileMethod::Sampling => sample_and_combine_run_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Mean     => mean_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Median   => median_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Histograms => histogram_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Actual => Vec::new(), // Shouldn't happen in "estimate" phase
    }
}

// --------------------------------------------------------------------------------------------
//  1) Sampling
// --------------------------------------------------------------------------------------------

fn sample_and_combine_run_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles_per_run: usize,
) -> Vec<Vec<u8>> {
    // 1) For each run, compute partial quantiles (lexicographic)
    let mut all_partial_keys = Vec::new();
    for run in runs.iter() {
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }

        // Indices for this run
        let mut quantile_indices = Vec::new();
        for i in 1..=num_quantiles_per_run {
            let idx = (i * total_tuples) / (num_quantiles_per_run + 1);
            quantile_indices.push(idx);
        }

        // Collect partial keys for these indices
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

    // 2) Sort them lexicographically
    all_partial_keys.sort();

    // 3) Pick out "estimated" quantiles
    let total_keys = all_partial_keys.len();
    if total_keys == 0 {
        return Vec::new();
    }

    let mut estimated = Vec::with_capacity(num_quantiles_per_run);
    for i in 1..=num_quantiles_per_run {
        let idx = (i * total_keys) / (num_quantiles_per_run + 1);
        estimated.push(all_partial_keys[idx].clone());
    }

    estimated
}

// --------------------------------------------------------------------------------------------
//  2) Mean (Element-wise Byte Average)
// --------------------------------------------------------------------------------------------

/// Computes quantiles for each run, then element-wise averages them.
fn mean_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Collect run-specific quantiles into run_quantiles:
    // run_quantiles[i][j] = j-th quantile for run i, each is Vec<u8>
    let mut run_quantiles: Vec<Vec<Vec<u8>>> = Vec::new();

    // For each run, compute `num_quantiles` partial quantiles
    for run in runs.iter() {
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }

        // Indices for partial quantiles
        let mut indices = Vec::new();
        for i in 1..=num_quantiles {
            let idx = (i * total_tuples) / (num_quantiles + 1);
            indices.push(idx);
        }

        // Gather the partial quantiles from this run
        let mut current_run_q = Vec::new();
        let mut current_index = 0;
        let mut q = 0;
        for (key, _) in run.scan() {
            if q >= indices.len() {
                break;
            }
            if current_index == indices[q] {
                current_run_q.push(key.clone());
                q += 1;
            }
            current_index += 1;
        }

        // If we found exactly `num_quantiles` partial quantiles, push
        if current_run_q.len() == num_quantiles {
            run_quantiles.push(current_run_q);
        }
    }

    if run_quantiles.is_empty() {
        return Vec::new();
    }

    // We want to produce a final set of `num_quantiles` arrays,
    // each is the element-wise average across runs for that quantile index.
    // E.g. final_mean[j] = average( run_quantiles[all runs][j] ) (element-wise).
    let num_runs = run_quantiles.len();
    let mut result = Vec::with_capacity(num_quantiles);

    for q_idx in 0..num_quantiles {
        // All runs have a q_idx-th quantile: each is a Vec<u8>
        // We'll produce one averaged Vec<u8> of the same length
        let first_len = run_quantiles[0][q_idx].len();

        // Accumulator: use u16 to avoid overflow if many runs + large bytes
        let mut acc = vec![0u16; first_len];

        // Accumulate
        for r_idx in 0..num_runs {
            let bytes = &run_quantiles[r_idx][q_idx];
            // If different runs have different lengths, pick the min.
            let len = first_len.min(bytes.len());
            for i in 0..len {
                acc[i] = acc[i].saturating_add(bytes[i] as u16);
            }
        }

        // Now produce the averaged result by dividing each sum by num_runs
        let mut avg_bytes = vec![0u8; first_len];
        for i in 0..first_len {
            avg_bytes[i] = (acc[i] / (num_runs as u16)) as u8;
        }

        result.push(avg_bytes);
    }

    result
}

// --------------------------------------------------------------------------------------------
//  3) Median (Lexicographic Across Runs)
// --------------------------------------------------------------------------------------------

/// Computes quantiles for each run, then picks the lexicographic median
/// among those corresponding quantiles.
fn median_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Similar logic as mean-based, but we'll store each run's partial quantiles,
    // then take the median among them.
    let mut run_quantiles: Vec<Vec<Vec<u8>>> = Vec::new();

    // For each run, gather partial quantiles
    for run in runs.iter() {
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }
        let mut indices = Vec::new();
        for i in 1..=num_quantiles {
            let idx = (i * total_tuples) / (num_quantiles + 1);
            indices.push(idx);
        }

        let mut current_run_q = Vec::new();
        let mut current_index = 0;
        let mut q = 0;
        for (key, _) in run.scan() {
            if q >= indices.len() {
                break;
            }
            if current_index == indices[q] {
                current_run_q.push(key.clone());
                q += 1;
            }
            current_index += 1;
        }

        if current_run_q.len() == num_quantiles {
            run_quantiles.push(current_run_q);
        }
    }

    if run_quantiles.is_empty() {
        return Vec::new();
    }

    // Now, for each quantile index q_idx, we have `run_quantiles.len()` arrays.
    // Sort them lexicographically, pick the median.
    let num_runs = run_quantiles.len();
    let median_index = num_runs / 2; // integer division

    let mut result = Vec::with_capacity(num_quantiles);

    for q_idx in 0..num_quantiles {
        // Collect the q_idx-th partial from each run
        let mut arrays: Vec<Vec<u8>> = run_quantiles.iter().map(|rq| rq[q_idx].clone()).collect();
        // Sort lexicographically
        arrays.sort();
        // Take the middle
        result.push(arrays[median_index].clone());
    }

    result
}

// --------------------------------------------------------------------------------------------
//  4) Histograms - (Stub) - Lexicographic Approach is Non-Trivial
// --------------------------------------------------------------------------------------------

/// A "histogram-based" approach for arbitrary byte arrays is non-trivial,
/// because there's no straightforward numeric range from `global_min` to
/// `global_max`. You could do lexicographic buckets, but that's more complex.
/// 
/// For demonstration, here's a stub that just picks a few sample points
/// lexicographically and returns them. (This won't behave like a numeric histogram.)
fn histogram_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Flatten all keys
    let mut all_keys = Vec::new();
    for run in runs.iter() {
        for (key, _) in run.scan() {
            all_keys.push(key.clone());
        }
    }
    if all_keys.is_empty() {
        return Vec::new();
    }
    // Sort lexicographically
    all_keys.sort();

    // We'll just pick evenly spaced samples as "histogram" quantiles:
    let total = all_keys.len();
    let mut result = Vec::new();
    for i in 1..=num_quantiles {
        let idx = (i * total) / (num_quantiles + 1);
        result.push(all_keys[idx].clone());
    }
    result
}

// --------------------------------------------------------------------------------------------
//  EVALUATION FUNCTIONS
// --------------------------------------------------------------------------------------------

/// Example that compares gold vs. merged and writes an evaluation JSON.
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
    let merged_record: QuantileRecord = serde_json::from_str(&merged_file)
        .map_err(|e| ExecError::Storage(format!("Merged parse error: {:?}", e)))?;

    // 3) Compute error (purely byte-wise difference, e.g. sum of absolute differences)
    let (mse, max_abs) = compute_error_metrics(&gold_record.quantiles, &merged_record.quantiles);

    // 4) Build final evaluation struct
    let evaluation = QuantileEvaluation {
        data_source: gold_record.data_source.clone(),
        query: gold_record.query.clone(),
        method: merged_record.method.clone(),
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

pub fn evaluate_and_store_quantiles_custom(
    estimated: &[Vec<u8>],
    actual: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    method: QuantileMethod,
    evaluation_output: &str,
) -> Result<(), ExecError> {
    let (mse, max_abs_error) = compute_error_metrics(estimated, actual);

    let evaluation = QuantileEvaluation {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        method: method.to_string(),
        mse,
        max_abs_error,
    };

    let serialized = serde_json::to_string_pretty(&evaluation)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(evaluation_output, serialized)
        .map_err(|e| ExecError::Storage(format!("Write error: {:?}", e)))?;

    println!("Evaluation saved to: {}", evaluation_output);
    Ok(())
}

// --------------------------------------------------------------------------------------------
//  HELPER FUNCTIONS
// --------------------------------------------------------------------------------------------

/// Compares two arrays of `Vec<u8>` lexicographically and measures difference byte by byte.
/// This is purely a sample. You might define "error" differently.
fn compute_error_metrics(gold: &[Vec<u8>], merged: &[Vec<u8>]) -> (f64, f64) {
    let length = gold.len().min(merged.len());
    if length == 0 {
        return (0.0, 0.0);
    }

    let mut squared_diff_sum = 0.0;
    let mut max_abs = 0.0;

    for i in 0..length {
        // Compare gold[i] and merged[i] lexicographically.
        // Let's define "distance" as sum of absolute diffs for each byte.
        let dist = byte_wise_distance(&gold[i], &merged[i]);
        squared_diff_sum += dist * dist;
        if dist > max_abs {
            max_abs = dist;
        }
    }

    let mse = squared_diff_sum / length as f64;
    (mse, max_abs)
}

/// Sum of absolute differences (SAD) for two byte arrays, truncated to the min length.
/// Example:
/// gold:   [2, 10, 200]
/// merged: [3, 9, 199]
/// dist = |2-3| + |10-9| + |200-199| = 1 + 1 + 1 = 3
fn byte_wise_distance(a: &[u8], b: &[u8]) -> f64 {
    let length = a.len().min(b.len());
    let mut sum = 0.0;
    for i in 0..length {
        let diff = (a[i] as i32 - b[i] as i32).abs();
        sum += diff as f64;
    }
    // If arrays differ in length, you might treat extra bytes as penalty.
    // For now, we ignore extra bytes in the longer array.
    sum
}

/// Writes quantiles to JSON, ensuring each quantile sub-array is on one line.
pub fn write_quantiles_to_json(
    quantiles: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    method: QuantileMethod,
    output_path: &str,
) -> Result<(), ExecError> {
    // Format the main fields
    let mut json_string = format!(
        "{{\n  \"data_source\": \"{}\",\n  \"query\": \"q{}\",\n  \"method\": \"{}\",\n  \"quantiles\": [\n",
        data_source,
        query_id,
        method.to_string()
    );

    // Format each quantile as a single line of bytes
    for (i, quantile) in quantiles.iter().enumerate() {
        let quantile_str = quantile
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if i < quantiles.len() - 1 {
            json_string.push_str(&format!("    [{}],\n", quantile_str)); 
        } else {
            json_string.push_str(&format!("    [{}]\n", quantile_str));
        }
    }

    // Close the JSON object
    json_string.push_str("  ]\n}");

    std::fs::write(output_path, json_string)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;

    println!("Quantiles written to: {}", output_path);
    Ok(())
}

/// Reads quantiles from a JSON file
pub fn load_quantiles_from_json(
    data_source: &str,
    query_id: u8,
    num_quantiles: usize,
    actual_store_json: &str,
) -> Result<Vec<Vec<u8>>, ExecError> {
    let file_str = std::fs::read_to_string(actual_store_json)
        .map_err(|e| ExecError::Storage(format!("Reading file error: {:?}", e)))?;
    let record: QuantileRecord = serde_json::from_str(&file_str)
        .map_err(|e| ExecError::Conversion(format!("Deserialization error: {:?}", e)))?;

    // Optionally check matching data_source, query
    if record.data_source != data_source || record.query != format!("q{}", query_id) {
        return Err(ExecError::Conversion(
            "Mismatch in data_source/query_id while loading actual quantiles.".to_string(),
        ));
    }
    if record.quantiles.len() < num_quantiles {
        println!("Warning: loaded fewer quantiles than expected!");
    }

    Ok(record.quantiles)
}

/// Check if "actual" quantiles exist in the given JSON
pub fn check_actual_quantiles_exist(
    data_source: &str,
    query_id: u8,
    num_quantiles: usize,
    actual_store_json: &str,
) -> Result<bool, ExecError> {
    match std::fs::read_to_string(actual_store_json) {
        Ok(json_str) => {
            let parsed: serde_json::Result<QuantileRecord> = serde_json::from_str(&json_str);
            if let Ok(rec) = parsed {
                if rec.data_source == data_source 
                    && rec.query == format!("q{}", query_id)
                    && rec.quantiles.len() == num_quantiles 
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(_) => Ok(false),
    }
}

