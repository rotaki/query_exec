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
//  COMPUTING ACTUAL QUANTILES
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
//  ESTIMATING QUANTILES
// --------------------------------------------------------------------------------------------


/// Function for estimating quantiles
pub fn estimate_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles_per_run: usize,
    method: QuantileMethod,
) -> Vec<Vec<u8>> {
    match method {
        QuantileMethod::Sampling => sample_and_combine_run_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Mean => mean_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Median => median_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Histograms => histogram_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Actual => Vec::new(), // Should not occur hopefully
    }
}


/// Approximates global quantiles by sampling and combining quantiles from individual sorted runs.
/// This is a type of "multilevel" quantile estimation that:
/// 1) First samples quantiles from each sorted run 
/// 2) Then combines these samples to estimate global quantiles
///
/// Example with 2 runs, requesting 3 quantiles:
/// Run 1: [2,4,6,8,10,12]  -> samples: [2,6,12]
/// Run 2: [3,5,7,9,11,13]  -> samples: [3,7,13]
/// Combined samples: [2,3,6,7,12,13]
/// Final quantiles: [2,7,13] (evenly spaced picks from combined sorted samples)
fn sample_and_combine_run_quantiles<M: MemPool>(
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

/// Estimates global quantiles by computing quantiles for each run and taking their mean.
/// This is a simple averaging approach that:
/// 1) First computes quantiles independently for each sorted run
/// 2) Then averages corresponding quantiles across runs
///
/// Example with 2 runs, requesting 3 quantiles:
/// Run 1: [2,4,6,8,10,12]  -> quantiles: [2,6,12]
/// Run 2: [3,5,7,9,11,13]  -> quantiles: [3,7,13]
/// Final quantiles: [(2+3)/2, (6+7)/2, (12+13)/2] = [2.5,6.5,12.5]
fn mean_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Collect quantiles from each run
    let mut run_quantiles = Vec::new();
    for run in runs.iter() {
        let mut current_run_quantiles = Vec::new();
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }

        // Calculate indices for quantiles
        let mut current_index = 0;
        for i in 1..=num_quantiles {
            let target_idx = (i * total_tuples) / (num_quantiles + 1);
            for (key, _) in run.scan().skip(current_index) {
                if current_index == target_idx {
                    current_run_quantiles.push(to_big_endian_u64(&key));
                    break;
                }
                current_index += 1;
            }
        }
        run_quantiles.push(current_run_quantiles);
    }

    // Average corresponding quantiles
    let mut result = Vec::new();
    for i in 0..num_quantiles {
        let sum: u64 = run_quantiles.iter().map(|rq| rq[i]).sum();
        let avg = sum / run_quantiles.len() as u64;
        result.push(avg.to_be_bytes().to_vec());
    }

    result
}

/// Estimates global quantiles by computing quantiles for each run and taking their median.
/// This approach is similar to mean-based but more robust to outliers:
/// 1) First computes quantiles independently for each sorted run
/// 2) Then takes the median of corresponding quantiles across runs
///
/// Example with 3 runs, requesting 3 quantiles:
/// Run 1: [2,4,6,8,10,12]   -> quantiles: [2,6,12]
/// Run 2: [1,5,7,9,11,13]   -> quantiles: [1,7,13]
/// Run 3: [3,4,6,8,10,11]   -> quantiles: [3,6,11]
/// Final quantiles: [med(2,1,3), med(6,7,6), med(12,13,11)] = [2,6,12]
///
/// Note: This method is more robust to outliers than mean-based quantiles,
/// making it potentially more reliable when some runs contain anomalous values.
fn median_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Collect quantiles from each run
    let mut run_quantiles = Vec::new();
    for run in runs.iter() {
        let mut current_run_quantiles = Vec::new();
        let total_tuples = run.scan().count();
        if total_tuples == 0 {
            continue;
        }

        // Calculate indices for quantiles
        let mut current_index = 0;
        for i in 1..=num_quantiles {
            let target_idx = (i * total_tuples) / (num_quantiles + 1);
            for (key, _) in run.scan().skip(current_index) {
                if current_index == target_idx {
                    current_run_quantiles.push(to_big_endian_u64(&key));
                    break;
                }
                current_index += 1;
            }
        }
        run_quantiles.push(current_run_quantiles);
    }

    // Take median of corresponding quantiles
    let mut result = Vec::new();
    for i in 0..num_quantiles {
        let mut values: Vec<u64> = run_quantiles.iter().map(|rq| rq[i]).collect();
        values.sort();
        let median = values[values.len() / 2];
        result.push(median.to_be_bytes().to_vec());
    }

    result
}

/// Estimates global quantiles using a histogram-based approach.
/// This method builds approximate distribution using fixed-width buckets:
/// 1) Scans each run to determine global min/max
/// 2) Creates fixed-width buckets spanning the range
/// 3) Counts values in each bucket across all runs
/// 4) Uses bucket counts to estimate quantiles
///
/// Example with range [0-100] split into 10 buckets, 2 runs, wanting median:
/// Run 1: [10,20,30,40,50]
/// Run 2: [15,25,35,45,55]
/// Buckets: [0-10):0, [10-20):2, [20-30):2, [30-40):2, [40-50):2, [50-60):2, [60-100):0
/// Total count = 10, median = 30 (middle of bucket containing 5th element)
///
/// Note: Accuracy depends on bucket width. Wider buckets are faster but less precise.
fn histogram_based_quantiles<M: MemPool>(
    runs: &[Arc<SortedRunStore<M>>],
    num_quantiles: usize,
) -> Vec<Vec<u8>> {
    if runs.is_empty() {
        return Vec::new();
    }

    // Find global min and max
    let mut global_min = u64::MAX;
    let mut global_max = u64::MIN;
    for run in runs.iter() {
        if let Some((first, _)) = run.scan().next() {
            let val = to_big_endian_u64(&first);
            global_min = global_min.min(val);
        }
        if let Some((last, _)) = run.scan().last() {
            let val = to_big_endian_u64(&last);
            global_max = global_max.max(val);
        }
    }

    // Create buckets (using 100 buckets)
    const NUM_BUCKETS: u64 = 100;
    let bucket_size = (global_max - global_min + 1) / NUM_BUCKETS;
    let mut bucket_counts = vec![0u64; NUM_BUCKETS as usize];

    // Count values in buckets
    for run in runs.iter() {
        for (key, _) in run.scan() {
            let val = to_big_endian_u64(&key);
            let bucket_idx = ((val - global_min) / bucket_size) as usize;
            let bucket_idx = bucket_idx.min(NUM_BUCKETS as usize - 1);
            bucket_counts[bucket_idx] += 1;
        }
    }

    // Calculate total count
    let total_count: u64 = bucket_counts.iter().sum();

    // Estimate quantiles using bucket counts
    let mut result = Vec::new();
    for i in 1..=num_quantiles {
        let target_count = (i * total_count as usize) / (num_quantiles + 1);
        let mut cumulative_count = 0;
        let mut bucket_idx = 0;
        
        // Find bucket containing this quantile
        while bucket_idx < NUM_BUCKETS as usize && cumulative_count < target_count {
            cumulative_count += bucket_counts[bucket_idx] as usize;
            bucket_idx += 1;
        }

        // Estimate value within bucket
        let bucket_start = global_min + (bucket_idx as u64 - 1) * bucket_size;
        let estimated_value = bucket_start + bucket_size / 2;
        result.push(estimated_value.to_be_bytes().to_vec());
    }

    result
}

// --------------------------------------------------------------------------------------------
//  EVALUATION FUNCTIONS
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
    let merged_record: QuantileRecord = serde_json::from_str(&merged_file)
        .map_err(|e| ExecError::Storage(format!("Merged parse error: {:?}", e)))?;

    // 3) Compute error
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

/// Evaluate the difference between `estimated` vs. `actual` sets of quantiles, 
/// store results in `evaluation_output` as JSON.
pub fn evaluate_and_store_quantiles_custom(
    estimated: &[Vec<u8>],
    actual: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    method: QuantileMethod,
    evaluation_output: &str,
) -> Result<(), ExecError> {
    // Example error metric: use the same approach as `compute_error_metrics` above
    let (mse, max_abs_error) = compute_error_metrics(estimated, actual);

    // Build the final record
    let evaluation = QuantileEvaluation {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        method: method.to_string(),
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


// --------------------------------------------------------------------------------------------
//  HELPER FUNCTIONS
// --------------------------------------------------------------------------------------------


fn to_big_endian_u64(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    for (i, b) in bytes.iter().rev().enumerate().take(8) {
        array[7 - i] = *b;
    }
    u64::from_be_bytes(array)
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

/// Writes quantiles to JSON
pub fn write_quantiles_to_json(
    quantiles: &[Vec<u8>],
    data_source: &str,
    query_id: u8,
    method: QuantileMethod,
    output_path: &str,
) -> Result<(), ExecError> {
    let record = QuantileRecord {
        data_source: data_source.to_string(),
        query: format!("q{}", query_id),
        method: method.to_string(),
        quantiles: quantiles.to_vec(),
    };
    
    let serialized = serde_json::to_string_pretty(&record)
        .map_err(|e| ExecError::Conversion(format!("Serialize error: {:?}", e)))?;
    std::fs::write(output_path, serialized)
        .map_err(|e| ExecError::Storage(format!("File create error: {:?}", e)))?;

    println!("Quantiles written to: {}", output_path);
    Ok(())
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
    let record: QuantileRecord = serde_json::from_str(&file_str)
        .map_err(|e| ExecError::Conversion(format!("Deserialization error: {:?}", e)))?;

    // Optionally, verify record matches your data_source, query_id, etc.
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
            let maybe_existing: serde_json::Result<QuantileRecord> = serde_json::from_str(&json_str);
            if let Ok(rec) = maybe_existing {
                if rec.data_source == data_source 
                    && rec.query == format!("q{}", query_id)
                    && rec.quantiles.len() == num_quantiles 
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
