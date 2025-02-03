use serde::{Deserialize, Serialize};
use core::num;
use std::{collections::HashMap, fs::File, io::Write, sync::Arc};
use fbtree::{access_method::sorted_run_store::{SortedRunStore, BigSortedRunStore},  bp::MemPool, prelude::AppendOnlyStore};
use std::cmp::Ordering;


use crate::error::ExecError;

// --------------------------------------------------------------------------------------------
//  TYPE DEFINITIONS AND STRUCTS
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum QuantileMethod {
    TPCH_100,
    GENSORT_1,
    GENSORT_4,
    Mean,
    Median,
    Sampling,
    Histograms,
    GKSketch,
}

impl QuantileMethod {
    pub fn to_string(&self) -> String {
        match self {
            QuantileMethod::TPCH_100 => "TPCH".to_string(),
            QuantileMethod::GENSORT_1 => "GENSORT_1".to_string(),
            QuantileMethod::GENSORT_4 => "GENSORT_4".to_string(),
            QuantileMethod::Mean => "mean".to_string(),
            QuantileMethod::Median => "median".to_string(),
            QuantileMethod::Sampling => "sampling".to_string(),
            QuantileMethod::Histograms => "histograms".to_string(),
            QuantileMethod::GKSketch => "GK Sketch".to_string(),
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
    runs: &[Arc<BigSortedRunStore<M>>],
    num_quantiles_per_run: usize,
    method: QuantileMethod,
) -> Vec<Vec<u8>> {
    match method {
        QuantileMethod::Sampling => vec![],
        // sample_and_combine_run_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Mean     => mean_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Median   => median_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::Histograms => histogram_based_quantiles(runs, num_quantiles_per_run),
        QuantileMethod::GKSketch => gk_sketch_quantiles(runs, num_quantiles_per_run, 0.00005),
        QuantileMethod::TPCH_100 | QuantileMethod::GENSORT_1 | QuantileMethod::GENSORT_4 => {
            let baseline = match method {
                QuantileMethod::TPCH_100 => baseline_tpch100(),
                QuantileMethod::GENSORT_1 => baseline_gensort1(),
                QuantileMethod::GENSORT_4 => baseline_gensort4(),
                _ => unreachable!(),
            };
            filter_quantiles(&baseline, num_quantiles_per_run)
        },
        _ => panic!("implemented quantile estimation"),
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
    runs: &[Arc<BigSortedRunStore<M>>],
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
    runs: &[Arc<BigSortedRunStore<M>>],
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
    runs: &[Arc<BigSortedRunStore<M>>],
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
//  5) GK sketches
// --------------------------------------------------------------------------------------------

/// A tuple in the GK sketch for tracking rank information
struct GKTuple {
    value: Vec<u8>,
    g: usize,     // Gap from previous rank
    delta: usize, // Maximum error
}

/// Compute quantiles using the GK sketch algorithm 
pub fn gk_sketch_quantiles<M: MemPool>(
    runs: &[Arc<BigSortedRunStore<M>>],
    num_quantiles: usize,
    epsilon: f64,
) -> Vec<Vec<u8>> {
    // Validate inputs and handle edge cases
    if runs.is_empty() || num_quantiles == 0 {
        return Vec::new();
    }

    // Initialize data structures
    let mut tuples: Vec<GKTuple> = Vec::new();
    let mut total_tuples = 0;

    // First pass: Count total tuples and scan initial samples
    for run in runs.iter() {
        total_tuples += run.scan().count();
    }

    if total_tuples == 0 {
        return Vec::new();
    }

    // Compute initial sampling rate based on epsilon
    let alpha = (2.0 * epsilon * total_tuples as f64).floor() as usize;
    let sample_rate = (alpha / runs.len()).max(1);

    // Build initial sketch from sampled tuples
    for run in runs.iter() {
        let mut current_count = 0;
        let mut last_value: Option<Vec<u8>> = None;
        let mut current_g = 0;

        for (value, _) in run.scan() {
            current_count += 1;
            current_g += 1;

            // Process value based on sampling rate
            if current_count % sample_rate == 0 
                || last_value.as_ref().map_or(true, |v| v != &value) {
                
                // Compute delta based on position
                let delta = if tuples.is_empty() {
                    alpha 
                } else {
                    (alpha - current_g).min(current_g)
                };

                tuples.push(GKTuple {
                    value: value.clone(),
                    g: current_g,
                    delta,
                });

                current_g = 0;
                last_value = Some(value);
            }
        }
    }

    // Sort tuples by value for merging
    tuples.sort_by(|a, b| a.value.cmp(&b.value));

    // Merge adjacent tuples while maintaining error bounds
    let mut merged = Vec::new();
    let mut i = 0;
    while i < tuples.len() {
        let mut j = i + 1;
        let mut g_sum = tuples[i].g;

        while j < tuples.len() {
            let next_g_sum = g_sum + tuples[j].g;
            if next_g_sum + tuples[j].delta <= alpha {
                g_sum = next_g_sum;
                j += 1;
            } else {
                break;
            }
        }

        merged.push(GKTuple {
            value: tuples[i].value.clone(),
            g: g_sum,
            delta: tuples[i].delta,
        });
        i = j;
    }

    // Extract final quantiles
    let mut result = Vec::with_capacity(num_quantiles);
    let mut current_rank = 0;

    for i in 1..=num_quantiles {
        let target_rank = (i * total_tuples) / (num_quantiles + 1);
        
        // Find tuple containing target rank
        while current_rank < merged.len() {
            let tuple = &merged[current_rank];
            let upper_rank = tuple.g + tuple.delta;
            
            if target_rank <= upper_rank {
                result.push(tuple.value.clone());
                break;
            }
            
            current_rank += 1;
        }

        // Handle edge case for last quantile
        if current_rank >= merged.len() && result.len() < num_quantiles {
            if let Some(last) = merged.last() {
                result.push(last.value.clone());
            }
        }
    }

    result
}

/// Helper function to check if two tuples can be merged within error bounds
fn can_merge(t1: &GKTuple, t2: &GKTuple, alpha: usize) -> bool {
    let merged_g = t1.g + t2.g;
    merged_g + t1.delta.max(t2.delta) <= alpha
}

/// Implementation for parallel processing using multiple runs
pub fn parallel_gk_quantiles<M: MemPool>(
    runs: &[Arc<BigSortedRunStore<M>>],
    num_quantiles: usize,
    epsilon: f64,
    num_threads: usize,
) -> Vec<Vec<u8>> {
    if runs.len() <= num_threads {
        return gk_sketch_quantiles(runs, num_quantiles, epsilon);
    }

    // Split runs among threads
    let runs_per_thread = runs.len() / num_threads;
    let mut thread_results = Vec::with_capacity(num_threads);

    // Process each chunk of runs
    for chunk in runs.chunks(runs_per_thread) {
        let sketch_result = gk_sketch_quantiles(chunk, num_quantiles, epsilon / 2.0);
        thread_results.push(sketch_result);
    }

    // Merge results from all threads
    let mut merged = thread_results[0].clone();
    for other in thread_results.iter().skip(1) {
        let mut combined = Vec::new();
        let mut i = 0;
        let mut j = 0;

        // Merge sorted sequences
        while i < merged.len() && j < other.len() {
            match merged[i].cmp(&other[j]) {
                Ordering::Less => {
                    combined.push(merged[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    combined.push(other[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    combined.push(merged[i].clone());
                    i += 1;
                    j += 1;
                }
            }
        }

        // Add remaining elements
        combined.extend_from_slice(&merged[i..]);
        combined.extend_from_slice(&other[j..]);

        merged = combined;
    }

    // Extract final number of quantiles
    merged.into_iter().take(num_quantiles).collect()
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


/// Helper function that “filters” a full set of quantiles to the requested count.
///
/// Given a slice of quantile arrays (of length n), we select the desired number
/// by evenly spacing indices according to the formula
///
///  idx = ⎣(i * total) / (desired + 1)⎦,  for i = 1, 2, …, desired.
///
/// (If the caller requests as many or more quantiles than available, we simply return a copy.)
fn filter_quantiles(full: &[Vec<u8>], desired: usize) -> Vec<Vec<u8>> {
    let total = full.len();
    if desired >= total {
        return full.to_vec();
    }
    let mut result = Vec::with_capacity(desired);
    for i in 1..=desired {
        let idx = (i * total) / (desired + 1);
        result.push(full[idx].clone());
    }
    result
}

fn baseline_tpch100() -> Vec<Vec<u8>> {
    vec![
        vec![0, 0, 0, 0, 0, 0, 0, 0, 1],
        vec![0, 0, 0, 0, 0, 0, 0, 1, 161],
        vec![0, 0, 0, 0, 0, 0, 0, 3, 65],
        vec![0, 0, 0, 0, 0, 0, 0, 4, 227],
        vec![0, 0, 0, 0, 0, 0, 0, 6, 132],
        vec![0, 0, 0, 0, 0, 0, 0, 8, 40],
        vec![0, 0, 0, 0, 0, 0, 0, 9, 199],
        vec![0, 0, 0, 0, 0, 0, 0, 11, 107],
        vec![0, 0, 0, 0, 0, 0, 0, 13, 10],
        vec![0, 0, 0, 0, 0, 0, 0, 14, 169],
        vec![0, 0, 0, 0, 0, 0, 0, 16, 73],
        vec![0, 0, 0, 0, 0, 0, 0, 17, 237],
        vec![0, 0, 0, 0, 0, 0, 0, 19, 135],
        vec![0, 0, 0, 0, 0, 0, 0, 21, 43],
        vec![0, 0, 0, 0, 0, 0, 0, 22, 204],
        vec![0, 0, 0, 0, 0, 0, 0, 24, 109],
        vec![0, 0, 0, 0, 0, 0, 0, 26, 13],
        vec![0, 0, 0, 0, 0, 0, 0, 27, 172],
        vec![0, 0, 0, 0, 0, 0, 0, 29, 73],
        vec![0, 0, 0, 0, 0, 0, 0, 30, 234],
        vec![0, 0, 0, 0, 0, 0, 0, 32, 139],
        vec![0, 0, 0, 0, 0, 0, 0, 34, 45],
        vec![0, 0, 0, 0, 0, 0, 0, 35, 207],
        vec![0, 0, 0, 0, 0, 0, 0, 37, 111],
        vec![0, 0, 0, 0, 0, 0, 0, 39, 16],
    ]
}



/// Baseline quantiles for GENSORT_1. (These are placeholder values; replace them with the true ones.)
fn baseline_gensort1() -> Vec<Vec<u8>> {
    vec![
        vec![0, 50, 48, 50, 48, 50, 50, 55, 98, 53, 57, 51, 101, 51, 97, 50, 51, 50, 53, 54, 48],
        vec![0, 50, 48, 55, 98, 54, 52, 52, 55, 53, 55, 51, 53, 52, 102, 53, 48, 50, 102, 52, 98],
        vec![0, 50, 49, 55, 57, 51, 53, 52, 53, 52, 100, 55, 98, 52, 51, 51, 55, 52, 97, 51, 100],
        vec![0, 50, 50, 55, 53, 52, 55, 53, 100, 55, 56, 53, 57, 52, 51, 50, 57, 50, 49, 50, 54],
        vec![0, 50, 51, 55, 48, 50, 50, 54, 99, 53, 98, 53, 97, 52, 55, 54, 99, 51, 100, 50, 48],
        vec![0, 50, 52, 54, 97, 54, 99, 52, 51, 55, 98, 55, 50, 52, 97, 50, 53, 50, 51, 52, 55],
        vec![0, 50, 53, 54, 56, 53, 55, 50, 54, 50, 52, 50, 56, 55, 53, 53, 56, 50, 53, 53, 49],
        vec![0, 50, 54, 54, 52, 52, 101, 52, 52, 52, 49, 54, 101, 50, 55, 55, 52, 50, 53, 53, 48],
        vec![0, 50, 55, 54, 48, 52, 54, 50, 54, 54, 54, 51, 51, 52, 48, 51, 54, 50, 98, 50, 56],
        vec![0, 50, 56, 53, 99, 54, 51, 53, 48, 53, 99, 50, 50, 55, 56, 54, 55, 51, 49, 51, 98],
        vec![0, 50, 57, 53, 97, 51, 102, 54, 53, 51, 52, 51, 102, 55, 56, 50, 57, 53, 98, 53, 52],
        vec![0, 50, 97, 53, 54, 50, 57, 52, 48, 52, 55, 50, 57, 55, 53, 54, 99, 54, 101, 53, 55],
        vec![0, 50, 98, 53, 50, 52, 53, 54, 56, 52, 54, 53, 102, 52, 102, 55, 97, 52, 52, 51, 100],
        vec![0, 50, 99, 52, 102, 52, 50, 53, 101, 54, 97, 52, 101, 53, 57, 51, 102, 52, 100, 52, 50],
        vec![0, 50, 100, 52, 57, 54, 51, 55, 54, 50, 51, 54, 98, 54, 51, 53, 53, 50, 97, 55, 52],
        vec![0, 50, 101, 52, 56, 54, 51, 50, 98, 55, 54, 51, 53, 50, 99, 51, 99, 53, 50, 54, 98],
        vec![0, 50, 102, 51, 102, 52, 51, 53, 100, 54, 55, 50, 48, 55, 51, 50, 54, 50, 98, 51, 100],
        vec![0, 51, 48, 51, 98, 53, 55, 50, 101, 51, 100, 51, 97, 50, 53, 54, 50, 50, 98, 51, 102],
        vec![0, 51, 49, 51, 56, 53, 52, 52, 54, 54, 98, 50, 99, 50, 97, 52, 53, 55, 56, 53, 50],
        vec![0, 51, 50, 51, 49, 53, 101, 51, 98, 52, 99, 50, 100, 53, 55, 52, 53, 54, 54, 50, 51],
        vec![0, 51, 51, 50, 101, 52, 51, 53, 53, 53, 52, 50, 52, 52, 51, 53, 56, 50, 97, 54, 102],
        vec![0, 51, 52, 50, 55, 55, 52, 52, 97, 55, 98, 55, 48, 53, 102, 55, 52, 54, 97, 52, 54],
        vec![0, 51, 53, 50, 48, 51, 50, 55, 51, 53, 49, 52, 55, 51, 50, 52, 56, 53, 98, 52, 57],
        vec![0, 51, 53, 55, 97, 50, 50, 53, 55, 55, 51, 51, 98, 52, 53, 53, 101, 55, 57, 54, 50],
        vec![0, 51, 54, 55, 50, 51, 100, 52, 56, 51, 57, 54, 98, 53, 99, 54, 101, 54, 54, 55, 52],
        vec![0, 51, 55, 54, 100, 53, 55, 51, 49, 53, 98, 55, 51, 55, 99, 55, 99, 52, 54, 53, 57],
        vec![0, 51, 56, 54, 97, 51, 97, 52, 48, 51, 51, 54, 53, 52, 52, 51, 48, 55, 56, 53, 48],
        vec![0, 51, 57, 54, 55, 50, 53, 51, 102, 54, 48, 55, 48, 50, 50, 52, 100, 53, 54, 52, 98],
        vec![0, 51, 97, 54, 51, 51, 56, 53, 102, 53, 52, 50, 98, 52, 54, 52, 97, 50, 102, 50, 56],
        vec![0, 51, 98, 54, 49, 52, 55, 55, 101, 54, 56, 52, 101, 52, 55, 52, 56, 52, 54, 53, 49],
        vec![0, 51, 99, 53, 56, 50, 102, 54, 98, 50, 52, 54, 98, 55, 100, 53, 102, 51, 51, 53, 101],
        vec![0, 51, 100, 53, 49, 52, 99, 50, 97, 55, 98, 54, 97, 54, 56, 55, 51, 53, 97, 52, 99],
        vec![0, 51, 101, 52, 99, 54, 98, 52, 100, 53, 100, 53, 57, 50, 49, 53, 50, 54, 48, 52, 56],
        vec![0, 51, 102, 52, 55, 54, 98, 55, 54, 55, 53, 51, 56, 54, 52, 51, 99, 54, 97, 54, 51],
        vec![0, 52, 48, 52, 51, 54, 98, 53, 51, 50, 50, 53, 53, 51, 50, 53, 100, 55, 51, 50, 100],
        vec![0, 52, 49, 51, 99, 51, 102, 55, 101, 55, 50, 54, 51, 54, 98, 53, 56, 54, 49, 55, 52],
        vec![0, 52, 50, 51, 54, 53, 102, 50, 99, 52, 56, 55, 53, 52, 50, 55, 51, 53, 55, 55, 98],
        vec![0, 52, 51, 51, 51, 54, 53, 50, 57, 53, 99, 50, 97, 53, 97, 53, 50, 55, 100, 54, 56],
        vec![0, 52, 52, 50, 101, 52, 97, 53, 99, 50, 53, 55, 99, 55, 56, 53, 52, 54, 51, 50, 98],
        vec![0, 52, 53, 50, 54, 55, 98, 55, 50, 55, 101, 54, 99, 52, 49, 50, 50, 51, 100, 54, 50],
        vec![0, 52, 54, 50, 48, 51, 102, 51, 100, 52, 52, 51, 102, 54, 99, 55, 49, 50, 49, 51, 99],
        vec![0, 52, 54, 55, 98, 55, 48, 52, 56, 52, 53, 52, 98, 50, 53, 50, 49, 53, 100, 54, 51],
        vec![0, 52, 55, 55, 54, 54, 100, 51, 54, 55, 101, 50, 56, 51, 57, 51, 100, 55, 100, 55, 51],
        vec![0, 52, 56, 54, 102, 55, 98, 53, 102, 50, 99, 52, 99, 51, 49, 54, 52, 51, 52, 55, 48],
        vec![0, 52, 57, 54, 99, 52, 54, 50, 49, 52, 52, 54, 53, 50, 97, 50, 54, 53, 54, 51, 97],
        vec![0, 52, 97, 54, 54, 53, 98, 52, 99, 54, 99, 55, 50, 54, 51, 50, 49, 52, 53, 55, 49],
        vec![0, 52, 98, 53, 101, 55, 57, 51, 101, 50, 100, 55, 49, 51, 101, 52, 53, 50, 99, 50, 56],
        vec![0, 52, 99, 53, 98, 50, 99, 55, 101, 53, 97, 51, 53, 55, 53, 54, 55, 55, 52, 50, 55],
        vec![0, 52, 100, 53, 53, 52, 101, 51, 102, 51, 99, 51, 98, 54, 49, 54, 98, 50, 99, 52, 50],
        vec![0, 52, 101, 53, 48, 55, 52, 52, 54, 51, 97, 55, 49, 52, 48, 51, 97, 50, 49, 55, 56],
        vec![0, 52, 102, 52, 57, 55, 52, 55, 52, 54, 54, 50, 52, 53, 50, 51, 53, 53, 55, 55, 56],
        vec![0, 53, 48, 52, 54, 54, 56, 54, 101, 51, 56, 54, 53, 53, 102, 54, 102, 55, 100, 54, 55],
        vec![0, 53, 49, 52, 51, 54, 102, 51, 54, 53, 56, 50, 53, 55, 55, 51, 56, 54, 49, 50, 56],
        vec![0, 53, 50, 52, 48, 55, 57, 50, 56, 51, 102, 54, 48, 54, 55, 52, 48, 52, 97, 55, 50],
        vec![0, 53, 51, 51, 97, 55, 49, 50, 50, 54, 99, 53, 54, 51, 97, 53, 57, 51, 53, 52, 97],
        vec![0, 53, 52, 51, 56, 50, 50, 55, 52, 53, 55, 50, 54, 55, 54, 51, 53, 54, 99, 55, 52],
        vec![0, 53, 53, 51, 51, 50, 55, 52, 51, 54, 48, 53, 55, 55, 49, 54, 51, 51, 101, 54, 57],
        vec![0, 53, 54, 50, 99, 55, 101, 52, 51, 54, 55, 50, 50, 50, 51, 50, 56, 51, 50, 54, 55],
        vec![0, 53, 55, 50, 54, 54, 55, 53, 48, 54, 101, 52, 52, 54, 48, 51, 56, 52, 101, 53, 97],
        vec![0, 53, 56, 50, 50, 50, 48, 52, 100, 50, 50, 54, 51, 54, 101, 53, 101, 51, 50, 54, 49],
        vec![0, 53, 56, 55, 99, 50, 52, 54, 48, 51, 55, 51, 50, 50, 99, 51, 101, 53, 50, 51, 56],
        vec![0, 53, 57, 55, 55, 54, 99, 53, 100, 54, 55, 53, 54, 54, 102, 51, 53, 51, 100, 52, 102],
        vec![0, 53, 97, 55, 52, 55, 52, 51, 55, 54, 51, 50, 100, 50, 51, 52, 56, 51, 52, 52, 98],
        vec![0, 53, 98, 55, 49, 53, 51, 53, 49, 54, 102, 52, 51, 53, 53, 50, 99, 51, 100, 50, 55],
        vec![0, 53, 99, 54, 98, 53, 51, 51, 56, 53, 98, 53, 51, 55, 49, 52, 98, 51, 99, 52, 51],
        vec![0, 53, 100, 54, 54, 52, 50, 51, 52, 50, 53, 55, 55, 53, 51, 50, 99, 50, 53, 50, 51],
        vec![0, 53, 101, 54, 48, 54, 101, 52, 102, 52, 101, 51, 56, 53, 53, 53, 51, 54, 98, 52, 98],
        vec![0, 53, 102, 53, 100, 51, 102, 55, 49, 51, 48, 53, 51, 52, 97, 50, 56, 50, 48, 53, 53],
        vec![0, 54, 48, 53, 57, 51, 53, 53, 97, 50, 48, 52, 54, 52, 57, 50, 99, 53, 50, 54, 98],
        vec![0, 54, 49, 53, 49, 51, 97, 54, 97, 53, 98, 55, 98, 54, 51, 51, 49, 53, 55, 52, 51],
        vec![0, 54, 50, 52, 56, 51, 53, 52, 100, 54, 99, 52, 51, 53, 100, 54, 49, 51, 97, 54, 56],
        vec![0, 54, 51, 52, 51, 53, 53, 55, 98, 55, 52, 50, 98, 54, 50, 52, 99, 52, 49, 55, 50],
        vec![0, 54, 52, 51, 100, 54, 50, 53, 101, 51, 56, 53, 54, 53, 49, 52, 51, 55, 57, 52, 50],
        vec![0, 54, 53, 51, 57, 55, 54, 53, 50, 54, 99, 54, 53, 55, 48, 54, 57, 51, 52, 51, 49],
        vec![0, 54, 54, 51, 52, 53, 52, 50, 98, 51, 52, 53, 51, 53, 56, 54, 51, 54, 53, 52, 50],
        vec![0, 54, 55, 51, 49, 52, 50, 54, 101, 55, 49, 53, 52, 52, 48, 55, 54, 50, 52, 51, 55],
        vec![0, 54, 56, 50, 99, 54, 101, 51, 57, 54, 98, 55, 98, 52, 50, 52, 53, 53, 100, 53, 54],
        vec![0, 54, 57, 50, 57, 52, 57, 55, 101, 51, 98, 52, 56, 51, 49, 53, 57, 54, 51, 53, 55],
        vec![0, 54, 97, 50, 51, 54, 97, 52, 54, 51, 50, 51, 98, 51, 99, 51, 48, 55, 53, 50, 56],
        vec![0, 54, 97, 55, 100, 51, 102, 52, 56, 55, 51, 53, 54, 52, 48, 55, 99, 50, 49, 51, 57],
        vec![0, 54, 98, 55, 97, 51, 48, 53, 100, 50, 99, 53, 56, 50, 49, 51, 102, 52, 97, 53, 100],
        vec![0, 54, 99, 55, 52, 52, 98, 54, 57, 50, 102, 51, 51, 55, 49, 50, 55, 53, 101, 54, 98],
        vec![0, 54, 100, 55, 49, 50, 49, 53, 48, 52, 52, 54, 55, 51, 48, 50, 52, 54, 98, 52, 50],
        vec![0, 54, 101, 54, 98, 51, 101, 51, 50, 50, 55, 55, 99, 51, 99, 52, 52, 53, 55, 52, 54],
        vec![0, 54, 102, 54, 56, 54, 51, 50, 99, 53, 98, 53, 49, 53, 98, 51, 48, 53, 55, 53, 54],
        vec![0, 55, 48, 54, 49, 54, 97, 54, 54, 51, 50, 51, 53, 50, 102, 52, 57, 54, 54, 50, 52],
        vec![0, 55, 49, 53, 100, 54, 97, 53, 55, 53, 100, 50, 98, 53, 53, 53, 52, 55, 98, 53, 100],
        vec![0, 55, 50, 53, 98, 50, 50, 52, 52, 50, 53, 54, 56, 54, 102, 54, 102, 54, 101, 54, 48],
        vec![0, 55, 51, 53, 55, 55, 51, 52, 53, 54, 52, 50, 50, 55, 98, 51, 97, 52, 97, 51, 98],
        vec![0, 55, 52, 53, 50, 51, 53, 50, 57, 55, 97, 52, 53, 51, 57, 54, 100, 52, 98, 50, 50],
        vec![0, 55, 53, 52, 101, 52, 51, 51, 55, 51, 50, 52, 102, 51, 54, 53, 56, 54, 50, 51, 51],
        vec![0, 55, 54, 52, 97, 51, 101, 54, 56, 50, 101, 55, 97, 53, 57, 53, 100, 55, 49, 50, 52],
        vec![0, 55, 55, 52, 53, 50, 51, 50, 100, 52, 53, 50, 57, 53, 48, 51, 54, 54, 52, 52, 102],
        vec![0, 55, 56, 51, 102, 53, 100, 52, 101, 54, 54, 55, 101, 55, 97, 52, 101, 55, 49, 53, 51],
        vec![0, 55, 57, 51, 56, 53, 48, 54, 56, 55, 50, 53, 99, 53, 98, 50, 56, 52, 100, 50, 97],
        vec![0, 55, 97, 51, 52, 55, 98, 51, 53, 52, 98, 51, 53, 54, 100, 54, 97, 52, 52, 51, 50],
        vec![0, 55, 98, 51, 48, 54, 48, 50, 49, 52, 102, 51, 49, 54, 102, 50, 52, 52, 102, 53, 48],
        vec![0, 55, 99, 50, 99, 54, 100, 52, 101, 50, 97, 53, 55, 53, 98, 51, 99, 50, 99, 54, 51],
        vec![0, 55, 100, 50, 56, 50, 52, 55, 54, 52, 100, 52, 100, 54, 100, 50, 50, 54, 100, 54, 99],
        vec![0, 55, 101, 50, 53, 51, 102, 53, 97, 51, 99, 53, 101, 53, 57, 55, 51, 53, 99, 54, 53],
        vec![0, 55, 101, 55, 101, 55, 100, 50, 98, 55, 52, 52, 102, 50, 98, 50, 100, 54, 55, 55, 100],
        vec![0, 55, 101, 55, 101, 55, 100, 50, 98, 55, 52, 52, 102, 50, 98, 50, 100, 54, 55, 55, 100],
    ]
}

fn baseline_gensort4() -> Vec<Vec<u8>> {
    vec![
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 32, 32, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 48, 48, 48, 48, 49, 49, 49, 49, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 57, 67, 54, 32, 32, 54, 54, 54, 54, 52, 52, 52, 52, 52, 52, 52, 52, 55, 55, 55, 55, 65, 65, 65, 65, 48, 48, 48, 48, 69, 69, 69, 69, 70, 70, 70, 70, 57, 57, 57, 57, 69, 69, 69, 69, 50, 50, 50, 50, 67, 67, 67, 67, 66, 66, 66, 66, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 51, 56, 67, 32, 32, 50, 50, 50, 50, 70, 70, 70, 70, 68, 68, 68, 68, 56, 56, 56, 56, 53, 53, 53, 53, 54, 54, 54, 54, 54, 54, 54, 54, 68, 68, 68, 68, 53, 53, 53, 53, 54, 54, 54, 54, 65, 65, 65, 65, 69, 69, 69, 69, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 68, 53, 51, 32, 32, 56, 56, 56, 56, 66, 66, 66, 66, 52, 52, 52, 52, 69, 69, 69, 69, 54, 54, 54, 54, 54, 54, 54, 54, 48, 48, 48, 48, 52, 52, 52, 52, 54, 54, 54, 54, 48, 48, 48, 48, 51, 51, 51, 51, 67, 67, 67, 67, 67, 67, 67, 67, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 55, 49, 57, 32, 32, 48, 48, 48, 48, 51, 51, 51, 51, 66, 66, 66, 66, 69, 69, 69, 69, 52, 52, 52, 52, 52, 52, 52, 52, 53, 53, 53, 53, 56, 56, 56, 56, 48, 48, 48, 48, 68, 68, 68, 68, 53, 53, 53, 53, 54, 54, 54, 54, 69, 69, 69, 69, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 48, 68, 70, 32, 32, 66, 66, 66, 66, 54, 54, 54, 54, 50, 50, 50, 50, 54, 54, 54, 54, 53, 53, 53, 53, 55, 55, 55, 55, 53, 53, 53, 53, 57, 57, 57, 57, 69, 69, 69, 69, 50, 50, 50, 50, 55, 55, 55, 55, 50, 50, 50, 50, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 65, 65, 54, 32, 32, 54, 54, 54, 54, 69, 69, 69, 69, 67, 67, 67, 67, 53, 53, 53, 53, 67, 67, 67, 67, 66, 66, 66, 66, 57, 57, 57, 57, 65, 65, 65, 65, 54, 54, 54, 54, 70, 70, 70, 70, 66, 66, 66, 66, 54, 54, 54, 54, 66, 66, 66, 66, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 52, 52, 54, 67, 32, 32, 48, 48, 48, 48, 67, 67, 67, 67, 65, 65, 65, 65, 53, 53, 53, 53, 70, 70, 70, 70, 69, 69, 69, 69, 52, 52, 52, 52, 56, 56, 56, 56, 56, 56, 56, 56, 51, 51, 51, 51, 56, 56, 56, 56, 56, 56, 56, 56, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 52, 69, 51, 51, 32, 32, 50, 50, 50, 50, 51, 51, 51, 51, 50, 50, 50, 50, 67, 67, 67, 67, 68, 68, 68, 68, 65, 65, 65, 65, 49, 49, 49, 49, 51, 51, 51, 51, 50, 50, 50, 50, 67, 67, 67, 67, 48, 48, 48, 48, 69, 69, 69, 69, 67, 67, 67, 67, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 53, 55, 70, 57, 32, 32, 65, 65, 65, 65, 49, 49, 49, 49, 54, 54, 54, 54, 53, 53, 53, 53, 65, 65, 65, 65, 68, 68, 68, 68, 67, 67, 67, 67, 52, 52, 52, 52, 55, 55, 55, 55, 50, 50, 50, 50, 66, 66, 66, 66, 56, 56, 56, 56, 69, 69, 69, 69, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 54, 49, 66, 70, 32, 32, 50, 50, 50, 50, 65, 65, 65, 65, 70, 70, 70, 70, 48, 48, 48, 48, 57, 57, 57, 57, 51, 51, 51, 51, 54, 54, 54, 54, 51, 51, 51, 51, 48, 48, 48, 48, 53, 53, 53, 53, 69, 69, 69, 69, 52, 52, 52, 52, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 54, 66, 56, 54, 32, 32, 70, 70, 70, 70, 48, 48, 48, 48, 68, 68, 68, 68, 65, 65, 65, 65, 51, 51, 51, 51, 55, 55, 55, 55, 49, 49, 49, 49, 51, 51, 51, 51, 54, 54, 54, 54, 69, 69, 69, 69, 52, 52, 52, 52, 48, 48, 48, 48, 66, 66, 66, 66, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 55, 53, 52, 67, 32, 32, 50, 50, 50, 50, 55, 55, 55, 55, 53, 53, 53, 53, 68, 68, 68, 68, 69, 69, 69, 69, 48, 48, 48, 48, 54, 54, 54, 54, 54, 54, 54, 54, 67, 67, 67, 67, 53, 53, 53, 53, 54, 54, 54, 54, 50, 50, 50, 50, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 55, 70, 49, 50, 32, 32, 69, 69, 69, 69, 57, 57, 57, 57, 67, 67, 67, 67, 55, 55, 55, 55, 70, 70, 70, 70, 55, 55, 55, 55, 67, 67, 67, 67, 52, 52, 52, 52, 52, 52, 52, 52, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 56, 56, 68, 57, 32, 32, 54, 54, 54, 54, 54, 54, 54, 54, 52, 52, 52, 52, 67, 67, 67, 67, 66, 66, 66, 66, 55, 55, 55, 55, 68, 68, 68, 68, 66, 66, 66, 66, 53, 53, 53, 53, 49, 49, 49, 49, 49, 49, 49, 49, 65, 65, 65, 65, 69, 69, 69, 69, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 57, 50, 57, 70, 32, 32, 51, 51, 51, 51, 50, 50, 50, 50, 70, 70, 70, 70, 49, 49, 49, 49, 70, 70, 70, 70, 66, 66, 66, 66, 68, 68, 68, 68, 70, 70, 70, 70, 69, 69, 69, 69, 65, 65, 65, 65, 53, 53, 53, 53, 54, 54, 54, 54, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 57, 67, 54, 54, 32, 32, 54, 54, 54, 54, 70, 70, 70, 70, 67, 67, 67, 67, 52, 52, 52, 52, 66, 66, 66, 66, 55, 55, 55, 55, 67, 67, 67, 67, 53, 53, 53, 53, 49, 49, 49, 49, 57, 57, 57, 57, 67, 67, 67, 67, 65, 65, 65, 65, 66, 66, 66, 66, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 65, 54, 50, 67, 32, 32, 53, 53, 53, 53, 49, 49, 49, 49, 52, 52, 52, 52, 65, 65, 65, 65, 70, 70, 70, 70, 52, 52, 52, 52, 52, 52, 52, 52, 54, 54, 54, 54, 57, 57, 57, 57, 67, 67, 67, 67, 51, 51, 51, 51, 67, 67, 67, 67, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 65, 70, 70, 50, 32, 32, 51, 51, 51, 51, 48, 48, 48, 48, 50, 50, 50, 50, 66, 66, 66, 66, 54, 54, 54, 54, 53, 53, 53, 53, 67, 67, 67, 67, 48, 48, 48, 48, 57, 57, 57, 57, 52, 52, 52, 52, 65, 65, 65, 65, 65, 65, 65, 65, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 66, 57, 66, 57, 32, 32, 55, 55, 55, 55, 54, 54, 54, 54, 51, 51, 51, 51, 66, 66, 66, 66, 54, 54, 54, 54, 51, 51, 51, 51, 68, 68, 68, 68, 68, 68, 68, 68, 50, 50, 50, 50, 56, 56, 56, 56, 55, 55, 55, 55, 67, 67, 67, 67, 69, 69, 69, 69, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 67, 51, 55, 70, 32, 32, 56, 56, 56, 56, 50, 50, 50, 50, 50, 50, 50, 50, 65, 65, 65, 65, 68, 68, 68, 68, 67, 67, 67, 67, 68, 68, 68, 68, 53, 53, 53, 53, 48, 48, 48, 48, 70, 70, 70, 70, 67, 67, 67, 67, 56, 56, 56, 56, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 67, 68, 52, 53, 32, 32, 65, 65, 65, 65, 50, 50, 50, 50, 50, 50, 50, 50, 48, 48, 48, 48, 69, 69, 69, 69, 55, 55, 55, 55, 69, 69, 69, 69, 52, 52, 52, 52, 70, 70, 70, 70, 65, 65, 65, 65, 49, 49, 49, 49, 67, 67, 67, 67, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 68, 55, 48, 67, 32, 32, 70, 70, 70, 70, 53, 53, 53, 53, 66, 66, 66, 66, 49, 49, 49, 49, 51, 51, 51, 51, 65, 65, 65, 65, 65, 65, 65, 65, 54, 54, 54, 54, 56, 56, 56, 56, 56, 56, 56, 56, 49, 49, 49, 49, 54, 54, 54, 54, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 69, 48, 68, 50, 32, 32, 68, 68, 68, 68, 50, 50, 50, 50, 53, 53, 53, 53, 65, 65, 65, 65, 54, 54, 54, 54, 54, 54, 54, 54, 49, 49, 49, 49, 68, 68, 68, 68, 50, 50, 50, 50, 54, 54, 54, 54, 53, 53, 53, 53, 52, 52, 52, 52, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 69, 65, 57, 57, 32, 32, 52, 52, 52, 52, 70, 70, 70, 70, 50, 50, 50, 50, 67, 67, 67, 67, 69, 69, 69, 69, 53, 53, 53, 53, 52, 52, 52, 52, 65, 65, 65, 65, 55, 55, 55, 55, 56, 56, 56, 56, 68, 68, 68, 68, 69, 69, 69, 69, 69, 69, 69, 69, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 70, 52, 53, 70, 32, 32, 48, 48, 48, 48, 53, 53, 53, 53, 69, 69, 69, 69, 51, 51, 51, 51, 65, 65, 65, 65, 54, 54, 54, 54, 56, 56, 56, 56, 54, 54, 54, 54, 70, 70, 70, 70, 54, 54, 54, 54, 51, 51, 51, 51, 65, 65, 65, 65, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 70, 69, 50, 53, 32, 32, 50, 50, 50, 50, 69, 69, 69, 69, 55, 55, 55, 55, 48, 48, 48, 48, 68, 68, 68, 68, 67, 67, 67, 67, 50, 50, 50, 50, 68, 68, 68, 68, 51, 51, 51, 51, 70, 70, 70, 70, 49, 49, 49, 49, 69, 69, 69, 69, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 48, 55, 69, 67, 32, 32, 50, 50, 50, 50, 49, 49, 49, 49, 53, 53, 53, 53, 70, 70, 70, 70, 53, 53, 53, 53, 68, 68, 68, 68, 56, 56, 56, 56, 53, 53, 53, 53, 48, 48, 48, 48, 56, 56, 56, 56, 70, 70, 70, 70, 48, 48, 48, 48, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 49, 49, 66, 50, 32, 32, 52, 52, 52, 52, 70, 70, 70, 70, 48, 48, 48, 48, 52, 52, 52, 52, 68, 68, 68, 68, 48, 48, 48, 48, 56, 56, 56, 56, 67, 67, 67, 67, 55, 55, 55, 55, 52, 52, 52, 52, 70, 70, 70, 70, 69, 69, 69, 69, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 49, 66, 55, 56, 32, 32, 53, 53, 53, 53, 69, 69, 69, 69, 48, 48, 48, 48, 67, 67, 67, 67, 57, 57, 57, 57, 52, 52, 52, 52, 50, 50, 50, 50, 66, 66, 66, 66, 67, 67, 67, 67, 70, 70, 70, 70, 55, 55, 55, 55, 50, 50, 50, 50, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 50, 53, 51, 70, 32, 32, 53, 53, 53, 53, 67, 67, 67, 67, 57, 57, 57, 57, 56, 56, 56, 56, 48, 48, 48, 48, 68, 68, 68, 68, 55, 55, 55, 55, 65, 65, 65, 65, 49, 49, 49, 49, 68, 68, 68, 68, 65, 65, 65, 65, 67, 67, 67, 67, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 50, 70, 48, 53, 32, 32, 57, 57, 57, 57, 65, 65, 65, 65, 55, 55, 55, 55, 49, 49, 49, 49, 51, 51, 51, 51, 54, 54, 54, 54, 54, 54, 54, 54, 50, 50, 50, 50, 52, 52, 52, 52, 68, 68, 68, 68, 50, 50, 50, 50, 48, 48, 48, 48, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 51, 56, 67, 67, 32, 32, 65, 65, 65, 65, 68, 68, 68, 68, 57, 57, 57, 57, 48, 48, 48, 48, 53, 53, 53, 53, 48, 48, 48, 48, 50, 50, 50, 50, 48, 48, 48, 48, 57, 57, 57, 57, 69, 69, 69, 69, 67, 67, 67, 67, 65, 65, 65, 65, 53, 53, 53, 53, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 52, 50, 57, 50, 32, 32, 57, 57, 57, 57, 68, 68, 68, 68, 48, 48, 48, 48, 54, 54, 54, 54, 70, 70, 70, 70, 48, 48, 48, 48, 48, 48, 48, 48, 49, 49, 49, 49, 48, 48, 48, 48, 48, 48, 48, 48, 65, 65, 65, 65, 56, 56, 56, 56, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 52, 67, 53, 56, 32, 32, 67, 67, 67, 67, 53, 53, 53, 53, 68, 68, 68, 68, 69, 69, 69, 69, 56, 56, 56, 56, 65, 65, 65, 65, 67, 67, 67, 67, 68, 68, 68, 68, 66, 66, 66, 66, 55, 55, 55, 55, 54, 54, 54, 54, 67, 67, 67, 67, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 53, 54, 49, 70, 32, 32, 66, 66, 66, 66, 51, 51, 51, 51, 70, 70, 70, 70, 50, 50, 50, 50, 50, 50, 50, 50, 65, 65, 65, 65, 55, 55, 55, 55, 51, 51, 51, 51, 48, 48, 48, 48, 54, 54, 54, 54, 49, 49, 49, 49, 69, 69, 69, 69, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 53, 70, 69, 53, 32, 32, 50, 50, 50, 50, 50, 50, 50, 50, 48, 48, 48, 48, 66, 66, 66, 66, 53, 53, 53, 53, 67, 67, 67, 67, 50, 50, 50, 50, 67, 67, 67, 67, 65, 65, 65, 65, 52, 52, 52, 52, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 54, 57, 65, 66, 32, 32, 65, 65, 65, 65, 66, 66, 66, 66, 66, 66, 66, 66, 48, 48, 48, 48, 69, 69, 69, 69, 69, 69, 69, 69, 53, 53, 53, 53, 52, 52, 52, 52, 68, 68, 68, 68, 48, 48, 48, 48, 55, 55, 55, 55, 55, 55, 55, 55, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 55, 51, 55, 50, 32, 32, 49, 49, 49, 49, 53, 53, 53, 53, 57, 57, 57, 57, 56, 56, 56, 56, 50, 50, 50, 50, 50, 50, 50, 50, 65, 65, 65, 65, 68, 68, 68, 68, 52, 52, 52, 52, 57, 57, 57, 57, 53, 53, 53, 53, 50, 50, 50, 50, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 55, 68, 51, 56, 32, 32, 57, 57, 57, 57, 70, 70, 70, 70, 49, 49, 49, 49, 55, 55, 55, 55, 65, 65, 65, 65, 66, 66, 66, 66, 67, 67, 67, 67, 48, 48, 48, 48, 67, 67, 67, 67, 52, 52, 52, 52, 54, 54, 54, 54, 54, 54, 54, 54, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 56, 54, 70, 70, 32, 32, 53, 53, 53, 53, 52, 52, 52, 52, 68, 68, 68, 68, 53, 53, 53, 53, 57, 57, 57, 57, 65, 65, 65, 65, 55, 55, 55, 55, 54, 54, 54, 54, 50, 50, 50, 50, 70, 70, 70, 70, 57, 57, 57, 57, 48, 48, 48, 48, 48, 48, 48, 48, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 57, 48, 67, 53, 32, 32, 50, 50, 50, 50, 50, 50, 50, 50, 65, 65, 65, 65, 52, 52, 52, 52, 66, 66, 66, 66, 57, 57, 57, 57, 55, 55, 55, 55, 52, 52, 52, 52, 67, 67, 67, 67, 52, 52, 52, 52, 50, 50, 50, 50, 52, 52, 52, 52, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 57, 65, 56, 66, 32, 32, 52, 52, 52, 52, 55, 55, 55, 55, 51, 51, 51, 51, 55, 55, 55, 55, 57, 57, 57, 57, 50, 50, 50, 50, 50, 50, 50, 50, 55, 55, 55, 55, 55, 55, 55, 55, 51, 51, 51, 51, 56, 56, 56, 56, 57, 57, 57, 57, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 65, 52, 53, 50, 32, 32, 69, 69, 69, 69, 52, 52, 52, 52, 52, 52, 52, 52, 57, 57, 57, 57, 55, 55, 55, 55, 65, 65, 65, 65, 48, 48, 48, 48, 51, 51, 51, 51, 67, 67, 67, 67, 69, 69, 69, 69, 70, 70, 70, 70, 67, 67, 67, 67, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 65, 69, 49, 56, 32, 32, 51, 51, 51, 51, 55, 55, 55, 55, 68, 68, 68, 68, 70, 70, 70, 70, 57, 57, 57, 57, 70, 70, 70, 70, 67, 67, 67, 67, 66, 66, 66, 66, 55, 55, 55, 55, 54, 54, 54, 54, 54, 54, 54, 54, 48, 48, 48, 48, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 66, 55, 68, 69, 32, 32, 66, 66, 66, 66, 53, 53, 53, 53, 52, 52, 52, 52, 70, 70, 70, 70, 57, 57, 57, 57, 68, 68, 68, 68, 48, 48, 48, 48, 49, 49, 49, 49, 65, 65, 65, 65, 54, 54, 54, 54, 53, 53, 53, 53, 49, 49, 49, 49, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 67, 49, 65, 53, 32, 32, 65, 65, 65, 65, 69, 69, 69, 69, 67, 67, 67, 67, 50, 50, 50, 50, 69, 69, 69, 69, 48, 48, 48, 48, 54, 54, 54, 54, 51, 51, 51, 51, 50, 50, 50, 50, 68, 68, 68, 68, 50, 50, 50, 50, 54, 54, 54, 54, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 67, 66, 54, 66, 32, 32, 57, 57, 57, 57, 65, 65, 65, 65, 54, 54, 54, 54, 66, 66, 66, 66, 69, 69, 69, 69, 57, 57, 57, 57, 48, 48, 48, 48, 48, 48, 48, 48, 50, 50, 50, 50, 55, 55, 55, 55, 57, 57, 57, 57, 66, 66, 66, 66, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 68, 53, 51, 50, 32, 32, 50, 50, 50, 50, 52, 52, 52, 52, 68, 68, 68, 68, 52, 52, 52, 52, 53, 53, 53, 53, 65, 65, 65, 65, 66, 66, 66, 66, 55, 55, 55, 55, 49, 49, 49, 49, 49, 49, 49, 49, 65, 65, 65, 65, 54, 54, 54, 54, 70, 70, 70, 70, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 68, 69, 70, 56, 32, 32, 66, 66, 66, 66, 49, 49, 49, 49, 66, 66, 66, 66, 53, 53, 53, 53, 68, 68, 68, 68, 67, 67, 67, 67, 70, 70, 70, 70, 52, 52, 52, 52, 52, 52, 52, 52, 68, 68, 68, 68, 53, 53, 53, 53, 65, 65, 65, 65, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 69, 56, 66, 69, 32, 32, 49, 49, 49, 49, 54, 54, 54, 54, 70, 70, 70, 70, 51, 51, 51, 51, 65, 65, 65, 65, 68, 68, 68, 68, 65, 65, 65, 65, 55, 55, 55, 55, 50, 50, 50, 50, 67, 67, 67, 67, 49, 49, 49, 49, 66, 66, 66, 66, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 70, 50, 56, 53, 32, 32, 54, 54, 54, 54, 56, 56, 56, 56, 51, 51, 51, 51, 70, 70, 70, 70, 65, 65, 65, 65, 57, 57, 57, 57, 54, 54, 54, 54, 48, 48, 48, 48, 53, 53, 53, 53, 70, 70, 70, 70, 50, 50, 50, 50, 56, 56, 56, 56, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 70, 67, 52, 66, 32, 32, 51, 51, 51, 51, 57, 57, 57, 57, 69, 69, 69, 69, 70, 70, 70, 70, 56, 56, 56, 56, 66, 66, 66, 66, 52, 52, 52, 52, 66, 66, 66, 66, 54, 54, 54, 54, 67, 67, 67, 67, 65, 65, 65, 65, 68, 68, 68, 68, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 48, 54, 49, 49, 32, 32, 48, 48, 48, 48, 54, 54, 54, 54, 56, 56, 56, 56, 57, 57, 57, 57, 53, 53, 53, 53, 51, 51, 51, 51, 48, 48, 48, 48, 52, 52, 52, 52, 56, 56, 56, 56, 54, 54, 54, 54, 48, 48, 48, 48, 66, 66, 66, 66, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 48, 70, 68, 56, 32, 32, 55, 55, 55, 55, 55, 55, 55, 55, 70, 70, 70, 70, 53, 53, 53, 53, 52, 52, 52, 52, 54, 54, 54, 54, 56, 56, 56, 56, 49, 49, 49, 49, 67, 67, 67, 67, 57, 57, 57, 57, 53, 53, 53, 53, 52, 52, 52, 52, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 49, 57, 57, 69, 32, 32, 65, 65, 65, 65, 67, 67, 67, 67, 65, 65, 65, 65, 67, 67, 67, 67, 49, 49, 49, 49, 67, 67, 67, 67, 56, 56, 56, 56, 68, 68, 68, 68, 70, 70, 70, 70, 69, 69, 69, 69, 69, 69, 69, 69, 53, 53, 53, 53, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 50, 51, 54, 53, 32, 32, 50, 50, 50, 50, 57, 57, 57, 57, 48, 48, 48, 48, 68, 68, 68, 68, 53, 53, 53, 53, 51, 51, 51, 51, 49, 49, 49, 49, 52, 52, 52, 52, 68, 68, 68, 68, 65, 65, 65, 65, 50, 50, 50, 50, 65, 65, 65, 65, 50, 50, 50, 50, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 50, 68, 50, 66, 32, 32, 69, 69, 69, 69, 70, 70, 70, 70, 56, 56, 56, 56, 57, 57, 57, 57, 48, 48, 48, 48, 57, 57, 57, 57, 66, 66, 66, 66, 53, 53, 53, 53, 67, 67, 67, 67, 50, 50, 50, 50, 66, 66, 66, 66, 70, 70, 70, 70, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 51, 54, 70, 49, 32, 32, 48, 48, 48, 48, 56, 56, 56, 56, 55, 55, 55, 55, 53, 53, 53, 53, 67, 67, 67, 67, 53, 53, 53, 53, 65, 65, 65, 65, 56, 56, 56, 56, 50, 50, 50, 50, 55, 55, 55, 55, 65, 65, 65, 65, 68, 68, 68, 68, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 52, 48, 66, 56, 32, 32, 53, 53, 53, 53, 65, 65, 65, 65, 65, 65, 65, 65, 55, 55, 55, 55, 67, 67, 67, 67, 67, 67, 67, 67, 70, 70, 70, 70, 65, 65, 65, 65, 54, 54, 54, 54, 65, 65, 65, 65, 52, 52, 52, 52, 69, 69, 69, 69, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 52, 65, 55, 69, 32, 32, 67, 67, 67, 67, 69, 69, 69, 69, 54, 54, 54, 54, 69, 69, 69, 69, 48, 48, 48, 48, 56, 56, 56, 56, 51, 51, 51, 51, 48, 48, 48, 48, 57, 57, 57, 57, 69, 69, 69, 69, 65, 65, 65, 65, 70, 70, 70, 70, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 53, 52, 52, 52, 32, 32, 48, 48, 48, 48, 49, 49, 49, 49, 56, 56, 56, 56, 69, 69, 69, 69, 50, 50, 50, 50, 68, 68, 68, 68, 66, 66, 66, 66, 54, 54, 54, 54, 70, 70, 70, 70, 70, 70, 70, 70, 48, 48, 48, 48, 52, 52, 52, 52, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 53, 69, 48, 66, 32, 32, 52, 52, 52, 52, 65, 65, 65, 65, 52, 52, 52, 52, 57, 57, 57, 57, 48, 48, 48, 48, 68, 68, 68, 68, 50, 50, 50, 50, 66, 66, 66, 66, 65, 65, 65, 65, 57, 57, 57, 57, 68, 68, 68, 68, 49, 49, 49, 49, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 54, 55, 68, 49, 32, 32, 65, 65, 65, 65, 51, 51, 51, 51, 48, 48, 48, 48, 51, 51, 51, 51, 69, 69, 69, 69, 70, 70, 70, 70, 67, 67, 67, 67, 68, 68, 68, 68, 54, 54, 54, 54, 50, 50, 50, 50, 52, 52, 52, 52, 70, 70, 70, 70, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 55, 49, 57, 56, 32, 32, 66, 66, 66, 66, 53, 53, 53, 53, 65, 65, 65, 65, 65, 65, 65, 65, 48, 48, 48, 48, 69, 69, 69, 69, 50, 50, 50, 50, 52, 52, 52, 52, 66, 66, 66, 66, 48, 48, 48, 48, 52, 52, 52, 52, 56, 56, 56, 56, 57, 57, 57, 57, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 55, 66, 53, 69, 32, 32, 57, 57, 57, 57, 66, 66, 66, 66, 56, 56, 56, 56, 65, 65, 65, 65, 56, 56, 56, 56, 54, 54, 54, 54, 52, 52, 52, 52, 57, 57, 57, 57, 56, 56, 56, 56, 66, 66, 66, 66, 55, 55, 55, 55, 57, 57, 57, 57, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 56, 53, 50, 52, 32, 32, 50, 50, 50, 50, 66, 66, 66, 66, 54, 54, 54, 54, 52, 52, 52, 52, 67, 67, 67, 67, 53, 53, 53, 53, 48, 48, 48, 48, 65, 65, 65, 65, 48, 48, 48, 48, 49, 49, 49, 49, 49, 49, 49, 49, 69, 69, 69, 69, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 56, 69, 69, 66, 32, 32, 68, 68, 68, 68, 57, 57, 57, 57, 66, 66, 66, 66, 70, 70, 70, 70, 55, 55, 55, 55, 54, 54, 54, 54, 68, 68, 68, 68, 57, 57, 57, 57, 65, 65, 65, 65, 49, 49, 49, 49, 69, 69, 69, 69, 51, 51, 51, 51, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 57, 56, 66, 49, 32, 32, 70, 70, 70, 70, 55, 55, 55, 55, 48, 48, 48, 48, 66, 66, 66, 66, 68, 68, 68, 68, 69, 69, 69, 69, 67, 67, 67, 67, 52, 52, 52, 52, 66, 66, 66, 66, 53, 53, 53, 53, 70, 70, 70, 70, 49, 49, 49, 49, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 65, 50, 55, 55, 32, 32, 56, 56, 56, 56, 51, 51, 51, 51, 52, 52, 52, 52, 56, 56, 56, 56, 55, 55, 55, 55, 66, 66, 66, 66, 68, 68, 68, 68, 65, 65, 65, 65, 57, 57, 57, 57, 55, 55, 55, 55, 65, 65, 65, 65, 48, 48, 48, 48, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 65, 67, 51, 69, 32, 32, 68, 68, 68, 68, 55, 55, 55, 55, 50, 50, 50, 50, 70, 70, 70, 70, 52, 52, 52, 52, 51, 51, 51, 51, 68, 68, 68, 68, 51, 51, 51, 51, 52, 52, 52, 52, 53, 53, 53, 53, 52, 52, 52, 52, 51, 51, 51, 51, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 66, 54, 48, 52, 32, 32, 49, 49, 49, 49, 54, 54, 54, 54, 66, 66, 66, 66, 69, 69, 69, 69, 70, 70, 70, 70, 54, 54, 54, 54, 52, 52, 52, 52, 55, 55, 55, 55, 66, 66, 66, 66, 56, 56, 56, 56, 51, 51, 51, 51, 56, 56, 56, 56, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 66, 70, 67, 66, 32, 32, 68, 68, 68, 68, 67, 67, 67, 67, 52, 52, 52, 52, 48, 48, 48, 48, 56, 56, 56, 56, 48, 48, 48, 48, 50, 50, 50, 50, 67, 67, 67, 67, 50, 50, 50, 50, 65, 65, 65, 65, 70, 70, 70, 70, 53, 53, 53, 53, 52, 52, 52, 52, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 67, 57, 57, 49, 32, 32, 68, 68, 68, 68, 54, 54, 54, 54, 48, 48, 48, 48, 49, 49, 49, 49, 49, 49, 49, 49, 65, 65, 65, 65, 49, 49, 49, 49, 69, 69, 69, 69, 65, 65, 65, 65, 50, 50, 50, 50, 57, 57, 57, 57, 51, 51, 51, 51, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 68, 51, 53, 55, 32, 32, 50, 50, 50, 50, 50, 50, 50, 50, 69, 69, 69, 69, 68, 68, 68, 68, 55, 55, 55, 55, 57, 57, 57, 57, 70, 70, 70, 70, 56, 56, 56, 56, 53, 53, 53, 53, 67, 67, 67, 67, 53, 53, 53, 53, 50, 50, 50, 50, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 68, 68, 49, 69, 32, 32, 70, 70, 70, 70, 66, 66, 66, 66, 66, 66, 66, 66, 55, 55, 55, 55, 50, 50, 50, 50, 53, 53, 53, 53, 48, 48, 48, 48, 56, 56, 56, 56, 52, 52, 52, 52, 67, 67, 67, 67, 48, 48, 48, 48, 68, 68, 68, 68, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 69, 54, 69, 52, 32, 32, 65, 65, 65, 65, 53, 53, 53, 53, 48, 48, 48, 48, 69, 69, 69, 69, 70, 70, 70, 70, 70, 70, 70, 70, 51, 51, 51, 51, 69, 69, 69, 69, 65, 65, 65, 65, 52, 52, 52, 52, 53, 53, 53, 53, 50, 50, 50, 50, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 70, 48, 65, 65, 32, 32, 53, 53, 53, 53, 48, 48, 48, 48, 53, 53, 53, 53, 68, 68, 68, 68, 65, 65, 65, 65, 49, 49, 49, 49, 65, 65, 65, 65, 51, 51, 51, 51, 68, 68, 68, 68, 70, 70, 70, 70, 51, 51, 51, 51, 53, 53, 53, 53, 55, 55, 55, 55, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 70, 65, 55, 49, 32, 32, 52, 52, 52, 52, 68, 68, 68, 68, 65, 65, 65, 65, 48, 48, 48, 48, 67, 67, 67, 67, 51, 51, 51, 51, 65, 65, 65, 65, 66, 66, 66, 66, 65, 65, 65, 65, 56, 56, 56, 56, 51, 51, 51, 51, 53, 53, 53, 53, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 48, 52, 51, 55, 32, 32, 51, 51, 51, 51, 50, 50, 50, 50, 67, 67, 67, 67, 68, 68, 68, 68, 50, 50, 50, 50, 51, 51, 51, 51, 52, 52, 52, 52, 55, 55, 55, 55, 56, 56, 56, 56, 50, 50, 50, 50, 48, 48, 48, 48, 52, 52, 52, 52, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 48, 68, 70, 69, 32, 32, 50, 50, 50, 50, 67, 67, 67, 67, 67, 67, 67, 67, 65, 65, 65, 65, 69, 69, 69, 69, 53, 53, 53, 53, 54, 54, 54, 54, 51, 51, 51, 51, 49, 49, 49, 49, 70, 70, 70, 70, 68, 68, 68, 68, 55, 55, 55, 55, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 49, 55, 67, 52, 32, 32, 49, 49, 49, 49, 56, 56, 56, 56, 66, 66, 66, 66, 54, 54, 54, 54, 56, 56, 56, 56, 69, 69, 69, 69, 70, 70, 70, 70, 68, 68, 68, 68, 52, 52, 52, 52, 53, 53, 53, 53, 54, 54, 54, 54, 67, 67, 67, 67, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 50, 49, 56, 65, 32, 32, 56, 56, 56, 56, 67, 67, 67, 67, 55, 55, 55, 55, 52, 52, 52, 52, 67, 67, 67, 67, 66, 66, 66, 66, 53, 53, 53, 53, 52, 52, 52, 52, 66, 66, 66, 66, 48, 48, 48, 48, 49, 49, 49, 49, 70, 70, 70, 70, 55, 55, 55, 55, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 50, 66, 53, 49, 32, 32, 69, 69, 69, 69, 55, 55, 55, 55, 50, 50, 50, 50, 70, 70, 70, 70, 66, 66, 66, 66, 55, 55, 55, 55, 55, 55, 55, 55, 67, 67, 67, 67, 52, 52, 52, 52, 54, 54, 54, 54, 68, 68, 68, 68, 55, 55, 55, 55, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 51, 53, 49, 55, 32, 32, 68, 68, 68, 68, 57, 57, 57, 57, 54, 54, 54, 54, 70, 70, 70, 70, 65, 65, 65, 65, 68, 68, 68, 68, 49, 49, 49, 49, 67, 67, 67, 67, 56, 56, 56, 56, 56, 56, 56, 56, 66, 66, 66, 66, 54, 54, 54, 54, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 51, 69, 68, 68, 32, 32, 69, 69, 69, 69, 48, 48, 48, 48, 55, 55, 55, 55, 65, 65, 65, 65, 48, 48, 48, 48, 57, 57, 57, 57, 55, 55, 55, 55, 69, 69, 69, 69, 56, 56, 56, 56, 48, 48, 48, 48, 51, 51, 51, 51, 69, 69, 69, 69, 65, 65, 65, 65, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 52, 56, 65, 52, 32, 32, 66, 66, 66, 66, 54, 54, 54, 54, 54, 54, 54, 54, 67, 67, 67, 67, 54, 54, 54, 54, 53, 53, 53, 53, 68, 68, 68, 68, 50, 50, 50, 50, 49, 49, 49, 49, 66, 66, 66, 66, 56, 56, 56, 56, 54, 54, 54, 54, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 53, 50, 54, 65, 32, 32, 65, 65, 65, 65, 67, 67, 67, 67, 50, 50, 50, 50, 69, 69, 69, 69, 51, 51, 51, 51, 57, 57, 57, 57, 68, 68, 68, 68, 67, 67, 67, 67, 68, 68, 68, 68, 69, 69, 69, 69, 48, 48, 48, 48, 57, 57, 57, 57, 55, 55, 55, 55, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 53, 67, 51, 49, 32, 32, 65, 65, 65, 65, 50, 50, 50, 50, 52, 52, 52, 52, 56, 56, 56, 56, 65, 65, 65, 65, 66, 66, 66, 66, 69, 69, 69, 69, 48, 48, 48, 48, 70, 70, 70, 70, 69, 69, 69, 69, 55, 55, 55, 55, 57, 57, 57, 57, 54, 54, 54, 54, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 54, 53, 70, 55, 32, 32, 56, 56, 56, 56, 53, 53, 53, 53, 68, 68, 68, 68, 57, 57, 57, 57, 53, 53, 53, 53, 57, 57, 57, 57, 48, 48, 48, 48, 66, 66, 66, 66, 70, 70, 70, 70, 48, 48, 48, 48, 54, 54, 54, 54, 56, 56, 56, 56, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 54, 70, 66, 68, 32, 32, 69, 69, 69, 69, 50, 50, 50, 50, 50, 50, 50, 50, 67, 67, 67, 67, 55, 55, 55, 55, 69, 69, 69, 69, 50, 50, 50, 50, 50, 50, 50, 50, 51, 51, 51, 51, 48, 48, 48, 48, 56, 56, 56, 56, 48, 48, 48, 48, 65, 65, 65, 65, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 55, 57, 56, 52, 32, 32, 52, 52, 52, 52, 51, 51, 51, 51, 70, 70, 70, 70, 49, 49, 49, 49, 70, 70, 70, 70, 53, 53, 53, 53, 52, 52, 52, 52, 66, 66, 66, 66, 65, 65, 65, 65, 54, 54, 54, 54, 65, 65, 65, 65, 48, 48, 48, 48, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 56, 51, 52, 65, 32, 32, 52, 52, 52, 52, 66, 66, 66, 66, 53, 53, 53, 53, 65, 65, 65, 65, 50, 50, 50, 50, 48, 48, 48, 48, 70, 70, 70, 70, 69, 69, 69, 69, 69, 69, 69, 69, 56, 56, 56, 56, 70, 70, 70, 70, 51, 51, 51, 51, 55, 55, 55, 55, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 56, 68, 49, 48, 32, 32, 54, 54, 54, 54, 51, 51, 51, 51, 55, 55, 55, 55, 70, 70, 70, 70, 48, 48, 48, 48, 48, 48, 48, 48, 52, 52, 52, 52, 68, 68, 68, 68, 51, 51, 51, 51, 49, 49, 49, 49, 67, 67, 67, 67, 66, 66, 66, 66, 49, 49, 49, 49, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 57, 54, 68, 55, 32, 32, 52, 52, 52, 52, 54, 54, 54, 54, 48, 48, 48, 48, 57, 57, 57, 57, 57, 57, 57, 57, 52, 52, 52, 52, 69, 69, 69, 69, 65, 65, 65, 65, 51, 51, 51, 51, 57, 57, 57, 57, 49, 49, 49, 49, 65, 65, 65, 65, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 65, 48, 57, 68, 32, 32, 55, 55, 55, 55, 48, 48, 48, 48, 67, 67, 67, 67, 51, 51, 51, 51, 57, 57, 57, 57, 68, 68, 68, 68, 51, 51, 51, 51, 68, 68, 68, 68, 52, 52, 52, 52, 57, 57, 57, 57, 67, 67, 67, 67, 50, 50, 50, 50, 65, 65, 65, 65, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 65, 65, 54, 52, 32, 32, 52, 52, 52, 52, 67, 67, 67, 67, 49, 49, 49, 49, 57, 57, 57, 57, 48, 48, 48, 48, 48, 48, 48, 48, 51, 51, 51, 51, 56, 56, 56, 56, 54, 54, 54, 54, 54, 54, 54, 54, 66, 66, 66, 66, 65, 65, 65, 65, 68, 68, 68, 68, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 66, 52, 50, 65, 32, 32, 57, 57, 57, 57, 55, 55, 55, 55, 67, 67, 67, 67, 53, 53, 53, 53, 54, 54, 54, 54, 70, 70, 70, 70, 66, 66, 66, 66, 68, 68, 68, 68, 53, 53, 53, 53, 48, 48, 48, 48, 68, 68, 68, 68, 68, 68, 68, 68, 55, 55, 55, 55, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 66, 68, 70, 48, 32, 32, 69, 69, 69, 69, 65, 65, 65, 65, 54, 54, 54, 54, 51, 51, 51, 51, 52, 52, 52, 52, 55, 55, 55, 55, 56, 56, 56, 56, 50, 50, 50, 50, 51, 51, 51, 51, 56, 56, 56, 56, 48, 48, 48, 48, 53, 53, 53, 53, 49, 49, 49, 49, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 67, 55, 66, 55, 32, 32, 57, 57, 57, 57, 68, 68, 68, 68, 56, 56, 56, 56, 65, 65, 65, 65, 49, 49, 49, 49, 65, 65, 65, 65, 67, 67, 67, 67, 66, 66, 66, 66, 69, 69, 69, 69, 50, 50, 50, 50, 67, 67, 67, 67, 67, 67, 67, 67, 56, 56, 56, 56, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 68, 49, 55, 69, 32, 32, 51, 51, 51, 51, 48, 48, 48, 48, 67, 67, 67, 67, 54, 54, 54, 54, 66, 66, 66, 66, 49, 49, 49, 49, 57, 57, 57, 57, 65, 65, 65, 65, 55, 55, 55, 55, 48, 48, 48, 48, 70, 70, 70, 70, 70, 70, 70, 70, 51, 51, 51, 51, 13, 10],
        vec![0, 32, 32, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 68, 49, 55, 69, 32, 32, 51, 51, 51, 51, 48, 48, 48, 48, 67, 67, 67, 67, 54, 54, 54, 54, 66, 66, 66, 66, 49, 49, 49, 49, 57, 57, 57, 57, 65, 65, 65, 65, 55, 55, 55, 55, 48, 48, 48, 48, 70, 70, 70, 70, 70, 70, 70, 70, 51, 51, 51, 51, 13, 10],
    ]
}