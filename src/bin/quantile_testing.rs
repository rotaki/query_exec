use std::env;
use std::sync::Arc;
use query_exec::{
    prelude::{quantile_generation_execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph, TxnOptions, DatabaseId},
    BufferPool, ContainerId, OnDiskStorage,
    quantile_lib::QuantileMethod,
};

fn run_quantile_generation(
    memory_size: usize,
    bp: Arc<BufferPool>,
    query_id: u32,
    method: QuantileMethod,
    num_quantiles: usize,
    data_source: &str,
    base_path: &str,
) -> Result<(), String> {
    let temp_c_id = 1000;
    let exclude_last_pipeline = true;

    // Initialize storage
    let storage = Arc::new(OnDiskStorage::<BufferPool>::load(&bp));

    // Load database
    let (db_id, catalog) = load_db(&storage, data_source)
        .map_err(|e| format!("Failed to load DB: {:?}", e))?;

    // Read SQL query
    let query_path = format!("tpch/queries/q{}.sql", query_id);
    let sql_string = std::fs::read_to_string(&query_path)
        .map_err(|e| format!("Failed to read SQL file {}: {}", query_path, e))?;

    let logical = to_logical(db_id, &catalog, &sql_string)
        .map_err(|e| format!("Failed to convert to logical: {:?}", e))?;

    let physical = to_physical(logical);

    // Set memory policy
    let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(memory_size));

    // Create pipeline graph
    let exe = OnDiskPipelineGraph::new(
        db_id,
        temp_c_id as ContainerId,
        &catalog,
        &storage,
        &bp,
        &mem_policy,
        physical.clone(),
        exclude_last_pipeline,
    );

    // Setup file paths
    let method_str = method.to_string();
    let estimated_path = format!("{}/q{}_{}_estimated.json", base_path, query_id, method_str);
    let actual_path = format!("{}/q{}_actual.json", base_path, query_id);
    let eval_path = format!("{}/q{}_{}_eval.json", base_path, query_id, method_str);

    // Execute quantile generation
    let _result = quantile_generation_execute(
        db_id,
        &storage,
        exe,
        false, // verbose
        data_source,
        query_id as u8,
        method,
        num_quantiles,
        &estimated_path,
        &actual_path,
        &eval_path,
    );

    println!("Quantile generation completed successfully for method: {:?}", method);
    Ok(())
}

fn main() {
    // Retrieve environment variables or use default values
    let buffer_pool_size = env::var("BENCH_BP_SIZE")
        .unwrap_or_else(|_| "10000".to_string())
        .parse::<usize>()
        .expect("Invalid buffer pool size");

    let query_id = env::var("BENCH_QUERY_ID")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<u32>()
        .expect("Invalid query ID");

    let memory_size = env::var("BENCH_MEMORY_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .expect("Invalid memory size");

    let path = "bp-dir-tpch-sf-1";
    let base_path = "quantile_data";
    
    // Create output directory
    std::fs::create_dir_all(base_path)
        .expect("Failed to create output directory");

    // Initialize the BufferPool
    let bp = Arc::new(
        BufferPool::new(path, buffer_pool_size, false)
            .expect("Failed to initialize BufferPool"),
    );

    // Define methods to test
    let methods = vec![
        QuantileMethod::Sampling,
        QuantileMethod::Mean,
        QuantileMethod::Median,
    ];

    // Run quantile generation for each method
    for method in methods {
        if let Err(e) = run_quantile_generation(
            memory_size,
            bp.clone(),
            query_id,
            method,
            10, // num_quantiles
            "TPCH_SF1",
            base_path,
        ) {
            eprintln!("Error during quantile generation for {:?}: {}", method, e);
            std::process::exit(1);
        }
    }
}