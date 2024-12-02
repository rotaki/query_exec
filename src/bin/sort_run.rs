// src/bin/sort_run.rs

use std::env;
use std::sync::Arc;
use query_exec::{
    prelude::{
        execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph,
    },
    BufferPool, ContainerId, OnDiskStorage,
};

fn run_sort(
    memory_size: usize,
    bp: Arc<BufferPool>,
    query_id: u32,
) -> Result<(), String> {
    let temp_c_id = 1000;
    let exclude_last_pipeline = true;

    // OnDiskStorage::load does not return a Result
    let storage = Arc::new(OnDiskStorage::load(&bp));

    // load_db returns Result<(u32, Catalog), QueryExecutorError>
    let (db_id, catalog) = load_db(&storage, "TPCH")
        .map_err(|e| format!("Failed to load DB: {:?}", e))?;

    // Read SQL query
    let query_path = format!("tpch/queries/q{}.sql", query_id);
    let sql_string = std::fs::read_to_string(&query_path)
        .map_err(|e| format!("Failed to read SQL file {}: {}", query_path, e))?;

    // to_logical returns Result<LogicalRelExpr, QueryExecutorError>
    let logical = to_logical(db_id, &catalog, &sql_string)
        .map_err(|e| format!("Failed to convert to logical: {:?}", e))?;

    // to_physical likely does not return a Result
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

    // Execute the pipeline
    // Assuming execute does not return a Result
    let _result = execute(db_id, &storage, exe, false);

    println!("Sort execution completed successfully.");

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

    let path = "bp-dir-tpch-sf-1"; // You might want to make this configurable

    // Initialize the BufferPool
    let bp = Arc::new(
        BufferPool::new(path, buffer_pool_size, false)
            .expect("Failed to initialize BufferPool"),
    );

    // Run the sort
    if let Err(e) = run_sort(memory_size, bp, query_id) {
        eprintln!("Error during sort execution: {}", e);
        std::process::exit(1);
    }
}