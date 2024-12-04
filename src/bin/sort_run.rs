// src/bin/benchmark_queries.rs

use clap::Parser;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{CpuExt, System, SystemExt};
use query_exec::{
    prelude::{
        execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph,
    },
    BufferPool, ContainerId, OnDiskStorage,
};

#[derive(Debug, Parser)]
#[clap(
    name = "Query Benchmark",
    version = "1.0",
    author = "Your Name",
    about = "Benchmarking query_exec by executing SQL queries."
)]
pub struct BenchmarkOpt {
    /// Buffer pool size (number of frames)
    #[clap(short = 'b', long = "buffer-pool-size", default_value = "524288")]
    pub buffer_pool_size: usize,

    /// Memory size per operator
    #[clap(short = 'm', long = "memory-size", default_value = "131072")]
    pub memory_size_per_operator: usize,

    /// Number of iterations for the benchmark
    #[clap(short = 'n', long = "num-iterations", default_value = "10")]
    pub num_iterations: usize,

    /// Query IDs to benchmark (e.g., 1 2 3)
    #[clap(short = 'q', long = "queries", required = true)]
    pub queries: Vec<u32>,

    /// Path to buffer pool directory
    #[clap(short = 'p', long = "path", default_value = "bp-dir-tpch-sf-1")]
    pub path: String,
}

fn get_system_metrics(system: &mut System) -> (f32, f32) {
    system.refresh_all();
    let cpu_usage = system.global_cpu_info().cpu_usage();
    let memory = system.used_memory() as f32 / 1024.0; // Convert KB to MB
    (cpu_usage, memory)
}

fn run_query(
    bp: Arc<BufferPool>,
    query_id: u32,
    memory_size: usize,
    system: &mut System,
) -> Result<(), String> {
    let temp_c_id = 1000;
    let exclude_last_pipeline = true;

    let storage = Arc::new(OnDiskStorage::load(&bp));

    let (db_id, catalog) = load_db(&storage, "TPCH")
        .map_err(|e| format!("Failed to load DB: {:?}", e))?;

    // Read SQL query
    let query_path = format!("tpch/queries/q{}.sql", query_id);
    let sql_string = std::fs::read_to_string(&query_path)
        .map_err(|e| format!("Failed to read SQL file {}: {}", query_path, e))?;

    // Convert SQL string to logical expression
    let logical = to_logical(db_id, &catalog, &sql_string)
        .map_err(|e| format!("Failed to convert to logical: {:?}", e))?;

    // Convert logical expression to physical expression
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

    // Capture metrics before execution
    let (cpu_start, mem_start) = get_system_metrics(system);
    println!(
        "Start - CPU: {:.2}%, Memory: {:.2} MB",
        cpu_start,
        mem_start
    );

    let start_time = Instant::now();

    // Execute the pipeline
    let _result = execute(db_id, &storage, exe, false);

    let duration = start_time.elapsed();

    // Capture metrics after execution
    let (cpu_end, mem_end) = get_system_metrics(system);
    println!(
        "End - CPU: {:.2}%, Memory: {:.2} MB, Runtime: {:.2?}",
        cpu_end,
        mem_end,
        duration
    );

    println!("Query execution completed successfully.");

    Ok(())
}

fn main() {
    let opt = BenchmarkOpt::parse();

    let mut system = System::new_all();

    // Initialize the BufferPool
    let bp = Arc::new(
        BufferPool::new(&opt.path, opt.buffer_pool_size, false)
            .expect("Failed to initialize BufferPool"),
    );

    for &query_id in &opt.queries {
        for itr in 0..opt.num_iterations {
            println!("Running query {} - Iteration {}", query_id, itr + 1);
            if let Err(e) = run_query(bp.clone(), query_id, opt.memory_size_per_operator, &mut system) {
                eprintln!("Error during query execution: {}", e);
                std::process::exit(1);
            }
        }
    }
}