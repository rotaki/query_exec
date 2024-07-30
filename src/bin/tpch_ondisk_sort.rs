use clap::Parser;
use query_exec::{
    prelude::{
        execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph, TupleBuffer,
    },
    BufferPool, ContainerId, LRUEvictionPolicy, OnDiskStorage,
};
use std::sync::Arc;

#[derive(Debug, Parser)]
#[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
pub struct SortParam {
    /// Load database from path. If not set, load from scratch.
    #[clap(short = 'p', long = "path", default_value = "bp-dir-tpch-sf-0.1")]
    pub path: String,
    /// Buffer pool size. Default is 16GB = 32K * 524288
    #[clap(short = 'b', long = "buffer pool size", default_value = "524288")]
    pub buffer_pool_size: usize,
    /// Memory size per operator. Default is 4GB = 32K * 131072
    #[clap(
        short = 'm',
        long = "memory size per operator",
        default_value = "131072"
    )]
    pub memory_size_per_operator: usize,
    /// Input query
    #[clap(short = 'q', long = "query id", default_value = "100")]
    pub query_id: usize,
    /// Temp c_id. This is the container id for the intermediate results.
    #[clap(short = 't', long = "temp c_id", default_value = "1000")]
    pub temp_c_id: usize,
    /// Exclude last pipeline. If set, the last pipeline is excluded.
    #[clap(short = 'e', long = "exclude last pipeline", default_value = "true")]
    pub exclude_last_pipeline: bool,
}

fn clear_cache() {
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg("echo 3 > /proc/sys/vm/drop_caches")
        .output();
}

fn main() {
    clear_cache();

    let opt = SortParam::parse();

    let bp = Arc::new(
        BufferPool::<LRUEvictionPolicy>::new(&opt.path, opt.buffer_pool_size, false).unwrap(),
    );
    let storage = Arc::new(OnDiskStorage::load(&bp));
    let (db_id, catalog) = load_db(&storage, "TPCH").unwrap();

    bp.reset_stats();

    println!("Data loaded and flushed. Running query...");
    let query_path = format!("tpch/queries/q{}.sql", opt.query_id);
    let sql_string = std::fs::read_to_string(query_path).unwrap();
    let logical = to_logical(db_id, &catalog, &sql_string).unwrap();
    let physical = to_physical(logical);

    let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(opt.memory_size_per_operator));
    let temp_c_id = opt.temp_c_id as ContainerId;
    let exe = OnDiskPipelineGraph::new(
        db_id,
        temp_c_id,
        &catalog,
        &storage,
        &bp,
        &mem_policy,
        physical.clone(),
        opt.exclude_last_pipeline,
    );

    let time = std::time::Instant::now();
    let result = execute(db_id, &storage, exe, true);
    println!("Time: {} ms", time.elapsed().as_millis());

    println!("stats: \n{}", bp.stats());
    println!("Result num rows: {}", result.num_tuples());
}
