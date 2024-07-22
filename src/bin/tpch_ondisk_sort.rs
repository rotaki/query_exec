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
    /// Buffer pool size.
    #[clap(short = 'b', long = "buffer pool size", default_value = "1024")]
    pub buffer_pool_size: usize,
    /// Memory size per operator.
    #[clap(short = 'm', long = "memory size per operator", default_value = "10")]
    pub memory_size_per_operator: usize,
    /// Input query
    #[clap(short = 'q', long = "query id", default_value = "100")]
    pub query_id: usize,
    /// Temp c_id. This is the container id for the intermediate results.
    #[clap(short = 't', long = "temp c_id", default_value = "1000")]
    pub temp_c_id: usize,
}

fn main() {
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

    let mem_policy = Arc::new(MemoryPolicy::FixedSize(opt.memory_size_per_operator));
    let temp_c_id = opt.temp_c_id as ContainerId;
    let exe = OnDiskPipelineGraph::new(
        db_id,
        temp_c_id,
        &catalog,
        &storage,
        &bp,
        &mem_policy,
        physical.clone(),
        true,
    );

    let result = execute(db_id, &storage, exe, true);

    println!("stats: \n{}", bp.stats());

    println!("Result num rows: {}", result.num_tuples());
}
