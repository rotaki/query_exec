use criterion::{black_box, criterion_group, criterion_main, Criterion};
use query_exec::{
    prelude::{execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph},
    BufferPool, OnDiskStorage, ContainerId,
};
use std::sync::Arc;

fn run_sort_benchmark(memory_size: usize, bp: Arc<BufferPool>) {
    let query_id = 100;
    let temp_c_id = 1000;
    let exclude_last_pipeline = true;

    let storage = Arc::new(OnDiskStorage::load(&bp));
    let (db_id, catalog) = load_db(&storage, "TPCH").unwrap();

    let query_path = format!("tpch/queries/q{}.sql", query_id);
    let sql_string = std::fs::read_to_string(query_path).unwrap();
    let logical = to_logical(db_id, &catalog, &sql_string).unwrap();
    let physical = to_physical(logical);

    let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(memory_size));
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

    let _result = execute(db_id, &storage, exe, false);
}

fn criterion_benchmark(c: &mut Criterion) {
    let path = "bp-dir-tpch-sf-1";
    // let buffer_pool_size = 20000;
    let buffer_pool_size = 10000;
    let bp = Arc::new(BufferPool::new(path, buffer_pool_size, false).unwrap());

    // Get the memory size from the environment variable
    let memory_size = std::env::var("BENCH_MEMORY_SIZE")
        .unwrap_or_else(|_| "100".to_string()) // Default to 100 if not set
        .parse::<usize>()
        .expect("Invalid memory size");

    c.bench_function(&format!("sort with memory size {}", memory_size), |b| {
        b.iter(|| run_sort_benchmark(black_box(memory_size), bp.clone()));
        // println!("stats: \n{:?}", bp.stats());
    });
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10) // Set the number of samples
        .warm_up_time(std::time::Duration::from_secs(3)) 
        .measurement_time(std::time::Duration::from_secs(60)) 
        .configure_from_args() 
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);