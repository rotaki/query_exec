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

    let _result = execute(db_id, &storage, exe, true);
}

fn criterion_benchmark(c: &mut Criterion) {
    let path = "bp-dir-tpch-sf-0.1";
    let buffer_pool_size = 524288;
    let bp = Arc::new(BufferPool::new(path, buffer_pool_size, false).unwrap());

    let memory_sizes = vec![100, 500, 1000, 2000, 3000]; // Memory sizes to benchmark (num pages)
    for &size in &memory_sizes {
        c.bench_function(&format!("sort with memory size {}", size), |b| {
            b.iter(|| run_sort_benchmark(black_box(size), bp.clone()));
        });
    }
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10) // Reduced sample size to limit the number of iterations
        .measurement_time(std::time::Duration::new(5, 0)) // Adjusted measurement time to 5 seconds
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);