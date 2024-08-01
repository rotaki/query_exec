use clap::Parser;
use query_exec::{
    prelude::{
        execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph, TupleBuffer,
    },
    BufferPool, ContainerId, LRUEvictionPolicy, OnDiskStorage,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Parser)]
#[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
pub struct TpchOpt {
    /// Load database from path. If not set, load from scratch.
    #[clap(short = 'p', long = "path", default_value = "bp-dir-tpch-sf-0.1")]
    pub path: String,
    /// Run all queries. Default is false.
    #[clap(short = 'a', long = "all", default_value = "false")]
    pub bench_all: bool,
    /// Buffer pool size. Default is 16GB = 32K * 524288
    #[clap(short = 'b', long = "buffer pool size", default_value = "524288")]
    pub buffer_pool_size: usize,
    /// Memory size per operator. Default is 1000 pages: 32MB = 32K * 1000
    #[clap(short = 'm', long = "memory size per operator", default_value = "1000")]
    pub memory_size_per_operator: usize,
    /// Input query
    #[clap(short = 'q', long = "query id", default_value = "100")]
    pub query_id: usize,
    /// Temp c_id. This is the container id for the intermediate results.
    #[clap(short = 't', long = "temp c_id", default_value = "1000")]
    pub temp_c_id: usize,
}

fn clear_cache() {
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg("echo 3 > /proc/sys/vm/drop_caches")
        .output();
}

fn main() {
    clear_cache();

    let opt = TpchOpt::parse();

    let bp = Arc::new(
        BufferPool::<LRUEvictionPolicy>::new(&opt.path, opt.buffer_pool_size, false).unwrap(),
    );
    let storage = Arc::new(OnDiskStorage::load(&bp));
    let (db_id, catalog) = load_db(&storage, "TPCH").unwrap();

    bp.reset_stats();

    if opt.bench_all {
        let mut results = BTreeMap::new();

        for query_id in 1..=22 {
            if query_id == 2
                || query_id == 7
                || query_id == 9
                || query_id == 10
                || query_id == 11
                || query_id == 17
                || query_id == 21
            {
                // 17 was ok with sf=1, page_size=56K
                continue;
            }
            let query_path = format!("tpch/queries/q{}.sql", query_id);
            let query = std::fs::read_to_string(query_path).unwrap();
            let logical = to_logical(db_id, &catalog, &query).unwrap();
            let physical = to_physical(logical);
            // Run query 3 times to warm up the cache
            println!("===== Warming up cache for query {} =====", query_id);
            for _ in 0..3 {
                let exec = OnDiskPipelineGraph::new(
                    db_id,
                    opt.temp_c_id as ContainerId,
                    &catalog,
                    &storage,
                    &bp,
                    &Arc::new(MemoryPolicy::FixedSizeLimit(opt.memory_size_per_operator)),
                    physical.clone(),
                    false,
                );
                let result = execute(db_id, &storage, exec, true);
                println!("Result num rows: {}", result.num_tuples());
            }
            println!("===== Measuring query {} =====", query_id);
            for _ in 0..10 {
                let exec = OnDiskPipelineGraph::new(
                    db_id,
                    opt.temp_c_id as ContainerId,
                    &catalog,
                    &storage,
                    &bp,
                    &Arc::new(MemoryPolicy::FixedSizeLimit(opt.memory_size_per_operator)),
                    physical.clone(),
                    false,
                );
                let start = std::time::Instant::now();
                let result = execute(db_id, &storage, exec, true);
                let elapsed = start.elapsed();
                println!(
                    "Query {} took {:?}, num rows: {}",
                    query_id,
                    elapsed,
                    result.num_tuples()
                );
                results
                    .entry(query_id)
                    .or_insert_with(Vec::new)
                    .push(elapsed);
            }
        }

        // print the results as csv with the following format:
        // query_id, time1, time2, time3, time4, time5, time6, time7, time8, time9, time10
        // Name the file tpch_ondisk_results_sf_<scale_factor>.csv
        let file_name = "tpch_ondisk_results.csv".to_string();
        let mut writer = csv::Writer::from_path(&file_name).unwrap();
        writer
            .write_record([
                "query_id", "time1", "time2", "time3", "time4", "time5", "time6", "time7", "time8",
                "time9", "time10",
            ])
            .unwrap();
        for (query_id, times) in results {
            let mut record = Vec::with_capacity(1 + times.len());
            record.push(query_id.to_string());
            for time in times {
                record.push(time.as_millis().to_string());
            }
            writer.write_record(&record).unwrap();
        }
        println!("Results written to {}.", file_name);
    } else {
        let query_path = format!("tpch/queries/q{}.sql", opt.query_id);
        let sql_string = std::fs::read_to_string(query_path).unwrap();
        let logical = to_logical(db_id, &catalog, &sql_string).unwrap();
        let physical = to_physical(logical);

        println!("===== Running query {} =====", opt.query_id);
        println!("Plan");
        physical.pretty_print();

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
            false,
        );

        let time = std::time::Instant::now();
        let result = execute(db_id, &storage, exe, true);
        println!("Time: {} ms", time.elapsed().as_millis());

        println!("stats: \n{}", bp.stats());
        println!("Result num rows: {}", result.num_tuples());
    }
}
