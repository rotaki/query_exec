use clap::Parser;
use query_exec::{
    prelude::{
        execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph, TupleBuffer,
    },
    BufferPool, LRUEvictionPolicy, OnDiskStorage,
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
    let temp_c_id = 1000;
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
    // Print the first 10 rows
    // let result_iter = result.iter();
    // for i in 0..10 {
    //     let tuple = result_iter.next().unwrap().unwrap();
    //     println!("Row {}: {:?}", i, tuple);
    // }

    /*
    if opt.bench_all {
        let mut results = BTreeMap::new(); // query_id -> Vec<Time>

        for query_id in 1..=22 {
            let query_path = format!("tpch/queries/q{}.sql", query_id);
            let query = std::fs::read_to_string(query_path).unwrap();
            let logical = to_logical(db_id, &catalog, &query).unwrap();
            let physical = to_physical(logical);
            // Run the query 3 times to warm up the cache
            println!("====== Warming up query {} ======", query_id);
            for _ in 0..3 {
                let exec = InMemPipelineGraph::new(&catalog, &storage, physical.clone());
                let result = execute(db_id, &storage, exec, false);
                println!("Warm up result num rows: {}", result.num_tuples());
            }
            println!("====== Measuring query {} ======", query_id);
            for _ in 0..10 {
                let exec = InMemPipelineGraph::new(&catalog, &storage, physical.clone());
                let start = std::time::Instant::now();
                let result = execute(db_id, &storage, exec, false);
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

        // Print the results as csv with the following format:
        // query_id, time1, time2, time3, ...
        // Name the file tpch_volcano_results_sf_<scale_factor>.csv
        let file_name = format!("tpch_volcano_results_sf_{}.csv", opt.scale_factor);
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
        let query = std::fs::read_to_string(query_path).unwrap();
        let logical = to_logical(db_id, &catalog, &query).unwrap();
        let physical = to_physical(logical);

        println!("====== Running query {} ======", opt.query_id);
        println!("Plan");
        physical.pretty_print();

        let exec = InMemPipelineGraph::new(&catalog, &storage, physical.clone());
        println!("====== Executing query {} ======", opt.query_id);
        println!("{}", exec.to_pretty_string());

        let start = std::time::Instant::now();
        let result = execute(db_id, &storage, exec, false);
        let elapsed = start.elapsed();
        println!(
            "Query {} took {:?}, num rows: {}",
            opt.query_id,
            elapsed,
            result.num_tuples()
        );
    }
    */
}
