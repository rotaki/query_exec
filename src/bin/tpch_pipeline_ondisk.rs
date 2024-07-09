use clap::Parser;
use query_exec::{
    prelude::{Catalog, DatabaseEngine, Executor, PipelineQueue},
    ContainerType, InMemStorage, OnDiskStorage,
};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

#[derive(Debug, Parser)]
#[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
pub struct TpchOpt {
    /// Run all queries.
    #[clap(short = 'a', long = "all", default_value = "false")]
    pub bench_all: bool,
    /// Num warmups.
    #[clap(short = 'w', long = "warmups", default_value = "2")]
    pub warmups: usize,
    /// Num runs.
    #[clap(short = 'r', long = "runs", default_value = "3")]
    pub runs: usize,
    /// Query ID. Should be in range [1, 22].
    #[clap(short = 'q', long = "query", default_value = "15")]
    pub query_id: usize,
    /// Scale factor. Should be in range [0.01, 100].
    #[clap(short = 's', long = "scale factor", default_value = "1")]
    pub scale_factor: f64,
}

fn get_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

fn get_in_mem_storage() -> Arc<InMemStorage> {
    Arc::new(InMemStorage::new())
}

fn get_on_disk_storage() -> Arc<OnDiskStorage> {
    Arc::new(OnDiskStorage::new(1000))
}

fn main() {
    let opt = TpchOpt::parse();

    // check if data dir exists (BASE_DIR/sql/tpch/data/sf-<scale_factor>)
    let data_dir = format!("tpch/data/sf-{}", opt.scale_factor);
    if !PathBuf::from(&data_dir).exists() {
        panic!(
            "Data directory {} does not exist. Generate data first.",
            data_dir
        );
    }

    let db = DatabaseEngine::new(get_on_disk_storage());
    let db_id = db.create_db("TPCH");
    let query_executor = db.get_executor(db_id);

    let table_names = vec![
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ];

    println!("Creating tables...");

    let mut tables = HashMap::new();

    for table in &table_names {
        let path = format!("tpch/tables/{}.sql", table);
        let sql = std::fs::read_to_string(path).unwrap();
        let c_id = query_executor
            .create_table_from_sql(&sql, ContainerType::BTree)
            .unwrap();
        tables.insert(table, c_id);
    }

    println!("Tables created. Loading data...");

    for table in &table_names {
        let c_id = tables[table];
        let path = format!("tpch/data/sf-{}/input/{}.csv", opt.scale_factor, table);
        query_executor.import_csv(c_id, path, true, b'|').unwrap();
    }

    println!("Data loaded. Running query...");

    if opt.bench_all {
        let mut results = BTreeMap::new(); // query_id -> Vec<Time>

        for query_id in 1..=22 {
            let query_path = format!("tpch/queries/q{}.sql", query_id);
            let query = std::fs::read_to_string(query_path).unwrap();
            let logical = query_executor.to_logical(&query).unwrap();
            let physical = query_executor.to_physical(logical);
            // Run the query 3 times to warm up the cache
            println!("====== Warming up query {} ======", query_id);
            for _ in 0..opt.warmups {
                let exec =
                    query_executor.to_executable::<PipelineQueue<OnDiskStorage>>(physical.clone());
                let result = query_executor.execute(exec).unwrap();
                println!("Warm up result num rows: {}", result.num_tuples().unwrap());
            }
            println!("====== Measuring query {} ======", query_id);
            for _ in 0..opt.runs {
                let exec =
                    query_executor.to_executable::<PipelineQueue<OnDiskStorage>>(physical.clone());
                let start = std::time::Instant::now();
                let result = query_executor.execute(exec).unwrap();
                let elapsed = start.elapsed();
                println!(
                    "Query {} took {:?}, num rows: {}",
                    query_id,
                    elapsed,
                    result.num_tuples().unwrap()
                );
                results
                    .entry(query_id)
                    .or_insert_with(Vec::new)
                    .push(elapsed);
            }
        }

        // Print the results as csv with the following format:
        // query_id, time1, time2, time3, ...
        // Name the file tpch_pipeline_results_sf_<scale_factor>.csv
        let file_name = format!("tpch_pipeline_results_sf_{}.csv", opt.scale_factor);
        let mut writer = csv::Writer::from_path(&file_name).unwrap();
        let mut record = vec!["query_id".to_string()];
        for i in 1..=opt.runs {
            record.push(format!("time{}", i));
        }
        writer.write_record(&record).unwrap();
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
        let logical = query_executor.to_logical(&query).unwrap();
        let physical = query_executor.to_physical(logical);

        println!("====== Running query {} ======", opt.query_id);
        println!("Plan");
        physical.pretty_print();

        let start = std::time::Instant::now();
        let exec = query_executor.to_executable::<PipelineQueue<OnDiskStorage>>(physical);
        println!("{}", exec.to_pretty_string());
        let result = query_executor.execute(exec).unwrap();
        let elapsed = start.elapsed();
        println!(
            "Query {} took {:?}, num rows: {}",
            opt.query_id,
            elapsed,
            result.num_tuples().unwrap()
        );
    }
}
