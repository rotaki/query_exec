use clap::Parser;
use query_exec::{
    prelude::{print_tuples, Catalog, DatabaseEngine, Executor, QueryExecutor, VolcanoIterator},
    ContainerType, InMemStorage,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(Debug, Parser)]
#[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
pub struct TpchOpt {
    /// Query ID. Should be in range [1, 22].
    #[clap(short = 'q', long = "query", default_value = "21")]
    pub query_id: usize,
    /// Scale factor. Should be in range [0.01, 100].
    #[clap(short = 's', long = "scale factor", default_value = "0.01")]
    pub scale_factor: f64,
}

fn get_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

fn get_in_mem_storage() -> Arc<InMemStorage> {
    Arc::new(InMemStorage::new())
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

    let db = DatabaseEngine::new(get_in_mem_storage());
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
        query_executor
            .import_csv(c_id, path, true, '|' as u8)
            .unwrap();
    }

    println!("Data loaded. Running query...");

    let query_path = format!("tpch/queries/q{}.sql", opt.query_id);
    let query = std::fs::read_to_string(query_path).unwrap();
    let logical = query_executor.to_logical(&query).unwrap();
    println!("=== Logical Plan ===");
    logical.pretty_print();
    let physical = query_executor.to_physical(logical);
    println!("=== Physical Plan ===");
    physical.pretty_print();
    let exec = query_executor.to_executable::<VolcanoIterator<InMemStorage>>(physical);
    println!("=== Exec ===");
    println!("{}", exec.to_pretty_string());
    let result = query_executor.execute(exec).unwrap();

    print_tuples(&result);
    println!("Count: {}", result.len());
}
