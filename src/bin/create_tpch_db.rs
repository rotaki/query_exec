use clap::Parser;
use query_exec::{
    prelude::{create_db, create_table_from_sql, import_csv, Catalog},
    BufferPool, ContainerDS, ContainerType, OnDiskStorage,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(Debug, Parser)]
#[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
pub struct TpchOpt {
    /// Scale factor. Should be in range [0.01, 100].
    #[clap(short = 's', long = "scale factor", default_value = "0.1")]
    pub scale_factor: f64,
    /// Buffer pool size.
    #[clap(short = 'b', long = "buffer pool size", default_value = "1000000")]
    // 4K * 1000000 = 4GB
    pub buffer_pool_size: usize,
}

fn get_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

fn get_bp(dir: &str, num_frames: usize) -> Arc<BufferPool> {
    Arc::new(BufferPool::new(dir, num_frames, false).unwrap())
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
    } else {
        println!("Importing data from {}.", data_dir);
    }

    let catalog = get_catalog();
    let bp_name = format!("bp-dir-tpch-sf-{}", opt.scale_factor);

    if PathBuf::from(&bp_name).exists() {
        panic!("Buffer pool directory {} already exists.", bp_name);
    }

    let bp = get_bp(&bp_name, opt.buffer_pool_size);
    let storage = Arc::new(OnDiskStorage::new(&bp));
    let db_id = create_db(&storage, "TPCH").unwrap();

    let table_names = vec![
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ];
    // let table_names = vec!["lineitem"];

    println!("Creating tables...");

    let mut tables = HashMap::new();

    for table in &table_names {
        let path = format!("tpch/tables/{}.sql", table);
        let sql = std::fs::read_to_string(path).unwrap();
        let c_id = create_table_from_sql(
            &catalog,
            &storage,
            db_id,
            sql.as_ref(),
            ContainerDS::AppendOnly,
        )
        .unwrap();
        tables.insert(table, c_id);
    }

    println!("Tables created. Loading data...");

    for table in &table_names {
        let c_id = tables[table];
        let path = format!("tpch/data/sf-{}/input/{}.csv", opt.scale_factor, table);
        import_csv(&catalog, &storage, db_id, c_id, path, true, b'|').unwrap();
    }

    println!("Data written to {}.", bp_name);
}
