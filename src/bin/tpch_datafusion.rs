#[cfg(feature = "datafusion_bench")]
mod bench {
    use clap::Parser;
    use datafusion::prelude::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    #[derive(Debug, Parser)]
    #[clap(name = "TPC-H", about = "TPC-H Benchmarks.")]
    pub struct TpchOpt {
        /// Run all queries.
        #[clap(short = 'a', long = "all", default_value = "false")]
        pub bench_all: bool,
        /// Query ID. Should be in range [1, 22].
        #[clap(short = 'q', long = "query", default_value = "15")]
        pub query_id: usize,
        /// Scale factor. Should be in range [0.01, 100].
        #[clap(short = 's', long = "scale factor", default_value = "0.1")]
        pub scale_factor: f64,
    }

    #[tokio::main]
    pub async fn main() -> datafusion::error::Result<()> {
        let opt = TpchOpt::parse();

        // Verify data directory existence
        let data_dir = format!("tpch/data/sf-{}", opt.scale_factor);
        if !PathBuf::from(&data_dir).exists() {
            panic!(
                "Data directory {} does not exist. Generate data first.",
                data_dir
            );
        }

        // Initialize DataFusion context
        let ctx = SessionContext::new();

        // Register tables
        let table_names = vec![
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

        println!("Registering tables...");
        for table in &table_names {
            let file_path = format!("{}/input/{}.csv", data_dir, table);
            ctx.register_csv(table, &file_path, CsvReadOptions::new().delimiter(b'|'))
                .await?;
        }

        println!("Tables registered. Running queries...");

        if opt.bench_all {
            let mut results = BTreeMap::new();

            for query_id in 1..=22 {
                let query_path = format!("tpch/queries/q{}.sql", query_id);
                let query = fs::read_to_string(query_path)?;
                println!("====== Warming up query {} ======", query_id);
                for _ in 0..3 {
                    let df = ctx.sql(&query).await?;
                    let result = df.collect().await?;
                    println!("Warm up result produced {} rows.", result.len());
                }

                println!("====== Running query {} ======", query_id);
                for _ in 0..10 {
                    let start = Instant::now();
                    let df = ctx.sql(&query).await?;
                    let result = df.collect().await?;
                    let elapsed = start.elapsed();
                    println!(
                        "Query {} took {:?}, produced {} rows.",
                        query_id,
                        elapsed,
                        result.iter().map(|batch| batch.num_rows()).sum::<usize>()
                    );
                    results
                        .entry(query_id)
                        .or_insert_with(Vec::new)
                        .push(elapsed);
                }
            }

            // Print the results as csv with the following format:
            // query_id, time1, time2, time3, ...
            // Name the file tpch_datafusion_results_sf_<scale_factor>.csv
            let file_name = format!("tpch_datafusion_results_sf_{}.csv", opt.scale_factor);
            let mut writer = csv::Writer::from_path(&file_name).unwrap();
            writer
                .write_record(&[
                    "query_id", "time1", "time2", "time3", "time4", "time5", "time6", "time7",
                    "time8", "time9", "time10",
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
            let query = fs::read_to_string(query_path)?;
            let plan = ctx.sql(&query).await?;
            let plan = plan.into_optimized_plan()?;

            println!("====== Running query {} ======", opt.query_id);
            println!("Logical plan:\n{:?}", plan);

            let start = Instant::now();
            let df = ctx.execute_logical_plan(plan).await?;
            let result = df.collect().await?;
            let elapsed = start.elapsed();

            println!(
                "Query {} took {:?}, produced {} rows.",
                opt.query_id,
                elapsed,
                result.iter().map(|batch| batch.num_rows()).sum::<usize>()
            );
        }

        Ok(())
    }
}

fn main() {
    #[cfg(feature = "datafusion_bench")]
    bench::main().unwrap();
    #[cfg(not(feature = "datafusion_bench"))]
    println!("Please enable the 'datafusion_bench' feature to run this example.");
}
