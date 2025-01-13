rm -rf bp-dir-tpch-sf-1/0/??*
cargo run --release --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1            
rm -rf bp-dir-tpch-sf-1/0/??*
