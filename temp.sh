#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=150000
NUM_THREADS=1
WORKING_MEM=60

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM

# Clean up previous runs
rm -rf bp-dir-tpch-sf-1/0/??*

# Run the benchmark with the specified parameters
#cargo run --release --bin benchmark_queries -- -q 1 -p bp-dir-uniform -n 1 -b $BUFFER_POOL_SIZE
perf stat -d cargo run --release --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE
#cargo flamegraph --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE

# Clean up after the run
rm -rf bp-dir-tpch-sf-1/0/??*
