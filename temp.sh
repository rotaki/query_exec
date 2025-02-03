#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=$1
NUM_THREADS=$2
WORKING_MEM=$3

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM

# Clean up previous runs
rm -rf bp-dir-tpch-sf-6005720-uniform/0/??*

cargo run --release --bin benchmark_queries -- -q 1 -p bp-dir-gensort-sf-6005720-uniform -n 1 -b $BUFFER_POOL_SIZE
#sudo perf stat -d cargo run --release --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE
#cargo flamegraph --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE

# Clean up after the run
rm -rf bp-dir-tpch-sf-sf-6005720-uniform/0/??*
