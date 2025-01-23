#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=$1
NUM_THREADS=$2
WORKING_MEM=$3

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM

# Clean up previous runs
rm -rf bp-dir-tpch-sf-1/0/??*

# Run the benchmark with the specified parameters
cargo run --release --bin benchmark_queries -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE

# Clean up after the run
rm -rf bp-dir-tpch-sf-1/0/??*
