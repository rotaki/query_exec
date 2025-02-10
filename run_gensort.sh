#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=150000
NUM_THREADS=16
WORKING_MEM=200

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=GENSORT_1

# Clean up previous runs
rm -rf bp-dir-gesnort-sf-6005720-uniform/0/??*

cargo run --release --bin benchmark_queries -- -q 1 -p bp-dir-gensort-sf-6005720-uniform -n 1 -b $BUFFER_POOL_SIZE

# Clean up after the run
rm -rf bp-dir-gensort-sf-6005720-uniform/0/??*
