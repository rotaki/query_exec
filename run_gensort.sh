#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=150000
NUM_THREADS=16
WORKING_MEM=450
QUERY=1
SF=10576511
#xtx need way of conveying skewed data
# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=GENSORT_1_10576511
export DATA_SOURCE="GENSORT"
export SF=$SF
export QUERY_NUM=$QUERY 
export NUM_TUPLES=$SF

BP_DIR="bp-dir-gensort-sf-$SF-uniform"

# Clean up previous runs
rm -rf "$BP_DIR/0/??*"

cargo run --release --bin sort_run -- -q "$QUERY" -p "$BP_DIR" -n 1 -b "$BUFFER_POOL_SIZE"

# Clean up after the run
rm -rf "$BP_DIR/0/??*"
