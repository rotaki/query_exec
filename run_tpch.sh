#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=100000
NUM_THREADS=16
WORKING_MEM=1420

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=TPCH_100
export NUM_TUPLES=6005720 
# export NUM_TUPLES=30028600

# Clean up previous runs
rm -rf bp-dir-tpch-sf-1/0/??*

cargo run --release --bin sort_run  -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b "$BUFFER_POOL_SIZE"

rm -rf bp-dir-tpch-sf-1/0/??*
