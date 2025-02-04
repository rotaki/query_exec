#!/bin/bash

# Parameters
BUFFER_POOL_SIZE=150000
NUM_THREADS=16
WORKING_MEM=300

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM

# Clean up previous runs
rm -rf bp-dir-tpch-sf-1/0/??*

cargo run --release --bin sort_run -- -q 100 -p bp-dir-tpch-sf-1 -n 1 -b $BUFFER_POOL_SIZE
# Clean up after the run
rm -rf bp-dir-tpch-sf-1/0/??*
