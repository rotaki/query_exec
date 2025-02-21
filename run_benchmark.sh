#!/bin/bash

# Check if all required parameters are provided
if [ "$#" -ne 6 ]; then
    echo "Usage: $0 DATA_SOURCE BUFFER_POOL_SIZE NUM_THREADS WORKING_MEM QUERY SF"
    exit 1
fi

# Parameters passed from Python script
DATA_SOURCE=$1
BUFFER_POOL_SIZE=$2
NUM_THREADS=$3
WORKING_MEM=$4
QUERY=$5
SF=$6

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export DATA_SOURCE=$DATA_SOURCE
export SF=$SF
export QUERY_NUM=$QUERY
export NUM_TUPLES=0  # This will be set below based on data source

# Set data source specific variables
case $DATA_SOURCE in
    "TPCH")
        export NUM_TUPLES=$((6005720 * SF))
        BP_DIR="bp-dir-tpch-sf-${SF}"
        ;;
    "GENSORT")
        export NUM_TUPLES=$SF
        BP_DIR="bp-dir-gensort-sf-${SF}-uniform"
        ;;
    *)
        echo "Unsupported data source: $DATA_SOURCE"
        exit 1
        ;;
esac

# Clean up previous runs
rm -rf "$BP_DIR/0/??*"

# Run the benchmark
cargo run --release --bin sort_run -- -q "$QUERY" -p "$BP_DIR" -n 1 -b "$BUFFER_POOL_SIZE"

# Clean up after the run
rm -rf "$BP_DIR/0/??*"