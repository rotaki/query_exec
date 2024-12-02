#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Create the output and tempfiles directories if they don't exist
mkdir -p benchmark_results
mkdir -p tempfiles

# Clear the tempfiles directory every time the script runs
rm -rf tempfiles/*

# List of buffer pool sizes
bp_sizes=(10000)

# List of query IDs
query_ids=(100)

# List of memory sizes
# memory_sizes=(100 500 1000 1500 2000 3000 4000 5000 6000)
memory_sizes=(100)

# List of quantiles to use (if applicable)
num_quantiles_list=(9)

# List of chunk sizes (if applicable)
chunk_sizes=(1)

echo "Running merge.sh"

# Iterate over query IDs first
for query_id in "${query_ids[@]}"; do
  # Then iterate over buffer pool sizes
  for bp_size in "${bp_sizes[@]}"; do
    # Filename for the output
    output_file="benchmark_results/sort_run_bp_${bp_size}_qid_${query_id}.txt"

    # Clear or create the file
    echo "Benchmark results for BP size $bp_size and Query ID $query_id:" > "$output_file"

    # Iterate over each num_quantiles and memory size
    for num_quantiles in "${num_quantiles_list[@]}"; do
      for mem_size in "${memory_sizes[@]}"; do
        for chunk_size in "${chunk_sizes[@]}"; do

          # Clear the relevant directories before each benchmark run
          rm -rf bp-dir-tpch-sf-0.1/0/100*
          rm -rf bp-dir-tpch-sf-0.1/321
          rm -rf bp-dir-tpch-sf-1/0/100*
          rm -rf bp-dir-tpch-sf-1/321
          rm -rf bp-dir-tpch-sf-10/0/100*
          rm -rf bp-dir-tpch-sf-10/321

          echo "Running sort with memory size: $mem_size, BP size: $bp_size, Q_ID: $query_id, num_quantiles: $num_quantiles, num_chunks: $chunk_size"

          # Export environment variables
          export BENCH_MEMORY_SIZE=$mem_size
          export BENCH_BP_SIZE=$bp_size
          export BENCH_QUERY_ID=$query_id
          export BENCH_NUM_QUANTILES=$num_quantiles
          export BENCH_CHUNK_SIZE=$chunk_size

          # Build the sort_run binary in release mode for performance
          echo "Building sort_run binary..."
          cargo build --bin sort_run --release

          # Define the path to the built binary
          binary_path="target/release/sort_run"

          # Check if the binary exists
          if [ ! -f "$binary_path" ]; then
            echo "Error: Binary $binary_path not found!"
            exit 1
          fi

          # Capture the start time
          start_time=$(date +%s.%N)

          # Execute the binary and capture output
          echo "Executing sort_run..."
          "$binary_path" 2>&1 | tee -a "$output_file"

          # Capture the end time
          end_time=$(date +%s.%N)

          # Calculate the elapsed time using bc for floating-point arithmetic
          elapsed=$(echo "$end_time - $start_time" | bc)
          echo "Elapsed time: $elapsed seconds" >> "$output_file"

          # Add a separator for readability
          echo "-----------------------------------" >> "$output_file"

          echo "Completed run with memory size: $mem_size"
            
          # Clear the relevant directories after each benchmark run
          rm -rf bp-dir-tpch-sf-0.1/0/100*
          rm -rf bp-dir-tpch-sf-0.1/321
          rm -rf bp-dir-tpch-sf-1/0/100*
          rm -rf bp-dir-tpch-sf-1/321
          rm -rf bp-dir-tpch-sf-10/0/100*
          rm -rf bp-dir-tpch-sf-10/321

        done
      done
    done
  done
done

echo "All sorting runs completed. Results are in the benchmark_results/directory."fa  