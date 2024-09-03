#!/bin/bash

# Define an array of memory sizes
memory_sizes=(100 200 300 400 500 1000 2000 3000 5000 10000)
# memory_sizes=(10000 500)

# Output file for the benchmark results
output_file="benchmark_results.txt"

# Clear the output file before starting
echo "Benchmark results:" > $output_file

# Loop through each memory size
for memory_size in "${memory_sizes[@]}"
do
  echo "Running benchmark with memory size: $memory_size"

  # Clear directories before each benchmark run
  rm -rf bp-dir-tpch-sf-0.1/0/100*
  rm -rf bp-dir-tpch-sf-0.1/321

  rm -rf bp-dir-tpch-sf-1/0/100*
  rm -rf bp-dir-tpch-sf-1/321
  # Export the memory size as an environment variable
  export BENCH_MEMORY_SIZE=$memory_size

  # Run the cargo bench command, filter the relevant output, and append to the results file
#   cargo bench --bench sort_bench 2>&1 | grep -E "Benchmarking sort with memory size|time:|change:|No change in performance detected" >> $output_file
  cargo bench --bench sort_bench

  echo "Completed benchmark with memory size: $memory_size"
done