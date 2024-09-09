#!/bin/bash

# Create the output directory if it doesn't exist
mkdir -p benchmark_results

# List of buffer pool sizes
bp_sizes=(10000 20000)

# List of query IDs
query_ids=(100 101)
# query_ids=(101)

# List of memory sizes (example)
memory_sizes=(100 200 300 400 500 600 700 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000 3000 4000 5000 6000 7000 8000 9000 10000)
# memory_sizes=(400 500 600 700 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000 3000 4000 5000 6000 7000 8000 9000 10000)


num_quantiles=20
# Iterate over each combination of buffer pool size and query ID
for bp_size in "${bp_sizes[@]}"; do
  for query_id in "${query_ids[@]}"; do




    # Filename for the output, based on buffer pool size and query ID
    output_file="benchmark_results/bp_${bp_size}-qid_${query_id}.txt"
    
    # Clear or create the file
    echo "Benchmark results for BP size $bp_size and Query ID $query_id:" > "$output_file"

    # Iterate over each memory size
    for mem_size in "${memory_sizes[@]}"; do

      # Clear the relevant directories before each benchmark run
      rm -rf bp-dir-tpch-sf-1/0/100*
      rm -rf bp-dir-tpch-sf-1/321
      echo "Running benchmark with memory size: $mem_size"



      # Run the benchmark, passing the memory size, buffer pool size, and query ID as environment variables
    #   BENCH_MEMORY_SIZE=$mem_size BENCH_BP_SIZE=$bp_size BENCH_QUERY_ID=$query_id cargo bench --bench sort_bench >> "$output_file"
    BENCH_MEMORY_SIZE=$mem_size BENCH_BP_SIZE=$bp_size BENCH_QUERY_ID=$query_id BENCH_NUM_QUANTILES=$num_quantiles cargo bench --bench sort_bench

      echo "Completed benchmark with memory size: $mem_size" >> "$output_file"
    done

    echo "-----------------------------------" >> "$output_file"
  done
done