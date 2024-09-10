#!/bin/bash

# Create the output directory if it doesn't exist
mkdir -p benchmark_results

# List of buffer pool sizes
bp_sizes=(100000)

# List of query IDs
query_ids=(100 101)

# List of memory sizes
memory_sizes=(100 500 1000 2000) 

# List of quantiles to use
num_quantiles_list=(2 6 11 16 21 26 31 36 41)

echo "running updated bench.sh"

# Iterate over query IDs first
for query_id in "${query_ids[@]}"; do
  # Then iterate over buffer pool sizes
  for bp_size in "${bp_sizes[@]}"; do
    # Filename for the output, based on num_quantiles, buffer pool size, and query ID
    output_file="benchmark_results/bp_${bp_size}-qid_${query_id}.txt"

    # Clear or create the file
    echo "Benchmark results for BP size $bp_size and Query ID $query_id:" > "$output_file"

    # Iterate over each memory size
    for mem_size in "${memory_sizes[@]}"; do
      # Then iterate over num_quantiles
      for num_quantiles in "${num_quantiles_list[@]}"; do

        # Clear the relevant directories before each benchmark run
        rm -rf bp-dir-tpch-sf-1/0/100*
        rm -rf bp-dir-tpch-sf-1/321

        echo "Running benchmark with memory size: $mem_size, BP size $bp_size, Q_ID $query_id, and num quantiles $num_quantiles"

        # Run the benchmark, passing the memory size, buffer pool size, query ID, and num_quantiles as environment variables
        BENCH_MEMORY_SIZE=$mem_size BENCH_BP_SIZE=$bp_size BENCH_QUERY_ID=$query_id BENCH_NUM_QUANTILES=$num_quantiles cargo bench --bench sort_bench >> "$output_file"

        echo "Completed benchmark with memory size: $mem_size and num_quantiles: $num_quantiles"  >> "$output_file"
      done
    done

    echo "-----------------------------------" >> "$output_file"
  done
done
