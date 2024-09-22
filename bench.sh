#!/bin/bash

# Create the output and tempfiles directories if they don't exist
mkdir -p benchmark_results
mkdir -p tempfiles

# Clear the tempfiles directory every time the script runs
rm -rf tempfiles/*

# List of buffer pool sizes
bp_sizes=(20000)

# List of query IDs
query_ids=(5)

# List of memory sizes
#memory_sizes=(100 200 300 400 500 600 700 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000)

memory_sizes=(100)

# List of quantiles to use
num_quantiles_list=(2)

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
    #for mem_size in "${memory_sizes[@]}"; do
    for num_quantiles in "${num_quantiles_list[@]}"; do
      # Then iterate over num_quantiles
      #for num_quantiles in "${num_quantiles_list[@]}"; do
      for mem_size in "${memory_sizes[@]}"; do

        # Clear the relevant directories before each benchmark run
        rm -rf bp-dir-tpch-sf-1/0/100*
        rm -rf bp-dir-tpch-sf-1/321

        echo "Running benchmark with memory size: $mem_size, BP size $bp_size, Q_ID $query_id, and num quantiles $num_quantiles"

        # Initialize variables to track metrics
        run_generation_times=()
        run_merge_times=()
        num_runs=()
        num_threads=()

        # Capture the output of the benchmark command directly
        output=$(BENCH_MEMORY_SIZE=$mem_size BENCH_BP_SIZE=$bp_size BENCH_QUERY_ID=$query_id BENCH_NUM_QUANTILES=$num_quantiles \
        cargo bench --bench sort_bench)

        # Parse the output line by line
        while IFS= read -r line; do
          # Capture "Run generation took"
          if [[ $line == *"Run generation took"* ]]; then
            run_gen_time=$(echo "$line" | awk '{print $4}' | sed 's/s//g') # Remove 's'
            run_generation_times+=("$run_gen_time")
          fi
          # Capture "Run merge took"
          if [[ $line == *"Run merge took"* ]]; then
            run_merge_time=$(echo "$line" | awk '{print $4}' | sed 's/s//g') # Remove 's'
            run_merge_times+=("$run_merge_time")
          fi
          # Capture "num runs"
          if [[ $line == *"num runs"* ]]; then
            num_run=$(echo "$line" | awk '{print $3}')
            num_runs+=("$num_run")
          fi
          # Capture "num_threads"
          if [[ $line == *"num_threads"* ]]; then
            num_thread=$(echo "$line" | awk '{print $3}')
            num_threads+=("$num_thread")
          fi
        done <<< "$output"

        # Calculate averages
        function calculate_average() {
          local sum=0
          local count=${#@}
          if (( count == 0 )); then
            echo "0"
            return
          fi
          for val in "$@"; do
            sum=$(echo "$sum + $val" | bc)
          done
          echo "scale=2; $sum / $count" | bc
        }

        # Compute averages
        avg_run_gen=$(calculate_average "${run_generation_times[@]}")
        avg_run_merge=$(calculate_average "${run_merge_times[@]}")
        avg_num_runs=$(calculate_average "${num_runs[@]}")
        avg_num_threads=$(calculate_average "${num_threads[@]}")

        echo "Calculating averages..."
        echo "Averages for BP size $bp_size, Query ID $query_id, memory size $mem_size, and num_quantiles $num_quantiles:" >> "$output_file"
        echo "Average Run Generation Time: $avg_run_gen seconds" >> "$output_file"
        echo "Average Run Merge Time: $avg_run_merge seconds" >> "$output_file"
        echo "Average Number of Runs: $avg_num_runs" >> "$output_file"
        echo "Average Number of Threads: $avg_num_threads" >> "$output_file"
        echo "Finished calculating averages."

        # Add Cargo bench results to the file
        echo "$output" | grep -A 4 'sort with memory size' >> "$output_file"
        echo "-----------------------------------" >> "$output_file"
      done
    done
  done
done
