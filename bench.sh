#!/bin/bash

# Create the output directory if it doesn't exist
mkdir -p benchmark_results

# List of buffer pool sizes
bp_sizes=(100000)

# List of query IDs
# query_ids=(100 101)
query_ids=(100)

# List of memory sizes
#memory_sizes=(100 500 1000 2000) 
memory_sizes=(100)

# List of quantiles to use
#num_quantiles_list=(2 6 11 16 21 26 31 36 41 46)
num_quantiles_list=(5 6 7 8)

echo "running updated bench.sh"

# Create a temporary file for storing intermediate results
temp_file=$(mktemp ./tempfile.XXXXXX)
echo "Temporary file created at: $temp_file"

# Iterate over query IDs first
for query_id in "${query_ids[@]}"; do
  # Then iterate over buffer pool sizes
  for bp_size in "${bp_sizes[@]}"; do
    # Filename for the output, based on num_quantiles, buffer pool size, and query ID
    output_file="benchmark_results/bp_${bp_size}-qid_${query_id}.txt"

    # Clear or create the file
    echo "Benchmark results for BP size $bp_size and Query ID $query_id:" > "$output_file"

    # Initialize arrays to store metrics for averaging later
    run_generation_times=()
    run_merge_times=()
    num_runs=()
    num_threads=()

    # Iterate over each memory size
    for mem_size in "${memory_sizes[@]}"; do
      # Then iterate over num_quantiles
      for num_quantiles in "${num_quantiles_list[@]}"; do

        # Clear the relevant directories before each benchmark run
        rm -rf bp-dir-tpch-sf-1/0/100*
        rm -rf bp-dir-tpch-sf-1/321

        echo "Running benchmark with memory size: $mem_size, BP size $bp_size, Q_ID $query_id, and num quantiles $num_quantiles"

        # Run the benchmark and store output to the temp file and output file
        BENCH_MEMORY_SIZE=$mem_size BENCH_BP_SIZE=$bp_size BENCH_QUERY_ID=$query_id BENCH_NUM_QUANTILES=$num_quantiles \
        cargo bench --bench sort_bench | tee -a "$temp_file" "$output_file"

        # Extract relevant data from the temporary file using grep/awk without printing them
        run_gen_time=$(grep "Run generation took" "$temp_file" | tail -1 | awk '{print $4}')
        run_merge_time=$(grep "Run merge took" "$temp_file" | tail -1 | awk '{print $4}')
        num_run=$(grep "num runs" "$temp_file" | tail -1 | awk '{print $3}')
        num_thread=$(grep "num_threads" "$temp_file" | tail -1 | awk '{print $3}')

        # Store the extracted values into arrays
        run_generation_times+=("$run_gen_time")
        run_merge_times+=("$run_merge_time")
        num_runs+=("$num_run")
        num_threads+=("$num_thread")

      done
    done

    # Calculate averages
    function calculate_average() {
      local sum=0
      for val in "${@}"; do
        sum=$(echo "$sum + $val" | bc)
      done
      echo "scale=2; $sum / ${#@}" | bc
    }

    avg_run_gen=$(calculate_average "${run_generation_times[@]}")
    avg_run_merge=$(calculate_average "${run_merge_times[@]}")
    avg_num_runs=$(calculate_average "${num_runs[@]}")
    avg_num_threads=$(calculate_average "${num_threads[@]}")

    # Output the averages to the file and the terminal
    echo "Averages for BP size $bp_size and Query ID $query_id:" | tee -a "$output_file"
    echo "Average Run Generation Time: $avg_run_gen seconds" | tee -a "$output_file"
    echo "Average Run Merge Time: $avg_run_merge seconds" | tee -a "$output_file"
    echo "Average Number of Runs: $avg_num_runs" | tee -a "$output_file"
    echo "Average Number of Threads: $avg_num_threads" | tee -a "$output_file"

    # Output Cargo Benchmark Results (preserved in full)
    echo "Benchmark Results from Cargo:" >> "$output_file"
    grep -A 4 'sort with memory size' "$temp_file" | tee -a "$output_file" # Capture the performance section
    echo "-----------------------------------" | tee -a "$output_file"
  done
done

# Cleanup the temporary file
rm "$temp_file"