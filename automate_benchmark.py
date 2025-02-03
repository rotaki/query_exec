import subprocess
import csv
import os
import math

buffer_pool_sizes = [150000]
num_threads_options = [1, 2, 4, 8, 16]
working_mem_options = [300]

# Constants for the new columns
MACHINE = "Lincoln"
METHOD = "Parallel_BSS"
MEMORY_TYPE = "tank/local"
QUERY_NUM = "100"  
SF = "1" 
QUANTILE_METHOD = "Actual"
mem_size = 288 * int(SF)

# Output CSV file
output_csv = "benchmark_results.csv"

# Open the CSV file for writing
with open(output_csv, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([
        "Machine", "Method", "Memory Type", "Query Num", "SF", "Bp_Size",
        "Num Threads (Run Merge)", "Merge Time", "Quantile Method",
        "Working mem size", "Num Merge steps", "New pages", "Pages read", "Pages written"
    ])

    # Iterate through all configurations
    for bp_size in buffer_pool_sizes:
        for num_threads in num_threads_options:
            for working_mem in working_mem_options:
                print(f"Running benchmark with BP={bp_size}, Threads={num_threads}, WorkingMem={working_mem}")

                # Set environment variables
                env = os.environ.copy()
                env["NUM_THREADS"] = str(num_threads)
                env["WORKING_MEM"] = str(working_mem)

                # Run the benchmark
                result = subprocess.run(
                    ["./temp.sh", str(bp_size), str(num_threads), str(working_mem)],
                    env=env,
                    capture_output=False,
                    text=True,
                )

                # Check for errors
                if result.returncode != 0:
                    print(f"Benchmark failed with error code {result.returncode}")
                    continue  # Skip to the next configuration

                # Parse the output
                output = result.stdout
                merge_duration = None
                stats_after = None

                for line in output.splitlines():
                    if "merge duration" in line:
                        merge_duration = line.split("merge duration ")[1].strip()
                        merge_duration = merge_duration.rstrip("s")  # Remove the 's' from the end
                    if "stats after" in line:
                        stats_after = line.strip("stats after ").strip("()").split(", ")

                # Write the results to the CSV in the desired format
                if merge_duration is not None and stats_after is not None:
                    # Extract Num Merge steps, New pages, Pages read, Pages written from stats_after
                    num_merge_steps = math.ceil(mem_size / working_mem)
                    new_pages = stats_after[0]
                    pages_read = stats_after[1]
                    pages_written = stats_after[2]

                    # Write the row
                    writer.writerow([
                        MACHINE, METHOD, MEMORY_TYPE, QUERY_NUM, SF, bp_size,
                        num_threads, merge_duration, QUANTILE_METHOD,
                        working_mem, num_merge_steps, new_pages, pages_read, pages_written
                    ])
                    file.flush()  # Force write to disk

print(f"Benchmark results saved to {output_csv}")
