import subprocess
import csv
import os
import math
from datetime import datetime
import git
import argparse

def get_git_hash():
    """Get the current git commit hash."""
    repo = git.Repo(search_parent_directories=True)
    return repo.head.object.hexsha[:8]

def calculate_num_tuples(data_source, sf):
    """Calculate number of tuples based on data source and scale factor."""
    if data_source == "TPCH":
        return 6005720 * sf
    elif data_source == "GENSORT":
        return sf
    else:
        raise ValueError(f"Unknown data source: {data_source}")

def get_bp_dir(data_source, sf, distribution="uniform"):
    """Generate buffer pool directory name."""
    if data_source == "TPCH":
        return f"bp-dir-tpch-sf-{sf}"
    elif data_source == "GENSORT":
        return f"bp-dir-gensort-sf-{sf}-{distribution}"
    else:
        raise ValueError(f"Unknown data source: {data_source}")

def parse_benchmark_output(output):
    """Parse benchmark output for times and stats."""
    generation_time = None
    merge_duration = None
    stats_after = None

    for line in output.splitlines():
        if "generation duration" in line:
            generation_time = line.split("generation duration ")[1].strip().rstrip("s")
        elif "merge duration" in line:
            merge_duration = line.split("merge duration ")[1].strip().rstrip("s")
        elif "stats after" in line:
            stats_after = line.strip("stats after ").strip("()").split(", ")

    return generation_time, merge_duration, stats_after

def get_default_configs():
    """Return default configuration settings."""
    return {
        "TPCH": {
            "data_source": "TPCH",
            "machine": "Lincoln",
            "method": "Parallel_BSS",
            "memory_type": "tank/local",
            "query_options": [100],
            "sf_options": [1, 2],
            "working_mem_options": [1420],
            "bp_sizes": [100000],
            "num_threads_options": [1, 2, 4, 8, 16]
        },
        "GENSORT": {
            "data_source": "GENSORT",
            "machine": "Lincoln",
            "method": "Parallel_BSS",
            "memory_type": "tank/local",
            "query_options": [1],
            "sf_options": [10576511],
            "working_mem_options": [450],
            "bp_sizes": [150000],
            "num_threads_options": [1, 2, 4, 8, 16]
        }
    }

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run benchmarks with optional parameters')
    parser.add_argument('-d', '--data-source', choices=['TPCH', 'GENSORT'],
                        help='Data source (TPCH or GENSORT)')
    parser.add_argument('-s', '--scale-factor', type=int,
                        help='Scale factor')
    parser.add_argument('-q', '--query', type=int,
                        help='Query number')
    parser.add_argument('-t', '--threads', type=int,
                        help='Number of threads')
    parser.add_argument('-w', '--working-mem', type=int,
                        help='Working memory size')
    parser.add_argument('-b', '--buffer-pool', type=int,
                        help='Buffer pool size')
    parser.add_argument('-m', '--machine',
                        help='Machine name')
    return parser.parse_args()


def is_single_config():
    """Check if we're running a single manual configuration."""
    args = parse_args()
    return args.data_source is not None

def create_run_configs(args):
    """Create run configurations based on args and defaults."""
    default_configs = get_default_configs()
    
    # If no data source specified, run all default configurations
    if not args.data_source:
        return default_configs
    
    # Start with the default config for the specified data source
    config = default_configs[args.data_source].copy()
    
    # Override with any specified command line arguments
    if args.scale_factor is not None:
        config["sf_options"] = [args.scale_factor]
    if args.query is not None:
        config["query_options"] = [args.query]
    if args.threads is not None:
        config["num_threads_options"] = [args.threads]
    if args.working_mem is not None:
        config["working_mem_options"] = [args.working_mem]
    if args.buffer_pool is not None:
        config["bp_sizes"] = [args.buffer_pool]
    if args.machine is not None:
        config["machine"] = args.machine
    
    return {args.data_source: config}

def run_benchmark(config, is_manual=False):
    """Run a single benchmark with given configuration."""
    # Set up environment variables
    env = os.environ.copy()
    env["NUM_THREADS"] = str(config["num_threads"])
    env["WORKING_MEM"] = str(config["working_mem"])
    env["DATA_SOURCE"] = config["data_source"]
    env["SF"] = str(config["sf"])
    env["QUERY_NUM"] = str(config["query"])
    env["NUM_TUPLES"] = str(calculate_num_tuples(config["data_source"], config["sf"]))

    bp_dir = get_bp_dir(config["data_source"], config["sf"])
    
    # Clean up previous runs
    subprocess.run(f"rm -rf {bp_dir}/0/??*", shell=True)

    # Run the benchmark script
    cmd = [
        "./run_benchmark.sh",
        config["data_source"],
        str(config["bp_size"]),
        str(config["num_threads"]),
        str(config["working_mem"]),
        str(config["query"]),
        str(config["sf"])
    ]

    if is_manual:
        # For manual runs, just execute and return success/failure
        result = subprocess.run(cmd, env=env)
        return result.returncode == 0
    else:
        # For automated runs, capture output for parsing
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        return result

def main():
    args = parse_args()
    
    # Check if any args were provided (manual mode)
    manual_mode = any(value is not None for value in vars(args).values())
    
    if manual_mode:
        # Use provided values or defaults for manual run
        config = {
            "data_source": args.data_source or "TPCH",
            "bp_size": args.buffer_pool or 100000,
            "num_threads": args.threads or 16,
            "working_mem": args.working_mem or 1420,
            "query": args.query or 100,
            "sf": args.scale_factor or 1
        }
        
        print(f"Running benchmark for {config['data_source']} (Query {config['query']}, SF {config['sf']}) "
              f"with BP={config['bp_size']}, Threads={config['num_threads']}, WorkingMem={config['working_mem']}")
        
        success = run_benchmark(config, is_manual=True)
        if not success:
            print("Benchmark failed!")
        return

    # No args provided - run automated benchmarks
    configs = get_default_configs()
    
    # Get git commit hash
    git_hash = get_git_hash()
    
    # Create benchmark_results directory if it doesn't exist
    os.makedirs("benchmark_results", exist_ok=True)
    os.makedirs(f"benchmark_results/{git_hash}", exist_ok=True)

    # Process each configuration
    for data_source, base_config in configs.items():
        for query in base_config["query_options"]:
            for sf in base_config["sf_options"]:
                # Generate output filename
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_csv = f"benchmark_results/{git_hash}/{data_source}_QID-{query}_SF-{sf}_TIME-{current_time}.csv"

                with open(output_csv, mode="w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        "Machine", "Method", "Memory Type", "Query Num", "SF", "Bp_Size",
                        "Working mem size", "Num Threads (Run Merge)", 
                        "Generation Time", "Merge Time", "Num Merge steps", 
                        "New pages", "Pages read", "Pages written"
                    ])

                    # Iterate through all parameter combinations
                    for working_mem in base_config["working_mem_options"]:
                        for bp_size in base_config["bp_sizes"]:
                            for num_threads in base_config["num_threads_options"]:
                                print(f"Running benchmark for {data_source} (Query {query}, SF {sf}) "
                                      f"with BP={bp_size}, Threads={num_threads}, WorkingMem={working_mem}")

                                # Create run configuration
                                run_config = {
                                    "bp_size": bp_size,
                                    "num_threads": num_threads,
                                    "working_mem": working_mem,
                                    "data_source": data_source,
                                    "query": query,
                                    "sf": sf
                                }

                                # Run benchmark
                                result = run_benchmark(run_config, is_manual=False)

                                # Process results
                                if result.returncode != 0:
                                    print(f"Benchmark failed with error code {result.returncode}")
                                    continue

                                # Parse the output
                                generation_time, merge_duration, stats_after = parse_benchmark_output(result.stdout)

                                if generation_time is not None and merge_duration is not None and stats_after is not None:
                                    num_merge_steps = math.ceil(calculate_num_tuples(data_source, sf) / working_mem)
                                    new_pages = stats_after[0]
                                    pages_read = stats_after[1]
                                    pages_written = stats_after[2]

                                    writer.writerow([
                                        base_config["machine"], base_config["method"],
                                        base_config["memory_type"], query,
                                        sf, bp_size,
                                        working_mem, num_threads,
                                        generation_time, merge_duration, num_merge_steps,
                                        new_pages, pages_read, pages_written
                                    ])
                                    file.flush()

                print(f"Benchmark results saved to {output_csv}")

if __name__ == "__main__":
    main()