import duckdb
import argparse
import os
import time
import csv

def parse_args():
    parser = argparse.ArgumentParser(description="TPC-H Benchmarks.")
    parser.add_argument('-a', '--all', action='store_true', default=False, help='Run all queries.')
    parser.add_argument('-w', '--warmups', type=int, default=2, help='Number of warmups.')
    parser.add_argument('-r', '--runs', type=int, default=3, help='Number of runs.')
    parser.add_argument('-q', '--query', type=int, default=15, help='Query ID (1-22).')
    parser.add_argument('-s', '--scale_factor', type=float, default=1, help='Scale factor (0.01-100).')
    return parser.parse_args()

def register_tables(con, data_dir):
    table_names = [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
    ]
    print("Registering tables...")
    for table in table_names:
        file_path = os.path.join(data_dir, f"{table}.csv")
        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS 
            SELECT * FROM read_csv_auto('{file_path}', delim='|');
        """)
    print("Tables registered.")

def run_query(con, query, warmups, runs):
    print(f"Warming up query...")
    for _ in range(warmups):
        result = con.execute(query).fetchall()
        print(f"Warm up result produced {len(result)} rows.")

    times = []
    print(f"Running query...")
    for _ in range(runs):
        start = time.time()
        result = con.execute(query).fetchall()
        elapsed = time.time() - start
        elapsed_in_ms = elapsed * 1000
        print(f"Query took {elapsed_in_ms:.2f} ms and produced {len(result)} rows.")
        times.append(elapsed)

    return times

def main():
    args = parse_args()
    data_dir = f"tpch/data/sf-{args.scale_factor}/input"
    
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory {data_dir} does not exist. Generate data first.")
    
    con = duckdb.connect(database=':memory:')
    register_tables(con, data_dir)

    if args.all:
        results = {}
        for query_id in range(1, 23):
            query_path = f"tpch/queries/q{query_id}.sql"
            with open(query_path, 'r') as file:
                query = file.read()
            times = run_query(con, query, args.warmups, args.runs)
            results[query_id] = times

        file_name = f"tpch_duckdb_results_sf_{args.scale_factor}.csv"
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            header = ['query_id'] + [f"time{i+1}" for i in range(args.runs)]
            writer.writerow(header)
            for query_id, times in results.items():
                writer.writerow([query_id] + times)
        print(f"Results written to {file_name}.")
    else:
        query_path = f"tpch/queries/q{args.query}.sql"
        with open(query_path, 'r') as file:
            query = file.read()
        run_query(con, query, args.warmups, args.runs)

if __name__ == "__main__":
    main()
