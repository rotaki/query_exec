# benches/duckdb_benchmark.py

import sys
import os
import duckdb
import psutil
import time

# Add the project root to sys.path to allow imports from 'util'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from util.measurement import print_log


def get_system_metrics():
    process = psutil.Process()
    cpu_percent = process.cpu_percent(interval=1)
    memory_info = process.memory_info()
    memory_usage = memory_info.rss / (1024 * 1024)  # Convert to MB
    return cpu_percent, memory_usage

def duckdb_in_memory_operation_one_partition(nums: int):
    engine: str = "duckdb"
    operation_type: str = "sum_of_total_amount"
    log_prefix = "one_partition"
    conn = duckdb.connect()

    for itr in range(nums):
        # Capture metrics before operation
        cpu_start, mem_start = get_system_metrics()
        print_log(log_prefix=log_prefix, engine=engine, itr=itr, phase="Start", operation_type=operation_type, cpu=cpu_start, memory=mem_start)
        
        start_time = time.time()
        file_name = "../data/yellow_tripdata_2024-01.csv"
        sql_query = "CREATE OR REPLACE VIEW parquet_table AS SELECT * FROM read_csv('{}')".format(file_name)

        
        # Execute the query
        conn.execute(sql_query)
        result = conn.execute("SELECT VendorID, SUM(total_amount) AS total_amount FROM parquet_table GROUP BY VendorID").fetchall()
        print(result)
        
        end_time = time.time()
        
        # Capture metrics after operation
        cpu_end, mem_end = get_system_metrics()
        runtime = end_time - start_time
        print_log(log_prefix=log_prefix, engine=engine, itr=itr, phase="Post_In_Memory", operation_type=operation_type, cpu=cpu_end, memory=mem_end, runtime=runtime)
    
    conn.close()

if __name__ == "__main__":
    duckdb_in_memory_operation_one_partition(nums=10)