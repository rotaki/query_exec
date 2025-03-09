# util/measurement.py

import csv
import os

def print_log(log_prefix, engine, itr, phase, operation_type, cpu, memory, runtime=None):
    """
    Logs the benchmarking metrics to a CSV file.

    Parameters:
    - log_prefix (str): Prefix for the log entry.
    - engine (str): Name of the engine (e.g., 'duckdb', 'query_exec').
    - itr (int): Iteration number.
    - phase (str): Phase of the operation ('Start' or 'Post_In_Memory').
    - operation_type (str): Type of operation being performed.
    - cpu (float): CPU usage percentage.
    - memory (float): Memory usage in MB.
    - runtime (float, optional): Runtime in seconds (only for 'Post_In_Memory' phase).
    """
    log_file = 'benchmark_logs.csv'
    file_exists = os.path.isfile(log_file)

    with open(log_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Write header if file does not exist
        if not file_exists:
            writer.writerow(['LogPrefix', 'Engine', 'Iteration', 'Phase', 'OperationType', 'CPU%', 'Memory(MB)', 'Runtime(s)'])

        if phase == "Start":
            writer.writerow([log_prefix, engine, itr, phase, operation_type, cpu, memory, ''])
        elif phase == "Post_In_Memory":
            writer.writerow([log_prefix, engine, itr, phase, operation_type, cpu, memory, runtime])