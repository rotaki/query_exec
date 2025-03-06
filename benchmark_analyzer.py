#!/usr/bin/env python3
"""
Benchmark results analyzer and visualization tool.
This script reads benchmark CSV results and generates enhanced visualizations
showing the effects of memory size and thread count on merge performance.

Example usage:
  # Basic usage (uses latest benchmark results):
  python benchmark_analyzer.py
  
  # Analyze specific data source and query:
  python benchmark_analyzer.py --data-source TPCH -q 100
  
  # Use a specific results directory:
  python benchmark_analyzer.py -d benchmark_results/b888acfb
  
  # Filter by scale factor:
  python benchmark_analyzer.py -s 1
"""

import os
import argparse
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from pathlib import Path

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Analyze benchmark results and generate visualizations')
    parser.add_argument('-d', '--directory', 
                        help='Directory containing benchmark CSV results (default: latest git hash dir)',
                        default=None)
    parser.add_argument('-o', '--output-dir',
                        help='Directory to save generated plots (default: plots subdir within benchmark directory)',
                        default=None)
    parser.add_argument('--data-source', choices=['TPCH', 'GENSORT'],
                        help='Filter by data source (default: all)',
                        default=None)
    parser.add_argument('-q', '--query', type=int,
                        help='Filter by query number (default: all)',
                        default=None)
    parser.add_argument('-s', '--scale-factor', type=int,
                        help='Filter by scale factor (default: all)',
                        default=None)
    return parser.parse_args()

def find_latest_results_dir():
    """Find the most recent results directory based on git hash."""
    root_dir = "benchmark_results"
    if not os.path.exists(root_dir):
        raise FileNotFoundError(f"Benchmark results directory '{root_dir}' not found")
    
    # Get all subdirectories (git hash directories)
    subdirs = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d))]
    
    if not subdirs:
        raise FileNotFoundError(f"No git hash directories found in '{root_dir}'")
    
    # Find the most recent directory by modification time
    latest_dir = max(subdirs, key=lambda d: os.path.getmtime(os.path.join(root_dir, d)))
    return os.path.join(root_dir, latest_dir)

def load_benchmark_data(directory, data_source=None, query=None, scale_factor=None):
    """Load and filter benchmark data from CSV files."""
    # Find all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in '{directory}'")
    
    # Load all CSV files into a single DataFrame
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Failed to read {file}: {e}")
    
    if not dfs:
        raise ValueError("No valid CSV data found. Could not load any benchmark results.")
    
    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Apply filters if specified
    if data_source:
        combined_df = combined_df[combined_df["Data Source"] == data_source]
    if query is not None:
        combined_df = combined_df[combined_df["Query Num"] == query]
    if scale_factor is not None:
        combined_df = combined_df[combined_df["SF"] == scale_factor]
    
    # Check if the filtered DataFrame is empty
    if combined_df.empty:
        raise ValueError(f"No data matches the filters: data_source={data_source}, query={query}, scale_factor={scale_factor}")
        
    # Convert numeric columns to appropriate types
    numeric_cols = [
        "Working mem size", "Num Threads (Run Merge)", 
        "Generation Time", "Merge Time", "Num Merge steps",
        "Throughput (GB/s)", "Throughput per Thread (GB/s)"
    ]
    
    for col in numeric_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    
    return combined_df
    
def plot_thread_scaling_by_memory(df, output_dir):
    """
    Generate thread scaling plots separated by memory size.
    Each plot shows how merge time varies with thread count for a specific memory size.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Group data by data source, query, scale factor, and memory size
    grouped = df.groupby(["Data Source", "Query Num", "SF", "Working mem size"])
    
    # Color palette
    colors = list(mcolors.TABLEAU_COLORS.values())
    
    for (data_source, query, sf, mem_size), group_df in grouped:
        # Skip if less than 2 thread counts (can't plot a line)
        if len(group_df["Num Threads (Run Merge)"].unique()) < 2:
            continue
            
        # Sort by thread count
        group_df = group_df.sort_values("Num Threads (Run Merge)")
        
        # Create plot
        plt.figure(figsize=(12, 8))
        
        # Plot merge time vs thread count
        plt.plot(group_df["Num Threads (Run Merge)"], group_df["Merge Time"], 
                 'bo-', linewidth=2, markersize=8, color=colors[0])
        
        # Add data points with merge step annotations
        for i, row in group_df.iterrows():
            plt.annotate(f"{row['Merge Time']:.2f}s ({int(row['Num Merge steps'])} steps)", 
                         (row["Num Threads (Run Merge)"], row["Merge Time"]),
                         textcoords="offset points",
                         xytext=(0, 10),
                         ha='center',
                         fontsize=9)
        
        # Set plot title and labels
        plt.title(f"Thread Scaling Analysis - Memory Size: {mem_size}\n"
                  f"{data_source} Query {query} SF={sf}",
                  fontsize=14, pad=20)
        
        plt.xlabel("Number of Threads", fontsize=12)
        plt.ylabel("Merge Time (seconds)", fontsize=12)
        
        # Set x-axis ticks to match thread counts
        threads = group_df["Num Threads (Run Merge)"].unique()
        plt.xticks(threads, threads)
        
        # Set y-axis range
        plt.ylim(0, group_df["Merge Time"].max() * 1.2)
        
        # Add grid
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # Add text with throughput information
        plt.figtext(0.5, 0.01, 
                    f"Average Throughput: {group_df['Throughput (GB/s)'].mean():.4f} GB/s | "
                    f"Per Thread: {group_df['Throughput per Thread (GB/s)'].mean():.4f} GB/s",
                    ha="center", fontsize=10, bbox={"facecolor":"lightgrey", "alpha":0.5, "pad":5})
        
        plt.tight_layout()
        
        # Save plot
        filename = f"{data_source}_Q{query}_SF{sf}_MEM{mem_size}_thread_scaling.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {save_path}")

def plot_merge_time_thread_scaling(df, output_dir):
    """
    Generate a plot showing merge time vs. thread count for all memory sizes on one graph.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Group data by data source, query, and scale factor
    grouped = df.groupby(["Data Source", "Query Num", "SF"])
    
    # Color palette
    colors = list(mcolors.TABLEAU_COLORS.values())
    
    for (data_source, query, sf), group_df in grouped:
        # Get unique memory sizes
        mem_sizes = sorted(group_df["Working mem size"].unique())
        
        if len(mem_sizes) < 1:
            continue
            
        # Create figure
        plt.figure(figsize=(12, 8))
        
        # Plot merge time vs. thread count for each memory size
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:  # Need at least 2 points for a line
                color = colors[i % len(colors)]
                plt.plot(mem_data["Num Threads (Run Merge)"], mem_data["Merge Time"], 
                         'o-', linewidth=2, markersize=8, color=color,
                         label=f"Memory: {mem_size}")
                
                # Add annotations with merge steps and time
                for _, row in mem_data.iterrows():
                    plt.annotate(f"{row['Merge Time']:.2f}s ({int(row['Num Merge steps'])} steps)", 
                               (row["Num Threads (Run Merge)"], row["Merge Time"]),
                               textcoords="offset points",
                               xytext=(0, 10),
                               ha='center',
                               fontsize=9)
        
        # Set plot title and labels
        plt.title(f"Merge Time vs. Thread Count\n{data_source} Query {query} SF={sf}",
                  fontsize=14, pad=20)
        
        plt.xlabel("Number of Threads", fontsize=12)
        plt.ylabel("Merge Time (seconds)", fontsize=12)
        
        # Add grid and legend
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(title="Working Memory Size", loc="best")
        
        plt.tight_layout()
        
        # Save plot
        filename = f"{data_source}_Q{query}_SF{sf}_merge_time_thread_scaling.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {save_path}")

def plot_throughput_analysis(df, output_dir):
    """
    Generate plots analyzing throughput metrics across different configurations.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Group data by data source, query, and scale factor
    grouped = df.groupby(["Data Source", "Query Num", "SF"])
    
    for (data_source, query, sf), group_df in grouped:
        # Get unique memory sizes
        mem_sizes = sorted(group_df["Working mem size"].unique())
        
        # Create figure
        plt.figure(figsize=(12, 8))
        
        # Plot throughput vs thread count for each memory size
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:  # Need at least 2 points for a line
                plt.plot(mem_data["Num Threads (Run Merge)"], mem_data["Throughput (GB/s)"], 
                        'o-', linewidth=2, markersize=8, 
                        label=f"Memory: {mem_size}")
                
                # Add annotations with throughput values
                for _, row in mem_data.iterrows():
                    plt.annotate(f"{row['Throughput (GB/s)']:.4f} GB/s", 
                               (row["Num Threads (Run Merge)"], row["Throughput (GB/s)"]),
                               textcoords="offset points",
                               xytext=(0, 10),
                               ha='center',
                               fontsize=9)
        
        # Set plot title and labels
        plt.title(f"Throughput Analysis\n{data_source} Query {query} SF={sf}",
                fontsize=14, pad=20)
        
        plt.xlabel("Number of Threads", fontsize=12)
        plt.ylabel("Throughput (GB/s)", fontsize=12)
        
        # Add grid and legend
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(title="Working Memory Size", loc="best")
        
        plt.tight_layout()
        
        # Save plot
        filename = f"{data_source}_Q{query}_SF{sf}_throughput.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {save_path}")

        # Also plot per-thread throughput
        plt.figure(figsize=(12, 8))
        
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:
                plt.plot(mem_data["Num Threads (Run Merge)"], mem_data["Throughput per Thread (GB/s)"], 
                        'o-', linewidth=2, markersize=8, 
                        label=f"Memory: {mem_size}")
                
                # Add annotations with throughput values
                for _, row in mem_data.iterrows():
                    plt.annotate(f"{row['Throughput per Thread (GB/s)']:.4f} GB/s", 
                               (row["Num Threads (Run Merge)"], row["Throughput per Thread (GB/s)"]),
                               textcoords="offset points",
                               xytext=(0, 10),
                               ha='center',
                               fontsize=9)
        
        plt.title(f"Per-Thread Throughput Analysis\n{data_source} Query {query} SF={sf}",
                fontsize=14, pad=20)
        
        plt.xlabel("Number of Threads", fontsize=12)
        plt.ylabel("Throughput per Thread (GB/s)", fontsize=12)
        
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(title="Working Memory Size", loc="best")
        
        plt.tight_layout()
        
        # Save plot
        filename = f"{data_source}_Q{query}_SF{sf}_per_thread_throughput.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {save_path}")

def main():
    """Main function to run the benchmark analyzer."""
    args = parse_args()
    
    # Determine results directory
    results_dir = args.directory
    if results_dir is None:
        try:
            results_dir = find_latest_results_dir()
            print(f"Using latest results directory: {results_dir}")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            return 1
    
    # Create output directory (as a subdirectory of the benchmark directory)
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = os.path.join(results_dir, "plots")
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Load benchmark data
        print(f"Loading benchmark data from {results_dir}...")
        df = load_benchmark_data(
            results_dir, 
            data_source=args.data_source,
            query=args.query,
            scale_factor=args.scale_factor
        )
        
        # Print summary information
        print(f"\nLoaded {len(df)} benchmark results")
        print(f"Data sources: {df['Data Source'].unique()}")
        print(f"Queries: {df['Query Num'].unique()}")
        print(f"Scale factors: {df['SF'].unique()}")
        print(f"Memory sizes: {sorted(df['Working mem size'].unique())}")
        print(f"Thread counts: {sorted(df['Num Threads (Run Merge)'].unique())}")
        
        # Generate plots
        print("\nGenerating thread scaling plots by memory size...")
        plot_thread_scaling_by_memory(df, output_dir)
        
        print("\nGenerating merge time vs. thread count plot...")
        plot_merge_time_thread_scaling(df, output_dir)
        
        print("\nGenerating throughput analysis plots...")
        plot_throughput_analysis(df, output_dir)
        
        # Get relative path to results_dir for cleaner output message
        try:
            rel_path = os.path.relpath(output_dir, os.path.dirname(results_dir))
            print(f"\nAll plots saved to {results_dir}/{rel_path}/")
        except:
            print(f"\nAll plots saved to {output_dir}/")
    
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())