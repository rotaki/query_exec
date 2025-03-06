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
    
    # Check if "Iteration" column exists
    if "Iteration" not in combined_df.columns:
        # Add a default iteration column if it doesn't exist
        combined_df["Iteration"] = 1
    
    return combined_df

def compute_aggregated_metrics(df):
    """
    Compute aggregated metrics (mean, std dev) for multiple iterations of the same configuration.
    Returns a new DataFrame with averaged metrics and standard deviations.
    """
    # Define the columns to group by (all configuration parameters except metrics)
    group_cols = [
        "Machine", "Data Source", "Quantile Method", "Memory Type", 
        "Query Num", "SF", "Bp_Size", "Working mem size", "Num Threads (Run Merge)"
    ]
    
    # Define metrics columns to average
    metric_cols = [
        "Generation Time", "Merge Time", "Num Merge steps",
        "Throughput (GB/s)", "Throughput per Thread (GB/s)"
    ]
    
    # Calculate aggregated statistics
    aggregated = df.groupby(group_cols)[metric_cols].agg(['mean', 'std', 'count']).reset_index()
    
    # Flatten the column hierarchy
    aggregated.columns = [
        col[0] if col[1] == '' else f"{col[0]}_{col[1]}" 
        for col in aggregated.columns
    ]
    
    return aggregated

def plot_thread_scaling_by_memory_with_error_bars(df, output_dir):
    """
    Generate thread scaling plots separated by memory size with error bars.
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
        
        # Plot merge time vs thread count with error bars
        plt.errorbar(
            group_df["Num Threads (Run Merge)"], 
            group_df["Merge Time_mean"], 
            yerr=group_df["Merge Time_std"],
            fmt='o-', linewidth=2, markersize=8, color=colors[0],
            capsize=5, elinewidth=1.5, capthick=1.5
        )
        
        # Add data points with merge step annotations and iteration counts
        for i, row in group_df.iterrows():
            annotation_text = f"{row['Merge Time_mean']:.2f}s ± {row['Merge Time_std']:.2f}s\n({int(row['Num Merge steps_mean'])} steps, n={int(row['Merge Time_count'])})"
            plt.annotate(
                annotation_text, 
                (row["Num Threads (Run Merge)"], row["Merge Time_mean"]),
                textcoords="offset points",
                xytext=(0, 12),
                ha='center',
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
            )
        
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
        y_min = max(0, group_df["Merge Time_mean"].min() - 2 * group_df["Merge Time_std"].max())
        y_max = group_df["Merge Time_mean"].max() + 2 * group_df["Merge Time_std"].max()
        plt.ylim(y_min, y_max * 1.1)
        
        # Add grid
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # Add text with throughput information
        plt.figtext(0.5, 0.01, 
                    f"Average Throughput: {group_df['Throughput (GB/s)_mean'].mean():.4f} GB/s | "
                    f"Per Thread: {group_df['Throughput per Thread (GB/s)_mean'].mean():.4f} GB/s",
                    ha="center", fontsize=10, bbox={"facecolor":"lightgrey", "alpha":0.5, "pad":5})
        
        plt.tight_layout()
        
        # Save plot
        filename = f"{data_source}_Q{query}_SF{sf}_MEM{mem_size}_thread_scaling.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {save_path}")

def plot_merge_time_thread_scaling_with_error_bars(df, output_dir):
    """
    Generate a plot showing merge time vs. thread count for all memory sizes with error bars.
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
        
        # Plot merge time vs. thread count for each memory size with error bars
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:  # Need at least 2 points for a line
                color = colors[i % len(colors)]
                
                plt.errorbar(
                    mem_data["Num Threads (Run Merge)"], 
                    mem_data["Merge Time_mean"], 
                    yerr=mem_data["Merge Time_std"],
                    fmt='o-', linewidth=2, markersize=8, color=color,
                    capsize=5, elinewidth=1.5, capthick=1.5,
                    label=f"Memory: {mem_size}"
                )
                
                # Add annotations with merge steps, time, and sample count
                for _, row in mem_data.iterrows():
                    annotation_text = f"{row['Merge Time_mean']:.2f}s ± {row['Merge Time_std']:.2f}s\n({int(row['Num Merge steps_mean'])} steps, n={int(row['Merge Time_count'])})"
                    plt.annotate(
                        annotation_text, 
                        (row["Num Threads (Run Merge)"], row["Merge Time_mean"]),
                        textcoords="offset points",
                        xytext=(0, 12),
                        ha='center',
                        fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
                    )
        
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

def plot_throughput_analysis_with_error_bars(df, output_dir):
    """
    Generate plots analyzing throughput metrics across different configurations with error bars.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Group data by data source, query, and scale factor
    grouped = df.groupby(["Data Source", "Query Num", "SF"])
    
    for (data_source, query, sf), group_df in grouped:
        # Get unique memory sizes
        mem_sizes = sorted(group_df["Working mem size"].unique())
        
        # Create figure for throughput
        plt.figure(figsize=(12, 8))
        
        # Plot throughput vs thread count for each memory size with error bars
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:  # Need at least 2 points for a line
                plt.errorbar(
                    mem_data["Num Threads (Run Merge)"], 
                    mem_data["Throughput (GB/s)_mean"], 
                    yerr=mem_data["Throughput (GB/s)_std"],
                    fmt='o-', linewidth=2, markersize=8,
                    capsize=5, elinewidth=1.5, capthick=1.5,
                    label=f"Memory: {mem_size}"
                )
                
                # Add annotations with throughput values and sample count
                for _, row in mem_data.iterrows():
                    annotation_text = f"{row['Throughput (GB/s)_mean']:.4f} ± {row['Throughput (GB/s)_std']:.4f} GB/s\n(n={int(row['Throughput (GB/s)_count'])})"
                    plt.annotate(
                        annotation_text, 
                        (row["Num Threads (Run Merge)"], row["Throughput (GB/s)_mean"]),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha='center',
                        fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
                    )
        
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

        # Also plot per-thread throughput with error bars
        plt.figure(figsize=(12, 8))
        
        for i, mem_size in enumerate(mem_sizes):
            mem_data = group_df[group_df["Working mem size"] == mem_size]
            mem_data = mem_data.sort_values("Num Threads (Run Merge)")
            
            if len(mem_data) >= 2:
                plt.errorbar(
                    mem_data["Num Threads (Run Merge)"], 
                    mem_data["Throughput per Thread (GB/s)_mean"], 
                    yerr=mem_data["Throughput per Thread (GB/s)_std"],
                    fmt='o-', linewidth=2, markersize=8,
                    capsize=5, elinewidth=1.5, capthick=1.5,
                    label=f"Memory: {mem_size}"
                )
                
                # Add annotations with throughput values and sample count
                for _, row in mem_data.iterrows():
                    annotation_text = f"{row['Throughput per Thread (GB/s)_mean']:.4f} ± {row['Throughput per Thread (GB/s)_std']:.4f} GB/s\n(n={int(row['Throughput per Thread (GB/s)_count'])})"
                    plt.annotate(
                        annotation_text, 
                        (row["Num Threads (Run Merge)"], row["Throughput per Thread (GB/s)_mean"]),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha='center',
                        fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
                    )
        
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
        raw_df = load_benchmark_data(
            results_dir, 
            data_source=args.data_source,
            query=args.query,
            scale_factor=args.scale_factor
        )
        
        # Compute aggregated metrics (means, std devs) for multiple iterations
        print("Computing aggregated metrics for multiple iterations...")
        df = compute_aggregated_metrics(raw_df)
        
        # Print summary information
        print(f"\nLoaded and processed {len(raw_df)} benchmark results")
        print(f"Data sources: {df['Data Source'].unique()}")
        print(f"Queries: {df['Query Num'].unique()}")
        print(f"Scale factors: {df['SF'].unique()}")
        print(f"Memory sizes: {sorted(df['Working mem size'].unique())}")
        print(f"Thread counts: {sorted(df['Num Threads (Run Merge)'].unique())}")
        
        # Generate plots with error bars
        print("\nGenerating thread scaling plots by memory size with error bars...")
        plot_thread_scaling_by_memory_with_error_bars(df, output_dir)
        
        print("\nGenerating merge time vs. thread count plot with error bars...")
        plot_merge_time_thread_scaling_with_error_bars(df, output_dir)
        
        print("\nGenerating throughput analysis plots with error bars...")
        plot_throughput_analysis_with_error_bars(df, output_dir)
        
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