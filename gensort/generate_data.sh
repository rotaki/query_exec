#!/bin/bash

# Default values for flags
sf=100
name="uniform"
skewed=0

# Parse flags
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -sf|--scale-factor) sf="$2"; shift ;;  # Scale factor (number of records to generate)
        -s|--skewed) skewed=1 ;;              # Skewed distribution flag
        -n|--name) name="$2"; shift ;;     # Distribution name
        -h|--help)                            # Help flag
            echo "Usage: $0 [options]"
            echo
            echo "This script generates data using gensort (https://www.ordinal.com/gensort.html),"
            echo "converts it to CSV, and prepares it for use in database operations."
            echo
            echo "Options:"
            echo "  -sf, --scale-factor [value]   Specify the scale factor (number of records to generate)."
            echo "                                Default: 100 (generates 100,000 records)."
            echo
            echo "  -s, --skewed                 Enable skewed distribution. Default: uniform distribution."
            echo
            echo "  -n, --name [value]        Specify the distribution name for the output file."
            echo "                                Default: 'uniform'."
            echo
            echo "  -h, --help                   Show this help message."
            echo
            echo "Example:"
            echo "  $0 -sf 200 -s -name skewed"
            echo
            echo "To add this data to the root of the buffer pool, use:"
            echo "  cargo run --release --bin create_csv_db"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Determine the skew flag
skew_flag=""
if [[ "$skewed" -eq 1 ]]; then
    skew_flag="-s"
    name="skewed"
fi

# Format the file name
output_name="sf-${sf}-$name"

# Run the gensort command
echo "Running gensort with the following parameters:"
echo "Scale factor (records): $sf"
echo "Output file name: $output_name"
echo "Distribution: $name (Skewed: $skewed)"
./gensort -a $skew_flag $((sf * 1000)) $output_name

# Run the Python conversion script
csv_name="${output_name}.csv"
echo "Converting to CSV: $csv_name"
python3 convert_to_csv.py $output_name $csv_name
rm $output_name

# Display completion message
echo "All tasks completed! The CSV file is ready: $csv_name"
echo "To add the data to the root of the buffer pool, run:"
echo "  cargo run --release --bin create_csv_db -h"
