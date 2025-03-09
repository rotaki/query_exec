#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 [-n NUM_RECORDS] [-a] [-s] [-b START_NUM] [-t THREADS]"
    echo "Options:"
    echo "  -n NUM_RECORDS  Number of records to generate (default: 1000000)"
    echo "  -a             Generate ASCII records instead of binary"
    echo "  -s             Generate skewed keys"
    echo "  -b START_NUM   Starting record number"
    echo "  -t THREADS     Number of threads for generation"
    exit 1
}

# Default values
num_records=1000000
gensort_opts="-a"  # Force ASCII mode
input_file="benchmark_input"
processed_file="benchmark_processed"
sorted_file="benchmark_sorted"

# Parse command line options
while getopts "n:asb:t:h" opt; do
    case $opt in
        n) num_records="$OPTARG" ;;
        s) gensort_opts="$gensort_opts -s" ;;
        b) gensort_opts="$gensort_opts -b$OPTARG" ;;
        t) gensort_opts="$gensort_opts -t$OPTARG" ;;
        h) usage ;;
        ?) usage ;;
    esac
done

# Check if gensort is available
if ! command -v ./gensort &> /dev/null; then
    echo "Error: gensort command not found"
    exit 1
fi

# Clean up old files if they exist
rm -f "$input_file" "$processed_file" "$sorted_file"

echo "Generating $num_records records..."
./gensort $gensort_opts $num_records "$input_file"

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate input data"
    exit 1
fi

echo "Processing records..."
awk '{print substr($1,1,10), substr($2,1,2)}' "$input_file" > "$processed_file"

echo "Starting sort benchmark..."
echo "Input file size: $(du -h "$processed_file" | cut -f1)"

# Time only the sort operation
TIMEFORMAT="Sort completed in %3R seconds"
time (sort -k1,10 "$processed_file" > "$sorted_file")

echo "Sorted file size: $(du -h "$sorted_file" | cut -f1)"

# Clean up
echo -n "Do you want to delete the generated files? [y/N] "
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    rm -f "$input_file" "$processed_file" "$sorted_file"
    echo "Files cleaned up"
fi