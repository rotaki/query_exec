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
gensort_opts=""
input_file="benchmark_input"
sorted_file="benchmark_sorted"

# Parse command line options
while getopts "n:asb:t:h" opt; do
    case $opt in
        n) num_records="$OPTARG" ;;
        a) gensort_opts="$gensort_opts -a" ;;
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
rm -f "$input_file" "$sorted_file"

echo "Generating $num_records records..."
./gensort $gensort_opts $num_records "$input_file"

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate input data"
    exit 1
fi

echo "Starting sort benchmark..."
echo "Input file size: $(du -h "$input_file" | cut -f1)"

# Time the sort operation
TIMEFORMAT="Sort completed in %3R seconds"
time (sort "$input_file" > "$sorted_file")

echo "Sorted file size: $(du -h "$sorted_file" | cut -f1)"

# Optional: Verify the sort was successful
if command -v valsort &> /dev/null; then
    echo "Verifying sort..."
    if valsort "$sorted_file"; then
        echo "Sort verification successful"
    else
        echo "Sort verification failed"
    fi
else
    echo "Note: valsort not found, skipping verification"
fi

# Clean up
echo -n "Do you want to delete the generated files? [y/N] "
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    rm -f "$input_file" "$sorted_file"
    echo "Files cleaned up"
fi
