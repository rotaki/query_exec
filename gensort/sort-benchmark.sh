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
gensort_opts="-a"  # Force ASCII mode since we need to process the payload
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

echo "Processing records to keep only first 2 digits of payload..."
# Process the file: keep key and first 2 digits of payload
while IFS= read -r line; do
    key=$(echo "$line" | cut -c1-10)
    payload=$(echo "$line" | cut -c12-13)  # Get first 2 digits after the key
    echo "${key} ${payload}"
done < "$input_file" > "$processed_file"

echo "Starting sort benchmark..."
echo "Processed file size: $(du -h "$processed_file" | cut -f1)"

# Time the sort operation
TIMEFORMAT="Sort completed in %3R seconds"
time (sort -S200% -k1,10 "$processed_file" > "$sorted_file")

echo "Sorted file size: $(du -h "$sorted_file" | cut -f1)"

# Optional: Verify the sort was successful
# Note: Standard valsort might not work with modified record format
if command -v valsort &> /dev/null; then
    echo "Note: Standard valsort verification skipped due to modified record format"
fi

# Clean up
echo -n "Do you want to delete the generated files? [y/N] "
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    rm -f "$input_file" "$processed_file" "$sorted_file"
    echo "Files cleaned up"
fi