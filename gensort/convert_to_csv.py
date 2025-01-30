import sys
import csv

def convert_to_csv(input_file, output_file):
    """
    Convert gensort output to CSV format and include a row number.
    Each gensort record is 100 bytes:
    - First 10 bytes are the key
    - Next 90 bytes are the payload
    """
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        writer = csv.writer(f_out)

        # Write header with row_number, key, payload
        writer.writerow(['row_number', 'key', 'payload'])

        row_number = 1
        while True:
            # Read exactly 100 bytes for each record xtx i think this is wrong
            record = f_in.read(100)
            if not record:  # End of file
                break

            # Check for incomplete record
            if len(record) != 100:
                print("Warning: Incomplete record found")
                break

            key = record[:10]
            payload = record[10:]

            # Write CSV row: [row_number, key, payload]
            writer.writerow([row_number, key, payload])
            row_number += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    output = "data/" + output_file  # Adjust as needed for your directory structure

    convert_to_csv(input_file, output)
    print(f"Conversion complete. CSV file saved as {output}")
