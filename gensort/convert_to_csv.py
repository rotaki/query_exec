import sys
import csv

def convert_to_csv(input_file, output_file):
    """
    Convert gensort binary output to CSV format and include a row number.
    Each gensort record is 100 bytes:
    - First 10 bytes are the key
    - Next 90 bytes are the payload
    """
    with open(input_file, 'rb') as f_in, open(output_file, 'w', newline='') as f_out:
        writer = csv.writer(f_out)

        # Write header with row_number, key, payload
        writer.writerow(['row_number', 'key', 'payload'])

        row_number = 1
        while True:
            # Read exactly 100 bytes for each record
            record = f_in.read(100)
            if not record:  # End of file
                break

            # Check for incomplete record
            if len(record) != 100:
                print(f"Warning: Incomplete record found at row {row_number}")
                break

            key = record[:10].hex()  # Convert to hex to preserve binary integrity
            payload = record[10:].hex()  # Store payload as hex string

            # Write CSV row: [row_number, key, payload]
            writer.writerow([row_number, key, payload])
            row_number += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    convert_to_csv(input_file, output_file)
    print(f"Conversion complete. CSV file saved as {output_file}")