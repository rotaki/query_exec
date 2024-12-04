# import pandas as pd
import sys


import pandas as pd
# print(pd.__version__)

# Replace 'input.parquet' with the path to your Parquet file
parquet_file = 'data/yellow_tripdata_2024-01.parquet'

# Replace 'output.csv' with the desired output CSV file path
csv_file = 'data/yellow_tripdata_2024-01.csv'

# Read the Parquet file into a pandas DataFrame
df = pd.read_parquet(parquet_file)

# Write the DataFrame to a CSV file
df.to_csv(csv_file, index=False)

print(f"Parquet file '{parquet_file}' has been converted to CSV file '{csv_file}'.")