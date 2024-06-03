import duckdb
import argparse
import tempfile
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--sf", default=0.01, type=float, help="Scale factor")

sf = parser.parse_args().sf

path = Path("./sf-{}".format(sf))
if path.exists():
    print("TPCH data already exists")
    # Ask the user if they want to overwrite the data
    answer = input("Do you want to overwrite the data? (y/n)")
    if answer != "y":
        print("Aborting")
        exit(0)
    else:
        # remove the directory
        os.system("rm -r {}".format(path))

# create the directory
path.mkdir(parents=True, exist_ok=True)

# Input contains the base table data
input = path / 'input'
input.mkdir(parents=True, exist_ok=True)

# Output contains the query results
output = path / 'output'
output.mkdir(parents=True, exist_ok=True)


plan = path / 'plan'
plan.mkdir(parents=True, exist_ok=True)

print("Generating TPCH data with scale factor {}".format(sf))

# create a temporary database
con = duckdb.connect(database=tempfile.mktemp())
con.execute("CALL dbgen(sf={})".format(sf))

tpch_tables = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

for table in tpch_tables:
    print("Generating table {}".format(table))
    file = input / "{}.csv".format(table)
    con.execute("COPY (SELECT * FROM {}) TO '{}' (DELIMITER '|');".format(table, file))
    print("Table {} generated".format(table))

for query in range(1, 23):
    if query == 15:
        print("Skipping query 15")
        continue
    print("Generating result of query {}".format(query))
    file = output / "query-{}.csv".format(query)
    query_string = open("../queries/q{}.sql".format(query)).read()
    # if there is a last semicolon or last semicolon followed by a newline, remove it
    if query_string[-1] == '\n':
        query_string = query_string[:-1]
    if query_string[-1] == ';':
        query_string = query_string[:-1]

    # create a file for a plan
    plan_file = plan / "query-{}.txt".format(query)
    explain = con.execute("EXPLAIN {}".format(query_string))
    result = explain.fetchall()
    with open(plan_file, 'a') as f:
        for row in result:
            f.write(row[1] + '\n')

    con.execute("COPY ({}) TO '{}' (DELIMITER '|');".format(query_string, file))
    print("Query {} generated".format(query))

con.close()