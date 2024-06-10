import duckdb
import argparse
import tempfile
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--query", default=1, type=int, help="Query number")
parser.add_argument("--sf", default=0.01, type=float, help="Scale factor")

q = parser.parse_args().query
sf = parser.parse_args().sf
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

def get_query_string(query):
    query_string = open("../queries/q{}.sql".format(query)).read()
    # if there is a last semicolon or last semicolon followed by a newline, remove it
    if query_string[-1] == '\n':
        query_string = query_string[:-1]
    if query_string[-1] == ';':
        query_string = query_string[:-1]
    return query_string

query_string = get_query_string(q)

explain = con.execute("EXPLAIN {}".format(query_string))
result = explain.fetchall()
for row in result:
    print(row[1])

result = con.execute(query_string)
result = result.fetchall()
for row in result:
    print(row)
print("Number of rows: {}".format(len(result)))

con.close()