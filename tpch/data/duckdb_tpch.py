import duckdb
import argparse
import tempfile
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--sf", default=0.01, type=float, help="Scale factor")

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

# query_string = get_query_string(1)
query_string = """
SELECT
    SUPP_NATION,
    CUST_NATION,
    L_YEAR,
    SUM(VOLUME) AS REVENUE
FROM
    (
        SELECT
            N1.N_NAME AS SUPP_NATION,
            N2.N_NAME AS CUST_NATION,
            EXTRACT(YEAR FROM L_SHIPDATE) AS L_YEAR,
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME
        FROM
            SUPPLIER,
            LINEITEM,
            ORDERS,
            CUSTOMER,
            NATION N1,
            NATION N2
        WHERE
                S_SUPPKEY = L_SUPPKEY
          AND O_ORDERKEY = L_ORDERKEY
          AND C_CUSTKEY = O_CUSTKEY
          AND S_NATIONKEY = N1.N_NATIONKEY
          AND C_NATIONKEY = N2.N_NATIONKEY
          AND (
                (N1.N_NAME = 'FRANCE' AND N2.N_NAME = 'GERMANY')
                OR (N1.N_NAME = 'GERMANY' AND N2.N_NAME = 'FRANCE')
            )
          AND L_SHIPDATE BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS SHIPPING
GROUP BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR
ORDER BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR;
"""

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