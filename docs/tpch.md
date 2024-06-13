# TPC-H Queries

The TPC-H benchmark consists of a suite of business-oriented ad-hoc queries and concurrent data modifications.

## Table sizes

| Table       | Number of rows |
|-------------|----------------|
| Region      | 5              |
| Nation      | 25             |
| Supplier    | 10,000 * sf    |
| Customer    | 150,000 * sf   |
| Part        | 200,000 * sf   |
| Partsupp    | 300,000 * sf   |
| Orders      | 1,500,000 * sf |
| Lineitem    | 6,000,000 * sf |

Below is the table that illustrates which of the 22 queries reference each table in the TPC-H schema. This schema includes tables such as `customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, and `supplier`. The presence of "x" in a cell indicates that the respective query references the table.

## Query references

| Table       | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 | Q10 | Q11 | Q12 | Q13 | Q14 | Q15 | Q16 | Q17 | Q18 | Q19 | Q20 | Q21 | Q22 |
|-------------|----|----|----|----|----|----|----|----|----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| Region      |    | x  |    |    | x  |    |    | x  |    |     |     |     |     |     |     |     |     |     |     |     |     |     |
| Nation      |    | x  |    |    | x  |    | x  | x  | x  | x   | x   |     |     |     |     |     |     |     |     | x   | x   |     |
| Supplier    |    | x  |    |    | x  |    | x  | x  | x  |     | x   |     |     |     | x   | x   |     |     |     | x   | x   |     |
| Customer    |    |    | x  |    | x  |    | x  | x  |    | x   |     |     | x   |     |     |     |     | x   |     |     |     | x   |
| Part        |    | x  |    |    |    |    |    | x  | x  |     |     |     |     | x   |     | x   | x   |     | x   | x   |     |     |
| Partsupp    |    | x  |    |    |    |    |    |    | x  |     | x   |     |     |     |     | x   |     |     |     | x   |     |     |
| Orders      |    |    | x  | x  | x  |    | x  | x  | x  | x   |     | x   | x   |     |     |     |     | x   |     |     | x   | x   |
| Lineitem    | x  |    | x  | x  | x  | x  | x  | x  | x  | x   |     | x   |     | x   | x   |     | x   | x   | x   | x   | x   |     |

This table covers all 22 queries and indicates their relationships with each of the specified tables.
