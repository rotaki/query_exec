SELECT
    S_NAME,
    S_ADDRESS
FROM
    SUPPLIER,
    NATION
WHERE
        S_SUPPKEY IN (
        SELECT
            PS_SUPPKEY
        FROM
            PARTSUPP
        WHERE
                PS_PARTKEY IN (
                SELECT
                    P_PARTKEY
                FROM
                    PART
                WHERE
                        P_NAME LIKE 'forest%'
            )
          AND PS_AVAILQTY > (
            SELECT
                    0.5 * SUM(L_QUANTITY)
            FROM
                LINEITEM
            WHERE
                    L_PARTKEY = PS_PARTKEY
              AND L_SUPPKEY = PS_SUPPKEY
              AND L_SHIPDATE >= DATE '1994-01-01'
              AND L_SHIPDATE < DATE '1994-01-01' + INTERVAL '1' YEAR
        )
    )
  AND S_NATIONKEY = N_NATIONKEY
  AND N_NAME = 'CANADA'
ORDER BY
    S_NAME;