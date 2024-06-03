SELECT
    C_COUNT,
    COUNT(*) AS CUSTDIST
FROM
    (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY)
        FROM
            CUSTOMER LEFT OUTER JOIN ORDERS ON
                        C_CUSTKEY = O_CUSTKEY
                    AND O_COMMENT NOT LIKE '%special%requests%'
        GROUP BY
            C_CUSTKEY
    ) AS C_ORDERS (C_CUSTKEY, C_COUNT)
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC;