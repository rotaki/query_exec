SELECT
            100.00 * SUM(CASE
                             WHEN P_TYPE LIKE 'PROMO%'
                                 THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
                             ELSE 0
            END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    LINEITEM,
    PART
WHERE
        L_PARTKEY = P_PARTKEY
  AND L_SHIPDATE >= DATE '1995-09-01'
  AND L_SHIPDATE < DATE '1995-10-01';