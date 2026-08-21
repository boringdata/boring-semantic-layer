SELECT
  *
FROM (
  SELECT
    "t1"."region",
    COUNT(*) AS "order_count",
    SUM("t1"."amount") AS "revenue"
  FROM (
    SELECT
      *
    FROM "memory"."main"."orders" AS "t0"
  ) AS "t1"
  GROUP BY
    1
) AS "t2"
