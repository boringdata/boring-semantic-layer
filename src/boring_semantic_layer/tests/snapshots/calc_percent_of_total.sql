SELECT
  "t3"."region",
  "t3"."revenue",
  "t3"."revenue" / "t3"."__bsl_totals__revenue" AS "revenue_share"
FROM (
  SELECT
    "t2"."region",
    SUM("t2"."amount") AS "revenue",
    ANY_VALUE("t2"."__bsl_totals__revenue") AS "__bsl_totals__revenue"
  FROM (
    SELECT
      "t1"."order_id",
      "t1"."customer_id",
      "t1"."region",
      "t1"."amount",
      "t1"."ts",
      SUM("t1"."amount") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "__bsl_totals__revenue"
    FROM (
      SELECT
        *
      FROM "memory"."main"."orders" AS "t0"
    ) AS "t1"
  ) AS "t2"
  GROUP BY
    1
) AS "t3"
