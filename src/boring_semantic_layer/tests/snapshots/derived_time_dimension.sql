SELECT
  *
FROM (
  SELECT
    "t1"."month",
    COUNT(*) AS "order_count"
  FROM (
    SELECT
      "t0"."order_id",
      "t0"."customer_id",
      "t0"."region",
      "t0"."amount",
      "t0"."ts",
      DATE_TRUNC('MONTH', "t0"."ts") AS "month"
    FROM "memory"."main"."orders" AS "t0"
  ) AS "t1"
  GROUP BY
    1
) AS "t2"
