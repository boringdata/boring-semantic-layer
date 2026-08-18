SELECT
  *
FROM (
  SELECT
    "t1"."region",
    SUM("t1"."amount") AS "revenue"
  FROM (
    SELECT
      "t0"."order_id",
      "t0"."customer_id",
      "t0"."region",
      "t0"."amount",
      "t0"."ts",
      DATE_TRUNC('MONTH', "t0"."ts") AS "month"
    FROM "memory"."main"."orders" AS "t0"
    WHERE
      "t0"."region" IN ('N', 'S')
  ) AS "t1"
  GROUP BY
    1
) AS "t2"
ORDER BY
  "t2"."revenue" DESC
LIMIT 3
