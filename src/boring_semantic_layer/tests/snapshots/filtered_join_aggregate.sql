SELECT
  *
FROM (
  SELECT
    "t6"."customers.segment",
    SUM("t6"."amount") AS "orders.revenue"
  FROM (
    SELECT
      "t5"."order_id",
      "t5"."__bsl_jk_customer_id" AS "customer_id",
      "t5"."region",
      "t5"."amount",
      "t5"."ts",
      "t5"."customer_id" AS "customer_id_right",
      "t5"."country",
      "t5"."segment",
      "t5"."region" AS "orders.region",
      "t5"."__bsl_jk_customer_id" AS "orders.customer_id",
      DATE_TRUNC('MONTH', "t5"."ts") AS "orders.month",
      "t5"."customer_id" AS "customers.customer_id",
      "t5"."country" AS "customers.country",
      "t5"."segment" AS "customers.segment"
    FROM (
      SELECT
        "t4"."order_id",
        "t4"."__bsl_jk_customer_id",
        "t4"."region",
        "t4"."amount",
        "t4"."ts",
        "t2"."customer_id",
        "t2"."country",
        "t2"."segment"
      FROM (
        SELECT
          "t0"."order_id",
          "t0"."customer_id" AS "__bsl_jk_customer_id",
          "t0"."region",
          "t0"."amount",
          "t0"."ts"
        FROM "memory"."main"."orders" AS "t0"
      ) AS "t4"
      LEFT OUTER JOIN "memory"."main"."customers" AS "t2"
        ON "t4"."__bsl_jk_customer_id" = "t2"."customer_id"
    ) AS "t5"
    WHERE
      "t5"."country" = 'US'
  ) AS "t6"
  GROUP BY
    1
) AS "t7"
