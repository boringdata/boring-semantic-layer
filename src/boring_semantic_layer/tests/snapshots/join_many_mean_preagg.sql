SELECT
  "t11"."orders.region",
  "t11"."_sum__orders.avg_amount" / "t11"."_count__orders.avg_amount" AS "orders.avg_amount"
FROM (
  SELECT
    "t10"."orders.region",
    SUM("t10"."_sum__orders.avg_amount") AS "_sum__orders.avg_amount",
    SUM("t10"."_count__orders.avg_amount") AS "_count__orders.avg_amount"
  FROM (
    SELECT
      "t9"."orders.region",
      "t9"."region",
      "t6"."_sum__orders.avg_amount",
      "t6"."_count__orders.avg_amount"
    FROM (
      SELECT DISTINCT
        "t7"."region" AS "orders.region",
        "t7"."region"
      FROM (
        SELECT
          "t5"."__bsl_jk_order_id",
          "t5"."customer_id",
          "t5"."region",
          "t5"."amount",
          "t5"."ts",
          "t2"."item_id",
          "t2"."order_id",
          "t2"."qty"
        FROM (
          SELECT
            "t0"."order_id" AS "__bsl_jk_order_id",
            "t0"."customer_id",
            "t0"."region",
            "t0"."amount",
            "t0"."ts"
          FROM "memory"."main"."orders" AS "t0"
        ) AS "t5"
        LEFT OUTER JOIN "memory"."main"."items" AS "t2"
          ON "t5"."__bsl_jk_order_id" = "t2"."order_id"
      ) AS "t7"
    ) AS "t9"
    LEFT OUTER JOIN (
      SELECT
        "t0"."region",
        SUM("t0"."amount") AS "_sum__orders.avg_amount",
        COUNT("t0"."amount") AS "_count__orders.avg_amount"
      FROM "memory"."main"."orders" AS "t0"
      GROUP BY
        1
    ) AS "t6"
      ON (
        "t9"."region" = "t6"."region"
      )
      OR (
        (
          "t9"."region" IS NULL
        ) AND (
          "t6"."region" IS NULL
        )
      )
  ) AS "t10"
  GROUP BY
    1
) AS "t11"
