WITH "t13" AS (
  SELECT
    "t12"."__bsl_jk_order_id",
    "t12"."customer_id",
    "t12"."region",
    "t12"."amount",
    "t12"."ts",
    "t12"."customer_id_right",
    "t12"."country",
    "t12"."segment",
    "t3"."item_id",
    "t3"."order_id",
    "t3"."qty"
  FROM (
    SELECT
      "t10"."order_id" AS "__bsl_jk_order_id",
      "t10"."__bsl_jk_customer_id" AS "customer_id",
      "t10"."region",
      "t10"."amount",
      "t10"."ts",
      "t10"."customer_id" AS "customer_id_right",
      "t10"."country",
      "t10"."segment"
    FROM (
      SELECT
        "t9"."order_id",
        "t9"."__bsl_jk_customer_id",
        "t9"."region",
        "t9"."amount",
        "t9"."ts",
        "t5"."customer_id",
        "t5"."country",
        "t5"."segment"
      FROM (
        SELECT
          "t0"."order_id",
          "t0"."customer_id" AS "__bsl_jk_customer_id",
          "t0"."region",
          "t0"."amount",
          "t0"."ts"
        FROM "memory"."main"."orders" AS "t0"
      ) AS "t9"
      LEFT OUTER JOIN "memory"."main"."customers" AS "t5"
        ON "t9"."__bsl_jk_customer_id" = "t5"."customer_id"
    ) AS "t10"
  ) AS "t12"
  LEFT OUTER JOIN "memory"."main"."items" AS "t3"
    ON "t12"."__bsl_jk_order_id" = "t3"."order_id"
)
SELECT
  "t31"."customers.country",
  COALESCE("t31"."orders.order_count", 0) AS "orders.order_count",
  "t31"."items.total_qty"
FROM (
  SELECT
    "t26"."customers.country",
    "t26"."orders.order_count",
    "t30"."items.total_qty"
  FROM (
    SELECT
      "t22"."customers.country",
      SUM("t22"."orders.order_count") AS "orders.order_count"
    FROM (
      SELECT
        "t18"."customers.country",
        "t18"."customer_id",
        "t18"."order_id",
        "t8"."orders.order_count"
      FROM (
        SELECT DISTINCT
          "t14"."country" AS "customers.country",
          "t14"."customer_id",
          "t14"."__bsl_jk_order_id" AS "order_id"
        FROM "t13" AS "t14"
      ) AS "t18"
      LEFT OUTER JOIN (
        SELECT
          "t0"."customer_id",
          "t0"."order_id",
          COUNT(*) AS "orders.order_count"
        FROM "memory"."main"."orders" AS "t0"
        GROUP BY
          1,
          2
      ) AS "t8"
        ON (
          (
            "t18"."customer_id" = "t8"."customer_id"
          )
          OR (
            (
              "t18"."customer_id" IS NULL
            ) AND (
              "t8"."customer_id" IS NULL
            )
          )
        )
        AND (
          (
            "t18"."order_id" = "t8"."order_id"
          )
          OR (
            (
              "t18"."order_id" IS NULL
            ) AND (
              "t8"."order_id" IS NULL
            )
          )
        )
    ) AS "t22"
    GROUP BY
      1
  ) AS "t26"
  LEFT OUTER JOIN (
    SELECT
      "t28"."customers.country",
      SUM("t28"."items.total_qty") AS "items.total_qty"
    FROM (
      SELECT
        "t19"."customers.country",
        "t19"."order_id_right2",
        "t27"."items.total_qty"
      FROM (
        SELECT DISTINCT
          "t14"."country" AS "customers.country",
          "t14"."order_id" AS "order_id_right2"
        FROM "t13" AS "t14"
      ) AS "t19"
      LEFT OUTER JOIN (
        SELECT
          "t23"."order_id" AS "order_id_right2",
          "t23"."items.total_qty"
        FROM (
          SELECT
            "t21"."order_id",
            SUM("t21"."qty") AS "items.total_qty"
          FROM (
            SELECT
              "t4"."item_id",
              "t4"."order_id",
              "t4"."qty"
            FROM "memory"."main"."items" AS "t4"
            INNER JOIN (
              SELECT DISTINCT
                "t14"."order_id"
              FROM "t13" AS "t14"
            ) AS "t20"
              ON "t4"."order_id" = "t20"."order_id"
          ) AS "t21"
          GROUP BY
            1
        ) AS "t23"
      ) AS "t27"
        ON (
          "t19"."order_id_right2" = "t27"."order_id_right2"
        )
        OR (
          (
            "t19"."order_id_right2" IS NULL
          ) AND (
            "t27"."order_id_right2" IS NULL
          )
        )
    ) AS "t28"
    GROUP BY
      1
  ) AS "t30"
    ON (
      "t26"."customers.country" = "t30"."customers.country"
    )
    OR (
      (
        "t26"."customers.country" IS NULL
      )
      AND (
        "t30"."customers.country" IS NULL
      )
    )
) AS "t31"
