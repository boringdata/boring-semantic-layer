WITH "t8" AS (
  SELECT
    "t6"."__bsl_jk_order_id",
    "t6"."customer_id",
    "t6"."region",
    "t6"."amount",
    "t6"."ts",
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
  ) AS "t6"
  LEFT OUTER JOIN "memory"."main"."items" AS "t2"
    ON "t6"."__bsl_jk_order_id" = "t2"."order_id"
)
SELECT
  *
FROM (
  SELECT
    "t21"."orders.region",
    "t21"."orders.revenue",
    "t25"."items.total_qty"
  FROM (
    SELECT
      "t17"."orders.region",
      SUM("t17"."orders.revenue") AS "orders.revenue"
    FROM (
      SELECT
        "t13"."orders.region",
        "t13"."region",
        "t7"."orders.revenue"
      FROM (
        SELECT DISTINCT
          "t9"."region" AS "orders.region",
          "t9"."region"
        FROM "t8" AS "t9"
      ) AS "t13"
      LEFT OUTER JOIN (
        SELECT
          "t0"."region",
          SUM("t0"."amount") AS "orders.revenue"
        FROM "memory"."main"."orders" AS "t0"
        GROUP BY
          1
      ) AS "t7"
        ON (
          "t13"."region" = "t7"."region"
        )
        OR (
          (
            "t13"."region" IS NULL
          ) AND (
            "t7"."region" IS NULL
          )
        )
    ) AS "t17"
    GROUP BY
      1
  ) AS "t21"
  LEFT OUTER JOIN (
    SELECT
      "t23"."orders.region",
      SUM("t23"."items.total_qty") AS "items.total_qty"
    FROM (
      SELECT
        "t14"."orders.region",
        "t14"."order_id_right",
        "t22"."items.total_qty"
      FROM (
        SELECT DISTINCT
          "t9"."region" AS "orders.region",
          "t9"."order_id" AS "order_id_right"
        FROM "t8" AS "t9"
      ) AS "t14"
      LEFT OUTER JOIN (
        SELECT
          "t18"."order_id" AS "order_id_right",
          "t18"."items.total_qty"
        FROM (
          SELECT
            "t16"."order_id",
            SUM("t16"."qty") AS "items.total_qty"
          FROM (
            SELECT
              "t3"."item_id",
              "t3"."order_id",
              "t3"."qty"
            FROM "memory"."main"."items" AS "t3"
            INNER JOIN (
              SELECT DISTINCT
                "t9"."order_id"
              FROM "t8" AS "t9"
            ) AS "t15"
              ON "t3"."order_id" = "t15"."order_id"
          ) AS "t16"
          GROUP BY
            1
        ) AS "t18"
      ) AS "t22"
        ON (
          "t14"."order_id_right" = "t22"."order_id_right"
        )
        OR (
          (
            "t14"."order_id_right" IS NULL
          ) AND (
            "t22"."order_id_right" IS NULL
          )
        )
    ) AS "t23"
    GROUP BY
      1
  ) AS "t25"
    ON (
      "t21"."orders.region" = "t25"."orders.region"
    )
    OR (
      (
        "t21"."orders.region" IS NULL
      ) AND (
        "t25"."orders.region" IS NULL
      )
    )
) AS "t26"
