/*
  avg_cost_gap_fill.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Cost of Goods (COGS) Time Series

  PROBLEM
  ────────
  Supplier orders arrive irregularly. When no order was placed in a given month,
  avg_cost_per_item is NULL, breaking margin calculations that rely on a
  continuous monthly cost series.

  SOLUTION — LEAD / LAG gap-fill via CROSS JOIN
  ───────────────────────────────────────────────
  Step 1  Use LEAD() / LAG() partitioned by (sku, year) to surface the nearest
          non-zero cost values on either side of every NULL gap.

  Step 2  A CROSS JOIN self-join on the integer primary key (pk) produces all
          (null_row, neighbour_row) pairs inside the same year-partition,
          letting us pick the chronologically closest non-zero cost to fill
          each gap — even for multi-month gaps.

  This avoids UDFs and works natively in Presto / Athena without any
  procedural code.
*/

SELECT
    base.*,
    fill.pk                            AS pk_cross_join,
    fill.avg_cost_per_item_month       AS avg_cost_per_item_month_cross_join
FROM (
    -- ── Step 1: annotate each row with the nearest non-zero neighbours ──────
    SELECT
        inner_tbl.*,
        LEAD(COALESCE(avg_cost_per_item_month, 0))
            OVER (PARTITION BY sku, year_,
                  COALESCE(avg_cost_per_item_month, 0) != 0)   AS next_not_zero_value,
        LEAD(month_)
            OVER (PARTITION BY sku, year_,
                  COALESCE(avg_cost_per_item_month, 0) != 0)   AS next_not_zero_month,
        LAG(COALESCE(avg_cost_per_item_month, 0))
            OVER (PARTITION BY sku, year_,
                  COALESCE(avg_cost_per_item_month, 0) != 0)   AS prev_not_zero_value,
        LAG(month_)
            OVER (PARTITION BY sku, year_,
                  COALESCE(avg_cost_per_item_month, 0) != 0)   AS previous_not_zero_month,
        ROW_NUMBER()
            OVER (PARTITION BY sku ORDER BY year_, month_)     AS pk
    FROM (
        -- Base CTE: one row per (sku, year, month) — NULLs where no order exists
        SELECT
            sku,
            year_,
            month_,
            avg_cost_per_item_month
        FROM ecommerce_reports.avg_cost_monthly_series
    ) AS inner_tbl
) AS base
-- ── Step 2: CROSS JOIN to find the closest non-zero cost for each NULL ──────
LEFT JOIN (
    SELECT
        pk                        AS pk_cross_join,
        avg_cost_per_item_month,
        sku,
        year_,
        month_
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku ORDER BY year_, month_) AS pk
        FROM ecommerce_reports.avg_cost_monthly_series
        WHERE COALESCE(avg_cost_per_item_month, 0) != 0   -- only non-zero anchor rows
    )
) AS fill
    ON  base.sku   = fill.sku
    AND base.year_ = fill.year_
    -- keep only the closest non-zero row (minimise absolute month distance)
    AND ABS(base.month_ - fill.month_) = (
        SELECT MIN(ABS(base2.month_ - fill2.month_))
        FROM ecommerce_reports.avg_cost_monthly_series AS base2
        CROSS JOIN (
            SELECT month_
            FROM ecommerce_reports.avg_cost_monthly_series
            WHERE sku = base.sku
              AND year_ = base.year_
              AND COALESCE(avg_cost_per_item_month, 0) != 0
        ) AS fill2
        WHERE base2.sku   = base.sku
          AND base2.year_ = base.year_
          AND base2.month_ = base.month_
    )
-- Only fill rows that are actually NULL
WHERE base.avg_cost_per_item_month IS NULL
   OR base.avg_cost_per_item_month = 0
ORDER BY base.sku, base.year_, base.month_;
