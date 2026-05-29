/*
  new_products_profitability.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Product Lifecycle / P&L
  Markets : United States + Canada

  WHAT IT DOES
  ─────────────
  Identifies ASINs that had ZERO cumulative gross margin in the prior 6 months
  (still in launch/investment phase) but turned profitable in a specified
  target period. These are "newly profitable" products that just crossed
  break-even and warrant increased marketing or sourcing investment.

  KEY TECHNIQUE — SUM() OVER window aggregate for conditional row filtering
  ──────────────────────────────────────────────────────────────────────────
  SUM(month_gross_margin) OVER (PARTITION BY asin) computes the rolling
  total across ALL rows in the prior-6-month window WITHOUT collapsing the
  result set. This lets us apply a WHERE sum_over = 0 filter that operates on
  the window aggregate rather than a GROUP BY, preserving the row-level detail
  needed for the subsequent join to the target period.

  This avoids a correlated subquery or a two-pass GROUP BY approach.

  PARAMETERS (adjust for each reporting cycle)
  ─────────────────────────────────────────────
  Target period  : months to evaluate profitability (e.g. Q1 2024 = 1,2,3)
  Prior window   : prior 6 months used to identify "zero profit" ASINs
*/

-- ── UNITED STATES ──────────────────────────────────────────────────────────
SELECT
    team,
    asin,
    SUM(principal)                              AS sum_principal,
    SUM(amount_reim)                            AS sum_reimbursements,
    SUM(month_gross_margin)                     AS sum_gross_margin,
    SUM(month_gross_margin) - SUM(amount_reim)  AS net_margin_after_reim
FROM ecommerce_reports.results_us
WHERE
    year_pdt = 2024
    AND month IN (1, 2, 3)          -- ← target period: Q1 2024
    AND asin IN (
        -- ASINs with profit > 0 in target period AND zero profit in prior window
        SELECT DISTINCT new.asin
        FROM (
            SELECT asin
            FROM ecommerce_reports.results_us
            WHERE year_pdt = 2024
              AND month IN (1, 2, 3)
              AND month_gross_margin > 0
        ) AS new                    -- profitable in target period
        LEFT JOIN (
            -- Prior 6-month window: keep only ASINs whose cumulative margin = 0
            SELECT DISTINCT asin
            FROM (
                SELECT
                    asin,
                    year_pdt,
                    month,
                    month_gross_margin,
                    -- Window aggregate: no GROUP BY collapse, filter afterwards
                    SUM(month_gross_margin) OVER (PARTITION BY asin) AS sum_over
                FROM ecommerce_reports.results_us
                WHERE year_pdt = 2023
                  AND month IN (7, 8, 9, 10, 11, 12)  -- prior 6 months
            )
            WHERE sum_over = 0      -- no cumulative profit → was in launch phase
        ) AS zero_profit_window
            ON new.asin = zero_profit_window.asin
        WHERE COALESCE(zero_profit_window.asin, 'None') != 'None'
    )
GROUP BY team, asin
ORDER BY sum_gross_margin DESC

UNION ALL

-- ── CANADA ─────────────────────────────────────────────────────────────────
SELECT
    team,
    asin,
    SUM(principal)                              AS sum_principal,
    SUM(amount_reim)                            AS sum_reimbursements,
    SUM(month_gross_margin)                     AS sum_gross_margin,
    SUM(month_gross_margin) - SUM(amount_reim)  AS net_margin_after_reim
FROM ecommerce_reports.results_ca
WHERE
    year_pdt = 2024
    AND month IN (3)                -- ← target period: March 2024 (CA lags US by 1-2 months)
    AND asin IN (
        SELECT DISTINCT tbl_1.asin
        FROM (
            SELECT asin
            FROM ecommerce_reports.results_ca
            WHERE year_pdt = 2024
              AND month IN (3)
              AND month_gross_margin > 0
        ) AS tbl_1
        LEFT JOIN (
            SELECT DISTINCT asin
            FROM (
                SELECT
                    asin, year_pdt, month, month_gross_margin,
                    SUM(month_gross_margin) OVER (PARTITION BY asin) AS sum_over
                FROM (
                    -- CA prior window spans across year boundary: Dec'23 + Jan-Feb'24
                    SELECT asin, year_pdt, month, month_gross_margin
                    FROM ecommerce_reports.results_ca
                    WHERE year_pdt = 2024 AND month IN (1, 2)
                    UNION ALL
                    SELECT asin, year_pdt, month, month_gross_margin
                    FROM ecommerce_reports.results_ca
                    WHERE year_pdt = 2023 AND month IN (9, 10, 11, 12)
                )
            )
            WHERE sum_over = 0
        ) AS last_six_months
            ON tbl_1.asin = last_six_months.asin
        WHERE COALESCE(last_six_months.asin, 'None') != 'None'
    )
GROUP BY team, asin
ORDER BY sum_gross_margin DESC;
