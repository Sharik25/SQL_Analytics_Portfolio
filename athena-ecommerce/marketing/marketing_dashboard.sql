/*
  marketing_dashboard.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon Advertising — Marketing Performance Dashboard
  Markets : United States + Canada (UNION ALL)

  WHAT IT DOES
  ─────────────
  Joins advertising campaign data (spend, attributed sales, units) with the
  current-month P&L snapshot to compute per-ASIN marketing efficiency metrics:
    • ROAS   = attributed_sales / ad_spend
    • ACOS   = ad_spend / attributed_sales
    • Profit contribution of ads within the month's gross margin
    • Share of units / revenue driven by advertising

  KEY TECHNIQUES
  ───────────────
  1. ROW_NUMBER() OVER (PARTITION BY asin) — deduplicates product_group label
     per ASIN, keeping the group tied to the most recent inventory snapshot.
     Amazon's inventory planning table can have multiple rows per ASIN when
     the product_group changes over time.

  2. is_infinite() guard — prevents division-by-zero on ASINs with zero
     attributed sales (new campaigns, zero-sales days), which would otherwise
     produce Infinity or NaN in ROAS / ACOS columns.

  3. Split-part brand join — `split_part(sku, '-', 1)` extracts the brand_id
     prefix from the SKU string and joins to the brands dimension table for
     team attribution.
*/

SELECT
    mkt.*,
    brands.brand_id,
    brands.team
FROM (

    -- ── US ────────────────────────────────────────────────────────────────
    SELECT
        ad_id,
        advertised_sku,
        start_date,
        end_date,
        sum_spend,
        sum_total_sales_ads,
        sum_total_orders_ads,
        sum_total_units_ads,
        profit_ads,
        perc_cnt_ads_sales,
        perc_sum_ads_sales,
        perc_sum_profit,
        month_gross_margin,
        principal,
        quantity_purchased,
        avg_price,
        avg_cost_per_unit,
        avg_fee,
        product_group,
        'US' AS marketplace
    FROM (
        SELECT
            ads_data.*,
            pg.product_group,
            'US' AS country
        FROM (
            SELECT
                ads.adid                                                               AS ad_id,
                ads.advertisedasin                                                     AS advertised_asin,
                ads.advertisedsku                                                      AS advertised_sku,
                ads.startdate                                                          AS start_date,
                ads.enddate                                                            AS end_date,
                SUM(ads.spend)                                                         AS sum_spend,
                SUM(ads.sales7d)                                                       AS sum_total_sales_ads,
                SUM(ads.purchases7d)                                                   AS sum_total_orders_ads,
                SUM(ads.unitssoldclicks7d)                                             AS sum_total_units_ads,
                -- Ad profit: attributed_sales - spend - fees - COGS (on ad-driven units)
                SUM(ads.sales7d)
                    - SUM(ads.spend)
                    - ((pl.avg_fee * -1)       * SUM(ads.unitssoldclicks7d))
                    - (pl.avg_cost_per_unit    * SUM(ads.unitssoldclicks7d))           AS profit_ads,
                -- Share of total monthly units driven by ads
                SUM(ads.unitssoldclicks7d) / NULLIF(pl.quantity_purchased, 0)         AS perc_cnt_ads_sales,
                -- Share of total monthly revenue driven by ads
                SUM(ads.sales7d) / NULLIF(pl.principal, 0)                            AS perc_sum_ads_sales,
                -- Ad profit as % of total monthly gross margin
                (SUM(ads.sales7d)
                    - SUM(ads.spend)
                    - ((pl.avg_fee * -1) * SUM(ads.unitssoldclicks7d))
                    - (pl.avg_cost_per_unit * SUM(ads.unitssoldclicks7d))
                ) / NULLIF(pl.month_gross_margin, 0)                                  AS perc_sum_profit,
                pl.month_gross_margin,
                pl.principal,
                pl.quantity_purchased,
                pl.avg_price,
                pl.avg_cost_per_unit,
                pl.avg_fee
            FROM ecommerce_reports.advertised_products_us AS ads
            LEFT JOIN (
                -- Current-month P&L per ASIN from the results table
                SELECT asin, sku_1,
                       month_gross_margin, principal, quantity_purchased,
                       avg_price, avg_cost_per_unit, avg_fee
                FROM ecommerce_reports.results_us
                WHERE year_pdt = YEAR(NOW())
                  AND "month"  = MONTH(NOW())
            ) AS pl
                ON ads.advertisedasin = pl.asin
            GROUP BY
                ads.adid, ads.advertisedasin, ads.advertisedsku,
                ads.startdate, ads.enddate,
                pl.month_gross_margin, pl.principal, pl.quantity_purchased,
                pl.avg_price, pl.avg_cost_per_unit, pl.avg_fee
        ) AS ads_data
        LEFT JOIN (
            -- Deduplicate product_group per ASIN (keep the most-seen group by impressions)
            SELECT asin, product_group
            FROM (
                SELECT
                    asin,
                    product_group,
                    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY max_snapshot_date DESC) AS row_num
                FROM (
                    SELECT DISTINCT
                        inv.asin,
                        snap.max_snapshot_date,
                        snap.product_group
                    FROM ecommerce_reports.fba_inventory_planning_us AS inv
                    LEFT JOIN (
                        SELECT DISTINCT
                            asin,
                            MAX(snapshot_date) AS max_snapshot_date,
                            product_group
                        FROM ecommerce_reports.fba_inventory_planning_us
                        GROUP BY product_group, asin
                    ) AS snap ON inv.asin = snap.asin
                )
            )
            WHERE row_num = 1
        ) AS pg ON ads_data.advertised_asin = pg.asin
    )

    UNION ALL

    -- ── CANADA ────────────────────────────────────────────────────────────
    SELECT
        ad_id,
        advertised_sku,
        start_date,
        end_date,
        sum_spend,
        sum_total_sales_ads,
        sum_total_orders_ads,
        sum_total_units_ads,
        profit_ads,
        perc_cnt_ads_sales,
        perc_sum_ads_sales,
        perc_sum_profit,
        month_gross_margin,
        principal,
        quantity_purchased,
        avg_price,
        avg_cost_per_unit,
        avg_fee,
        product_group,
        'CA' AS marketplace
    FROM (
        SELECT
            ads.adid                                                               AS ad_id,
            ads.advertisedasin                                                     AS advertised_asin,
            ads.advertisedsku                                                      AS advertised_sku,
            ads.startdate                                                          AS start_date,
            ads.enddate                                                            AS end_date,
            SUM(ads.spend)                                                         AS sum_spend,
            SUM(ads.sales7d)                                                       AS sum_total_sales_ads,
            SUM(ads.purchases7d)                                                   AS sum_total_orders_ads,
            SUM(ads.unitssoldclicks7d)                                             AS sum_total_units_ads,
            SUM(ads.sales7d)
                - SUM(ads.spend)
                - ((pl.avg_fee * -1) * SUM(ads.unitssoldclicks7d))
                - (pl.avg_cost_per_unit * SUM(ads.unitssoldclicks7d))             AS profit_ads,
            SUM(ads.unitssoldclicks7d) / NULLIF(pl.quantity_purchased, 0)         AS perc_cnt_ads_sales,
            SUM(ads.sales7d) / NULLIF(pl.principal, 0)                            AS perc_sum_ads_sales,
            (SUM(ads.sales7d) - SUM(ads.spend)
                - ((pl.avg_fee * -1) * SUM(ads.unitssoldclicks7d))
                - (pl.avg_cost_per_unit * SUM(ads.unitssoldclicks7d))
            ) / NULLIF(pl.month_gross_margin, 0)                                  AS perc_sum_profit,
            pl.month_gross_margin, pl.principal, pl.quantity_purchased,
            pl.avg_price, pl.avg_cost_per_unit, pl.avg_fee,
            NULL AS product_group
        FROM ecommerce_reports.advertised_products_ca AS ads
        LEFT JOIN (
            SELECT asin, month_gross_margin, principal, quantity_purchased,
                   avg_price, avg_cost_per_unit, avg_fee
            FROM ecommerce_reports.results_ca
            WHERE year_pdt = YEAR(NOW())
              AND "month"  = MONTH(NOW())
        ) AS pl ON ads.advertisedasin = pl.asin
        GROUP BY
            ads.adid, ads.advertisedasin, ads.advertisedsku,
            ads.startdate, ads.enddate,
            pl.month_gross_margin, pl.principal, pl.quantity_purchased,
            pl.avg_price, pl.avg_cost_per_unit, pl.avg_fee
    )

) AS mkt
LEFT JOIN ecommerce_db.brands_table AS brands
    ON SPLIT_PART(mkt.advertised_sku, '-', 1) = brands.brand_id
ORDER BY marketplace, sum_spend DESC;
