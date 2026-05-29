/*
  inventory_vs_projected_sales.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Restock / Sell-Through Planning
  Markets : United States (USD) + Canada (CAD)

  WHAT IT DOES
  ─────────────
  Compares current FBA on-hand + inbound inventory against projected monthly
  sales velocity to generate restock urgency signals per SKU and sourcing team.

  SALES PROJECTION METHOD
  ────────────────────────
  projected_sales = units_shipped_last_7_days × 4
  (4-week extrapolation from most recent 7-day window)

  COVERAGE
  ─────────
  US: orders_all + fba_manage_inventory (fulfillable + reserved + inbound)
  CA: orders_ca   + fba_manage_inventory_ca

  KEY METRIC
  ───────────
  difference_sum_inventory_and_projected_sales
    Negative → inventory insufficient to cover one month of projected demand → restock
    Positive → excess stock, potential storage fee risk
*/

-- ── UNITED STATES ──────────────────────────────────────────────────────────
SELECT
    (SELECT MAX(DATE(purchase_date))
     FROM ecommerce_db.orders_all)                                             AS max_purchased_date,
    DATE_ADD('day', -7,
     (SELECT MAX(DATE(purchase_date)) FROM ecommerce_db.orders_all))           AS window_start_date,
    (SELECT MAX(DATE(report_date))
     FROM ecommerce_reports.fba_manage_inventory)                              AS inventory_snapshot_date,
    inv.team,
    orders.sku,
    orders.shipped_quantity_sum,
    orders.projected_sales,
    inv.active_inventory,
    inv.inbound_quantity,
    inv.active_inventory + inv.inbound_quantity                                AS total_available,
    -- negative = likely stockout within the month
    (inv.active_inventory + inv.inbound_quantity) - orders.projected_sales     AS days_of_supply_signal
FROM (
    -- Last 7 days shipped units, ×4 as monthly proxy
    SELECT
        sku,
        SUM(quantity)      AS shipped_quantity_sum,
        SUM(quantity) * 4  AS projected_sales
    FROM ecommerce_db.orders_all
    WHERE order_status  = 'Shipped'
      AND DATE(purchase_date) BETWEEN
            DATE_ADD('day', -7, (SELECT MAX(DATE(purchase_date)) FROM ecommerce_db.orders_all))
            AND (SELECT MAX(DATE(purchase_date)) FROM ecommerce_db.orders_all)
    GROUP BY sku
) AS orders
LEFT JOIN (
    SELECT
        fba.sku,
        SPLIT_PART(fba.sku, '-', 1)                                             AS brand_id,
        brand.team,
        SUM("afn-fulfillable-quantity" + "afn-reserved-quantity")               AS active_inventory,
        SUM("afn-inbound-working-quantity"
          + "afn-inbound-shipped-quantity"
          + "afn-inbound-receiving-quantity")                                   AS inbound_quantity
    FROM ecommerce_reports.fba_manage_inventory AS fba
    LEFT JOIN ecommerce_db.brands_table AS brand
        ON SPLIT_PART(fba.sku, '-', 1) = brand.brand_id
    WHERE DATE(report_date) = (SELECT MAX(DATE(report_date))
                               FROM ecommerce_reports.fba_manage_inventory)
    GROUP BY fba.sku, SPLIT_PART(fba.sku, '-', 1), brand.team
) AS inv
    ON orders.sku = inv.sku

UNION ALL

-- ── CANADA ─────────────────────────────────────────────────────────────────
SELECT
    (SELECT MAX(DATE(purchase_date))
     FROM ecommerce_reports.orders_ca)                                         AS max_purchased_date,
    NULL                                                                       AS window_start_date,
    (SELECT MAX(DATE(download_date))
     FROM ecommerce_reports.fba_manage_inventory_ca)                           AS inventory_snapshot_date,
    inv.team,
    orders.sku,
    orders.shipped_quantity_sum,
    orders.projected_sales,
    inv.active_inventory,
    inv.inbound_quantity,
    inv.active_inventory + inv.inbound_quantity                                AS total_available,
    (inv.active_inventory + inv.inbound_quantity) - orders.projected_sales     AS days_of_supply_signal
FROM (
    SELECT sku,
           SUM(quantity)      AS shipped_quantity_sum,
           SUM(quantity) * 4  AS projected_sales
    FROM ecommerce_reports.orders_ca
    WHERE order_status = 'Shipped'
    GROUP BY sku
) AS orders
LEFT JOIN (
    SELECT fba.sku, brand.team,
           SUM("afn-fulfillable-quantity" + "afn-reserved-quantity")           AS active_inventory,
           SUM("afn-inbound-working-quantity"
             + "afn-inbound-shipped-quantity"
             + "afn-inbound-receiving-quantity")                               AS inbound_quantity
    FROM ecommerce_reports.fba_manage_inventory_ca AS fba
    LEFT JOIN ecommerce_db.brands_table AS brand
        ON SPLIT_PART(fba.sku, '-', 1) = brand.brand_id
    WHERE DATE(report_date) = (SELECT MAX(DATE(report_date))
                               FROM ecommerce_reports.fba_manage_inventory_ca)
    GROUP BY fba.sku, brand.team
) AS inv
    ON orders.sku = inv.sku

ORDER BY days_of_supply_signal ASC;  -- most urgent restocks at top
