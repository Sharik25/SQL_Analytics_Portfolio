/*
  fba_inventory_aging_by_team.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Inventory Planning

  WHAT IT DOES
  ─────────────
  Produces an inventory aging report segmented into 5 standard Amazon age
  buckets (0-90, 91-180, 181-270, 271-365, 365+ days), enriched with:
    • Sourcing team assignment (from the most recent supplier order per ASIN)
    • COGS (avg cost per unit, all-time average from supplier orders)
    • Dollar value of aged stock per bucket = units × COGS

  WHY ROW_NUMBER() HERE
  ──────────────────────
  A single ASIN can be sourced by multiple teams across different purchase
  orders. ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC) isolates
  the *most recent* team assignment, ensuring the report reflects current
  ownership without collapsing or duplicating rows.

  OUTPUT USE CASE
  ────────────────
  Feeds directly into liquidation and repricing decisions: high COGS locked
  in 365+ days bucket triggers escalation to the sourcing team.
*/

SELECT
    CAST(snapshot_date AS DATE)          AS snapshot_date,
    team,
    asin,
    -- Dollar value per aging bucket (units × avg COGS)
    SUM(inv_age_0_to_90_days_sum)        AS inv_age_0_to_90_days_usd,
    SUM(inv_age_91_to_180_days_sum)      AS inv_age_91_to_180_days_usd,
    SUM(inv_age_181_to_270_days_sum)     AS inv_age_181_to_270_days_usd,
    SUM(inv_age_271_to_365_days_sum)     AS inv_age_271_to_365_days_usd,
    SUM(inv_age_365_plus_days_sum)       AS inv_age_365_plus_days_usd
FROM (
    SELECT
        inv.snapshot_date,
        team_cost.team,
        inv.asin,
        -- Multiply units in each bucket by COGS; COALESCE handles missing cost data
        COALESCE(inv.inv_age_0_to_90_days   * team_cost.avg_cost_per_item, 0) AS inv_age_0_to_90_days_sum,
        COALESCE(inv.inv_age_91_to_180_days  * team_cost.avg_cost_per_item, 0) AS inv_age_91_to_180_days_sum,
        COALESCE(inv.inv_age_181_to_270_days * team_cost.avg_cost_per_item, 0) AS inv_age_181_to_270_days_sum,
        COALESCE(inv.inv_age_271_to_365_days * team_cost.avg_cost_per_item, 0) AS inv_age_271_to_365_days_sum,
        COALESCE(inv.inv_age_365_plus_days   * team_cost.avg_cost_per_item, 0) AS inv_age_365_plus_days_sum
    FROM ecommerce_reports.fba_inventory_planning_us AS inv
    LEFT JOIN (
        -- Attach the most recent team per ASIN + all-time average COGS
        SELECT
            latest_team.asin,
            latest_team.team,
            cost_tbl.avg_cost_per_item_all_time AS avg_cost_per_item
        FROM (
            -- ROW_NUMBER to get the latest team assignment per ASIN
            SELECT asin, team
            FROM (
                SELECT
                    date,
                    asin,
                    team,
                    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC) AS row_num
                FROM ecommerce_db.supplier_orders_us
                WHERE team IN ('ADAE', 'SF', 'AE', 'YK', 'VH', 'OR', 'RB')
            )
            WHERE row_num = 1
        ) AS latest_team
        LEFT JOIN (
            -- All-time average cost per unit (blended across all purchase orders)
            SELECT
                asin,
                AVG(cost_per_item) AS avg_cost_per_item_all_time
            FROM ecommerce_db.supplier_orders_us
            GROUP BY asin
        ) AS cost_tbl
            ON latest_team.asin = cost_tbl.asin
    ) AS team_cost
        ON inv.asin = team_cost.asin
    WHERE
        -- Only the most recent snapshot date
        snapshot_date = (SELECT MAX(snapshot_date) FROM ecommerce_reports.fba_inventory_planning_us)
        AND condition = 'New'
) AS enriched

GROUP BY snapshot_date, team, asin
ORDER BY team, asin;
