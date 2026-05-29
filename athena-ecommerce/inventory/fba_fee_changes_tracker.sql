/*
  fba_fee_changes_tracker.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Fee Monitoring

  WHAT IT DOES
  ─────────────
  Detects period-over-period FBA fee changes per ASIN / SKU / fee type.
  Useful for catching undocumented Amazon fee table updates that silently
  erode margins.

  KEY TECHNIQUE — LAG() window function
  ──────────────────────────────────────
  LAG(avg_fee_per_item, 1) OVER (PARTITION BY asin, sku, amount_description
                                  ORDER BY settlement_start_date ASC)
  retrieves the fee from the *previous* settlement period for the same
  (asin, sku, fee_type) combination, enabling delta calculation without a
  self-join.

  ADDITIONAL PATTERN — ROW_NUMBER() dedup
  ─────────────────────────────────────────
  Multiple payments rows can map to the same (settlement_id, sku, fee_type).
  We average them within the settlement period before applying LAG(), ensuring
  one clean row per period per fee type.

  OUTPUT COLUMNS
  ───────────────
    difference         – absolute fee delta (current − previous), in USD
    difference_percent – magnitude of relative change (ABS)
    Positive difference = fee increased (margin impact)
    Negative difference = fee decreased (or the SKU was reclassified)
*/

SELECT
    row_num,
    settlement_id,
    settlement_start_date,
    settlement_end_date,
    asin,
    sku,
    amount_description,
    avg_fee_per_item,
    previous_avg_fee_per_item,
    difference,
    difference_percent
FROM (
    SELECT
        *,
        -- Convert stored negatives to positive for readability, then delta
        ROUND(avg_fee_per_item * -1, 2)
            - ROUND(previous_avg_fee_per_item * -1, 2)              AS difference,
        ABS(ROUND(
            (ROUND(avg_fee_per_item * -1, 2) - ROUND(previous_avg_fee_per_item * -1, 2))
            / NULLIF(ROUND(avg_fee_per_item * -1, 2), 0)
        , 2))                                                        AS difference_percent
    FROM (
        SELECT
            -- Sequential row number per (asin, sku, fee_type) for timeline ordering
            ROW_NUMBER() OVER (
                PARTITION BY asin, sku, amount_description
                ORDER BY settlement_start_date ASC
            )                                                        AS row_num,
            *,
            -- LAG: bring previous period's fee into the current row
            LAG(avg_fee_per_item, 1) OVER (
                PARTITION BY asin, sku, amount_description
                ORDER BY settlement_start_date ASC
            )                                                        AS previous_avg_fee_per_item
        FROM (
            -- Collapse to one avg fee row per (settlement, sku, fee_type)
            SELECT
                settlement_id,
                settlement_start_date,
                settlement_end_date,
                asin,
                sku,
                amount_description,
                AVG(amount / NULLIF(quantity_purchased, 0))          AS avg_fee_per_item
            FROM (
                -- Enrich raw payments with settlement date metadata and ASIN lookup
                SELECT
                    meta.settlement_start_date,
                    meta.settlement_end_date,
                    pay.settlement_id,
                    pay.amount_description,
                    pay.posted_date_time,
                    pay.sku,
                    pay.amount,
                    pay.quantity_purchased,
                    asin_map.asin
                FROM (
                    SELECT settlement_id, currency, amount_description,
                           posted_date_time, sku, amount, quantity_purchased
                    FROM ecommerce_db.payments_unsorted_us
                    WHERE transaction_type  = 'Order'
                      AND currency         = 'USD'
                      AND amount_type      = 'ItemFees'
                      AND amount_description LIKE 'FBA%'
                ) AS pay
                LEFT JOIN ecommerce_db.payments_meta_us AS meta
                    ON pay.settlement_id = meta.settlement_id
                LEFT JOIN ecommerce_db.sku_asin_us AS asin_map
                    ON pay.sku = asin_map.sku
            ) AS enriched
            GROUP BY settlement_id, settlement_start_date, settlement_end_date,
                     asin, sku, amount_description
        ) AS per_period
        ORDER BY settlement_id
    ) AS with_lag
) AS with_delta
-- Filter: only rows where a fee actually changed
WHERE difference != 0
  AND previous_avg_fee_per_item IS NOT NULL
ORDER BY asin, sku, row_num;
