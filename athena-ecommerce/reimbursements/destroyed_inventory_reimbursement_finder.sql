/*
  destroyed_inventory_reimbursement_finder.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Reimbursement Recovery

  WHAT IT DOES
  ─────────────
  Identifies FBA inventory units destroyed at Amazon warehouses (reason = 'D')
  that have NOT yet been reimbursed, for both US and CA marketplaces.
  The output drives proactive case-filing before Amazon's 18-month claim window
  closes.

  KEY TECHNIQUE — date-range anti-subquery
  ─────────────────────────────────────────
  A destroyed ledger event is considered already reimbursed if a matching
  reimbursement record exists within [event_date, event_date + 30 days].
  We build a set of already-reimbursed reference_ids via a correlated
  date-range LEFT JOIN, then exclude them with NOT IN.

  WHY NOT A SIMPLE INNER JOIN?
  Amazon does not store a direct foreign key between ledger events and
  reimbursement records; we must match on (asin, fnsku) within a time window.

  DISPOSITIONS EXCLUDED
  ──────────────────────
  DEFECTIVE, DISTRIBUTOR_DAMAGED, CUSTOMER_DAMAGED — these are the seller's
  responsibility and are not eligible for Amazon reimbursement.
*/

-- ──────────────────────────────────────────────────────────────
-- CANADA — destroyed inventory without a corresponding reimbursement
-- ──────────────────────────────────────────────────────────────
SELECT
    date(date_correct)    AS date_correct,
    fnsku,
    asin,
    msku,
    reference_id,
    quantity,
    disposition,
    reason,
    reconciled_quantity,
    unreconciled_quantity
FROM ecommerce_reports.inventory_ledger_daily_adj_detailed_full_ca
WHERE
    reason = 'D'
    -- rolling 6-day window (yesterday's data; today not yet populated)
    AND date(date_correct) > date_add('day', -6, date(now()))
    AND date(date_correct) <= date_add('day', -5, date(now()))
    AND disposition NOT IN ('DEFECTIVE', 'DISTRIBUTOR_DAMAGED', 'CUSTOMER_DAMAGED')
    -- exclude reference_ids that already have a reimbursement within 30 days
    AND reference_id NOT IN (
        SELECT DISTINCT reference_id
        FROM (
            -- date-range LEFT JOIN: ledger event → reimbursement table
            SELECT
                ledger.reference_id,
                ledger.asin,
                ledger.fnsku,
                ledger.date_correct,
                reimb.approval_date,
                reimb.reimbursement_id
            FROM (
                SELECT date_correct, fnsku, asin, msku,
                       reference_id, quantity, disposition, reason,
                       reconciled_quantity, unreconciled_quantity
                FROM ecommerce_reports.inventory_ledger_daily_adj_detailed_full_ca
                WHERE reason = 'D'
                ORDER BY asin, reference_id, date(date_correct)
            ) AS ledger
            LEFT JOIN (
                SELECT approval_date, sku, fnsku, asin,
                       reimbursement_id, case_id, reason,
                       quantity_reimbursed_cash, quantity_reimbursed_inventory,
                       quantity_reimbursed_total
                FROM ecommerce_db.reimbursements_ca
                WHERE reason = 'Damaged_Warehouse'
                ORDER BY approval_date
            ) AS reimb
                ON  ledger.asin  = reimb.asin
                AND ledger.fnsku = reimb.fnsku
            WHERE
                -- reimbursement must fall within [event_date, event_date + 30 days]
                date(reimb.approval_date) >= date(ledger.date_correct)
                AND date(reimb.approval_date) <= date_add('day', 30, date(ledger.date_correct))
        ) AS already_reimbursed_ca
    )

UNION ALL

-- ──────────────────────────────────────────────────────────────
-- UNITED STATES — same logic against the US ledger and reimbursements table
-- ──────────────────────────────────────────────────────────────
SELECT
    date(date_correct)    AS date_correct,
    fnsku,
    asin,
    msku,
    reference_id,
    quantity,
    disposition,
    reason,
    reconciled_quantity,
    unreconciled_quantity
FROM ecommerce_reports.inventory_ledger_daily_adj_detailed_full_us
WHERE
    reason = 'D'
    AND date(date_correct) > date_add('day', -6, date(now()))
    AND date(date_correct) <= date_add('day', -5, date(now()))
    AND disposition NOT IN ('DEFECTIVE', 'DISTRIBUTOR_DAMAGED', 'CUSTOMER_DAMAGED')
    AND reference_id NOT IN (
        SELECT DISTINCT reference_id
        FROM (
            SELECT
                ledger.reference_id,
                ledger.asin,
                ledger.fnsku,
                ledger.date_correct,
                reimb.approval_date,
                reimb.reimbursement_id
            FROM (
                SELECT date_correct, fnsku, asin, msku,
                       reference_id, quantity, disposition, reason,
                       reconciled_quantity, unreconciled_quantity
                FROM ecommerce_reports.inventory_ledger_daily_adj_detailed_full_us
                WHERE reason = 'D'
                ORDER BY asin, reference_id, date(date_correct)
            ) AS ledger
            LEFT JOIN (
                SELECT approval_date, sku, fnsku, asin,
                       reimbursement_id, case_id, reason,
                       quantity_reimbursed_cash, quantity_reimbursed_inventory,
                       quantity_reimbursed_total
                FROM ecommerce_db.reimbursements
                WHERE reason = 'Damaged_Warehouse'
                ORDER BY approval_date
            ) AS reimb
                ON  ledger.asin  = reimb.asin
                AND ledger.fnsku = reimb.fnsku
            WHERE
                date(reimb.approval_date) >= date(ledger.date_correct)
                AND date(reimb.approval_date) <= date_add('day', 30, date(ledger.date_correct))
        ) AS already_reimbursed_us
    )
ORDER BY date_correct DESC, asin;
