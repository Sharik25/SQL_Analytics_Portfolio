/*
  reimbursement_net_profit.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Reimbursement P&L

  WHAT IT DOES
  ─────────────
  Calculates TRUE net reimbursement revenue per month, accounting for Amazon's
  reversal and retraction events that claw back previously approved payments.

  PROBLEM SOLVED
  ───────────────
  Amazon can issue a "Reimbursement_Reversal" or "Payment_Retraction" months
  after an original approval, reducing apparent revenue. A naive SUM of all
  reimbursement amounts would overstate earnings. This query:
    1. Isolates original approvals (case_id IS NOT NULL, original_reimbursement_id IS NULL)
    2. Left-joins reversals/retractions back to their originals via
       original_reimbursement_id = reimbursement_id
    3. Subtracts reversal amounts to produce a true net figure

  OUTPUT COLUMNS (aggregated by year / month)
  ─────────────────────────────────────────────
    sum_cnt_reimbursement_id        – count of approved claims
    sum_cnt_negative_reimbursement_id – count of reversals matched
    net_cnt_reimbursements          – net claim count
    sum_reim_with_case_id           – gross approved amount
    sum_error_reimb                 – total reversed / retracted
    sum_net_profit                  – true net reimbursement revenue
    profit_per_net_profit           – average net $ per claim
*/

SELECT
    year,
    month,
    SUM(cnt_reimbursement_id)                                           AS sum_cnt_reimbursement_id,
    SUM(cnt_negative_reimbursement_id)                                  AS sum_cnt_negative_reimbursement_id,
    SUM(cnt_reimbursement_id) - SUM(cnt_negative_reimbursement_id)      AS net_cnt_reimbursements,
    SUM(original_reimb_amount_with_case_id)                             AS sum_reim_with_case_id,
    SUM(negative_reimb_amount_with_error_case)                          AS sum_error_reimb,
    SUM(net_profit)                                                     AS sum_net_profit,
    SUM(net_profit) / NULLIF(SUM(cnt_reimbursement_id), 0)             AS profit_per_net_profit
FROM (
    -- ── ASIN / FNSKU level: join originals → reversals ─────────────────────
    SELECT
        orig.year,
        orig.month,
        orig.asin,
        orig.fnsku,
        COUNT(DISTINCT orig.reimbursement_id)               AS cnt_reimbursement_id,
        COUNT(DISTINCT rev.original_reimbursement_id)       AS cnt_negative_reimbursement_id,
        SUM(orig.amount_total)                              AS original_reimb_amount_with_case_id,
        COALESCE(SUM(rev.amount_total_minus), 0)            AS negative_reimb_amount_with_error_case,
        -- net = gross approval + (negative reversal amounts, already negative in source)
        SUM(orig.amount_total) + COALESCE(SUM(rev.amount_total_minus), 0) AS net_profit
    FROM (
        -- Original approvals: case_id present, not a reversal itself
        SELECT year, month, reimbursement_id, original_reimbursement_id,
               case_id, asin, fnsku, reason, amount_total,
               quantity_reimbursed_cash, quantity_reimbursed_inventory,
               quantity_reimbursed_total
        FROM ecommerce_db.reimbursements
        WHERE COALESCE(case_id, 1)                    != 1   -- has a case_id
          AND COALESCE(original_reimbursement_id, 1)  =  1   -- is NOT itself a reversal
        ORDER BY year, month
    ) AS orig
    LEFT JOIN (
        -- Reversals / retractions — matched back to their original claim
        SELECT asin, fnsku, reimbursement_id,
               original_reimbursement_id,
               amount_total AS amount_total_minus
        FROM ecommerce_db.reimbursements
        WHERE reason IN ('Reimbursement_Reversal', 'Payment_Retraction')
    ) AS rev
        ON  orig.reimbursement_id = rev.original_reimbursement_id
        AND orig.asin             = rev.asin
        AND orig.fnsku            = rev.fnsku
    GROUP BY orig.year, orig.month, orig.asin, orig.fnsku
    ORDER BY orig.year, orig.month
) AS asin_level

GROUP BY year, month
ORDER BY year, month;
