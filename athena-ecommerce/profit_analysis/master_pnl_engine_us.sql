/*
  master_pnl_engine_us.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : AWS Athena (Presto / Trino)
  Domain : Amazon FBA — Master P&L Engine (United States)

  WHAT IT DOES
  ─────────────
  Produces a complete monthly P&L row per ASIN, combining every revenue and
  cost component into a single denormalised output table used as the source
  of truth for all profitability reporting.

  COMPONENTS ASSEMBLED (per ASIN × year × month)
  ────────────────────────────────────────────────
  Revenue side:
    • principal          — product sale revenue (net of promotions)
    • quantity_purchased — units sold
    • tax_total          — seller-collected tax (GiftWrap + Shipping + Tax)
    • tax_collected_amz  — Marketplace Facilitator Tax (Amazon-remitted)
    • Promotion          — promotional shipping credits
    • revenue_unadjusted — gross revenue before any deductions

  Fee side:
    • Referral_fee_total — Amazon referral + variable closing fees
    • FBA_fee_total      — FBA fulfilment (per-unit + per-order + weight-based)
    • AMZ_Chargebacks    — shipping and gift-wrap chargebacks
    • salestaxservicefee — Amazon tax collection service fee

  Returns & refunds:
    • q_repackaged       — returned units repackaged as new
    • q_sellable_returns — customer returns re-added to sellable inventory
    • Refunded_amount    — gross refund total (20+ refund line-item types)
    • Refundamount_Adj   — refund net of refund taxes
    • refund_tax_total   — taxes on refunds

  Operations:
    • q_removed / removal_fee — FBA removal order quantity and fee
    • qty_adjustment / amount_adjustment — inventory discrepancy adjustments
    • supplier_reimbursement  — supplier-side discrepancy reimbursements

  COGS:
    • q (cogs_q)         — units received from supplier this month
    • cogs               — total COGS from supplier orders this month

  Reimbursements (two sources):
    • amount_reim / q_reim_cash / q_reim_inv — inventory reimbursements
    • amount_reim_2 / q_reim_cash_0           — customer-return reimbursements

  Cross-marketplace:
    • amount_ca_gs_usd / quantity_purchased_ca_gs — CA gross sales in USD
    • refund_amount_ca_gs_usd                     — CA refunds in USD
    • amount_mx_gs_usd / quantity_purchased_mx_gs — MX gross sales in USD
    • refund_amount_mx_gs_usd                     — MX refunds in USD

  ARCHITECTURE — Multi-Index CROSS JOIN spine
  ─────────────────────────────────────────────
  The query builds a complete calendar grid first:
    (all ASINs) × (2019–2026) × (1–12) = full monthly spine
  This ensures every ASIN appears for every month, even if it had zero
  activity, enabling correct period-over-period comparisons and preventing
  gaps from creating silent omissions in downstream reports.

  All component tables are LEFT JOINed onto this spine. NULL values are
  coalesced to 0 to make downstream aggregation safe.

  KEY TECHNIQUE — Pre-pivoted payments table
  ────────────────────────────────────────────
  Amazon payment reports contain one row per (order_id, transaction_type,
  amount_type). The table `payments_reporting_orders_usd_us_transpose` is a
  pre-computed pivot that spreads all 20+ fee/revenue types across columns.
  This query then sums each column with COALESCE, producing clean monthly
  totals without dynamic SQL or MAP_AGG.

  KEY TECHNIQUE — Three-way INNER JOIN for payments accuracy
  ────────────────────────────────────────────────────────────
  payments_order_dates_us  INNER JOIN  payments_order_quantities_us
                           INNER JOIN  payments_reporting_orders_usd_us_transpose
  all on (order_id, sku) ensures quantity and all fee amounts come from the
  same order event, preventing mismatched aggregation across payment tables.
*/

SELECT
    *
FROM (
    SELECT
        -- ── Core row ──────────────────────────────────────────────────────
        multiindex_tbl.*,
        -- Returns
        COALESCE(ret_rep.q_repackaged,        0)  AS q_repackaged,
        COALESCE(ret_sel.q_sellable_returns,   0)  AS q_sellable_returns,
        -- Removals
        COALESCE(rem.q_removed,                0)  AS q_removed,
        COALESCE(rem.removal_fee,              0)  AS removal_fee,
        -- Reimbursements: inventory events
        COALESCE(reim_inv.amount_reim,         0)  AS amount_reim,
        COALESCE(reim_inv.q_reim_cash,         0)  AS q_reim_cash,
        COALESCE(reim_inv.q_reim_inv,          0)  AS q_reim_inv,
        -- Reimbursements: customer return events
        COALESCE(reim_cr.amount_reim_2,        0)  AS amount_reim_2,
        COALESCE(reim_cr.q_reim_cash_0,        0)  AS q_reim_cash_0,
        -- Revenue & fees from pivoted payments
        COALESCE(pay.quantity_purchased,       0)  AS quantity_purchased,
        COALESCE(pay.principal,                0)  AS principal,
        COALESCE(pay.tax_total,                0)  AS tax_total,
        COALESCE(pay.amount,                   0)  AS amount,
        COALESCE(pay.revenue_unadjusted,       0)  AS revenue_unadjusted,
        COALESCE(pay.salestaxservicefee,       0)  AS salestaxservicefee,
        -- Amount net of all tax remittances
        COALESCE(pay.amount, 0)
            - COALESCE(COALESCE(pay.tax_total, 0) + COALESCE(pay.tax_collected_amz, 0), 0)
                                                   AS amount_2,
        COALESCE(pay.tax_collected_amz,        0)  AS tax_collected_amz,
        COALESCE(pay.tax_total, 0) + COALESCE(pay.tax_collected_amz, 0)
                                                   AS tax_remitted,
        COALESCE(pay.AMZ_Chargebacks,          0)  AS AMZ_Chargebacks,
        COALESCE(pay.Promotion,                0)  AS Promotion,
        COALESCE(pay.Referral_fee_total,       0)  AS Referral_fee_total,
        COALESCE(pay.FBA_fee_total,            0)  AS FBA_fee_total,
        -- Refunds
        COALESCE(ref.refund_tax_total,         0)  AS refund_tax_total,
        COALESCE(ref.Refunded_amount,          0)  AS Refunded_amount,
        COALESCE(ref.Refundamount_Adj,         0)  AS Refundamount_Adj,
        -- Inventory discrepancies
        COALESCE(dis.qty_adjustment,           0)  AS qty_adjustment,
        COALESCE(dis.supplier_reimbursement,   0)  AS supplier_reimbursement,
        COALESCE(dis.amount_adjustment,        0)  AS amount_adjustment,
        -- COGS from supplier orders
        COALESCE(sup.sum_quantity_supplier_orders, 0) AS q,
        COALESCE(ROUND(sup.sum_cogs_supplier_orders, 2), 0) AS cogs,
        -- Cross-marketplace gross sales (CA & MX, USD-equivalent)
        COALESCE(ordca.amount_ca_gs_usd,       0)  AS amount_ca_gs_usd,
        COALESCE(ordca.quantity_purchased_ca_gs,0) AS quantity_purchased_ca_gs,
        COALESCE(refca.refund_amount_ca_gs_usd,0)  AS refund_amount_ca_gs_usd,
        COALESCE(ordmx.amount_mx_gs_usd,       0)  AS amount_mx_gs_usd,
        COALESCE(ordmx.quantity_purchased_mx_gs,0) AS quantity_purchased_mx_gs,
        COALESCE(refmx.refund_amount_mx_gs_usd,0)  AS refund_amount_mx_gs_usd

    FROM (
        -- ── SPINE: all (asin, year, month) combinations ──────────────────
        -- Ensures zero-activity periods appear in output (no silent gaps)
        SELECT DISTINCT
            asin,
            CAST(y.year_  AS INT) AS year_,
            CAST(m.month_ AS INT) AS month_
        FROM (
            SELECT sku_asin.asin, 'join_month' AS join_month, 'join_year' AS join_year
            FROM awegoo_reporting_db.sku_asin_us AS sku_asin
        ) AS sku_asin_tbl
        LEFT JOIN (
            SELECT * FROM (VALUES
                (2019,'join_year'),(2020,'join_year'),(2021,'join_year'),
                (2022,'join_year'),(2023,'join_year'),(2024,'join_year'),
                (2025,'join_year'),(2026,'join_year')
            ) AS t(year_, join_year)
        ) AS y ON sku_asin_tbl.join_year = y.join_year
        LEFT JOIN (
            SELECT * FROM (VALUES
                (1,'join_month'),(2,'join_month'),(3,'join_month'),
                (4,'join_month'),(5,'join_month'),(6,'join_month'),
                (7,'join_month'),(8,'join_month'),(9,'join_month'),
                (10,'join_month'),(11,'join_month'),(12,'join_month')
            ) AS t(month_, join_month)
        ) AS m ON sku_asin_tbl.join_month = m.join_month
    ) AS multiindex_tbl

    -- ── Repackaged returns ───────────────────────────────────────────────
    LEFT JOIN (
        SELECT ret.year_pdt, ret."month" AS month_, core.asin,
               SUM(ret.q_repackaged) AS q_repackaged
        FROM awegoo_reporting_db.returns_repackaged_us AS ret
        LEFT JOIN awegoo_reports.core_sku_asin_us AS core ON ret.sku = core.sku
        GROUP BY ret.year_pdt, ret."month", core.asin
    ) AS ret_rep
        ON multiindex_tbl.asin = ret_rep.asin
       AND multiindex_tbl.year_ = ret_rep.year_pdt
       AND multiindex_tbl.month_ = ret_rep.month_

    -- ── Sellable returns ────────────────────────────────────────────────
    LEFT JOIN (
        SELECT ret.year_pdt, ret."month" AS month_, core.asin,
               SUM(ret.q_sellable_returns) AS q_sellable_returns
        FROM awegoo_reporting_db.returns_sellable_us AS ret
        LEFT JOIN awegoo_reporting_db.sku_asin_us AS core ON ret.sku = core.sku
        GROUP BY ret.year_pdt, ret."month", core.asin
    ) AS ret_sel
        ON multiindex_tbl.asin = ret_sel.asin
       AND multiindex_tbl.year_ = ret_sel.year_pdt
       AND multiindex_tbl.month_ = ret_sel.month_

    -- ── Removal orders ──────────────────────────────────────────────────
    LEFT JOIN (
        SELECT
            COALESCE(core.asin, rem.asin, 'none') AS final_asin,
            rem.year_pdt,
            rem.month AS month_,
            SUM(rem.q_removed)    AS q_removed,
            SUM(rem.removal_fee)  AS removal_fee
        FROM awegoo_reporting_db.removal_orders_reporting_us AS rem
        LEFT JOIN awegoo_reporting_db.sku_asin_us AS core ON rem.sku = core.sku
        GROUP BY COALESCE(core.asin, rem.asin, 'none'), rem.year_pdt, rem.month
    ) AS rem
        ON multiindex_tbl.asin = rem.final_asin
       AND multiindex_tbl.year_ = rem.year_pdt
       AND multiindex_tbl.month_ = rem.month_

    -- ── Inventory reimbursements (pre-aggregated in Glue/Python job) ────
    LEFT JOIN awegoo_reports.df_reimbursements_inv AS reim_inv
        ON multiindex_tbl.asin   = reim_inv.asin
       AND multiindex_tbl.year_  = reim_inv.year_pdt
       AND multiindex_tbl.month_ = reim_inv.month

    -- ── Customer-return reimbursements ──────────────────────────────────
    LEFT JOIN awegoo_reports.df_reimbursements_cr AS reim_cr
        ON multiindex_tbl.asin   = reim_cr.asin
       AND multiindex_tbl.year_  = reim_cr.year_pdt
       AND multiindex_tbl.month_ = reim_cr.month

    -- ── Revenue & fees: three-way INNER JOIN on (order_id, sku) ─────────
    -- Ensures quantity, dates, and pivoted fee amounts all refer to the
    -- same order event before aggregating to ASIN × month level.
    LEFT JOIN (
        SELECT
            asin_map.asin,
            dates.year_pdt,
            dates."month",
            SUM(COALESCE(qty.quantity_purchased, 0))        AS quantity_purchased,
            SUM(COALESCE(t.Principal,        0))            AS principal,
            SUM(COALESCE(t.GiftWrapTax,      0))
                + SUM(COALESCE(t.ShippingTax, 0))
                + SUM(COALESCE(t.Tax,         0))           AS tax_total,
            -- Full amount sum (all 27 line-item types)
            SUM(COALESCE(t.Commission,                    0))
                + SUM(COALESCE(t.FBAPerOrderFulfillmentFee,0))
                + SUM(COALESCE(t.FBAPerUnitFulfillmentFee, 0))
                + SUM(COALESCE(t.FBAWeightBasedFee,        0))
                + SUM(COALESCE(t.GiftWrap,                 0))
                + SUM(COALESCE(t.GiftWrapTax,              0))
                + SUM(COALESCE(t.GiftwrapChargeback,       0))
                + SUM(COALESCE(t.LowValueGoods_Principal,  0))
                + SUM(COALESCE(t.LowValueGoods_Shipping,   0))
                + SUM(COALESCE(t.LowValueGoodsTax_Other,   0))
                + SUM(COALESCE(t.LowValueGoodsTax_Principal,0))
                + SUM(COALESCE(t.LowValueGoodsTax_Shipping,0))
                + SUM(COALESCE(t.MarketplaceFacilitatorTax_Other,     0))
                + SUM(COALESCE(t.MarketplaceFacilitatorTax_Principal,  0))
                + SUM(COALESCE(t.MarketplaceFacilitatorTax_Shipping,   0))
                + SUM(COALESCE(t.MarketplaceFacilitatorVAT_Principal,  0))
                + SUM(COALESCE(t.MarketplaceFacilitatorVAT_Shipping,   0))
                + SUM(COALESCE(t.Principal,                0))
                + SUM(COALESCE(t.Promotion_Principal,      0))
                + SUM(COALESCE(t.Promotion_Shipping,       0))
                + SUM(COALESCE(t.SalesTaxServiceFee,       0))
                + SUM(COALESCE(t.Shipping,                 0))
                + SUM(COALESCE(t.ShippingChargeback,       0))
                + SUM(COALESCE(t.ShippingHB,               0))
                + SUM(COALESCE(t.ShippingTax,              0))
                + SUM(COALESCE(t.Tax,                      0))
                + SUM(COALESCE(t.VariableClosingFee,       0))           AS amount,
            -- Marketplace-facilitated tax (Amazon-remitted, not seller-remitted)
            SUM(COALESCE(t.MarketplaceFacilitatorTax_Other,     0))
                + SUM(COALESCE(t.MarketplaceFacilitatorTax_Principal,  0))
                + SUM(COALESCE(t.MarketplaceFacilitatorTax_Shipping,   0))
                + SUM(COALESCE(t.LowValueGoodsTax_Principal,   0))
                + SUM(COALESCE(t.LowValueGoodsTax_Shipping,    0))
                + ROUND(SUM(COALESCE(t.LowValueGoods_Principal,0)), 2)
                + ROUND(SUM(COALESCE(t.LowValueGoods_Shipping, 0)), 2)   AS tax_collected_amz,
            SUM(COALESCE(t.GiftwrapChargeback, 0))
                + SUM(COALESCE(t.ShippingChargeback, 0))                 AS AMZ_Chargebacks,
            SUM(COALESCE(t.Commission,         0))
                + SUM(COALESCE(t.VariableClosingFee, 0))                 AS Referral_fee_total,
            SUM(COALESCE(t.FBAPerUnitFulfillmentFee,  0))
                + SUM(COALESCE(t.FBAPerOrderFulfillmentFee, 0))
                + SUM(COALESCE(t.FBAWeightBasedFee,   0))                AS FBA_fee_total,
            -- Unadjusted revenue (before fees and chargebacks)
            SUM(COALESCE(t.Principal,          0))
                + SUM(COALESCE(t.Shipping,     0))
                + SUM(COALESCE(t.GiftWrap,     0))
                + SUM(COALESCE(t.Promotion_Shipping, 0))
                + SUM(COALESCE(t.Tax,          0))
                + SUM(COALESCE(t.ShippingTax,  0))
                + SUM(COALESCE(t.GiftWrapTax,  0))                       AS revenue_unadjusted,
            SUM(COALESCE(t.Promotion_Shipping, 0))                       AS Promotion,
            SUM(COALESCE(t.SalesTaxServiceFee, 0))                       AS salestaxservicefee
        FROM awegoo_reporting_db.payments_order_dates_us AS dates
        INNER JOIN awegoo_reporting_db.payments_order_quantities_us AS qty
            ON dates.order_id = qty.order_id AND dates.sku = qty.sku
        INNER JOIN awegoo_reporting_db.payments_reporting_orders_usd_us_transpose AS t
            ON dates.order_id = t.order_id AND dates.sku = t.sku
        LEFT JOIN awegoo_reporting_db.sku_asin_us AS asin_map
            ON dates.sku = asin_map.sku
        GROUP BY asin_map.asin, dates.year_pdt, dates."month"
    ) AS pay
        ON multiindex_tbl.asin   = pay.asin
       AND multiindex_tbl.year_  = pay.year_pdt
       AND multiindex_tbl.month_ = pay."month"

    -- ── Refunds (20+ refund line-item types summed) ─────────────────────
    LEFT JOIN (
        SELECT year_pdt, month, core.asin,
               Refunded_amount,
               refund_tax_total,
               COALESCE(Refunded_amount - refund_tax_total, 0) AS Refundamount_Adj
        FROM (
            SELECT year_pdt, month, core.asin,
                SUM(COALESCE(t.RefundTax,                              0))
                    + SUM(COALESCE(t.RefundPrincipal,                  0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Principal, 0))
                    + SUM(COALESCE(t.RefundCommission,                 0))
                    + SUM(COALESCE(t.RefundRefundCommission,           0))
                    + SUM(COALESCE(t.RefundShippingTax,                0))
                    + SUM(COALESCE(t.RefundShipping,                   0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Shipping, 0))
                    + SUM(COALESCE(t.RefundShippingChargeback,         0))
                    + SUM(COALESCE(t.RefundGoodwill,                   0))
                    + SUM(COALESCE(t.RefundVariableClosingFee,         0))
                    + SUM(COALESCE(t.RefundPromotion_Shipping,         0))
                    + SUM(COALESCE(t.RefundPromotion_Principal,        0))
                    + SUM(COALESCE(t.RefundRestockingFee,              0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_RestockingFee, 0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Other, 0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorVAT_Shipping, 0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorVAT_Principal, 0))
                    + SUM(COALESCE(t.RefundGiftWrap,                   0))
                    + SUM(COALESCE(t.RefundGiftwrapChargeback,         0))
                    + SUM(COALESCE(t.RefundGiftWrapTax,                0))
                    + SUM(COALESCE(t.RefundShippingHB,                 0))
                    + SUM(COALESCE(t.RefundLowValueGoodsTax_Shipping,  0))
                    + SUM(COALESCE(t.RefundLowValueGoodsTax_Principal, 0))  AS Refunded_amount,
                SUM(COALESCE(t.RefundShippingTax,                      0))
                    + SUM(COALESCE(t.RefundGiftWrapTax,                0))
                    + SUM(COALESCE(t.RefundLowValueGoodsTax_Principal, 0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Other, 0))
                    + SUM(COALESCE(t.RefundTax,                        0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Principal, 0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_RestockingFee, 0))
                    + SUM(COALESCE(t.RefundLowValueGoodsTax_Shipping,  0))
                    + SUM(COALESCE(t.RefundMarketplaceFacilitatorTax_Shipping, 0))  AS refund_tax_total
            FROM awegoo_reporting_db.payments_refund_dates_us AS dates
            LEFT JOIN awegoo_reporting_db.payments_reporting_refunds_usd_us_transpose AS t
                ON dates.order_id = t.order_id AND dates.sku = t.sku
            LEFT JOIN awegoo_reporting_db.sku_asin_us AS core ON dates.sku = core.sku
            GROUP BY year_pdt, month, core.asin
        )
    ) AS ref
        ON multiindex_tbl.asin   = ref.asin
       AND multiindex_tbl.year_  = ref.year_pdt
       AND multiindex_tbl.month_ = ref.month

    -- ── Inventory discrepancies ──────────────────────────────────────────
    LEFT JOIN (
        SELECT asin, year_pdt, "month" AS month_,
               SUM(qty_adjustment)          AS qty_adjustment,
               SUM(supplier_reimbursement)  AS supplier_reimbursement,
               SUM(amount_adjustment)       AS amount_adjustment
        FROM awegoo_reporting_db.discrepancies_us
        GROUP BY asin, year_pdt, "month"
    ) AS dis
        ON multiindex_tbl.asin   = dis.asin
       AND multiindex_tbl.year_  = dis.year_pdt
       AND multiindex_tbl.month_ = dis.month_

    -- ── Supplier orders (COGS) ───────────────────────────────────────────
    LEFT JOIN (
        SELECT YEAR(DATE(date)) AS yr, MONTH(DATE(date)) AS mo, asin,
               SUM(q) AS sum_quantity_supplier_orders,
               SUM(cogs) AS sum_cogs_supplier_orders
        FROM awegoo_reporting_db.supplier_orders_us
        GROUP BY YEAR(DATE(date)), MONTH(DATE(date)), asin
    ) AS sup
        ON multiindex_tbl.asin   = sup.asin
       AND multiindex_tbl.year_  = sup.yr
       AND multiindex_tbl.month_ = sup.mo

    -- ── Canada gross sales & refunds (USD-equivalent) ────────────────────
    LEFT JOIN awegoo_reports.df_orders_ca AS ordca
        ON multiindex_tbl.asin = ordca.asin AND multiindex_tbl.year_ = ordca.year_pdt AND multiindex_tbl.month_ = ordca.month
    LEFT JOIN awegoo_reports.df_refunds_ca AS refca
        ON multiindex_tbl.asin = refca.asin AND multiindex_tbl.year_ = refca.year_pdt AND multiindex_tbl.month_ = refca.month

    -- ── Mexico gross sales & refunds (USD-equivalent) ────────────────────
    LEFT JOIN awegoo_reports.df_orders_mx AS ordmx
        ON multiindex_tbl.asin = ordmx.asin AND multiindex_tbl.year_ = ordmx.year_pdt AND multiindex_tbl.month_ = ordmx.month
    LEFT JOIN awegoo_reports.df_refunded_mx AS refmx
        ON multiindex_tbl.asin = refmx.asin AND multiindex_tbl.year_ = refmx.year_pdt AND multiindex_tbl.month_ = refmx.month

    ORDER BY multiindex_tbl.asin, multiindex_tbl.year_, multiindex_tbl.month_
);
