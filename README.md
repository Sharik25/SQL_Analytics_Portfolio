# SQL Analytics Portfolio

Production SQL queries from real data analytics roles across **three companies** and **three distinct technology stacks**. The queries cover Amazon FBA e-commerce, video streaming support analytics, and retail banking operations — demonstrating breadth across domains, SQL engines, and problem complexity.

---

## Tech Stack Overview

| Engine | Dialect | Company | Domain |
|--------|---------|---------|--------|
| **AWS Athena** | Presto / Trino SQL | Awegoo (Amazon FBA Seller) | E-commerce analytics |
| **ClickHouse** | ClickHouse SQL | ivi.ru (Video Streaming) | Support & product analytics |
| **Oracle SQL** | Oracle + OFSAA | GBC (Retail Bank) | Banking CRM / Loan operations ETL |

---

## Repository Structure

```
sql-analytics-portfolio/
├── athena-ecommerce/
│   ├── reimbursements/
│   │   ├── destroyed_inventory_reimbursement_finder.sql
│   │   └── reimbursement_net_profit.sql
│   ├── inventory/
│   │   ├── fba_inventory_aging_by_team.sql
│   │   ├── fba_fee_changes_tracker.sql
│   │   ├── avg_cost_gap_fill.sql
│   │   └── inventory_vs_projected_sales.sql
│   ├── marketing/
│   │   └── marketing_dashboard.sql
│   └── profit_analysis/
│       ├── new_products_profitability.sql
│       └── master_pnl_engine_us.sql          ← NEW
├── clickhouse-support-analytics/
│   ├── contact_rate_by_category_7day_avg.sql
│   └── support_tickets_monthly_pivot.sql
└── oracle-banking-analytics/                  ← NEW
    ├── retail-accounts/
    │   └── current_account_open_etl.sql
    └── loan-operations/
        └── early_repayment_event_etl.sql
```

---

## Part 1 — AWS Athena: Amazon FBA E-commerce Analytics

**Context:** Analytics infrastructure for a multi-category Amazon FBA seller operating in the US and Canadian marketplaces. All queries run on AWS Athena against data stored in S3, using Presto/Trino SQL dialect. Data sources include FBA Inventory Ledger reports, Amazon Settlement/Payments reports, Advertising reports, and internal supplier order records.

---

### `reimbursements/destroyed_inventory_reimbursement_finder.sql`

**What it solves:** Amazon destroys FBA inventory at its warehouses and is obligated to reimburse sellers — but only if the seller files a case. This query automates the identification of destroyed units that have not yet received a reimbursement, feeding an automated case-filing workflow before Amazon's 18-month claim window closes.

**Techniques used:**

- **Date-range anti-subquery pattern** — since Amazon does not maintain a direct foreign key between ledger events and reimbursement records, matching is done on `(asin, fnsku)` within a `[event_date, event_date + 30 days]` window. A LEFT JOIN identifies which events have a matching reimbursement; `NOT IN` excludes them.
- **Multi-level nested subqueries** — the exclusion set itself is built from a full correlated join, resulting in a 4-level deep query that is still readable due to explicit aliasing.
- **UNION ALL for dual-marketplace coverage** — US and CA are stored in separate Athena tables; results are combined in a single output.
- **Rolling date window** — `date_add('day', -6, date(now()))` ensures the query always targets the most recent complete data partition without hardcoded dates.

---

### `reimbursements/reimbursement_net_profit.sql`

**What it solves:** Amazon issues reimbursements, but can later claw them back via `Reimbursement_Reversal` or `Payment_Retraction` events — sometimes months after the original approval. Naive revenue reporting that sums all positive reimbursements overstates earnings. This query produces true net reimbursement P&L.

**Techniques used:**

- **LEFT JOIN on surrogate key** — reversals reference their original claim via `original_reimbursement_id`. Joining `reimbursement_id → original_reimbursement_id` matches each positive claim to its potential reversal without a time-range join.
- **Two-level aggregation** — inner aggregation at `(year, month, asin, fnsku)` preserves ASIN-level detail; outer aggregation rolls up to month-level P&L.
- **COALESCE for sparse reversals** — not every claim has a reversal; `COALESCE(SUM(reversal_amount), 0)` prevents NULL propagation in the net calculation.
- **NULLIF division guard** — `NULLIF(cnt, 0)` in the average-per-claim metric prevents division-by-zero.

---

### `inventory/fba_inventory_aging_by_team.sql`

**What it solves:** Identifies aged inventory by sourcing team, expressed in dollar value (units × COGS) across 5 Amazon standard aging buckets. Drives liquidation and repricing decisions and enables buyer accountability by team.

**Techniques used:**

- **ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC)** — an ASIN may have been sourced by multiple teams over time. `ROW_NUMBER()` isolates the most recent team assignment per ASIN without collapsing the join to a GROUP BY that would lose the cost data.
- **Separated COGS join** — team assignment and average cost come from different subqueries of the same `supplier_orders_us` table. Splitting them avoids cross-join inflation when one ASIN has many orders.
- **COALESCE on dollar buckets** — handles ASINs with no matched cost record gracefully, defaulting to zero rather than NULL-propagating through the sum.

---

### `inventory/fba_fee_changes_tracker.sql`

**What it solves:** Amazon occasionally changes FBA fulfilment fees without adequate seller notification. This query detects period-over-period fee changes per ASIN and fee type, quantifying the impact in absolute dollars and percentage terms.

**Techniques used:**

- **LAG() window function** — `LAG(avg_fee_per_item, 1) OVER (PARTITION BY asin, sku, amount_description ORDER BY settlement_start_date)` retrieves the prior period's fee for the same (sku, fee type) combination in a single pass, eliminating the need for a self-join.
- **ROW_NUMBER() deduplication** — multiple payments rows can map to the same (settlement, sku, fee type) due to partial shipments. Averaging within the period before applying `LAG()` ensures one clean anchor per period.
- **Multi-table join chain** — raw payments → settlement metadata → SKU/ASIN mapping table, joined across three tables with explicit alias names for clarity.
- **ABS() on delta percent** — fee decreases are reported as positive percentages for consistent sorting.

---

### `inventory/avg_cost_gap_fill.sql`

**What it solves:** Supplier orders arrive irregularly, leaving months with no recorded cost per unit. These NULLs break margin calculations that require a continuous monthly COGS time series. This query fills gaps using the nearest non-zero cost value on either side of each gap.

**Techniques used:**

- **LEAD() / LAG() with conditional partition** — partitioning by `(sku, year_, value != 0)` surfaces the nearest non-zero neighbours above and below each gap row in a single window pass.
- **CROSS JOIN gap-fill** — a self-CROSS JOIN on the integer primary key `pk` generates all `(null_row, candidate_fill_row)` pairs within the same year-sku partition, enabling selection of the chronologically closest non-zero anchor.
- **No UDFs or procedural code** — the entire gap-fill is expressed in standard Presto SQL, making it portable and schedulable as a plain Athena query.

---

### `inventory/inventory_vs_projected_sales.sql`

**What it solves:** Identifies SKUs at risk of stockout by comparing current FBA inventory (active + inbound) against a 30-day projected demand based on the last 7 days of shipped units.

**Techniques used:**

- **Subquery scalar references** — `(SELECT MAX(...) FROM ...)` used inline in the SELECT list to stamp each row with the snapshot date without a separate CTE.
- **Composite inventory sum** — fulfillable + reserved + three inbound stages are summed into a single `total_available` figure per SKU.
- **SPLIT_PART for brand extraction** — SKU naming convention `BRAND-MODEL-SIZE` allows extracting the brand prefix via `split_part(sku, '-', 1)` for team attribution without a separate SKU master table.
- **UNION ALL for US + CA** — each marketplace has separate source tables; results are combined into a single ranked output sorted by urgency (most negative days-of-supply at top).

---

### `marketing/marketing_dashboard.sql`

**What it solves:** Joins Amazon Advertising campaign data with the monthly P&L snapshot to produce a comprehensive marketing efficiency report per ASIN: ROAS, ACOS, ad-attributed unit share, revenue share, and ad profit contribution to gross margin.

**Techniques used:**

- **ROW_NUMBER() for product group deduplication** — Amazon's inventory planning table can have multiple `product_group` labels per ASIN as inventory is reclassified. `ROW_NUMBER() OVER (PARTITION BY asin ORDER BY snapshot_date DESC)` keeps only the most recent group label per ASIN.
- **NULLIF() division guard** — replaces the original `is_infinite()` Presto function with the ANSI-standard `NULLIF(denominator, 0)` to prevent division-by-zero in ROAS/ACOS calculations on ASINs with zero attributed sales.
- **Multi-dimensional profit formula** — `profit_ads = attributed_sales − ad_spend − fees_on_ad_units − COGS_on_ad_units` integrates four cost components in a single computed column.
- **SPLIT_PART brand join** — brand dimension joined on the extracted SKU prefix for team-level rollup.
- **UNION ALL for US + CA** with a `marketplace` label column for BI tool filtering.

---

### `profit_analysis/new_products_profitability.sql`

**What it solves:** Identifies newly profitable products — ASINs that had zero cumulative gross margin in the prior 6 months (still in the launch / investment phase) but turned profitable in a target reporting period. These products have just crossed break-even and warrant increased investment.

**Techniques used:**

- **SUM() OVER (PARTITION BY asin) for conditional filtering** — computes the rolling 6-month total per ASIN as a window aggregate without collapsing the result set. Filtering on `sum_over = 0` then isolates the "zero-profit" ASIN set without a GROUP BY, preserving row-level detail for the subsequent join. This avoids a correlated subquery or a two-pass approach.
- **LEFT JOIN as existence check** — the join between current-period profitable ASINs and the prior-window zero-profit set acts as a semi-join; `COALESCE(joined_asin, 'None') != 'None'` filters to the overlap.
- **Cross-year window for Canada** — the CA prior window spans a year boundary (Sep–Dec 2023 + Jan–Feb 2024), requiring a `UNION ALL` of two separate date-range filters inside the window CTE.

---

### `profit_analysis/master_pnl_engine_us.sql`

**What it solves:** The single most complex query in this repository. Produces a complete monthly P&L row per ASIN combining every revenue and cost component: sales revenue, 6 fee types, 20+ refund line items, returns, removals, reimbursements (two sources), inventory discrepancies, COGS from supplier orders, and cross-marketplace gross sales for CA and MX (USD-equivalent). Used as the source-of-truth table for all downstream profitability reports.

**Techniques used:**

- **Multi-index CROSS JOIN spine** — generates the complete `(asin × year × month)` grid by cross-joining the SKU/ASIN dimension with a `VALUES`-based year table and a `VALUES`-based month table. This ensures every ASIN appears for every calendar period even with zero activity, preventing silent gaps in period-over-period comparisons.
- **Three-way INNER JOIN for payment accuracy** — `payments_order_dates` INNER JOIN `payments_order_quantities` INNER JOIN `payments_reporting_orders_usd_us_transpose` all on `(order_id, sku)`. The INNER JOIN (not LEFT) ensures quantity and fee columns come from the same matched order event, preventing cross-contamination in the aggregate.
- **Pre-pivoted transpose table** — Amazon payment reports are row-per-fee-type. A pre-computed Athena CTAS (`payments_reporting_orders_usd_us_transpose`) pivots 27 fee types into columns. This query then sums each with `COALESCE`, producing clean monthly totals without dynamic SQL.
- **10+ parallel LEFT JOINs onto the spine** — all data sources (returns, removals, reimbursements ×2, payments, refunds, discrepancies, COGS, CA data, MX data) are LEFT JOINed to the spine, with all NULL values coalesced to 0. This design ensures missing data components never suppress an ASIN from the output.
- **COALESCE-heavy defensive coding** — every metric uses `COALESCE(..., 0)` to handle periods where a component has no data, keeping arithmetic safe throughout the downstream P&L calculation.

---

## Part 2 — ClickHouse: Support Analytics at ivi.ru

**Context:** ivi.ru is Russia's largest video-on-demand platform. The support analytics stack runs on ClickHouse, querying billions of rows of behavioural events and support ticket data. The two queries below were used for daily operational dashboards and monthly executive reporting.

---

### `clickhouse-support-analytics/contact_rate_by_category_7day_avg.sql`

**What it solves:** Tracks the daily 7-day moving average contact rate (CR) per support ticket category, normalised per 100,000 daily active users. A rising CR in a specific category (e.g. Technical) signals a product incident requiring escalation, independent of overall traffic growth.

**Techniques used:**

- **arrayJoin(range(7))** — ClickHouse's native sliding-window technique. Exploding each date into 7 rows assigns each ticket or user-day to the rolling windows it belongs to, enabling a 7-day moving average without a self-join or window function frame.
- **uniq()** — ClickHouse's approximate distinct count (HyperLogLog). Chosen over `COUNT(DISTINCT)` for performance on the billion-row `groot3.events` table. The ~2% error margin is acceptable for a rate metric.
- **dictGetInt64OrDefault('family', 'parent_ivi_id', ...)** — dictionary join that resolves child account IDs to the household parent, so a family sharing a subscription counts as 1 DAU rather than N.
- **argMax(value, timestamp)** — returns the most-recently-updated field value per Pyrus ticket. Pyrus tickets are mutable (categories and channels can change); `argMax` ensures the final state is used.
- **UNION ALL of ITSM + Pyrus** — two separate ticket systems are merged into a unified ticket base before aggregation, with timezone correction applied to each (`dateAdd(hour, -3/+3)`).
- **INNER JOIN on date** — events (DAU) and tickets are joined on the exploded `date_events = date_itsm` key, ensuring the CR is always computed against the correct rolling window.

---

### `clickhouse-support-analytics/support_tickets_monthly_pivot.sql`

**What it solves:** Produces a wide-format monthly pivot of unique ticket counts by category and sub-category, used for executive monthly review presentations and trend tracking.

**Techniques used:**

- **Manual CASE WHEN pivot** — ClickHouse does not support native PIVOT syntax. `COUNT(DISTINCT CASE WHEN toStartOfMonth(date) = 'YYYY-MM-01' THEN id END)` per month column is the standard ClickHouse workaround, equivalent to SQL PIVOT and fully portable.
- **argMax for mutable ticket fields** — same pattern as the CR query; ensures Pyrus ticket categories reflect their final state.
- **UNION ALL source merge** — ITSM and Pyrus unified before pivoting, with per-source timezone normalization.

## Part 3 — Oracle SQL: Banking CRM & Loan Operations ETL at GBC

**Context:** GBC is a large Russian retail bank. The analytics stack is built on Oracle Financial Services Analytical Applications (OFSAA), a standard enterprise banking data platform. All source data is queried via Oracle database links (`@ehd_prod`) connecting to the production OFSAA schema. The queries build standardised CRM event rows that feed a bank-wide operations analytics platform tracking process performance, channel distribution, and product coverage.

---

### `retail-accounts/current_account_open_etl.sql`

**What it solves:** Extracts individual retail client (FL) current account opening events from the OFSAA operational system and transforms them into a standardised CRM event model. A new row appears in the output table for each account opened on the target date, with full product hierarchy, branch attribution, operator channel, and masked customer contact data.

**Techniques used:**

- **15-table join chain** — traverses OFSAA's normalised schema across: deal → product → product_group → account → customer_united → customer_physical → phone → operator → office → org_structure_star → product_star → channel mapping tables. Each join resolves one piece of the denormalised output row.
- **Time-valid operator join** — `STG_DEAL_OPERATORS WHERE account.open_dt BETWEEN d_open AND d_close` resolves which employee was responsible for the account on the specific event date, handling staff reassignments over a deal's lifetime without a subquery.
- **Channel derivation via three-step lookup chain** — channel is not stored on the deal; it is derived by: `operator.descr → GRSET_CURRENTACC_CHANNEL (operator→channel map) → grset_operation_channel (code→display name)`. Online-banking-initiated events (TIBCO, IBANK, RBR) are excluded from branch reporting at the WHERE clause.
- **NVL2 for flag derivation** — `NVL2(channel_name, 'N', 'Y')` elegantly derives both `TARGET_CHANNEL_FLAG` and `FRONT_OFFICE_FLAG` in a single expression: 'N' if a channel was resolved (online), 'Y' if no channel matched (in-person/branch).
- **Oracle DB links (@ehd_prod)** — all OFSAA source tables are accessed via database link to the production instance, a standard pattern in Oracle-based banking ETL where the analytics schema is isolated from operational systems.
- **`/*+ parallel(4) */` hint** — forces 4 parallel execution threads for the remote DB link read, overriding Oracle's default single-threaded behaviour for cross-instance queries.

---

### `loan-operations/early_repayment_event_etl.sql`

**What it solves:** Extracts partial and full early loan repayment events (ЧДП/ПДП) and resolves the correct end-of-day account balance at the time of each repayment. The core challenge: when a loan is migrated between core banking systems, the account_id on the contract changes — a naive join would return zero balance for migrated loans. The query tracks migration history to always retrieve the balance from the correct account.

**Techniques used:**

- **5-CTE chain (WITH clause)** — the query is structured as a sequential pipeline:
  `ro` (dedup source) → `acc_seed` (authoritative events) → `end_mig` (migration timeline) → `ost_contr` (balance during migration) → `acc_mig_1` (principal account balance) → `tel_num` (phone lookup) → main SELECT.
- **ROW_NUMBER() for source deduplication** — `ROW_NUMBER() OVER (PARTITION BY contract_id, dt ORDER BY prov_dt DESC)` eliminates duplicate provisional entries in the source repayment table, keeping only the most recently committed row per (contract, date).
- **LAG() for migration window boundary generation** — `LAG(migration_date + 1) OVER (PARTITION BY contract_id ORDER BY migration_date)` derives the start of each migration window from the previous migration's end date, enabling a date-range join without needing to pre-compute window boundaries separately.
- **Date-range join with NVL-bounded intervals** — `acc_seed.dt BETWEEN NVL(start_dt, dt_bgn) AND NVL(migration_date, dt_end)` handles open-ended first and last migration periods (where one boundary is NULL) without CASE WHEN branching or separate UNION blocks.
- **Time-valid product hierarchy** — the product name join is doubly constrained: `acc_seed.dt BETWEEN ma_product.date_from AND ma_product.date_to` AND `acc_seed.dt BETWEEN pr_star.date_from AND pr_star.date_to`, ensuring the product label reflects what was in the bank's catalogue on the specific transaction date, not the current state.
- **Oracle DB links + `/*+ parallel(4) */`** — same remote execution pattern as the accounts ETL, applied across all OFSAAIATOM and DTO schema references.

---

## SQL Patterns Reference

| Pattern | Description | Files |
|---------|-------------|-------|
| Date-range anti-JOIN | Match events to records within a time window; exclude matched rows | `destroyed_inventory_reimbursement_finder.sql` |
| Surrogate key reversal join | Join positive and negative records via `original_id → id` | `reimbursement_net_profit.sql` |
| ROW_NUMBER() dedup | Keep one row per partition (latest record, highest priority) | `fba_inventory_aging_by_team.sql`, `marketing_dashboard.sql`, `fba_fee_changes_tracker.sql`, `early_repayment_event_etl.sql` |
| LAG() period-over-period delta | Compare current row to previous period without self-join | `fba_fee_changes_tracker.sql` |
| LAG() migration window boundary | Derive interval start from previous row's end date | `early_repayment_event_etl.sql` |
| SUM() OVER window filter | Aggregate over a partition without GROUP BY; filter on the result | `new_products_profitability.sql` |
| LEAD/LAG gap-fill + CROSS JOIN | Fill NULL time-series gaps with nearest non-zero neighbour | `avg_cost_gap_fill.sql` |
| Multi-index CROSS JOIN spine | Generate complete (entity × year × month) calendar grid | `master_pnl_engine_us.sql` |
| Three-way INNER JOIN | Ensure multi-table aggregation uses consistent matched records | `master_pnl_engine_us.sql` |
| Pre-pivoted transpose table | 27-column fee pivot consumed by column-level SUM + COALESCE | `master_pnl_engine_us.sql` |
| NULLIF() division guard | Prevent division-by-zero in ratio metrics | `reimbursement_net_profit.sql`, `marketing_dashboard.sql`, `fba_fee_changes_tracker.sql` |
| UNION ALL multi-marketplace | Combine results from separate marketplace tables | All Athena queries |
| 5-CTE pipeline | Sequential data transformation chain (dedup → resolve → enrich) | `early_repayment_event_etl.sql` |
| Time-valid join (BETWEEN d_open AND d_close) | Resolve dimensional attributes valid on a specific event date | `current_account_open_etl.sql`, `early_repayment_event_etl.sql` |
| NVL2 flag derivation | Derive binary flag from presence/absence of a joined value | `current_account_open_etl.sql` |
| Oracle DB links (@prod) | Cross-instance remote table access in banking ETL | `current_account_open_etl.sql`, `early_repayment_event_etl.sql` |
| `/*+ parallel(N) */` hint | Force parallel execution for large remote DB link reads | `current_account_open_etl.sql`, `early_repayment_event_etl.sql` |
| arrayJoin(range(N)) sliding window | ClickHouse native moving average without a self-join | `contact_rate_by_category_7day_avg.sql` |
| uniq() approximate distinct | HyperLogLog COUNT(DISTINCT) for performance on large tables | `contact_rate_by_category_7day_avg.sql` |
| dictGet dictionary join | ClickHouse dictionary lookup replacing a dimension table join | `contact_rate_by_category_7day_avg.sql` |
| argMax(value, timestamp) | Latest-state value for mutable records in ClickHouse | `contact_rate_by_category_7day_avg.sql`, `support_tickets_monthly_pivot.sql` |
| CASE WHEN manual pivot | Month-column pivot without native PIVOT syntax | `support_tickets_monthly_pivot.sql` |

---

## Notes

- Table and database names have been sanitised for public sharing; the query logic is unchanged.
- All Athena queries use Presto/Trino SQL syntax. Minor adjustments may be needed for other ANSI SQL engines (e.g. `date_add` → `DATEADD` in Spark SQL).
- ClickHouse queries use functions specific to ClickHouse 21.x+.
- Oracle queries use Oracle 12c+ syntax and assume OFSAA schema structure with `@ehd_prod` database link access.
