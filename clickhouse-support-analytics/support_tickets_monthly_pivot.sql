/*
  support_tickets_monthly_pivot.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : ClickHouse
  Domain : Customer Support Analytics — Monthly Ticket Volume Pivot
  Company: ivi.ru (Russia's largest video streaming service)

  WHAT IT DOES
  ─────────────
  Produces a wide-format monthly pivot of unique support ticket counts,
  sourced from the merged ITSM + Pyrus dataset (see contact_rate query for
  the full data model). Each column is one calendar month; each row is a
  ticket category (service_type) and sub-category (service_folder_2).

  USE CASE
  ─────────
  This pivot feeds directly into executive dashboards and monthly support
  review presentations, enabling quick month-over-month trend comparison
  without requiring the reader to pivot the data themselves in Excel.

  PATTERN — manual column pivot with CASE WHEN
  ─────────────────────────────────────────────
  ClickHouse does not support a native PIVOT syntax (as of the query's
  authoring date). The standard workaround is:
    COUNT(DISTINCT CASE WHEN toStartOfMonth(date) = 'YYYY-MM-01'
                        THEN ticket_id END) AS month_label
  applied once per calendar month. This is equivalent to a SQL PIVOT but
  works across all SQL dialects that support conditional aggregation.

  NOTE: Add/remove CASE WHEN blocks to extend or narrow the month range.
*/

SELECT
    service_type,
    service_folder_2,
    -- ── 2020 ──────────────────────────────────────────────────────────────
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-01-01'
                        THEN service_number END)                      AS jan_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-02-01'
                        THEN service_number END)                      AS feb_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-03-01'
                        THEN service_number END)                      AS mar_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-04-01'
                        THEN service_number END)                      AS apr_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-05-01'
                        THEN service_number END)                      AS may_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-06-01'
                        THEN service_number END)                      AS jun_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-07-01'
                        THEN service_number END)                      AS jul_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-08-01'
                        THEN service_number END)                      AS aug_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-09-01'
                        THEN service_number END)                      AS sep_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-10-01'
                        THEN service_number END)                      AS oct_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-11-01'
                        THEN service_number END)                      AS nov_2020,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2020-12-01'
                        THEN service_number END)                      AS dec_2020,
    -- ── 2021 ──────────────────────────────────────────────────────────────
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2021-01-01'
                        THEN service_number END)                      AS jan_2021,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2021-02-01'
                        THEN service_number END)                      AS feb_2021,
    COUNT(DISTINCT CASE WHEN toStartOfMonth(true_interaction_date) = '2021-03-01'
                        THEN service_number END)                      AS mar_2021
FROM (
    -- ── ITSM tickets ──────────────────────────────────────────────────────
    SELECT
        service_number,
        toDate(dateAdd(hour, -3, interaction_date))                   AS true_interaction_date,
        service_type,
        service_folder_2
    FROM `exter`.itsm_service

    UNION ALL

    -- ── Pyrus tickets (latest state of each ticket) ───────────────────────
    SELECT
        id                                                            AS service_number,
        toDate(dateAdd(hour, 3, p.create_date))                       AS true_interaction_date,
        argMax(category_1,  p.last_modified_date)                     AS service_type,
        argMax(category_2,  p.last_modified_date)                     AS service_folder_2
    FROM `ext`.pyrus AS p
    GROUP BY id, toDate(dateAdd(hour, 3, p.create_date))
)
GROUP BY service_type, service_folder_2
ORDER BY service_type, service_folder_2;
