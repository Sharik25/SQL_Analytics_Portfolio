/*
  contact_rate_by_category_7day_avg.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine : ClickHouse
  Domain : Customer Support Analytics — Contact Rate (CR) per 100K DAU
  Company: ivi.ru (Russia's largest video streaming service)

  WHAT IT DOES
  ─────────────
  Computes the daily 7-day moving average contact rate per support ticket
  category per 100,000 unique daily active users.

  Formula:
    CR_category = (7-day rolling unique tickets in category / 7-day rolling DAU)
                  × 100,000

  This normalises raw ticket volume against platform traffic so that a spike
  in CR reflects a genuine product issue, not just audience growth.

  DATA SOURCES (joined)
  ──────────────────────
  1. groot3.events          — ivi.ru page-impression event stream (ClickHouse)
                              Used to count DAU via unique device/user IDs.
  2. external.itsm_service  — ITSM ticketing system (Jira Service Desk export)
  3. external.pyrus         — Pyrus workflow tool (supplementary ticket source)
                              UNION ALL of ITSM + Pyrus gives full ticket coverage.

  KEY CLICKHOUSE TECHNIQUES
  ──────────────────────────
  arrayJoin(range(7))
    Explodes each date into 7 rows (one per day in the rolling window), so
    that a ticket created on day D is counted in the windows for D, D+1, …, D+6.
    This is ClickHouse's native sliding-window approach without a self-join.

  uniq()
    Approximate distinct count (HyperLogLog). Used instead of COUNT(DISTINCT)
    for performance on billion-row event tables. Acceptable precision for CR.

  dictGetInt64OrDefault('family', 'parent_id', ...)
    Dictionary lookup: resolves child account IDs to the household parent ID,
    ensuring a family sharing one subscription counts as 1 unique user, not N.

  argMax(value, timestamp)
    Returns the value associated with the maximum timestamp — used to pick the
    most recent category/channel assignment for each Pyrus ticket, which can be
    updated over time.

  TICKET CATEGORIES (service_type values translated from Russian)
  ───────────────────────────────────────────────────────────────
    tech_class      — Technical issues (playback errors, app crashes)
    fin_class       — Financial / billing issues (payment, subscription)
    cont_class      — Content requests / complaints
    prod_class      — Product feedback
    partners_class  — Partner / B2B enquiries

  CHANNELS INCLUDED (contact channels)
    Mail, App (iOS/Android), Phone, Chat, Telegram, Facebook, Viber
    (Internal / bot / automation channels excluded)
*/

SELECT
    date_events,
    avg_mov_7_events                                                  AS dau_7day_avg,
    -- Contact rate per 100K DAU by ticket category
    (avg_mov_7_tech_class    / avg_mov_7_events) * 100000             AS cr_technical,
    (avg_mov_7_fin_class     / avg_mov_7_events) * 100000             AS cr_financial,
    (avg_mov_7_cont_class    / avg_mov_7_events) * 100000             AS cr_content,
    (avg_mov_7_prod_class    / avg_mov_7_events) * 100000             AS cr_product,
    (avg_mov_7_partners_class / avg_mov_7_events) * 100000            AS cr_partners
FROM (
    -- ── DAU: 7-day moving average from the events stream ──────────────────
    SELECT DISTINCT
        rocket_date + arrayJoin(range(7))                             AS date_events,
        uniq(
            dictGetInt64OrDefault(
                'family', 'parent_id',
                toUInt64(ivi_id), toInt64(ivi_id)
            )
        )                                                             AS uniqs,
        uniq(
            toString(
                dictGetInt64OrDefault('family','parent_id',
                    toUInt64(i_id), toInt64(i_id))
            ) || ',' || toString(rock_date)
        )                                                             AS uniqdays,
        (uniqdays / 7)                                                AS avg_mov_7_events
    FROM gt3.events
    WHERE
        name         = 'xxxxxx'
        AND country  = 'Russia'
        -- 14-day warm-up so that the first reported date has a full 7-day window
        AND rocket_date >= toDate('2019-12-25') - 14
        AND rocket_date  < today()
        AND date_events >= toDate('2019-12-25')
        AND date_events  < today()
    GROUP BY date_events
) AS events

INNER JOIN (
    -- ── Tickets: ITSM + Pyrus merged, 7-day moving average by category ────
    SELECT DISTINCT
        toDate(true_interaction_date) + arrayJoin(range(7))           AS date_itsm,
        uniq(service_number)                                          AS uniqs,
        uniq(
            toString(service_number) || ',' || toString(true_interaction_date)
        )                                                             AS uniqdays,
        -- Category-level rolling unique counts
        uniq(CASE WHEN service_type = 'Technical issue'
             THEN toString(service_number) || ',' || toString(true_interaction_date) END)
                                                                      AS uniqdays_tech_class,
        uniq(CASE WHEN service_type = 'Financial issue'
             THEN toString(service_number) || ',' || toString(true_interaction_date) END)
                                                                      AS uniqdays_fin_class,
        uniq(CASE WHEN service_type = 'Content'
             THEN toString(service_number) || ',' || toString(true_interaction_date) END)
                                                                      AS uniqdays_cont_class,
        uniq(CASE WHEN service_type = 'Product'
             THEN toString(service_number) || ',' || toString(true_interaction_date) END)
                                                                      AS uniqdays_prod_class,
        uniq(CASE WHEN service_type = 'Partners'
             THEN toString(service_number) || ',' || toString(true_interaction_date) END)
                                                                      AS uniqdays_partners_class,
        -- 7-day moving averages
        (uniqdays / 7)                                                AS avg_mov_7_itsm,
        (uniqdays_tech_class     / 7)                                 AS avg_mov_7_tech_class,
        (uniqdays_fin_class      / 7)                                 AS avg_mov_7_fin_class,
        (uniqdays_cont_class     / 7)                                 AS avg_mov_7_cont_class,
        (uniqdays_prod_class     / 7)                                 AS avg_mov_7_prod_class,
        (uniqdays_partners_class / 7)                                 AS avg_mov_7_partners_class
    FROM (
        -- ITSM tickets
        SELECT
            service_number,
            toDate(dateAdd(hour, -3, interaction_date))               AS true_interaction_date,
            service_type,
            service_folder_2,
            service_channel
        FROM `external`.itsm_service

        UNION ALL

        -- Pyrus tickets (take most-recently-updated category values)
        SELECT
            id                                                        AS service_number,
            toDate(dateAdd(hour, 3, p.create_date))                   AS true_interaction_date,
            argMax(category_1,          p.last_modified_date)         AS service_type,
            argMax(category_2,          p.last_modified_date)         AS service_folder_2,
            argMax(p.application_source,p.last_modified_date)         AS service_channel
        FROM `external`.pyrus AS p
        GROUP BY id, toDate(dateAdd(hour, 3, p.create_date))
    )
    WHERE
        toDate(true_interaction_date) >= toDate('2019-12-25') - 14
        AND toDate(true_interaction_date)  < today()
        AND date_itsm >= toDate('2019-12-25')
        AND date_itsm  < today()
        -- Exclude internal / system / unclassified ticket types
        AND service_type NOT IN ('Internal', 'Automation', 'Test', 'Moderation', 'None')
        -- Only real customer-facing channels
        AND service_channel IN (
            'Mail', 'App (iOS)', 'App (Android)', 'Phone',
            'Chat', 'Telegram', 'Facebook', 'Viber'
        )
    GROUP BY date_itsm
) AS tickets
    ON events.date_events = tickets.date_itsm

ORDER BY date_events;
