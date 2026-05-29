/*
  early_repayment_event_etl.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine  : Oracle SQL (WITH clause / CTE + DB Link @ehd_prod)
  Platform: Oracle Financial Services Analytical Applications (OFSAA)
  Domain  : Retail Banking — Loan Operations (Early Repayment Events)
  Company : GBC (large Russian bank)

  WHAT IT DOES
  ─────────────
  Extracts partial and full early loan repayment events (ЧДП/ПДП — Частичное
  Досрочное Погашение / Полное Досрочное Погашение) from the banking
  operational system and transforms them into the standardised CRM event model.

  The query handles a complex accounting challenge: when a loan is migrated
  between core banking systems, the account_id on the contract changes.
  Balance lookups must track across both the old and the new account_id to
  retrieve the correct end-of-day balance at the time of the repayment event.

  CTE CHAIN (5 steps)
  ─────────────────────
  ro (deduplicated repayment events)
  ╰── ROW_NUMBER() OVER (PARTITION BY contract_id, dt ORDER BY prov_dt DESC)
      Deduplicates the source table pdb_chdp_acc29, which can have multiple
      provisional entries per (contract, date). Keeps the most recently
      provisioned row (latest prov_dt) as the authoritative event.

  acc_seed (authoritative event rows)
  ╰── Filters ro WHERE numm = 1. This is the driving table for all
      subsequent joins — one row per (contract_id, repayment_date).

  end_mig (migration periods per contract)
  ╰── Joins STG_ACCOUNT_CONTRACTS to MIGRATION_ACC_LINK to build a timeline
      of which account_id was active for which date range per contract.
      LAG(migration_date + 1) produces the start of each migration window,
      enabling date-range joins without explicit window boundaries.

  ost_contr (end-of-day balance during migration)
  ╰── Joins the balance table (ER_ost, account 100-22) to end_mig using a
      date-range predicate: dt BETWEEN NVL(start_dt, dt_bgn) AND
      NVL(migration_date, dt_end). The NVL handles the first/last migration
      period where one boundary is NULL.

  acc_mig_1 (active loan account with balance for the event date)
  ╰── Resolves the principal loan account (link_type = 1) and retrieves the
      end-of-day balance for the repayment date from the balance table.
      Necessary because migrations change the account_id linked to the
      contract, so a simple join on contract_id alone would miss migrated data.

  tel_num (latest primary mobile phone per customer)
  ╰── ROW_NUMBER() OVER (PARTITION BY hid_party ORDER BY startdate DESC)
      Ensures one phone row per customer even when multiple history records
      exist. Filtered to primary_flag = 1 and phone_type = 3 (mobile).

  MAIN SELECT — ~15-table join
  ──────────────────────────────
  Joins all CTEs together with:
    • OFSAA office star (branch name and address)
    • OFSAA operator table (employee name)
    • HFCDI customer tables (individual name, masked phone)
    • OFSAA product hierarchy (product name via MA_product + product_star)
    • Currency lookup (internal code → ISO code)

  KEY ORACLE TECHNIQUE — LAG() for migration window boundaries
  ─────────────────────────────────────────────────────────────
  LAG(migration_date + 1) OVER (PARTITION BY contract_id ORDER BY migration_date)
  produces the day after the previous migration as the start of the current
  period. Combined with NVL(..., dt_bgn) this covers the first migration
  segment where there is no preceding row.

  KEY TECHNIQUE — date-range join on NVL-bounded intervals
  ──────────────────────────────────────────────────────────
  acc_seed.dt BETWEEN NVL(end_mig.start_dt, end_mig.dt_bgn)
                  AND NVL(migration_date,   end_mig.dt_end)
  handles open-ended intervals (first/last period) gracefully without
  separate UNION or CASE WHEN branches.

  KEY ORACLE TECHNIQUE — /*+ parallel(4) */ hint
  ───────────────────────────────────────────────
  Applied on the final SELECT to parallelise remote DB link reads against
  the production OFSAA instance.
*/

-- create table ER_final_early_repay as  -- uncomment for materialization

WITH ro AS (
    -- Step 1: Deduplicate source repayment events
    -- Multiple provisional rows can exist per (contract_id, date).
    -- Keep only the most recently provisioned (highest prov_dt).
    SELECT
        pdb_chdp_acc29.*,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id, dt
            ORDER BY prov_dt DESC
        ) AS numm
    FROM pdb_chdp_acc29
    WHERE operator_cre_id != 27877  -- exclude system/automated entries
),

acc_seed AS (
    -- Step 2: Authoritative event rows (one per contract × date)
    SELECT
        source_id, id, dt, sem_id, db_acc, ag_id,
        db_cust_id, contract_id, operator_cre_id, db_accnt_id
    FROM ro
    WHERE numm = 1
),

end_mig AS (
    -- Step 3: Build migration period timeline per contract
    -- LAG produces the start of the current window from the previous migration date.
    SELECT
        t1.contract_id,
        t1.account_id,
        t1.link_type,
        t1.dt_bgn,
        t1.dt_end,
        t2.migration_date,
        LAG(migration_date + 1) OVER (
            PARTITION BY contract_id ORDER BY migration_date
        ) AS start_dt  -- day after previous migration = start of this window
    FROM OFSAAIATOM.STG_ACCOUNT_CONTRACTS_OPN@ehd_prod t1
    LEFT JOIN OFSAAIATOM.MIGRATION_ACC_LINK@ehd_prod t2
        ON t1.account_id = t2.old_acc_id
    WHERE link_type = 457  -- special contract-account link type for repayment accounts
      AND fl_del    = 0
      AND t1.type   = 'ActiveDeal'
),

ost_contr AS (
    -- Step 4: End-of-day balance on the repayment date (migration-aware)
    -- Date-range join with NVL handles open-ended first/last period.
    SELECT *
    FROM ER_ost  -- pre-aggregated balance table for account prefix 10022
    LEFT JOIN end_mig
        ON  ER_ost.accnt_id = end_mig.account_id
        AND ER_ost.dt BETWEEN NVL(end_mig.start_dt, end_mig.dt_bgn)
                          AND NVL(end_mig.migration_date, end_mig.dt_end)
),

acc_mig_1 AS (
    -- Step 5: Principal loan account (link_type = 1) + balance at event date
    SELECT
        t1.contract_id, t1.account_id, t1.link_type,
        t1.dt_bgn, t1.dt_end, t3.dt, t3.acct_blnc_eod
    FROM OFSAAIATOM.STG_ACCOUNT_CONTRACTS_OPN@ehd_prod t1
    LEFT JOIN acc_seed
        ON t1.contract_id = acc_seed.contract_id
    LEFT JOIN ofsaaiatom.stg_account_balances_opn@ehd_prod t3
        ON t1.account_id = t3.accnt_id AND acc_seed.dt = t3.dt
    WHERE t1.fl_del        = 0
      AND t1.link_type     = 1          -- principal loan account
      AND t1.type          = 'ActiveDeal'
      AND t3.source_id     = '3C'
      AND t3.acct_blnc_eod IS NOT NULL
),

tel_num AS (
    -- Step 6: Latest primary mobile phone per customer
    SELECT hid_phone, hid_party, startdate, is_deleted,
           countrycode, citycode, telephone,
           ROW_NUMBER() OVER (
               PARTITION BY hid_party ORDER BY startdate DESC
           ) AS rn
    FROM OFSAAIATOM.HFCDI_PHONE@ehd_prod
    WHERE is_deleted    = 0
      AND primary_flag  = 1
      AND phone_type    = 3  -- mobile
)

-- ── Main output: standardised CRM event row ───────────────────────────────
SELECT /*+ parallel(4) */
    acc_seed.id || '-' || acc_seed.source_id            AS OPERATION_ID,
    acc_seed.source_id                                  AS SOURCE_CODE,
    acc_seed.sem_id                                     AS OPERATION_SOURCE_ID,
    'Branch'                                            AS OPERATION_CHANNEL_CODE,
    acc_seed.dt                                         AS OPERATION_START_DTTM,
    acc_seed.dt                                         AS OPERATION_END_DTTM,
    'Loan Operations'                                   AS PROCESS_NAME,
    'Loan Operations'                                   AS PROCESS_STAGE_NAME,
    'Early Loan Repayment (Partial/Full)'               AS OPERATION_NAME,
    'Not Defined'                                       AS OPERATION_DIRECTION,
    'Not Defined'                                       AS OPERATION_METHOD_NAME,
    'Not Defined'                                       AS OPERATION_KIND_NAME,
    'Retail Banking'                                    AS OPERATION_TYPE_NAME,
    'Loan Operations'                                   AS OPERATION_GRP_NAME,
    'FL'                                                AS PARTYTYPE_CODE,  -- Individual
    0                                                   AS STANDARD_DURATION,
    'U'                                                 AS STANDARD_PASSED_FLAG,
    UPPER(pr_star.product_name)                         AS PRODUCT_NAME,
    UPPER(pr_star.group_name)                           AS PRODUCTGRP_NAME,
    'N'                                                 AS TARGET_CHANNEL_FLAG,
    'Y'                                                 AS FRONT_OFFICE_FLAG,
    'Completed'                                         AS OPERATION_STATUS_NAME,
    'N'                                                 AS CANCEL_FLAG,
    custun.hid_party                                    AS PARTY_ID,
    'Not Defined'                                       AS PARTY_SEGMENT_CODE,
    UPPER(tbl_phy.surname || ' ' || tbl_phy.name || ' ' || tbl_phy.patronymic)
                                                        AS PARTY_FULL_NAME,
    -- Masked phone: +CC(***)last-7-digits
    NVL2(tel_num.telephone,
         '+' || tel_num.countrycode || '(***)' || tel_num.telephone,
         NULL)                                          AS PARTY_PHONE_NO,
    'U'                                                 AS IB_REGISTRATION_FLAG,
    'U'                                                 AS MB_REGISTRATION_FLAG,
    grset_currency.target_code                          AS CURRENCY_CODE,
    -- Balance at time of repayment event (end-of-day balance on repayment date)
    NVL(ost_contr.Acct_Blnc_Eod, 0)                    AS OPERATION_NM_AMT,
    0                                                   AS OPERATION_EQ_RUB,
    0                                                   AS COMMISSION_NM_AMT,
    0                                                   AS COMMISSION_EQ_RUB,
    tbl_oper.id                                         AS EMPLOYEE_ID,
    0                                                   AS EMPLOYEE_CODE,
    UPPER(tbl_oper.descr)                               AS EMPLOYEE_FULL_NAME,
    tbl_office.id                                       AS ORGSTRUCT_BRANCH_ID,
    off_st.id                                           AS ORGSTRUCT_BRANCH_CODE,
    UPPER(off_st.office)                                AS ORGSTRUCT_BRANCH_NAME,
    0                                                   AS ATM_ID,
    UPPER(off_st.address)                               AS ADDINFO_DESC

FROM acc_seed

-- Resolve migration window for this repayment event date
LEFT JOIN end_mig
    ON  acc_seed.contract_id = end_mig.contract_id
    AND acc_seed.dt BETWEEN NVL(end_mig.start_dt, end_mig.dt_bgn)
                        AND NVL(end_mig.migration_date, end_mig.dt_end)

-- Principal loan account balance on event date
LEFT JOIN acc_mig_1
    ON acc_seed.contract_id = acc_mig_1.contract_id
   AND acc_seed.dt           = acc_mig_1.dt

-- Branch info
LEFT JOIN OFSAAIATOM.STG_OFFICES_OPN@ehd_prod tbl_office
    ON acc_seed.ag_id = tbl_office.id AND tbl_office.source_id = '3C'
LEFT JOIN dto.drc_office_star@ehd_prod off_st
    ON tbl_office.mngm_office_id = off_st.id

-- Operator (employee who processed the repayment)
LEFT JOIN ofsaaiatom.stg_operators_opn@ehd_prod tbl_oper
    ON acc_seed.operator_cre_id = tbl_oper.id AND tbl_oper.source_id = '3C'

-- Customer identity (unified record → physical person)
LEFT JOIN OFSAAIATOM.HFCDI_CUSTOMER_UNITED@ehd_prod custun
    ON acc_seed.db_cust_id = custun.id AND custun.fl_del = '0'
LEFT JOIN OFSAAIATOM.HFCDI_CUSTOMER_PHY@ehd_prod tbl_phy
    ON custun.hid_party = tbl_phy.hid_party AND tbl_phy.is_deleted = 0

-- Latest primary mobile phone (deduplicated via CTE)
LEFT JOIN tel_num
    ON custun.hid_party = tel_num.hid_party AND tel_num.rn = 1

-- Product hierarchy (time-valid: event date must fall within product validity)
LEFT JOIN dto.agg_ma_product@ehd_prod ma_product
    ON  acc_seed.contract_id       = ma_product.contract_id
    AND ma_product.account_id      = acc_mig_1.account_id
    AND acc_seed.dt BETWEEN ma_product.date_from AND ma_product.date_to
    AND ma_product.contract_type   = 'A'
LEFT JOIN dto.drc_product_star@ehd_prod pr_star
    ON ma_product.product_group_id = pr_star.group3_id
   AND acc_seed.dt BETWEEN pr_star.date_from AND pr_star.date_to

-- Repayment account balance on event date
LEFT JOIN ost_contr
    ON end_mig.account_id = ost_contr.accnt_id
   AND acc_seed.dt         = ost_contr.dt

-- Currency code resolution (internal Oracle code → ISO)
LEFT JOIN ofsaaiatom.stg_accounts_opn@ehd_prod acc
    ON acc_seed.db_accnt_id = acc.id
LEFT JOIN grset_currency
    ON acc.crnc = grset_currency.source_code
   AND acc_seed.source_id = grset_currency.source_id;
