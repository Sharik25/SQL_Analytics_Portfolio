/*
  current_account_open_etl.sql
  ─────────────────────────────────────────────────────────────────────────────
  Engine  : Oracle SQL (via Oracle Database Link @ehd_prod)
  Platform: Oracle Financial Services Analytical Applications (OFSAA)
  Domain  : Retail Banking — CRM Operations Event Feed
  Company : GBC (large Russian bank)

  WHAT IT DOES
  ─────────────
  Extracts current account opening events for individual retail clients
  (физические лица / FL) and transforms them into a standardised CRM
  operations event row. The output table feeds the bank's CRM analytics
  platform, which tracks process performance, channel distribution, and
  product group coverage for branch operations.

  OUTPUT SCHEMA (standardised CRM event model)
  ──────────────────────────────────────────────
  OPERATION_ID          – composite key: account_id + source_system + '-OPN'
  OPERATION_CHANNEL_CODE– derived from operator → channel mapping table
  PRODUCT_NAME          – product hierarchy resolved via OFSAA product star
  PRODUCTGRP_NAME       – product group from drc_product_star
  PARTY_FULL_NAME       – masked: surname + name + patronymic (N/A if missing)
  PARTY_PHONE_NO        – masked phone: +CC(***)<last-7-digits>
  ORGSTRUCT_BRANCH_NAME – branch name from org structure star table
  CURRENCY_CODE         – mapped from internal currency code (810 → RUR)

  ARCHITECTURE — 15-table join chain
  ────────────────────────────────────
  The query must traverse OFSAA's normalised schema to resolve:
    deal → deal_type → product → product_group
    deal → account → operator → office → org_structure
    deal → customer_united → customer_physical (name, deleted flag)
    deal → customer_united → phone (primary phone)
    account → deal_operator (time-valid: open_dt BETWEEN d_open AND d_close)
    deal → contract → passive_deal → office (for address resolution)
    passive_deal → MA_product → product_star (for product name hierarchy)
    office → drc_office_star (branch code and address from reporting dimension)

  KEY ORACLE TECHNIQUE — database links (@ehd_prod)
  ───────────────────────────────────────────────────
  All OFSAAIATOM and DTO tables are queried via Oracle DB link to the
  production OFSAA schema. This pattern is common in banking ETL where the
  analytics schema is a separate Oracle instance from the operational system.

  KEY TECHNIQUE — time-valid operator join
  ─────────────────────────────────────────
  The operator who opened the account is resolved via:
    STG_DEAL_OPERATORS_OPN WHERE account.open_dt BETWEEN d_open AND d_close
  This handles staff reassignments: a deal may have multiple operators over
  its lifetime, and we need the one active on the specific event date.

  KEY TECHNIQUE — channel derivation via lookup chain
  ─────────────────────────────────────────────────────
  Channel is not stored directly on the deal. It is derived by:
    operator.descr → GRSET_CURRENTACC_CHANNEL (operator-to-channel map)
    → grset_operation_channel (channel code → display name)
  Operators in ('TIBCO', 'IBANK', 'RBR') are online-banking-initiated
  events and are excluded from branch-level operations reporting.

  KEY TECHNIQUE — NVL2 for target/front-office flag derivation
  ──────────────────────────────────────────────────────────────
  NVL2(oper_chann.operation_channel_name, 'N', 'Y')
    → 'N' if channel was resolved (online) → not a branch target-channel event
    → 'Y' if no channel match (branch / in-person) → is a front-office event

  KEY ORACLE HINT — /*+ parallel(4) */
  ──────────────────────────────────────
  Forces Oracle to use 4 parallel query processes against the DB link tables,
  which are remote and large. Without this hint the query runs single-threaded
  against the production OFSAA instance.
*/

DROP TABLE tbl_curracc_open;

CREATE TABLE tbl_curracc_open AS
SELECT /*+ parallel(4) */
    t8.id || '-' || t8.source_id || '-OPN'              AS OPERATION_ID,
    t1.source_id                                         AS SOURCE_CODE,
    '0'                                                  AS OPERATION_CODE,
    NVL(oper_chann.operation_channel_name, 'Unknown')    AS OPERATION_CHANNEL_CODE,
    t1.deal_fd                                           AS OPERATION_START_DTTM,
    '0'                                                  AS OPERATION_END_DTTM,
    'TBD'                                                AS PROCESS_NAME,
    'TBD'                                                AS PROCESS_STAGE_NAME,
    tbl_grprc.operation_name                             AS OPERATION_NAME,
    UPPER('Self-service')                                AS OPERATION_METHOD_NAME,
    tbl_grprc.operation_type_name                        AS OPERATION_TYPE_NAME,
    tbl_grprc.OPERATION_GRP_NAME                         AS OPERATION_GRP_NAME,
    t10.type_code                                        AS PARTYTYPE_CODE,
    '0'                                                  AS STANDART_DURATION,
    '0'                                                  AS STANDART_PASSED_FLAG,
    UPPER(pr_star.product_name)                          AS PRODUCT_NAME,
    UPPER(pr_star.group_name2)                           AS PRODUCTGRP_NAME,
    -- NVL2: channel resolved → online (N), no channel → branch (Y)
    NVL2(oper_chann.operation_channel_name, 'N', 'Y')    AS TARGET_CHANNEL_FLAG,
    NVL2(oper_chann.operation_channel_name, 'N', 'Y')    AS FRONT_OFFICE_FLAG,
    UPPER('Completed')                                   AS OPERATION_STATUS_NAME,
    'N'                                                  AS CANCEL_FLAG,
    t5.id                                                AS PARTY_ID,
    ' '                                                  AS PARTY_SEGMENT_CODE,
    UPPER(NVL(t6.surname || ' ' || t6.name || ' ' || t6.patronymic, 'N/A'))
                                                         AS PARTY_FULL_NAME,
    -- Masked phone: +CC(***)last-7-digits
    NVL2(t9.telephone, '+' || t9.countrycode || '(***)' || t9.telephone, NULL)
                                                         AS PARTY_PHONE_NO,
    '0'                                                  AS IB_REGESTRATION_FLAG,
    '0'                                                  AS MB_REGISTRATION_FLAG,
    (CASE WHEN t8.crnc = 810 THEN 'RUR' END)             AS CURRENCY_CODE,
    '0'                                                  AS OPERATION_NM_AMT,
    '0'                                                  AS OPERATION_EQ_RUB,
    'TBD'                                                AS COMMISION_NM_AMT,
    'TBD'                                                AS COMISSION_EQ_RUB,
    tbl_oper.id                                          AS EMPLOYEE_ID,
    '0'                                                  AS EMPLOYEE_CODE,
    UPPER(tbl_oper.descr)                                AS EMPLOYEE_FULL_NAME,
    UPPER(tbl_office.id)                                 AS ORGSTRUCT_BRANCH_ID,
    UPPER(drc_off.id)                                    AS ORGSTRUCT_BRANCH_CODE,
    UPPER(drc_off.office)                                AS ORGSTRUCT_BRANCH_NAME,
    '0'                                                  AS ATM_ID,
    UPPER(tbl_office.ag_addr)                            AS ADDINFO_DESC

FROM ofsaaiatom.stg_deals_opn@ehd_prod t1

-- Deal metadata
LEFT JOIN OFSAAIATOM.STG_DEAL_TYPES_OPN@ehd_prod    t2  ON t2.id = t1.deal_type_id
LEFT JOIN OFSAAIATOM.STG_PRODUCTS_OPN@ehd_prod       t3  ON t1.product_id = t3.id
LEFT JOIN OFSAAIATOM.STG_PRODUCT_GROUPS_OPN@ehd_prod t4  ON t3.product_group = t4.id

-- Customer: unified → physical person (name, masked)
LEFT JOIN OFSAAIATOM.HFCDI_CUSTOMER_UNITED@ehd_prod  t5  ON t1.cust_id = t5.id AND t5.fl_del = '0'
LEFT JOIN OFSAAIATOM.HFCDI_CUSTOMER_PHY@ehd_prod     t6  ON t5.hid_party = t6.hid_party AND t6.is_deleted = 0

-- Account linked to deal
LEFT JOIN OFSAAIATOM.STG_RL_ACCOUNT_DEALS@ehd_prod   t7  ON t1.id = t7.deal_id
LEFT JOIN ofsaaiatom.stg_accounts_opn@ehd_prod        t8  ON t7.account_id = t8.id AND t8.source_id IN ('3C', 'NOM')

-- Primary contact phone
LEFT JOIN OFSAAIATOM.HFCDI_PHONE@ehd_prod             t9  ON t5.hid_party = t9.hid_party
                                                          AND t9.is_deleted = 0 AND t9.primary_flag = 1

-- Party type code (individual / corporate)
LEFT JOIN grset_partytype_code                        t10 ON t5.party_type = t10.source_name

-- Operator valid on account open date (time-valid join)
LEFT JOIN OFSAAIATOM.STG_DEAL_OPERATORS_OPN@ehd_prod tbl_rl_deal_oper
    ON t1.id = tbl_rl_deal_oper.deal_id
   AND t8.open_dt BETWEEN tbl_rl_deal_oper.d_open AND tbl_rl_deal_oper.d_close
   AND tbl_rl_deal_oper.source_id IN ('3C', 'NOM')
LEFT JOIN ofsaaiatom.stg_operators_opn@ehd_prod tbl_oper
    ON tbl_rl_deal_oper.operator_id = tbl_oper.id AND tbl_oper.source_id IN ('3C', 'NOM')

-- Office resolution: deal → contract → passive_deal → office → org_structure
LEFT JOIN ofsaaiatom.stg_deal_contract_opn@ehd_prod tbl_deal_contr ON t1.id = tbl_deal_contr.deal_id
LEFT JOIN OFSAAIATOM.STG_PASSIVEDEAL_OFFICES_OPN@ehd_prod tbl_pass_off
    ON tbl_deal_contr.passivedeal_id = tbl_pass_off.passivedeal_id
   AND t8.open_dt BETWEEN tbl_pass_off.dt_bgn AND tbl_pass_off.dt_end
   AND tbl_pass_off.source_id IN ('3C', 'NOM')
LEFT JOIN OFSAAIATOM.STG_OFFICES_OPN@ehd_prod tbl_office
    ON tbl_office.id = tbl_pass_off.ag_id AND tbl_office.source_id IN ('3C', 'NOM')

-- Operation code from product group → lookup → operations catalogue
LEFT JOIN GRSET_CURRENTACC_OPERATION   GR_CURR_OPERATION ON GR_CURR_OPERATION.SOURCE_ID = t4.id
LEFT JOIN grprc_operation              tbl_grprc          ON GR_CURR_OPERATION.TARGET = tbl_grprc.operation_code

-- Channel derivation: operator name → channel map → channel display name
LEFT JOIN grset_operation_x_channel channel        ON channel.operation_code = tbl_grprc.operation_code
LEFT JOIN GRSET_CURRENTACC_CHANNEL  curr_chann     ON tbl_oper.descr = curr_chann.source
LEFT JOIN grset_operation_channel   oper_chann     ON oper_chann.operation_channel_code = curr_chann.target

-- Product name hierarchy from OFSAA product star
LEFT JOIN dto.agg_ma_product@ehd_prod ma_product
    ON tbl_deal_contr.passivedeal_id = ma_product.contract_id
   AND ma_product.account_id = t7.account_id
   AND t1.deal_fd BETWEEN ma_product.date_from AND ma_product.date_to
   AND ma_product.contract_type = 'P'
LEFT JOIN dto.drc_product_star@ehd_prod pr_star ON ma_product.product_group_id = pr_star.group3_id

-- Branch reporting dimension
LEFT JOIN dto.drc_office_star@ehd_prod drc_off ON tbl_office.mngm_office_id = drc_off.id

WHERE
    t1.source_id IN ('3C', 'NOM')
    -- Product group filter: current account product groups only
    AND t4.id IN (106, 19, 961, 3286, 381)
    -- Exclude online-banking-initiated events (tracked in separate pipeline)
    AND tbl_oper.descr NOT IN ('TIBCO', 'RBR', 'IBANK')
    -- Date range: events from 2017 onwards
    AND (t1.deal_fd >= DATE '2017-01-01'
         OR NVL(t1.deal_closed_date, DATE '5555-01-01') >= DATE '2017-01-01')
    -- Incremental load: target date (parameterise for scheduled runs)
    AND t1.deal_fd    = DATE '2018-10-05'
    AND t8.open_dt    = DATE '2018-10-05';
