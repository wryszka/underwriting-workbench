# Databricks notebook source
# MAGIC %md
# MAGIC # 05b · THE CRUX — the underwriting decision engine (deterministic UC functions)
# MAGIC
# MAGIC Appetite → authority → accumulation → sanctions → technical price → recommendation.
# MAGIC Deterministic, explicable SQL — not ML — because these are the checks an underwriter
# MAGIC must be able to defend line-by-line. Models advise (priority, risk quality); these
# MAGIC functions decide what the *rules* say; a **human** quotes, refers or declines.
# MAGIC
# MAGIC GOTCHAS honoured: scalar UDF bodies aggregate (`any_value`/`collect_list`) so they are
# MAGIC provably one row; `CREATE OR REPLACE FUNCTION` revokes agent EXECUTE grants → the reset
# MAGIC job never re-runs this notebook (fn changes ⇒ redeploy agents).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"


def create_fn(sql):
    spark.sql(sql.format(F=fqn))
    print("  created:", sql.split("FUNCTION")[1].split("(")[0].strip())

# COMMAND ----------

# MAGIC %md ## fn_extract_summary — the dossier header

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_extract_summary(sid STRING)
RETURNS STRUCT<submission_public_id STRING, company_name STRING, company_number STRING,
               trade_group STRING, segment STRING, channel STRING, broker_id STRING,
               postcode_district STRING, n_locations INT, turnover_stated BIGINT,
               filed_turnover BIGINT, turnover_mismatch_ratio DOUBLE, sic_mismatch BOOLEAN,
               accounts_overdue BOOLEAN, employees INT, total_property_si BIGINT, bi_si BIGINT,
               bi_indemnity_months INT, el_limit BIGINT, pl_limit BIGINT, total_si BIGINT,
               target_premium BIGINT, incumbent_insurer STRING, flood_band STRING,
               flood_rivers STRING, crime_count INT, crime_imputed BOOLEAN, n_documents BIGINT,
               doc_hazards ARRAY<STRING>, flood_disclosed BOOLEAN, data_complete BOOLEAN,
               lifecycle_state STRING, received_ts STRING>
COMMENT 'Underwriting dossier header for one submission: who/what/how-much + enrichment signals (turnover mismatch vs filed accounts = Insurance Act 2015 fair-presentation flag; SIC mismatch; flood band + EA river evidence; crime with GMP data-gap flag; document-extraction hazards). Input: submission_public_id like sub:900002.'
RETURN SELECT named_struct(
  'submission_public_id', any_value(submission_public_id), 'company_name', any_value(company_name),
  'company_number', any_value(company_number), 'trade_group', any_value(trade_group),
  'segment', any_value(segment), 'channel', any_value(channel), 'broker_id', any_value(broker_id),
  'postcode_district', any_value(postcode_district), 'n_locations', any_value(n_locations),
  'turnover_stated', any_value(turnover_stated), 'filed_turnover', any_value(filed_turnover),
  'turnover_mismatch_ratio', any_value(turnover_mismatch_ratio), 'sic_mismatch', any_value(sic_mismatch),
  'accounts_overdue', any_value(accounts_overdue), 'employees', any_value(employees),
  'total_property_si', any_value(total_property_si), 'bi_si', any_value(bi_si),
  'bi_indemnity_months', any_value(bi_indemnity_months), 'el_limit', any_value(el_limit),
  'pl_limit', any_value(pl_limit), 'total_si', any_value(total_si),
  'target_premium', any_value(target_premium), 'incumbent_insurer', any_value(incumbent_insurer),
  'flood_band', any_value(flood_band), 'flood_rivers', any_value(rivers),
  'crime_count', any_value(crime_count), 'crime_imputed', any_value(crime_imputed),
  'n_documents', coalesce(any_value(n_documents), 0),
  'doc_hazards', coalesce(any_value(doc_hazards), array()),
  'flood_disclosed', coalesce(any_value(flood_disclosed), false),
  'data_complete', any_value(data_complete), 'lifecycle_state', any_value(lifecycle_state),
  'received_ts', any_value(received_ts))
FROM {F}.silver_submissions WHERE submission_public_id = sid
""")

# COMMAND ----------

# MAGIC %md ## fn_appetite_check

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_appetite_check(sid STRING)
RETURNS STRUCT<trade_group STRING, appetite_status STRING, in_appetite BOOLEAN, hazard_grade INT,
               decline_code STRING, guide_section STRING, note STRING>
COMMENT 'Appetite check: is this trade core/selective/excluded per the underwriting guide (ref_appetite)? Returns the coded decline reason + guide citation for excluded trades (e.g. APP-EXCL-WASTE / UG-9.2). Input: submission_public_id.'
RETURN SELECT named_struct(
  'trade_group', any_value(trade_group), 'appetite_status', any_value(appetite_status),
  'in_appetite', any_value(appetite_status) <> 'excluded', 'hazard_grade', any_value(hazard_grade),
  'decline_code', any_value(appetite_decline_code), 'guide_section', any_value(guide_section),
  'note', any_value(appetite_note))
FROM {F}.silver_submissions WHERE submission_public_id = sid
""")

# COMMAND ----------

# MAGIC %md ## fn_authority_check — who can sign this

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_authority_check(sid STRING)
RETURNS STRUCT<total_si BIGINT, technical_base_premium DOUBLE, hazard_grade INT, flood_band STRING,
               etrade_eligible BOOLEAN, required_grade STRING, escalate_to STRING,
               suggested_underwriter STRING, triggers ARRAY<STRING>>
COMMENT 'Authority check vs ref_authority_matrix: the minimum grade whose limits (total SI, premium, hazard grade, flood-High permission) cover this submission, the referral route, and a suggested named underwriter on the right desk. Also whether it fits e-trade system authority (straight-through). Input: submission_public_id.'
RETURN SELECT named_struct(
  'total_si', any_value(s.tsi), 'technical_base_premium', any_value(s.tech),
  'hazard_grade', any_value(s.hz), 'flood_band', any_value(s.fb),
  'etrade_eligible', any_value(s.tsi <= et.si AND s.tech <= et.prem AND s.hz <= et.hz
                               AND (s.fb <> 'High' OR et.fh)),
  'required_grade', min_by(m.grade, m.max_total_si) FILTER (WHERE adequate),
  'escalate_to', min_by(m.escalate_to, m.max_total_si) FILTER (WHERE adequate),
  'suggested_underwriter',
     min_by(u.underwriter_name, concat(lpad(cast(m.max_total_si AS STRING), 12, '0'), u.underwriter_id))
       FILTER (WHERE adequate AND u.underwriter_name IS NOT NULL),
  'triggers', any_value(filter(array(
     CASE WHEN s.tsi > 5000000 THEN concat('Total SI GBP ', format_number(s.tsi, 0), ' above the GBP 5m underwriter band') END,
     CASE WHEN s.fb = 'High' THEN 'Flood band HIGH requires senior authority' END,
     CASE WHEN s.hz >= 4 THEN concat('Hazard grade ', s.hz, ' requires senior authority') END), x -> x IS NOT NULL)))
FROM (SELECT any_value(total_si) AS tsi, any_value(technical_base_premium) AS tech,
             any_value(hazard_grade) AS hz, coalesce(any_value(flood_band), 'Low') AS fb,
             any_value(segment) AS seg
      FROM {F}.silver_submissions WHERE submission_public_id = sid) s
CROSS JOIN (SELECT any_value(max_total_si) AS si, any_value(max_gross_premium) AS prem,
                   any_value(max_hazard_grade) AS hz, any_value(flood_high_allowed) AS fh
            FROM {F}.ref_authority_matrix WHERE grade = 'system_etrade') et
CROSS JOIN LATERAL (SELECT a.*, a.grade <> 'system_etrade' AND a.max_total_si >= s.tsi
                           AND a.max_gross_premium >= s.tech AND a.max_hazard_grade >= s.hz
                           AND (a.flood_high_allowed OR s.fb <> 'High') AS adequate
                    FROM {F}.ref_authority_matrix a) m
LEFT JOIN {F}.ref_underwriter u
  ON u.grade = m.grade AND u.desk = CASE WHEN s.seg = 'mid_market' THEN 'mid_market' ELSE 'sme_package' END
""")

# COMMAND ----------

# MAGIC %md ## fn_accumulation_impact — marginal exposure vs district capacity

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_accumulation_impact(sid STRING)
RETURNS STRUCT<districts ARRAY<STRUCT<postcode_district STRING, marginal_si BIGINT, in_force_si BIGINT,
               capacity BIGINT, current_util_pct DOUBLE, post_util_pct DOUBLE, flood_band STRING,
               status STRING>>, worst_status STRING, worst_district STRING, worst_post_util_pct DOUBLE>
COMMENT 'Marginal property accumulation at point of quote: for each district this submission touches, in-force property SI vs capacity appetite before and after binding. post >=100% = breach, >=80% = referral (the HX7 87 percent beat). Most insurers reconcile this quarterly in a spreadsheet; here it runs per submission. Input: submission_public_id.'
RETURN SELECT named_struct(
  'districts', collect_list(named_struct(
      'postcode_district', l.postcode_district, 'marginal_si', l.marginal_si,
      'in_force_si', a.in_force_property_si, 'capacity', a.property_capacity_gbp,
      'current_util_pct', a.utilisation_pct,
      'post_util_pct', round((a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp * 100, 1),
      'flood_band', a.flood_band,
      'status', CASE WHEN (a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp >= 1.0 THEN 'breach'
                     WHEN (a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp >= 0.8 THEN 'referral'
                     ELSE 'ok' END)),
  'worst_status', CASE coalesce(max(CASE WHEN (a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp >= 1.0 THEN 2
                                         WHEN (a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp >= 0.8 THEN 1
                                         ELSE 0 END), 0)
                  WHEN 2 THEN 'breach' WHEN 1 THEN 'referral' ELSE 'a_ok' END,
  'worst_district', max_by(l.postcode_district, (a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp),
  'worst_post_util_pct', round(max((a.in_force_property_si + l.marginal_si) / a.property_capacity_gbp) * 100, 1))
FROM (SELECT postcode_district, sum(property_si) AS marginal_si
      FROM {F}.silver_locations_enriched WHERE submission_public_id = sid GROUP BY 1) l
JOIN {F}.gold_accumulation a USING (postcode_district)
""")

# COMMAND ----------

# MAGIC %md ## fn_technical_price — burning cost + rate guide + NAMED loadings (+IPT, commission)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_technical_price(sid STRING)
RETURNS STRUCT<property_component DOUBLE, bi_component DOUBLE, el_component DOUBLE, pl_component DOUBLE,
               base_premium DOUBLE, crime_theft_loading DOUBLE, flood_loading DOUBLE,
               claims_experience_loading DOUBLE, technical_premium DOUBLE, ipt_pct DOUBLE,
               ipt_amount DOUBLE, total_inc_ipt DOUBLE, commission_pct DOUBLE,
               target_premium BIGINT, adequacy_pct DOUBLE, verdict STRING, basis STRING>
COMMENT 'Technical price build-up: rate-guide components (property/BI/EL/PL) + NAMED loadings — crime-derived theft & malicious damage (real police.uk counts), flood (RoFRS High-band property share), claims-experience (cohort burning cost) — then IPT at 12 percent and commission. Adequacy = broker target vs technical. The full frequency/severity GLM engine is the Pricing Workbench (cross-linked), never duplicated here. Input: submission_public_id.'
RETURN SELECT named_struct(
  'property_component', round(x.prop_c, 0), 'bi_component', round(x.bi_c, 0),
  'el_component', round(x.el_c, 0), 'pl_component', round(x.pl_c, 0),
  'base_premium', round(x.base, 0),
  'crime_theft_loading', round(x.crime_l, 0), 'flood_loading', round(x.flood_l, 0),
  'claims_experience_loading', round(x.exp_l, 0),
  'technical_premium', round(x.base + x.crime_l + x.flood_l + x.exp_l, 0),
  'ipt_pct', 12.0, 'ipt_amount', round((x.base + x.crime_l + x.flood_l + x.exp_l) * 0.12, 0),
  'total_inc_ipt', round((x.base + x.crime_l + x.flood_l + x.exp_l) * 1.12, 0),
  'commission_pct', CASE WHEN x.seg = 'sme' THEN 22.5 ELSE 20.0 END,
  'target_premium', x.target,
  'adequacy_pct', CASE WHEN x.target IS NOT NULL
                       THEN round(x.target / (x.base + x.crime_l + x.flood_l + x.exp_l) * 100, 1) END,
  'verdict', CASE WHEN x.target IS NULL THEN 'priced_to_guide'
                  WHEN x.target / (x.base + x.crime_l + x.flood_l + x.exp_l) >= 0.95 THEN 'target_achievable'
                  WHEN x.target / (x.base + x.crime_l + x.flood_l + x.exp_l) >= 0.85 THEN 'negotiate'
                  ELSE 'target_materially_below_technical' END,
  'basis', 'Rate guide + cohort burning cost. Loadings evidenced by bundled open data (police.uk crime, EA flood). Full GLM rating = Pricing Workbench.')
FROM (
  SELECT s.seg, s.target, s.prop_c, s.bi_c, s.el_c, s.pl_c,
         greatest(s.min_p, s.prop_c + s.bi_c + s.el_c + s.pl_c) AS base,
         (s.contents_stock * s.prop_rate / 1000) * (least(s.crime, 150) / 150.0) * 0.35 AS crime_l,
         coalesce(s.high_share, 0) * s.prop_c * 0.25 AS flood_l,
         CASE WHEN s.cohort_lr > 65 THEN (s.prop_c + s.bi_c) * 0.10
              WHEN s.cohort_lr < 35 THEN -(s.prop_c + s.bi_c) * 0.05 ELSE 0 END AS exp_l
  FROM (SELECT any_value(ss.segment) AS seg, any_value(ss.target_premium) AS target,
               any_value(ss.total_property_si) * any_value(r.property_rate_permille) / 1000 AS prop_c,
               any_value(ss.bi_si) * any_value(r.bi_rate_permille) / 1000 AS bi_c,
               any_value(ss.employees) * any_value(r.el_rate_per_employee) AS el_c,
               coalesce(any_value(ss.turnover_stated), 0) / 1000 * any_value(r.pl_rate_per_1k_turnover) AS pl_c,
               any_value(r.min_premium) AS min_p,
               any_value(r.property_rate_permille) AS prop_rate,
               any_value(ss.contents_si) + any_value(ss.stock_si) AS contents_stock,
               coalesce(any_value(ss.crime_count), 0) AS crime,
               coalesce(any_value(ss.cohort_loss_ratio_pct), 50) AS cohort_lr,
               sum(CASE WHEN l.flood_band = 'High' THEN l.property_si ELSE 0 END)
                 / nullif(sum(l.property_si), 0) AS high_share
        FROM {F}.silver_submissions ss
        JOIN {F}.ref_rate_guide r USING (trade_group)
        LEFT JOIN {F}.silver_locations_enriched l ON l.submission_public_id = ss.submission_public_id
        WHERE ss.submission_public_id = sid) s
) x
""")

# COMMAND ----------

# MAGIC %md ## fn_sanctions_screen — REAL OFSI list + internal watchlist, with false-positive resolution

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_sanctions_screen(sid STRING)
RETURNS STRUCT<subjects ARRAY<STRING>,
               ofsi_candidates ARRAY<STRUCT<subject STRING, listed_name STRING, group_type STRING,
                                            dob STRING, nationality STRING, regime STRING, resolution STRING>>,
               watchlist_hits ARRAY<STRUCT<subject STRING, watchlist_name STRING, reason STRING, source STRING>>,
               status STRING, guidance STRING>
COMMENT 'Point-of-quote screening of the company + directors against the REAL OFSI consolidated list (12k primary names, bundled extract) and the internal watchlist. Near-misses are surfaced WITH their resolution (no DOB/nationality corroboration = cleared false positive) because false-positive handling is where real screening lives. A true OFSI match freezes the submission and escalates to compliance — never a decline letter. Watchlist reasons are INTERNAL ONLY and never appear in broker communications. Input: submission_public_id.'
RETURN SELECT named_struct(
  'subjects',
     (SELECT any_value(array_append(from_json(directors_json, 'ARRAY<STRING>'), company_name))
      FROM {F}.silver_submissions WHERE submission_public_id = sid),
  'ofsi_candidates',
     (SELECT collect_list(named_struct('subject', subj, 'listed_name', o.name, 'group_type', o.group_type,
                                       'dob', o.dob, 'nationality', o.nationality, 'regime', o.regime,
                                       'resolution', 'Name similarity only - no DOB/nationality corroboration; cleared as false positive and logged'))
      FROM (SELECT explode(array_append(from_json(any_value(directors_json), 'ARRAY<STRING>'),
                                        any_value(company_name))) AS subj
            FROM {F}.silver_submissions WHERE submission_public_id = sid) s
      JOIN {F}.ref_sanctions_ofsi o
        ON length(o.name) > 7 AND levenshtein(lower(s.subj), lower(o.name)) <= 1),
  'watchlist_hits',
     (SELECT collect_list(named_struct('subject', subj, 'watchlist_name', wl.name,
                                       'reason', wl.reason, 'source', wl.source))
      FROM (SELECT explode(array_append(from_json(any_value(directors_json), 'ARRAY<STRING>'),
                                        any_value(company_name))) AS subj
            FROM {F}.silver_submissions WHERE submission_public_id = sid) s
      JOIN {F}.ref_internal_watchlist wl ON lower(s.subj) = lower(wl.name)),
  'status',
     CASE WHEN (SELECT count(*) FROM (SELECT explode(array_append(from_json(any_value(directors_json), 'ARRAY<STRING>'), any_value(company_name))) AS subj
                                      FROM {F}.silver_submissions WHERE submission_public_id = sid) s
                JOIN {F}.ref_internal_watchlist wl ON lower(s.subj) = lower(wl.name)) > 0 THEN 'internal_watchlist_hit'
          WHEN (SELECT count(*) FROM (SELECT explode(array_append(from_json(any_value(directors_json), 'ARRAY<STRING>'), any_value(company_name))) AS subj
                                      FROM {F}.silver_submissions WHERE submission_public_id = sid) s
                JOIN {F}.ref_sanctions_ofsi o ON length(o.name) > 7 AND levenshtein(lower(s.subj), lower(o.name)) <= 1) > 0
               THEN 'false_positive_resolved'
          ELSE 'clear' END,
  'guidance', 'True OFSI match = freeze + escalate to compliance (potential OFSI report), no broker letter. Internal watchlist reasons stay internal.')
""")

# COMMAND ----------

# MAGIC %md ## fn_treaty_check — net line vs gross line, facultative flag

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_treaty_check(sid STRING)
RETURNS STRUCT<gross_line_per_risk BIGINT, largest_risk_site STRING, net_retention BIGINT,
               ceded_to_treaty BIGINT, treaty_capacity_per_risk BIGINT, treaty_headroom BIGINT,
               facultative_required BOOLEAN, facultative_amount BIGINT, treaty STRING, note STRING>
COMMENT 'Outward reinsurance check at point of quote: the largest single-risk property line vs the surplus treaty (net retention, cession, per-risk capacity) and whether FACULTATIVE cover is required above treaty capacity. The treaty itself is underwritten in the sibling Reinsurance Workbench. Input: submission_public_id.'
RETURN SELECT named_struct(
  'gross_line_per_risk', l.gross, 'largest_risk_site', l.site,
  'net_retention', least(l.gross, t.ret),
  'ceded_to_treaty', greatest(least(l.gross, t.cap) - t.ret, 0),
  'treaty_capacity_per_risk', t.cap,
  'treaty_headroom', greatest(t.cap - l.gross, 0),
  'facultative_required', l.gross > t.cap,
  'facultative_amount', greatest(l.gross - t.cap, 0),
  'treaty', t.nm,
  'note', 'Per-risk basis = largest single location property line. Treaty written by Bricksurance Re (sibling workbench).')
FROM (SELECT max(property_si) AS gross, max_by(site_name, property_si) AS site
      FROM {F}.silver_locations_enriched WHERE submission_public_id = sid) l
CROSS JOIN (SELECT any_value(net_retention_per_risk) AS ret, any_value(per_risk_capacity) AS cap,
                   any_value(treaty_name) AS nm
            FROM {F}.ref_treaty_structure WHERE applies_to = 'property') t
""")

# COMMAND ----------

# MAGIC %md ## fn_underinsurance_check

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_underinsurance_check(sid STRING)
RETURNS STRUCT<declared_buildings_si BIGINT, rebuild_benchmark_gbp BIGINT, buildings_adequacy_pct DOUBLE,
               underinsured_flag BOOLEAN, bi_indemnity_months INT, bi_indemnity_flag BOOLEAN, note STRING>
COMMENT 'Underinsurance check: declared buildings SI vs floor-area x rebuild-cost benchmark (below 85 percent = average-clause conversation), and BI indemnity-period adequacy (manufacturing trades below 24 months flagged - the post-COVID market push). Input: submission_public_id.'
RETURN SELECT named_struct(
  'declared_buildings_si', any_value(declared_buildings_si),
  'rebuild_benchmark_gbp', any_value(rebuild_benchmark_gbp),
  'buildings_adequacy_pct', any_value(buildings_adequacy_pct),
  'underinsured_flag', coalesce(any_value(underinsured_flag), false),
  'bi_indemnity_months', any_value(bi_indemnity_months),
  'bi_indemnity_flag', coalesce(any_value(bi_indemnity_flag), false),
  'note', 'Benchmark = floor area x BCIS-shaped rebuild rate per construction type (illustrative rates, labelled).')
FROM {F}.gold_underinsurance WHERE submission_public_id = sid
""")

# COMMAND ----------

# MAGIC %md ## fn_recommendation — the composed decision (advisory; a human acts)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_recommendation(sid STRING)
RETURNS STRUCT<action STRING, refer_to_grade STRING, suggested_underwriter STRING,
               reasons ARRAY<STRING>, terms ARRAY<STRING>, subjectivities ARRAY<STRING>,
               decline_code_external STRING, external_reason STRING, internal_notes ARRAY<STRING>,
               straight_through BOOLEAN>
COMMENT 'The composed underwriting recommendation for one submission: quote (straight-through when e-trade authority fits) / refer (to whom, why) / decline (coded, external reason cites appetite ONLY) / request_information. Terms and subjectivities are built from the checks (flood excess, risk survey, Insurance Act 2015 fair-presentation turnover confirmation, composite-panel confirmation). ADVISORY - a named human underwriter acts; agents never bind. Input: submission_public_id.'
RETURN SELECT named_struct(
  'action',
     CASE WHEN NOT app.in_appetite THEN 'decline'
          WHEN scr.status = 'internal_watchlist_hit' THEN 'refer'
          WHEN coalesce(acc.worst_status, 'a_ok') = 'breach' THEN 'refer'
          WHEN NOT es.data_complete THEN 'request_information'
          WHEN auth.required_grade IN ('senior_underwriter', 'head_of_underwriting')
               OR coalesce(acc.worst_status, 'a_ok') = 'referral'
               OR coalesce(es.turnover_mismatch_ratio, 1.0) >= 1.5
               OR prc.verdict = 'target_materially_below_technical' THEN 'refer'
          ELSE 'quote' END,
  'refer_to_grade',
     CASE WHEN NOT app.in_appetite THEN NULL
          WHEN scr.status = 'internal_watchlist_hit' THEN 'compliance_and_senior_underwriter'
          WHEN coalesce(acc.worst_status, 'a_ok') = 'breach' THEN 'head_of_underwriting'
          WHEN auth.required_grade IN ('senior_underwriter', 'head_of_underwriting') THEN auth.required_grade
          WHEN coalesce(acc.worst_status, 'a_ok') = 'referral' OR coalesce(es.turnover_mismatch_ratio, 1.0) >= 1.5
               OR prc.verdict = 'target_materially_below_technical' THEN 'senior_underwriter' END,
  'suggested_underwriter', auth.suggested_underwriter,
  'reasons', filter(array(
     CASE WHEN NOT app.in_appetite THEN concat('Excluded trade per underwriting guide ', app.guide_section, ' (', app.decline_code, ')') END,
     CASE WHEN scr.status = 'internal_watchlist_hit' THEN 'Internal watchlist hit on screening - INTERNAL ONLY, route via compliance' END,
     CASE WHEN scr.status = 'false_positive_resolved' THEN 'Sanctions near-miss resolved as false positive (logged)' END,
     CASE WHEN coalesce(acc.worst_status, 'a_ok') = 'breach' THEN concat('Accumulation BREACH in ', acc.worst_district, ' - post-bind ', acc.worst_post_util_pct, ' percent of capacity') END,
     CASE WHEN coalesce(acc.worst_status, 'a_ok') = 'referral' THEN concat('Accumulation in ', acc.worst_district, ' reaches ', acc.worst_post_util_pct, ' percent of district capacity (referral line = 80)') END,
     CASE WHEN auth.required_grade IN ('senior_underwriter', 'head_of_underwriting') THEN concat('Requires ', auth.required_grade, ' authority: ', array_join(auth.triggers, '; ')) END,
     CASE WHEN coalesce(es.turnover_mismatch_ratio, 1.0) >= 1.5 THEN concat('Fair presentation concern (Insurance Act 2015): filed accounts show turnover ', format_number(es.filed_turnover, 0), ' vs ', format_number(es.turnover_stated, 0), ' stated - PL/Products rating basis and BI sums affected; EL wageroll to be confirmed') END,
     CASE WHEN prc.verdict = 'target_materially_below_technical' THEN concat('Broker target ', format_number(prc.target_premium, 0), ' is ', prc.adequacy_pct, ' percent of technical ', format_number(prc.technical_premium, 0)) END,
     CASE WHEN NOT es.data_complete THEN 'Core facts incomplete - query back to broker' END,
     CASE WHEN app.in_appetite AND scr.status = 'clear' AND coalesce(acc.worst_status, 'a_ok') = 'a_ok'
               AND auth.required_grade NOT IN ('senior_underwriter', 'head_of_underwriting')
               AND coalesce(es.turnover_mismatch_ratio, 1.0) < 1.5 THEN 'All checks green - within appetite and authority' END
     ), x -> x IS NOT NULL),
  'terms', CASE WHEN app.in_appetite THEN filter(array(
     CASE WHEN es.flood_band = 'High' OR coalesce(acc.worst_status, 'a_ok') IN ('referral', 'breach')
          THEN 'GBP 100,000 flood excess at flood-band-High locations' END,
     CASE WHEN ui.underinsured_flag THEN 'Declared buildings sum insured below rebuild benchmark - average clause discussion / uplift required' END
     ), x -> x IS NOT NULL) ELSE array() END,
  'subjectivities', CASE WHEN app.in_appetite THEN filter(array(
     CASE WHEN coalesce(es.turnover_mismatch_ratio, 1.0) >= 1.5
          THEN 'Subject to audited turnover confirmation and revised PL/Products and BI estimates (and EL wageroll) within 14 days (Insurance Act 2015 fair presentation)' END,
     CASE WHEN app.hazard_grade >= 4 OR es.total_property_si > 2000000
          THEN 'Subject to satisfactory risk survey of principal site(s) within 60 days of inception' END,
     CASE WHEN exists(es.doc_hazards, h -> lower(h) LIKE '%composite%' OR lower(h) LIKE '%panel%')
          THEN 'Subject to confirmation of composite-panel percentage and frying/cooking protections' END,
     CASE WHEN ui.bi_indemnity_flag
          THEN 'Recommend 24-month BI indemnity period for manufacturing risk (12 months proposed)' END
     ), x -> x IS NOT NULL) ELSE array() END,
  'decline_code_external', CASE WHEN NOT app.in_appetite THEN app.decline_code END,
  'external_reason', CASE WHEN NOT app.in_appetite
     THEN concat('This class of business (', app.trade_group, ') sits outside our current underwriting appetite (guide ', app.guide_section, '). This trade sits with specialist markets.') END,
  'internal_notes', filter(array(
     CASE WHEN scr.status = 'internal_watchlist_hit' THEN 'Watchlist reason recorded in decision audit - NEVER disclosed to broker' END,
     CASE WHEN es.sic_mismatch THEN 'Declared trade differs from Companies House SIC code - verify actual activities' END,
     CASE WHEN es.accounts_overdue THEN 'Companies House accounts overdue - financial resilience question' END
     ), x -> x IS NOT NULL),
  'straight_through', app.in_appetite AND scr.status IN ('clear', 'false_positive_resolved')
     AND coalesce(acc.worst_status, 'a_ok') = 'a_ok'
     AND es.channel = 'etrade' AND auth.etrade_eligible AND es.data_complete
     AND coalesce(es.turnover_mismatch_ratio, 1.0) < 1.5)
FROM (SELECT {F}.fn_extract_summary(sid) AS es, {F}.fn_appetite_check(sid) AS app,
             {F}.fn_authority_check(sid) AS auth, {F}.fn_accumulation_impact(sid) AS acc,
             {F}.fn_sanctions_screen(sid) AS scr, {F}.fn_technical_price(sid) AS prc,
             {F}.fn_underinsurance_check(sid) AS ui) t
""")

# COMMAND ----------

# MAGIC %md ## Smoke the crux on the three heroes

# COMMAND ----------

for sid, want in (("sub:900001", "quote"), ("sub:900002", "refer"), ("sub:900003", "decline")):
    row = spark.sql(f"SELECT {fqn}.fn_recommendation('{sid}') AS r").first().r
    print(sid, "→", row["action"], "| refer_to:", row["refer_to_grade"], "| reasons:", row["reasons"][:2])
    assert row["action"] == want, f"{sid}: expected {want}, got {row['action']}"
acc = spark.sql(f"SELECT {fqn}.fn_accumulation_impact('sub:900002') AS r").first().r
hx7 = [d for d in acc["districts"] if d["postcode_district"] == "HX7"][0]
print("HX7 post-bind utilisation:", hx7["post_util_pct"], "% → status", hx7["status"])
assert 85.0 <= hx7["post_util_pct"] <= 89.0 and hx7["status"] == "referral"
print("✅ 05b complete — crux verified on all three heroes")
