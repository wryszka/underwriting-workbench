# Databricks notebook source
# MAGIC %md
# MAGIC # 05d · Lane E crux — referral-event derivation (ADDITIVE)
# MAGIC
# MAGIC New UC functions for the referral & pricing-discretion lane. The existing crux (`05b`) is
# MAGIC **not modified** — these functions *compose* it:
# MAGIC
# MAGIC * `fn_referral_events_from_checks(sid)` — maps the existing check-result structs to a set of
# MAGIC   referral-event rows (one per rule that FIRES). Feeds `gold_referral_events` (notebook `03c`).
# MAGIC * `fn_wageroll_check(sid)` — **created in E2** (`05e`), then this function is re-created to
# MAGIC   add the MAX_WAGEROLL arm. At E1 the function covers the four rules the crux already raises.
# MAGIC
# MAGIC Runs after `05b_crux` in the ml job (the functions it composes must exist first). Because
# MAGIC `CREATE OR REPLACE FUNCTION` revokes agent EXECUTE grants, this notebook — like `05b` — is
# MAGIC NOT in the reset path; re-run `underwriting_06b_agent` if you change these functions.

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

# MAGIC %md ## fn_referral_events_from_checks — check structs → referral-event rows
# MAGIC One row per referral rule that fires for this submission. `triggering_value` / `threshold_value`
# MAGIC are normalised to the rule's `unit` in `ref_referral_rules` (GBP / ratio / band). The MAX_WAGEROLL
# MAGIC arm is added by E2 when `fn_wageroll_check` exists.

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_referral_events_from_checks(sid STRING)
RETURNS ARRAY<STRUCT<rule_id STRING, rule_version STRING, triggering_value DOUBLE,
                     threshold_value DOUBLE, unit STRING>>
COMMENT 'Maps this submission''s existing crux check-result structs to referral-event rows - one per rule that FIRES (SI authority band, RoFRS flood-High, fair-presentation turnover mismatch, district accumulation over the referral line). Feeds gold_referral_events. The existing crux functions are unchanged; this composes them. Input: submission_public_id.'
RETURN SELECT filter(array(
    CASE WHEN t.auth.total_si > {F}.fn_rule_threshold('SI_AUTHORITY_BAND', 'default')
         THEN named_struct('rule_id', 'SI_AUTHORITY_BAND', 'rule_version', {F}.fn_rule_version('SI_AUTHORITY_BAND'),
                           'triggering_value', cast(t.auth.total_si AS DOUBLE),
                           'threshold_value', {F}.fn_rule_threshold('SI_AUTHORITY_BAND', 'default'), 'unit', 'GBP') END,
    CASE WHEN t.es.flood_band = 'High'
         THEN named_struct('rule_id', 'ROFRS_HIGH', 'rule_version', {F}.fn_rule_version('ROFRS_HIGH'),
                           'triggering_value', 1.0, 'threshold_value', {F}.fn_rule_threshold('ROFRS_HIGH', 'default'), 'unit', 'band') END,
    CASE WHEN coalesce(t.es.turnover_mismatch_ratio, 1.0) >= {F}.fn_rule_threshold('FAIR_PRESENTATION_MISMATCH', 'default')
         THEN named_struct('rule_id', 'FAIR_PRESENTATION_MISMATCH', 'rule_version', {F}.fn_rule_version('FAIR_PRESENTATION_MISMATCH'),
                           'triggering_value', t.es.turnover_mismatch_ratio,
                           'threshold_value', {F}.fn_rule_threshold('FAIR_PRESENTATION_MISMATCH', 'default'), 'unit', 'ratio') END,
    CASE WHEN coalesce(t.acc.worst_status, 'a_ok') IN ('referral', 'breach')
         THEN named_struct('rule_id', 'ACCUMULATION_CAPACITY', 'rule_version', {F}.fn_rule_version('ACCUMULATION_CAPACITY'),
                           'triggering_value', t.acc.worst_post_util_pct / 100.0,
                           'threshold_value', {F}.fn_rule_threshold('ACCUMULATION_CAPACITY', 'default'), 'unit', 'ratio') END
  ), x -> x IS NOT NULL)
FROM (SELECT {F}.fn_extract_summary(sid) AS es,
             {F}.fn_authority_check(sid) AS auth,
             {F}.fn_accumulation_impact(sid) AS acc) t
""")

# COMMAND ----------

# MAGIC %md ## fn_wageroll_check — the MAX_WAGEROLL rule
# MAGIC Declared EL wageroll vs the authority-band thresholds in `ref_referral_rules`. Same
# MAGIC single-row-aggregate shape as `fn_authority_check`. Reads the `silver_submission_wageroll`
# MAGIC sidecar (join on submission id — no existing table touched).

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_wageroll_check(sid STRING)
RETURNS STRUCT<declared_wageroll BIGINT, etrade_threshold BIGINT, standard_threshold BIGINT,
               senior_threshold BIGINT, fires BOOLEAN, required_grade STRING,
               extraction_confidence DOUBLE, note STRING>
COMMENT 'MAX_WAGEROLL referral check: declared employers-liability wageroll (the EL rating basis, from silver_submission_wageroll) vs the authority-band thresholds in ref_referral_rules. Fires when wageroll exceeds the e-trade band; required_grade is the lowest band whose threshold covers it. ADVISORY - a human refers. Input: submission_public_id.'
RETURN SELECT named_struct(
  'declared_wageroll', any_value(w.declared_wageroll),
  'etrade_threshold', any_value(t.et), 'standard_threshold', any_value(t.std), 'senior_threshold', any_value(t.snr),
  'fires', coalesce(any_value(w.declared_wageroll), 0) > any_value(t.et),
  'required_grade',
     CASE WHEN coalesce(any_value(w.declared_wageroll), 0) > any_value(t.snr) THEN 'head_of_underwriting'
          WHEN coalesce(any_value(w.declared_wageroll), 0) > any_value(t.std) THEN 'senior_underwriter'
          WHEN coalesce(any_value(w.declared_wageroll), 0) > any_value(t.et)  THEN 'underwriter'
          ELSE NULL END,
  'extraction_confidence', any_value(w.extraction_confidence),
  'note', CASE WHEN coalesce(any_value(w.declared_wageroll), 0) > any_value(t.std)
               THEN concat('Declared wageroll GBP ', format_number(any_value(w.declared_wageroll), 0),
                           ' exceeds the GBP ', format_number(any_value(t.std), 0), ' standard authority band')
               ELSE 'Wageroll within standard authority' END)
FROM (SELECT declared_wageroll, extraction_confidence FROM {F}.silver_submission_wageroll
      WHERE submission_public_id = sid) w
CROSS JOIN (SELECT cast(get_json_object(threshold_config, '$.etrade')   AS BIGINT) AS et,
                   cast(get_json_object(threshold_config, '$.standard') AS BIGINT) AS std,
                   cast(get_json_object(threshold_config, '$.senior')   AS BIGINT) AS snr
            FROM {F}.ref_referral_rules WHERE rule_id = 'MAX_WAGEROLL' AND valid_to IS NULL) t
""")

# COMMAND ----------

# MAGIC %md ## Re-create fn_referral_events_from_checks WITH the MAX_WAGEROLL arm
# MAGIC Now that `fn_wageroll_check` exists, add its event to the mapping. (Kept as a second
# MAGIC definition so the notebook is correct run top-to-bottom on a clean deploy.)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_referral_events_from_checks(sid STRING)
RETURNS ARRAY<STRUCT<rule_id STRING, rule_version STRING, triggering_value DOUBLE,
                     threshold_value DOUBLE, unit STRING>>
COMMENT 'Maps this submission''s existing crux check-result structs to referral-event rows - one per rule that FIRES (SI authority band, RoFRS flood-High, fair-presentation turnover mismatch, district accumulation over the referral line, MAX_WAGEROLL). Feeds gold_referral_events. The existing crux functions are unchanged; this composes them. Input: submission_public_id.'
RETURN SELECT filter(array(
    CASE WHEN t.auth.total_si > {F}.fn_rule_threshold('SI_AUTHORITY_BAND', 'default')
         THEN named_struct('rule_id', 'SI_AUTHORITY_BAND', 'rule_version', {F}.fn_rule_version('SI_AUTHORITY_BAND'),
                           'triggering_value', cast(t.auth.total_si AS DOUBLE),
                           'threshold_value', {F}.fn_rule_threshold('SI_AUTHORITY_BAND', 'default'), 'unit', 'GBP') END,
    CASE WHEN t.es.flood_band = 'High'
         THEN named_struct('rule_id', 'ROFRS_HIGH', 'rule_version', {F}.fn_rule_version('ROFRS_HIGH'),
                           'triggering_value', 1.0, 'threshold_value', {F}.fn_rule_threshold('ROFRS_HIGH', 'default'), 'unit', 'band') END,
    CASE WHEN coalesce(t.es.turnover_mismatch_ratio, 1.0) >= {F}.fn_rule_threshold('FAIR_PRESENTATION_MISMATCH', 'default')
         THEN named_struct('rule_id', 'FAIR_PRESENTATION_MISMATCH', 'rule_version', {F}.fn_rule_version('FAIR_PRESENTATION_MISMATCH'),
                           'triggering_value', t.es.turnover_mismatch_ratio,
                           'threshold_value', {F}.fn_rule_threshold('FAIR_PRESENTATION_MISMATCH', 'default'), 'unit', 'ratio') END,
    CASE WHEN coalesce(t.acc.worst_status, 'a_ok') IN ('referral', 'breach')
         THEN named_struct('rule_id', 'ACCUMULATION_CAPACITY', 'rule_version', {F}.fn_rule_version('ACCUMULATION_CAPACITY'),
                           'triggering_value', t.acc.worst_post_util_pct / 100.0,
                           'threshold_value', {F}.fn_rule_threshold('ACCUMULATION_CAPACITY', 'default'), 'unit', 'ratio') END,
    CASE WHEN t.wr.fires
         THEN named_struct('rule_id', 'MAX_WAGEROLL', 'rule_version', {F}.fn_rule_version('MAX_WAGEROLL'),
                           'triggering_value', cast(t.wr.declared_wageroll AS DOUBLE),
                           'threshold_value', cast(t.wr.etrade_threshold AS DOUBLE), 'unit', 'GBP') END
  ), x -> x IS NOT NULL)
FROM (SELECT {F}.fn_extract_summary(sid) AS es,
             {F}.fn_authority_check(sid) AS auth,
             {F}.fn_accumulation_impact(sid) AS acc,
             {F}.fn_wageroll_check(sid) AS wr) t
""")

# COMMAND ----------

# Hero 900004: MAX_WAGEROLL fires, and it is the ONLY named referral rule that fires (clean single-trigger).
wr4 = spark.sql(f"SELECT {fqn}.fn_wageroll_check('sub:900004') AS w").first().w
print("sub:900004 wageroll:", wr4["declared_wageroll"], "fires:", wr4["fires"], "grade:", wr4["required_grade"])
assert wr4["fires"] and wr4["declared_wageroll"] == 6_800_000 and wr4["required_grade"] == "senior_underwriter"
ev4 = spark.sql(f"SELECT {fqn}.fn_referral_events_from_checks('sub:900004') AS e").first().e
fired4 = sorted([r["rule_id"] for r in ev4])
print("sub:900004 fires:", fired4)
assert fired4 == ["MAX_WAGEROLL"], f"900004 must be single-trigger MAX_WAGEROLL, got {fired4}"

# COMMAND ----------

# Smoke: hero 900002 fires the accumulation + fair-presentation rules (it is the mid-market referral).
ev = spark.sql(f"SELECT {fqn}.fn_referral_events_from_checks('sub:900002') AS e").first().e
fired = sorted([r["rule_id"] for r in ev])
print("sub:900002 fires:", fired)
assert "ACCUMULATION_CAPACITY" in fired and "FAIR_PRESENTATION_MISMATCH" in fired, fired
# Hero 900001 (clean e-trade) fires nothing.
ev1 = spark.sql(f"SELECT {fqn}.fn_referral_events_from_checks('sub:900001') AS e").first().e
assert len(ev1) == 0, f"sub:900001 should fire no rules, got {[r['rule_id'] for r in ev1]}"
print("✅ 05d Lane E crux — fn_referral_events_from_checks verified on heroes")
