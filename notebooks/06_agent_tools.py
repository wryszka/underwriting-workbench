# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Agent tools — model-backed UC functions
# MAGIC
# MAGIC The two ML scorers exposed as UC functions (feature-vector contract: pre-aggregate the
# MAGIC deterministic feature row with `any_value`, then ONE `ai_query` call on the single row —
# MAGIC the claims_workbench pattern). Endpoint names are resolved by substring at creation time
# MAGIC (DAB dev-prefix safe). Together with the 05b crux functions these are the supervisor's
# MAGIC tool bench.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("mode", "full")  # full = create fns + batch-score · score_only = batch-score only (reset path — CREATE OR REPLACE FUNCTION revokes agent grants)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
fqn = f"{catalog}.{schema}"

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
eps = [e.name for e in w.serving_endpoints.list()]
EP_TRIAGE = next(n for n in eps if "underwriting-triage" in n)
EP_RISK = next(n for n in eps if "underwriting-risk" in n and "profile" not in n)
print("resolved endpoints:", EP_TRIAGE, "·", EP_RISK)

# COMMAND ----------

def _create_scoring_fns():
    spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_triage_score(sid STRING)
RETURNS STRUCT<bind_propensity_pct DOUBLE, priority_band STRING, basis STRING>
COMMENT 'ML triage priority for a submission: P(bind) from model_triage_priority @champion served on {EP_TRIAGE} (LightGBM over the UC Feature Store vector). Ranks the inbox: work the winnable business first. Advisory only. Input: submission_public_id.'
RETURN SELECT named_struct(
  'bind_propensity_pct', round(p.score * 100, 1),
  'priority_band', CASE WHEN p.score >= 0.5 THEN 'P1_work_first'
                        WHEN p.score >= 0.3 THEN 'P2_standard' ELSE 'P3_when_capacity' END,
  'basis', 'model_triage_priority @champion · Feature Store vector · LightGBM P(bind)')
FROM (
  SELECT ai_query('{EP_TRIAGE}', named_struct(
           'channel_e', f.channel_e, 'segment_e', f.segment_e, 'appetite_e', f.appetite_e,
           'flood_e', f.flood_e, 'hazard_grade', f.hazard_grade, 'log_total_si', f.log_total_si,
           'log_turnover', f.log_turnover, 'employees', f.employees, 'n_locations', f.n_locations,
           'target_vs_technical', f.target_vs_technical, 'cohort_loss_ratio_pct', f.cohort_loss_ratio_pct,
           'data_complete', f.data_complete),
         returnType => 'DOUBLE') AS score
  FROM (SELECT any_value(channel_e) channel_e, any_value(segment_e) segment_e,
               any_value(appetite_e) appetite_e, any_value(flood_e) flood_e,
               any_value(hazard_grade) hazard_grade, any_value(log_total_si) log_total_si,
               any_value(log_turnover) log_turnover, any_value(employees) employees,
               any_value(n_locations) n_locations, any_value(target_vs_technical) target_vs_technical,
               any_value(cohort_loss_ratio_pct) cohort_loss_ratio_pct, any_value(data_complete) data_complete
        FROM {fqn}.feature_submission WHERE submission_public_id = sid) f
) p
""")
    print("created: fn_triage_score")

    spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_risk_score(sid STRING)
RETURNS STRUCT<large_loss_propensity_pct DOUBLE, risk_band STRING, basis STRING>
COMMENT 'ML risk quality for a submission: P(large loss >= GBP 25k within 3 years) from model_risk_quality @champion served on {EP_RISK}, trained on the PAS book claims experience. Feeds rate adequacy and referral judgement. Advisory only. Input: submission_public_id.'
RETURN SELECT named_struct(
  'large_loss_propensity_pct', round(p.score * 100, 1),
  'risk_band', CASE WHEN p.score >= 0.35 THEN 'elevated'
                    WHEN p.score >= 0.18 THEN 'book_average' ELSE 'better_than_book' END,
  'basis', 'model_risk_quality @champion · trained on PAS claims experience · LightGBM')
FROM (
  SELECT ai_query('{EP_RISK}', named_struct(
           'hazard_grade', f.hazard_grade, 'construction_e', f.construction_e, 'flood_e', f.flood_e,
           'crime_count', f.crime_count, 'year_built', f.year_built, 'log_total_si', f.log_total_si,
           'log_turnover', f.log_turnover, 'employees', f.employees,
           'cohort_loss_ratio_pct', f.cohort_loss_ratio_pct),
         returnType => 'DOUBLE') AS score
  FROM (SELECT any_value(hazard_grade) hazard_grade, any_value(construction_e) construction_e,
               any_value(flood_e) flood_e, any_value(crime_count) crime_count,
               any_value(year_built) year_built, any_value(log_total_si) log_total_si,
               any_value(log_turnover) log_turnover, any_value(employees) employees,
               any_value(cohort_loss_ratio_pct) cohort_loss_ratio_pct
        FROM {fqn}.feature_submission WHERE submission_public_id = sid) f
) p
""")
    print("created: fn_risk_score")


if MODE == "full":
    _create_scoring_fns()
else:
    print("mode=score_only — skipping fn creation (CREATE OR REPLACE FUNCTION revokes agent grants)")

# COMMAND ----------

# MAGIC %md ## Batch-score the open inbox
# MAGIC The app NEVER calls scale-to-zero model endpoints interactively (claims_workbench speed
# MAGIC lesson) — the open pipeline is batch-scored here (real batch `ai_query` inference) into
# MAGIC `gold_inbox_priority`; live model calls happen only in Try-a-submission.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.gold_inbox_priority
TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench') AS
SELECT f.submission_public_id,
       round(ai_query('{EP_TRIAGE}', named_struct(
         'channel_e', f.channel_e, 'segment_e', f.segment_e, 'appetite_e', f.appetite_e,
         'flood_e', f.flood_e, 'hazard_grade', f.hazard_grade, 'log_total_si', f.log_total_si,
         'log_turnover', f.log_turnover, 'employees', f.employees, 'n_locations', f.n_locations,
         'target_vs_technical', f.target_vs_technical, 'cohort_loss_ratio_pct', f.cohort_loss_ratio_pct,
         'data_complete', f.data_complete), returnType => 'DOUBLE') * 100, 1) AS bind_propensity_pct,
       round(ai_query('{EP_RISK}', named_struct(
         'hazard_grade', f.hazard_grade, 'construction_e', f.construction_e, 'flood_e', f.flood_e,
         'crime_count', f.crime_count, 'year_built', f.year_built, 'log_total_si', f.log_total_si,
         'log_turnover', f.log_turnover, 'employees', f.employees,
         'cohort_loss_ratio_pct', f.cohort_loss_ratio_pct), returnType => 'DOUBLE') * 100, 1) AS large_loss_propensity_pct,
       current_timestamp() AS scored_at
FROM {fqn}.feature_submission f
JOIN {fqn}.gold_submission_lifecycle o USING (submission_public_id)
""")
spark.sql(f"""
ALTER TABLE {fqn}.gold_inbox_priority ALTER COLUMN submission_public_id SET NOT NULL
""")
n = spark.table(f"{fqn}.gold_inbox_priority").count()
print(f"gold_inbox_priority: {n} open submissions batch-scored")

for sid in ("sub:900001", "sub:900002"):
    r = spark.sql(f"SELECT {fqn}.fn_triage_score('{sid}') AS t, {fqn}.fn_risk_score('{sid}') AS r").first()
    print(sid, "triage:", r.t["bind_propensity_pct"], r.t["priority_band"], "| risk:", r.r["large_loss_propensity_pct"], r.r["risk_band"])
print("✅ 06 agent tools complete (note: first call may wait on scale-to-zero endpoint cold start)")
