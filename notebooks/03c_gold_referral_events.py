# Databricks notebook source
# MAGIC %md
# MAGIC # 03c · `gold_referral_events` — rule-grain referral fact (ADDITIVE)
# MAGIC
# MAGIC One row per referral rule that **fires**. Two sources, unioned:
# MAGIC
# MAGIC 1. **Live / current** — every open+closed submission in `silver_submissions`, mapped through
# MAGIC    `fn_referral_events_from_checks` (real crux composition). These are New Business submissions;
# MAGIC    `transaction_id` equals the submission id (NB convention until E3 gives them a transaction).
# MAGIC 2. **Generated history** — `landing_referral_events_generated`, the deterministic 2025 backfill
# MAGIC    produced in `00c` (E3a: MAX_WAGEROLL fires across NB/RN/MTA transactions, with the planted
# MAGIC    discretion signal and underwriter personas). Absent until E3a — this notebook tolerates that.
# MAGIC
# MAGIC Non-fires are NOT stored as rows (volume discipline) — the aggregate evaluated/fired counts go
# MAGIC to the DQ scorecard. Runs after `05d` (needs the function) in the ml job, and in reset after
# MAGIC the medallion refresh.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md ## Source 1 · live events from the current submission feed
# MAGIC `fn_referral_events_from_checks` returns an array of fired-rule structs per submission; explode
# MAGIC to rows and derive outcome/latency from the submission's own lifecycle fields.

# COMMAND ----------

# Step 1: materialise the per-submission event ARRAY (UDF call) to a temp table — a SQL UDF
# cannot be exploded in the same plan (Generate over a SQL function is unsupported).
spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW _lane_e_evarr AS
  SELECT s.submission_public_id AS submission_id, s.received_ts, s.decided_ts, s.outcome,
         {fqn}.fn_referral_events_from_checks(s.submission_public_id) AS events
  FROM {fqn}.silver_submissions s
""")
spark.table("_lane_e_evarr").write.mode("overwrite").saveAsTable(f"{fqn}._lane_e_evarr_tmp")

# Step 2: explode from the materialised table (no UDF in this plan).
live = spark.sql(f"""
  WITH fired AS (
    SELECT submission_id, received_ts, decided_ts, outcome, explode(events) AS ev
    FROM {fqn}._lane_e_evarr_tmp WHERE size(events) > 0
  )
  SELECT
    sha2(concat_ws('|', submission_id, ev.rule_id, coalesce(received_ts, '')), 256) AS referral_event_id,
    submission_id,
    submission_id AS transaction_id,          -- NB convention (no gold_transactions yet / feed = NB)
    ev.rule_id, ev.rule_version,
    ev.triggering_value, ev.threshold_value, ev.unit,
    to_timestamp(received_ts) AS fired_at,
    to_timestamp(decided_ts)  AS resolved_at,
    CAST(NULL AS STRING) AS decided_by,        -- feed rows carry no assigned underwriter
    CASE
      WHEN outcome IN ('declined', 'withdrawn') THEN 'declined'
      WHEN outcome = 'ntu' THEN 'ntu'
      WHEN outcome IN ('bound', 'lost', 'quote_expired') THEN 'quoted_as_recommended'
      ELSE 'request_information'
    END AS outcome,
    round((unix_timestamp(to_timestamp(decided_ts)) - unix_timestamp(to_timestamp(received_ts))) / 3600.0, 1)
      AS time_to_decision_hours,
    'live_feed' AS event_source
  FROM fired
""")
print(f"live-feed referral events: {live.count()}")

# COMMAND ----------

# MAGIC %md ## Source 2 · generated 2025 history (present from E3a onward)

# COMMAND ----------

EVENT_COLS = ["referral_event_id", "submission_id", "transaction_id", "rule_id", "rule_version",
              "triggering_value", "threshold_value", "unit", "fired_at", "resolved_at",
              "decided_by", "outcome", "time_to_decision_hours", "event_source"]

try:
    gen = spark.table(f"{fqn}.landing_referral_events_generated").select(*EVENT_COLS)
    print(f"generated-history referral events: {gen.count()}")
    events = live.select(*EVENT_COLS).unionByName(gen)
except AnalysisException:
    print("landing_referral_events_generated not present yet (pre-E3a) — live feed only")
    events = live.select(*EVENT_COLS)

# COMMAND ----------

events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_referral_events")
spark.sql(f"DROP TABLE IF EXISTS {fqn}._lane_e_evarr_tmp")
spark.sql(f"ALTER TABLE {fqn}.gold_referral_events SET TBLPROPERTIES "
          f"('layer'='gold', 'demo'='underwriting_workbench', 'lane'='referral_discretion')")
spark.sql(f"COMMENT ON TABLE {fqn}.gold_referral_events IS "
          f"'Rule-grain referral fact: one row per referral rule that fired (SI band, flood-High, "
          f"fair-presentation mismatch, accumulation, MAX_WAGEROLL). Live-feed events from the crux + "
          f"generated 2025 history. Non-fires are counted in the DQ scorecard, not stored here.'")

n = spark.table(f"{fqn}.gold_referral_events").count()
print(f"✅ 03c gold_referral_events: {n} rows")
for r in spark.sql(f"SELECT rule_id, count(*) c FROM {fqn}.gold_referral_events GROUP BY rule_id ORDER BY c DESC").collect():
    print(f"   {r.rule_id}: {r.c}")
