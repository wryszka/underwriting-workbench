# Databricks notebook source
# MAGIC %md
# MAGIC # 07d · Referral Control detection — findings feed + rule effectiveness (from telemetry)
# MAGIC
# MAGIC The scheduled detection pass. Reads `gold_referral_telemetry` through the `05e` functions and
# MAGIC materialises two tables (SUPERSEDES the E8 `gold_rule_effectiveness` + E9 `gold_rule_recommendations`):
# MAGIC
# MAGIC * `gold_rule_effectiveness` — the per-rule metric tuple + recommended action AS OF the anchor
# MAGIC   (the dashboard league table). Recomputed from telemetry via `fn_rule_metrics` / `fn_recommend_action`.
# MAGIC * `gold_referral_findings` — the ranked findings feed at four monthly `as_of` snapshots (anchor,
# MAGIC   +30, +60, +90) so the dashboard shows the trend and the demo's "advance a month" reveals the
# MAGIC   changed feed. The app's Today tab calls the functions LIVE for any scrubbed date (free scrub).
# MAGIC
# MAGIC Compliance-locked rules appear with their metrics but always carry `keep` (enforced in the fn).
# MAGIC Runs after `05e` (the functions) in the ml job, and on reset (telemetry is rebuilt by `00f`;
# MAGIC the functions persist).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
TODAY = datetime.date.today()
SNAPS = [TODAY, TODAY + datetime.timedelta(days=30), TODAY + datetime.timedelta(days=60),
         TODAY + datetime.timedelta(days=90)]
snap_sql = ", ".join(f"DATE'{d.isoformat()}'" for d in SNAPS)
print(f"Referral Control detection → {fqn}  anchor={TODAY}  snapshots={[d.isoformat() for d in SNAPS]}")

# COMMAND ----------

# MAGIC %md ## `gold_rule_effectiveness` — metric tuple + recommended action as of the anchor

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.gold_rule_effectiveness AS
WITH base AS (
  SELECT r.rule_id, r.category, r.rule_scope, r.rule_name,
         {fqn}.fn_rule_metrics(r.rule_id, DATE'{TODAY.isoformat()}') AS met,
         {fqn}.fn_recommend_action(r.rule_id, DATE'{TODAY.isoformat()}') AS rec
  FROM (SELECT rule_id, category, rule_scope, rule_name FROM {fqn}.ref_referral_rules WHERE valid_to IS NULL) r
)
SELECT DATE'{TODAY.isoformat()}' AS as_of_date, rule_id, rule_name, category, rule_scope,
       met.disposition, met.compliance_lock, met.fires, met.approvals, met.approval_pct,
       met.decline_walk_pct, met.price_walk_pct, met.noadj_pct, met.isolated_fires, met.isolated_share,
       met.avg_lr, met.isolated_lr, met.cofire_lr, met.gwp_bound, met.gwp_at_stake, met.shadow_fires,
       met.top_clause, met.top_clause_share, met.review_effort_hours, met.touch_cost_gbp,
       rec.action AS recommended_action, rec.severity, rec.headline, rec.reason
FROM base
""")
spark.sql(f"ALTER TABLE {fqn}.gold_rule_effectiveness SET TBLPROPERTIES "
          f"('layer'='gold','demo'='underwriting_workbench','lane'='referral_control')")
n_eff = spark.table(f"{fqn}.gold_rule_effectiveness").count()
print(f"  gold_rule_effectiveness: {n_eff} rules")

# COMMAND ----------

# MAGIC %md ## `gold_referral_findings` — ranked findings at four monthly as_of snapshots

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.gold_referral_findings AS
WITH snaps AS (SELECT explode(array({snap_sql})) AS as_of_snapshot),
rules AS (SELECT rule_id, category FROM {fqn}.ref_referral_rules
          WHERE valid_to IS NULL AND rule_scope IN ('workflow','analytics_only')),
base AS (
  SELECT s.as_of_snapshot, r.rule_id, r.category,
         {fqn}.fn_recommend_action(r.rule_id, s.as_of_snapshot) AS rec
  FROM rules r CROSS JOIN snaps s
)
SELECT as_of_snapshot, rule_id, category,
       rec.action AS recommended_action, rec.severity, rec.headline, rec.reason,
       rec.portfolio_gbp_note, rec.ops_note, rec.referrals_released, rec.hours_released, rec.gwp_impact,
       rec.evidence.fires, rec.evidence.approval_pct, rec.evidence.decline_walk_pct, rec.evidence.noadj_pct,
       rec.evidence.isolated_share, rec.evidence.gwp_at_stake, rec.evidence.recent_stake,
       rec.evidence.prior_stake, rec.evidence.touch_cost_gbp, rec.evidence.compliance_lock,
       CASE rec.severity WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'locked' THEN 0 ELSE 1 END AS severity_rank
FROM base
""")
spark.sql(f"ALTER TABLE {fqn}.gold_referral_findings SET TBLPROPERTIES "
          f"('layer'='gold','demo'='underwriting_workbench','lane'='referral_control')")
n_find = spark.table(f"{fqn}.gold_referral_findings").count()
print(f"  gold_referral_findings: {n_find} rows ({len(SNAPS)} snapshots)")

# COMMAND ----------

# MAGIC %md ## `gold_rule_cofire_partners` — "what else catches the rest" (isolation panel support)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.gold_rule_cofire_partners AS
SELECT a.rule_id, b.rule_id AS partner_rule_id, count(*) AS times_together
FROM {fqn}.gold_referral_telemetry a
JOIN {fqn}.gold_referral_telemetry b
  ON a.transaction_id = b.transaction_id AND a.rule_id <> b.rule_id AND a.fired AND b.fired
WHERE a.as_of_date <= DATE'{TODAY.isoformat()}'
GROUP BY a.rule_id, b.rule_id
""")
spark.sql(f"ALTER TABLE {fqn}.gold_rule_cofire_partners SET TBLPROPERTIES "
          f"('layer'='gold','demo'='underwriting_workbench','lane'='referral_control')")
print(f"  gold_rule_cofire_partners: {spark.table(f'{fqn}.gold_rule_cofire_partners').count()} pairs")

# COMMAND ----------

# MAGIC %md ## Verification — the anchor feed discovers the seeded storylines

# COMMAND ----------

eff = {r.rule_id: r for r in spark.table(f"{fqn}.gold_rule_effectiveness").collect()}
for rid in ("HAZARDOUS_ACTIVITY_HEIGHT", "DUAL_TRADE_DECLARED", "RENEWAL_UNCHANGED_RISK",
            "EVENT_ATTENDANCE_LIMIT", "NEW_VENTURE_TRADING_HISTORY", "SANCTIONS_SCREEN_HIT"):
    e = eff[rid]
    print(f"  {rid:32s} {e.recommended_action:24s} [{e.severity}] fires={e.fires} appr={e.approval_pct} iso={e.isolated_share}")

# Anchor feed (as_of = today) discovers S1/S3/S4/S5/S7 with the expected actions.
anchor = TODAY.isoformat()
feed = {r.rule_id: r for r in spark.sql(f"""
    SELECT * FROM {fqn}.gold_referral_findings WHERE as_of_snapshot = DATE'{anchor}'""").collect()}
assert feed["HAZARDOUS_ACTIVITY_HEIGHT"].recommended_action == "convert_to_auto_decline", \
    f"S1 expected convert, got {feed['HAZARDOUS_ACTIVITY_HEIGHT'].recommended_action}"
assert feed["EVENT_ATTENDANCE_LIMIT"].recommended_action == "auto_apply_clause", "S3 expected auto_apply_clause"
assert feed["DUAL_TRADE_DECLARED"].recommended_action == "remove", "S4 expected remove"
assert feed["NEW_VENTURE_TRADING_HISTORY"].recommended_action == "keep", "S5 expected keep"
assert feed["RENEWAL_UNCHANGED_RISK"].recommended_action == "re_threshold", "S7 expected re_threshold"
assert feed["SANCTIONS_SCREEN_HIT"].recommended_action == "keep", "S6 (locked) expected keep"

# Compliance-locked rules never carry a change recommendation.
locked_bad = spark.sql(f"""SELECT count(*) c FROM {fqn}.gold_referral_findings
    WHERE compliance_lock AND recommended_action <> 'keep'""").first().c
assert locked_bad == 0, f"{locked_bad} compliance-locked findings recommend a change — must be keep"

# Findings exist at every snapshot.
snap_counts = {str(r.as_of_snapshot): r.c for r in spark.sql(f"""
    SELECT as_of_snapshot, count(*) c FROM {fqn}.gold_referral_findings GROUP BY 1 ORDER BY 1""").collect()}
print("  findings by snapshot:", snap_counts)
assert len(snap_counts) == len(SNAPS), "expected a findings snapshot per as_of date"

print("✅ 07d Referral Control detection — effectiveness + findings materialised; storylines discovered")
