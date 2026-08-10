# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Metric View — `mv_underwriting_discipline` (ADDITIVE, Lane E semantic trunk)
# MAGIC
# MAGIC The single semantic trunk for referral effectiveness and pricing discretion. **All metric
# MAGIC logic lives here** — dashboards and Genie are consumers, not re-implementers. Built as a
# MAGIC Unity Catalog **Metric View** (`CREATE VIEW … WITH METRICS`) over a denormalised base that
# MAGIC joins the Lane E facts:
# MAGIC
# MAGIC `gold_transactions` ⋈ `gold_referral_events` (MAX_WAGEROLL etc.) ⋈ `ref_underwriter[_persona]`
# MAGIC ⋈ `landing_pas_policies` (trade) — one row per transaction, referral attributes attached.
# MAGIC
# MAGIC Measures: referral_rate · discretion_ratio · avg_giveaway_pts · rate_adequacy ·
# MAGIC time_to_decision_hours · transaction_count · gwp. Dimensions: rule_id · transaction_type ·
# MAGIC underwriter · persona · trade · product · effective_month · outcome.
# MAGIC
# MAGIC Additive: new base view + new metric view; nothing existing modified. Idempotent — safe in
# MAGIC the reset path (re-created from refreshed facts).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md ## 1 · Denormalised base — one row per transaction, referral attributes attached
# MAGIC Left-join the generated referral events (one MAX_WAGEROLL row per referred transaction) so
# MAGIC non-referred business is retained with `rule_id = NULL` (the referral-rate denominator).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gold_discipline_base AS
SELECT
  t.transaction_id, t.policy_id, t.submission_id, t.transaction_type, t.product,
  to_date(t.effective_date) AS effective_date,
  date_trunc('MONTH', to_date(t.effective_date)) AS effective_month,
  t.underwriter_id, u.underwriter_name, coalesce(pr.persona, 'unclassified') AS persona,
  p.trade_group AS trade,
  t.technical_premium, t.charged_premium,
  (t.technical_premium - t.charged_premium) AS giveaway_gbp,
  CASE WHEN t.technical_premium > 0
       THEN (t.technical_premium - t.charged_premium) / t.technical_premium * 100 END AS giveaway_pct,
  e.rule_id, e.outcome, e.time_to_decision_hours,
  CASE WHEN e.rule_id IS NOT NULL THEN 1 ELSE 0 END AS is_referred,
  -- E8: value band relative to threshold + rubber-stamp flag (for the effectiveness measures)
  CASE
    WHEN e.threshold_value IS NULL OR e.threshold_value = 0 THEN 'all'
    WHEN e.triggering_value / e.threshold_value < 1.2 THEN '1.0-1.2x'
    WHEN e.triggering_value / e.threshold_value < 1.5 THEN '1.2-1.5x'
    WHEN e.triggering_value / e.threshold_value < 2.0 THEN '1.5-2x'
    ELSE '>2x' END AS value_band,
  CASE WHEN e.outcome='quoted_as_recommended' AND abs(t.technical_premium - t.charged_premium) < 1.0
       THEN 1 ELSE 0 END AS is_rubber_stamp,
  CASE WHEN e.outcome='declined' THEN 1 ELSE 0 END AS is_declined
FROM {fqn}.gold_transactions t
LEFT JOIN {fqn}.landing_referral_events_generated e ON e.transaction_id = t.transaction_id
LEFT JOIN {fqn}.ref_underwriter u ON u.underwriter_id = t.underwriter_id
LEFT JOIN {fqn}.ref_underwriter_persona pr ON pr.underwriter_id = t.underwriter_id
LEFT JOIN {fqn}.landing_pas_policies p ON p.policy_number = t.policy_id
""")
spark.sql(f"COMMENT ON VIEW {fqn}.gold_discipline_base IS "
          f"'Denormalised transaction-grain base for the underwriting-discipline metric view: transaction "
          f"facts + referral-event attributes (rule_id/outcome/latency, null when not referred) + underwriter "
          f"persona + trade. One row per transaction. Source for mv_underwriting_discipline only.'")
print("created view: gold_discipline_base")
print("  rows:", spark.table(f"{fqn}.gold_discipline_base").count())

# COMMAND ----------

# MAGIC %md ## 2 · The Metric View — the semantic trunk
# MAGIC `MEASURE(...)` in a consuming query resolves these definitions. Genie and the dashboard tile
# MAGIC both read this — neither re-defines discretion or referral rate.

# COMMAND ----------

MV_YAML = """version: 0.1

source: {F}.gold_discipline_base

dimensions:
  - name: rule_id
    expr: rule_id
  - name: transaction_type
    expr: transaction_type
  - name: underwriter
    expr: underwriter_name
  - name: persona
    expr: persona
  - name: trade
    expr: trade
  - name: product
    expr: product
  - name: effective_month
    expr: effective_month
  - name: outcome
    expr: outcome
  - name: value_band
    expr: value_band

measures:
  - name: transaction_count
    expr: COUNT(DISTINCT transaction_id)
  - name: referral_count
    expr: COUNT(DISTINCT CASE WHEN is_referred = 1 THEN transaction_id END)
  - name: referral_rate
    expr: COUNT(DISTINCT CASE WHEN is_referred = 1 THEN transaction_id END) * 1.0 / COUNT(DISTINCT transaction_id)
  - name: gwp
    expr: SUM(charged_premium)
  - name: technical_gwp
    expr: SUM(technical_premium)
  - name: discretion_ratio
    expr: SUM(charged_premium) / NULLIF(SUM(technical_premium), 0)
  - name: rate_adequacy
    expr: AVG(charged_premium / NULLIF(technical_premium, 0)) * 100
  - name: avg_giveaway_pts
    expr: AVG(giveaway_pct)
  - name: total_giveaway_gbp
    expr: SUM(giveaway_gbp)
  - name: time_to_decision_hours
    expr: AVG(time_to_decision_hours)
  - name: rubber_stamp_rate
    expr: SUM(is_rubber_stamp) * 1.0 / NULLIF(SUM(is_referred), 0)
  - name: changed_answer_rate
    expr: 1 - (SUM(is_rubber_stamp) * 1.0 / NULLIF(SUM(is_referred), 0))
  - name: decline_rate
    expr: SUM(is_declined) * 1.0 / NULLIF(SUM(is_referred), 0)
""".replace("{F}", fqn)

# Escape for the SQL string literal ($$ dollar-quoting keeps the YAML readable).
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.mv_underwriting_discipline
WITH METRICS
LANGUAGE YAML
COMMENT 'Underwriting discipline semantic trunk (Lane E): referral effectiveness and pricing discretion over gold_discipline_base. Measures: transaction_count, referral_rate, gwp, discretion_ratio (Σcharged/Σtechnical), rate_adequacy, avg_giveaway_pts, total_giveaway_gbp, time_to_decision_hours. Dimensions: rule_id, transaction_type, underwriter, persona, trade, product, effective_month, outcome. Query with MEASURE(). All metric logic lives here - dashboards and Genie consume it.'
AS $${MV_YAML}$$
""")
print("created metric view: mv_underwriting_discipline")

# COMMAND ----------

# MAGIC %md ## 3 · Verify — the three scripted Genie questions answered via MEASURE()

# COMMAND ----------

MV = f"{fqn}.mv_underwriting_discipline"

# Q1 — the practitioner question: MAX_WAGEROLL-referred transactions split by type, technical vs charged.
print("Q1 · MAX_WAGEROLL referrals — technical vs charged by transaction type")
q1 = spark.sql(f"""
  SELECT transaction_type,
         MEASURE(transaction_count) AS n,
         round(MEASURE(technical_gwp)) AS technical,
         round(MEASURE(gwp)) AS charged,
         round(MEASURE(avg_giveaway_pts), 2) AS giveaway_pts
  FROM {MV} WHERE rule_id = 'MAX_WAGEROLL'
  GROUP BY transaction_type ORDER BY giveaway_pts DESC""")
q1.show(truncate=False)
rows = {r["transaction_type"]: r for r in q1.collect()}
assert rows and rows.get("RENEWAL") and rows["RENEWAL"]["giveaway_pts"] > rows["NEW_BUSINESS"]["giveaway_pts"], \
    "renewal give-away must exceed NB on wageroll referrals"

# Q2 — which underwriters discount most on wageroll referrals?
print("Q2 · discount by underwriter on MAX_WAGEROLL referrals (top 5)")
spark.sql(f"""
  SELECT underwriter, persona, round(MEASURE(avg_giveaway_pts), 2) AS giveaway_pts,
         MEASURE(transaction_count) AS n
  FROM {MV} WHERE rule_id = 'MAX_WAGEROLL'
  GROUP BY underwriter, persona ORDER BY giveaway_pts DESC LIMIT 5""").show(truncate=False)

# Q3 — discretion ratio trend by quarter.
print("Q3 · discretion ratio by quarter on MAX_WAGEROLL referrals")
q3 = spark.sql(f"""
  SELECT date_trunc('QUARTER', effective_month) AS quarter,
         round(MEASURE(discretion_ratio), 4) AS discretion_ratio,
         MEASURE(transaction_count) AS n
  FROM {MV} WHERE rule_id = 'MAX_WAGEROLL'
  GROUP BY date_trunc('QUARTER', effective_month) ORDER BY quarter""")
q3.show(truncate=False)
assert q3.count() > 0 and all(r["discretion_ratio"] is not None for r in q3.collect()), "Q3 must return non-null"

print("✅ 08 metric view — mv_underwriting_discipline verified on the three scripted questions")
