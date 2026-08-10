# Databricks notebook source
# MAGIC %md
# MAGIC # 07b · Lane E.2 (E8) — rule effectiveness layer (ADDITIVE)
# MAGIC
# MAGIC Turns the referral history into a tuning instrument. Two tails:
# MAGIC * **rubber-stamp** — rules that almost always end quoted-as-recommended, zero adjustment →
# MAGIC   pure friction → auto-accept / threshold-relaxation candidates.
# MAGIC * **auto-decline** — rule×band that overwhelmingly ends declined → hard-stop candidates.
# MAGIC
# MAGIC Builds `gold_rule_effectiveness` (rule × transaction_type × value_band × month), a
# MAGIC `ref_effectiveness_policy` config table, and a `review_effort_hours` column on
# MAGIC `ref_referral_rules`. A small deterministic top-up (rng 4244, APPEND only) guarantees the two
# MAGIC demo exemplars without regenerating anything. Additive; runs after the Lane E facts exist.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
import hashlib
import random

rnd = random.Random(4244)
TODAY = datetime.date.today()
YEAR_START = datetime.date(TODAY.year - 1, 1, 1)


def write(df, name, layer):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.{name}")
    spark.sql(f"ALTER TABLE {fqn}.{name} SET TBLPROPERTIES "
              f"('layer'='{layer}', 'demo'='underwriting_workbench', 'lane'='referral_discretion')")
    print(f"  {name}: {spark.table(f'{fqn}.{name}').count()} rows")

# COMMAND ----------

# MAGIC %md ## `ref_effectiveness_policy` — candidate thresholds (config, not hard-coded)

# COMMAND ----------

policy = [
    ("auto_accept_rubber_stamp_rate", 0.92, "Rubber-stamp rate at/above which a rule×band is an auto-accept / relaxation candidate"),
    ("auto_accept_min_fires", 50.0, "Minimum trailing-12m fires for an auto-accept candidate"),
    ("auto_decline_rate", 0.75, "Decline rate at/above which a rule×band is an auto-decline candidate"),
    ("auto_decline_min_fires", 30.0, "Minimum fires in the band for an auto-decline candidate"),
    ("min_history_months", 12.0, "Minimum months of history before any recommendation"),
]
write(spark.createDataFrame(policy, "policy_key string, policy_value double, description string"),
      "ref_effectiveness_policy", "reference")
POL = {r.policy_key: r.policy_value for r in spark.table(f"{fqn}.ref_effectiveness_policy").collect()}

# COMMAND ----------

# MAGIC %md ## `review_effort_hours` on `ref_referral_rules` (additive column, defaults by family)

# COMMAND ----------

# Effort per referral review, by rule family (illustrative). Compliance/history reviews cost more.
EFFORT_BY_RULE = {
    "MAX_WAGEROLL": 0.5, "SI_AUTHORITY_BAND": 0.4, "ROFRS_HIGH": 0.6, "FAIR_PRESENTATION_MISMATCH": 1.2,
    "ACCUMULATION_CAPACITY": 0.7, "MAX_TURNOVER": 0.5, "MAX_SUMS_INSURED": 0.5,
    "HAZARDOUS_ACTIVITY_HEIGHT": 0.9, "CLAIMS_FREQUENCY": 1.1, "FLOOD_POSTCODE": 0.6,
    "PRICE_BELOW_TECHNICAL_FLOOR": 0.8, "RENEWAL_RATE_CHANGE_TOLERANCE": 0.7,
}
if "review_effort_hours" not in [c.name for c in spark.table(f"{fqn}.ref_referral_rules").schema]:
    spark.sql(f"ALTER TABLE {fqn}.ref_referral_rules ADD COLUMN review_effort_hours DOUBLE")
for rid, hrs in EFFORT_BY_RULE.items():
    spark.sql(f"UPDATE {fqn}.ref_referral_rules SET review_effort_hours = {hrs} WHERE rule_id = '{rid}'")
print("  review_effort_hours set on ref_referral_rules")

# COMMAND ----------

# MAGIC %md ## Deterministic top-up (rng 4244) — guarantee the two demo exemplars
# MAGIC APPEND-only events (+ their LT- transactions) so `gold_rule_effectiveness` shows a clean
# MAGIC rubber-stamp story (MAX_TURNOVER, 1.0–1.2× band) and a clean hard-stop story
# MAGIC (HAZARDOUS_ACTIVITY_HEIGHT, >2× band). Idempotent: clears prior LT- rows first.

# COMMAND ----------

for t in ("landing_referral_events_generated", "gold_transactions", "gold_premium_components"):
    if spark.catalog.tableExists(f"{fqn}.{t}"):
        col = "transaction_id"
        spark.sql(f"DELETE FROM {fqn}.{t} WHERE {col} LIKE 'LT-%'")

pol = spark.sql(f"""SELECT policy_number, gross_premium,
                       coalesce(product_line, CASE WHEN segment='mid_market' THEN 'commercial_combined'
                                                   ELSE 'commercial_package' END) product
                    FROM {fqn}.landing_pas_policies WHERE policy_status='in_force' LIMIT 400""").collect()
uw_ids = [r.underwriter_id for r in spark.sql(f"SELECT underwriter_id FROM {fqn}.ref_underwriter_persona").collect()]

tup_txns, tup_comps, tup_events = [], [], []
_s = 700_000


def add(rule_id, band_mult, outcome, giveaway_pts, n, latency):
    """Append n events of rule_id at a triggering_value = band_mult × threshold, given outcome."""
    global _s
    for _ in range(n):
        p = rnd.choice(pol)
        uw = rnd.choice(uw_ids)
        ttype = "RENEWAL" if rule_id == "MAX_TURNOVER" else rnd.choice(["NEW_BUSINESS", "RENEWAL"])
        technical = int(p.gross_premium * rnd.uniform(0.95, 1.25))
        adj = round(technical * giveaway_pts / 100.0)
        charged = int(round(technical - adj))
        txn = f"LT-{_s:06d}"; _s += 1
        ed = YEAR_START + datetime.timedelta(days=rnd.randint(0, 364))
        entered = datetime.datetime.combine(ed, datetime.time(11, 0)).isoformat()
        comm = 0.225 if "package" in p.product else 0.20
        tup_txns.append((txn, p.policy_number, None, ttype, ed.isoformat(), p.product, uw,
                         float(technical), float(charged), float(round(charged * 0.12)), float(comm)))
        tup_comps.append((txn, "TECHNICAL", None, float(technical), uw, entered))
        if adj > 0:
            tup_comps.append((txn, "DISCOUNT", "RENEWAL_RETENTION_DISCOUNT", float(-adj), uw, entered))
        elif adj < 0:
            tup_comps.append((txn, "LOAD", "RISK_FEATURE_LOAD", float(-adj), uw, entered))
        tup_comps.append((txn, "IPT", None, float(round(charged * 0.12)), uw, entered))
        tup_comps.append((txn, "COMMISSION", None, float(round(charged * comm)), uw, entered))
        thr = {"MAX_TURNOVER": 20_000_000, "HAZARDOUS_ACTIVITY_HEIGHT": 1}[rule_id]
        eid = hashlib.sha256(f"{txn}|{rule_id}|{ed.isoformat()}".encode()).hexdigest()[:32]
        resolved = datetime.datetime.combine(ed, datetime.time(9, 0)) + datetime.timedelta(hours=latency)
        tup_events.append((eid, None, txn, rule_id, "v1", float(thr) * band_mult, float(thr), "unit",
                           datetime.datetime.combine(ed, datetime.time(9, 0)).isoformat(),
                           resolved.isoformat(), uw, outcome, float(latency), "generated_2025"))


# Exemplar 1 — MAX_TURNOVER, 1.0–1.2× band: ~95% rubber-stamp (friction/relaxation story).
add("MAX_TURNOVER", 1.1, "quoted_as_recommended", 0.0, 76, 6)   # rubber-stamped
add("MAX_TURNOVER", 1.1, "quoted_with_adjustment", 5.0, 4, 8)    # the ~5% that changed the answer
# Exemplar 2 — HAZARDOUS_ACTIVITY_HEIGHT, >2× band (extreme heights): ~85% decline (hard-stop story).
add("HAZARDOUS_ACTIVITY_HEIGHT", 2.5, "declined", 0.0, 34, 12)
add("HAZARDOUS_ACTIVITY_HEIGHT", 2.5, "quoted_with_adjustment", -4.0, 6, 12)

import pyspark.sql.functions as _F
spark.createDataFrame(tup_txns, "transaction_id string, policy_id string, submission_id string, "
    "transaction_type string, effective_date string, product string, underwriter_id string, "
    "technical_premium double, charged_premium double, ipt_amount double, commission_pct double"
    ).write.mode("append").saveAsTable(f"{fqn}.gold_transactions")
spark.createDataFrame(tup_comps, "transaction_id string, component_type string, reason_code string, "
    "amount double, entered_by string, entered_at string").write.mode("append").saveAsTable(f"{fqn}.gold_premium_components")
spark.createDataFrame(tup_events, "referral_event_id string, submission_id string, transaction_id string, "
    "rule_id string, rule_version string, triggering_value double, threshold_value double, unit string, "
    "fired_at string, resolved_at string, decided_by string, outcome string, time_to_decision_hours double, "
    "event_source string").write.mode("append").saveAsTable(f"{fqn}.landing_referral_events_generated")
print(f"  top-up appended: {len(tup_events)} events / {len(tup_txns)} txns (exemplars)")

# COMMAND ----------

# MAGIC %md ## `gold_rule_effectiveness` — rule × type × value_band × month
# MAGIC Value banding relative to threshold for numeric rules; a single 'all' band otherwise. Measures
# MAGIC + two candidate flags from `ref_effectiveness_policy`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.gold_rule_effectiveness AS
WITH ev AS (
  SELECT e.rule_id, t.transaction_type,
         date_trunc('MONTH', to_date(t.effective_date)) AS month,
         CASE
           WHEN e.threshold_value IS NULL OR e.threshold_value = 0 THEN 'all'
           WHEN e.triggering_value / e.threshold_value < 1.2 THEN '1.0-1.2x'
           WHEN e.triggering_value / e.threshold_value < 1.5 THEN '1.2-1.5x'
           WHEN e.triggering_value / e.threshold_value < 2.0 THEN '1.5-2x'
           ELSE '>2x' END AS value_band,
         e.outcome, e.time_to_decision_hours,
         (t.technical_premium - t.charged_premium) / nullif(t.technical_premium,0) * 100 AS giveaway_pct,
         abs(t.technical_premium - t.charged_premium) < 1.0 AS zero_adj
  FROM {fqn}.landing_referral_events_generated e
  JOIN {fqn}.gold_transactions t USING (transaction_id)
),
agg AS (
  SELECT rule_id, transaction_type, value_band, month,
         count(*) AS fires,
         avg(CASE WHEN outcome='quoted_as_recommended' AND zero_adj THEN 1.0 ELSE 0.0 END) AS rubber_stamp_rate,
         avg(CASE WHEN outcome='quoted_with_adjustment' THEN 1.0 ELSE 0.0 END) AS adjusted_rate,
         avg(CASE WHEN outcome='declined' THEN 1.0 ELSE 0.0 END) AS decline_rate,
         avg(CASE WHEN outcome='request_information' THEN 1.0 ELSE 0.0 END) AS info_request_rate,
         percentile_approx(time_to_decision_hours, 0.5) AS median_latency_hours,
         avg(giveaway_pct) AS avg_giveaway_pts
  FROM ev GROUP BY rule_id, transaction_type, value_band, month
)
SELECT a.*,
       (1 - a.rubber_stamp_rate) AS changed_answer_rate,
       round(a.fires * coalesce(r.review_effort_hours, 0.6), 1) AS est_uw_hours,
       -- Candidate flags evaluate at the rule×band level (trailing window), not per-month, so a
       -- clean tail isn't hidden by monthly fire-count splits.
       (band.band_rubber >= {POL['auto_accept_rubber_stamp_rate']} AND band.band_fires >= {POL['auto_accept_min_fires']}) AS auto_accept_candidate,
       (band.band_decline >= {POL['auto_decline_rate']} AND band.band_fires >= {POL['auto_decline_min_fires']}) AS auto_decline_candidate
FROM agg a
LEFT JOIN (SELECT rule_id, max(review_effort_hours) AS review_effort_hours
           FROM {fqn}.ref_referral_rules GROUP BY rule_id) r USING (rule_id)
LEFT JOIN (
  SELECT rule_id, value_band, sum(fires) AS band_fires,
         sum(fires*rubber_stamp_rate)/sum(fires) AS band_rubber,
         sum(fires*decline_rate)/sum(fires) AS band_decline
  FROM agg GROUP BY rule_id, value_band
) band USING (rule_id, value_band)
""")
spark.sql(f"ALTER TABLE {fqn}.gold_rule_effectiveness SET TBLPROPERTIES "
          f"('layer'='gold','demo'='underwriting_workbench','lane'='referral_discretion')")
spark.sql(f"COMMENT ON TABLE {fqn}.gold_rule_effectiveness IS "
          f"'Referral rule effectiveness at rule × transaction_type × value_band × month: fires, "
          f"rubber_stamp_rate, adjusted/decline/info-request rates, changed_answer_rate, median latency, "
          f"est_uw_hours, avg_giveaway_pts, and auto_accept / auto_decline candidate flags (thresholds "
          f"in ref_effectiveness_policy). The tuning instrument behind the AI recommendations.'")

print("  gold_rule_effectiveness rows:", spark.table(f"{fqn}.gold_rule_effectiveness").count())

# COMMAND ----------

# MAGIC %md ## Quick check — the two exemplars are findable

# COMMAND ----------

for r in spark.sql(f"""
    SELECT rule_id, value_band, sum(fires) fires,
           round(sum(fires*rubber_stamp_rate)/sum(fires),2) rubber, round(sum(fires*decline_rate)/sum(fires),2) decl,
           max(auto_accept_candidate) acc, max(auto_decline_candidate) dec
    FROM {fqn}.gold_rule_effectiveness
    WHERE rule_id IN ('MAX_TURNOVER','HAZARDOUS_ACTIVITY_HEIGHT')
    GROUP BY rule_id, value_band ORDER BY rule_id, value_band""").collect():
    print(f"  {r.rule_id} {r.value_band}: fires={r.fires} rubber={r.rubber} decl={r.decl} accept_cand={r.acc} decline_cand={r.dec}")

print("✅ 07b E8 rule effectiveness complete")
