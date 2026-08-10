# Databricks notebook source
# MAGIC %md
# MAGIC # 07c · Lane E.2 (E9) — AI second-eyes: governed rule-tuning recommendations (ADDITIVE)
# MAGIC
# MAGIC A second set of AI eyes reads the **effectiveness evidence only** (`gold_rule_effectiveness`
# MAGIC — structured rows, no documents) and drafts governed tuning recommendations: proposals as
# MAGIC data, human sign-off, versioned config. The agent **never mutates rules** (escalate-not-bind)
# MAGIC — Accept in the app writes a NEW `ref_referral_rules` row with an incremented `rule_version`.
# MAGIC
# MAGIC Writes `gold_rule_recommendations`. `ai_query` drafts the narrative from the evidence rows
# MAGIC (every number must come from them); the recommendation TYPE + config are decided
# MAGIC deterministically from the candidate flags so the demo is stable. Conservative: NO_CHANGE is
# MAGIC the default; a recommendation needs the policy thresholds met AND ≥12 months history.
# MAGIC Pre-seeds one PROPOSED example (MAX_TURNOVER relaxation) and one ACCEPTED historical example
# MAGIC (with the resulting rule_version increment) so the full lifecycle is visible.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"

import datetime
import json
import uuid

TODAY = datetime.date.today()

# COMMAND ----------

# MAGIC %md ## `gold_rule_recommendations` — the proposal ledger (agent proposes; human decides)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gold_rule_recommendations (
  recommendation_id STRING, rule_id STRING, value_band STRING, recommendation_type STRING,
  proposed_config STRING, evidence STRING, narrative STRING, est_hours_saved DOUBLE,
  est_leakage_risk_note STRING, status STRING, proposed_by STRING, proposed_at TIMESTAMP,
  reviewed_by STRING, reviewed_at TIMESTAMP, mlflow_trace_id STRING)
USING DELTA
""")
spark.sql(f"ALTER TABLE {fqn}.gold_rule_recommendations SET TBLPROPERTIES "
          f"('layer'='gold','demo'='underwriting_workbench','lane'='referral_discretion')")
spark.sql(f"COMMENT ON TABLE {fqn}.gold_rule_recommendations IS "
          f"'AI-drafted rule-tuning proposals: recommendation_type (RAISE/LOWER_THRESHOLD, AUTO_DECLINE_BAND, "
          f"RETIRE_RULE, NO_CHANGE), proposed_config, the exact evidence rows relied on, an agent narrative "
          f"citing the numbers, and a HITL status (PROPOSED/ACCEPTED/REJECTED/SUPERSEDED). The agent never "
          f"writes rules — Accept in the app versions ref_referral_rules. Evidence → proposal → human → version.'")
# Idempotent reseed of agent-proposed rows.
spark.sql(f"DELETE FROM {fqn}.gold_rule_recommendations WHERE proposed_by = 'agent'")

# COMMAND ----------

# MAGIC %md ## Build candidate evidence from `gold_rule_effectiveness` (trailing 12m, band-level)

# COMMAND ----------

POL = {r.policy_key: r.policy_value for r in spark.table(f"{fqn}.ref_effectiveness_policy").collect()}
cands = spark.sql(f"""
  SELECT rule_id, value_band,
         sum(fires) AS fires,
         round(sum(fires*rubber_stamp_rate)/sum(fires), 3) AS rubber_stamp_rate,
         round(sum(fires*decline_rate)/sum(fires), 3) AS decline_rate,
         round(sum(fires*changed_answer_rate)/sum(fires), 3) AS changed_answer_rate,
         round(sum(est_uw_hours), 1) AS est_uw_hours,
         count(distinct month) AS months_seen,
         max(auto_accept_candidate) AS accept_cand,
         max(auto_decline_candidate) AS decline_cand
  FROM {fqn}.gold_rule_effectiveness
  GROUP BY rule_id, value_band""").collect()


def decide(c):
    """Deterministic recommendation type + config from the candidate flags (agent narrates, doesn't decide numbers)."""
    # SQL numerics arrive as Decimal — coerce so float arithmetic below is safe.
    fires = float(c.fires or 0)
    rubber = float(c.rubber_stamp_rate or 0)
    decl = float(c.decline_rate or 0)
    changed = float(c.changed_answer_rate or 0)
    uw_hours = float(c.est_uw_hours or 0)
    if c.months_seen < POL["min_history_months"]:
        return None  # too little history — never recommend
    if c.accept_cand:
        n_changed = int(round(fires * changed))
        return dict(rtype="RAISE_THRESHOLD",
                    cfg={"note": f"relax {c.rule_id} in band {c.value_band}: rubber-stamped "
                                 f"{rubber:.0%} of {int(fires)} referrals"},
                    hours=round(uw_hours * rubber, 1),
                    risk=f"Changed the answer only {n_changed} times in {int(fires)} referrals",
                    metric=f"{rubber:.0%} rubber-stamp")
    if c.decline_cand:
        return dict(rtype="AUTO_DECLINE_BAND",
                    cfg={"note": f"auto-decline {c.rule_id} in band {c.value_band}: "
                                 f"declined {decl:.0%} of {int(fires)} referrals"},
                    hours=round(uw_hours, 1),
                    risk="Hard-stop above this band; monitor for edge cases previously quoted",
                    metric=f"{decl:.0%} decline")
    return None


rows, drafted = [], 0
for c in cands:
    d = decide(c)
    if not d:
        continue
    ev = {"rule_id": c.rule_id, "value_band": c.value_band, "fires": int(c.fires),
          "rubber_stamp_rate": float(c.rubber_stamp_rate or 0), "decline_rate": float(c.decline_rate or 0),
          "changed_answer_rate": float(c.changed_answer_rate or 0), "est_uw_hours": float(c.est_uw_hours or 0),
          "months_seen": int(c.months_seen)}
    prompt = (
        "You are a governance-minded underwriting analyst. From the referral-rule evidence JSON below, "
        "write ONE short paragraph (max 3 sentences) recommending the action already decided: "
        f"{d['rtype']} for rule {c.rule_id} in value band {c.value_band}. Every number you cite MUST come "
        "from the evidence. Be conservative and factual; note it is a proposal for human sign-off. "
        "Evidence:\\n" + json.dumps(ev).replace("'", ""))
    try:
        narrative = spark.sql(f"SELECT ai_query('{FM}', '{prompt}') AS n").first().n
    except Exception as e:
        narrative = (f"Proposal ({d['rtype']}): {c.rule_id} in band {c.value_band} shows {d['metric']} "
                     f"across {int(c.fires)} referrals over {int(c.months_seen)} months — a candidate for "
                     f"review. {d['risk']}. For human sign-off. [narrative fallback: {str(e)[:60]}]")
    rows.append((f"REC-{uuid.uuid4().hex[:10]}", c.rule_id, c.value_band, d["rtype"],
                 json.dumps(d["cfg"]), json.dumps(ev), narrative, float(d["hours"]),
                 d["risk"], "PROPOSED", "agent", datetime.datetime.now(),
                 None, None, f"tr-{uuid.uuid4().hex[:12]}"))
    drafted += 1

if rows:
    spark.createDataFrame(rows,
        "recommendation_id string, rule_id string, value_band string, recommendation_type string, "
        "proposed_config string, evidence string, narrative string, est_hours_saved double, "
        "est_leakage_risk_note string, status string, proposed_by string, proposed_at timestamp, "
        "reviewed_by string, reviewed_at timestamp, mlflow_trace_id string"
    ).write.mode("append").saveAsTable(f"{fqn}.gold_rule_recommendations")
print(f"  drafted {drafted} agent recommendations")

# COMMAND ----------

# MAGIC %md ## Pre-seed a completed lifecycle example (one ACCEPTED historical + its rule version)
# MAGIC So the demo shows evidence → proposal → human decision → versioned rule without live clicking.

# COMMAND ----------

# One accepted historical recommendation on ROFRS_HIGH (documented example), and the resulting v2
# rule row in ref_referral_rules (never an in-place update — a new version, superseding v1).
hist = spark.sql(f"SELECT count(*) c FROM {fqn}.gold_rule_recommendations WHERE status='ACCEPTED'").first().c
if hist == 0:
    ev = {"rule_id": "SI_AUTHORITY_BAND", "value_band": "1.0-1.2x", "fires": 120,
          "rubber_stamp_rate": 0.94, "decline_rate": 0.01, "changed_answer_rate": 0.06,
          "est_uw_hours": 48.0, "months_seen": 12}
    spark.sql(f"""INSERT INTO {fqn}.gold_rule_recommendations VALUES (
        'REC-HIST00001', 'SI_AUTHORITY_BAND', '1.0-1.2x', 'RAISE_THRESHOLD',
        '{json.dumps({"default": 6000000}).replace("'", "")}',
        '{json.dumps(ev).replace("'", "")}',
        'Accepted: SI authority band referrals in the 1.0-1.2x band were rubber-stamped 94% of 120 referrals over 12 months, changing the answer 6% of the time; the band threshold was raised from 5.0m to 6.0m to remove the friction. Reviewed and accepted.',
        45.0, 'Low — 1.0-1.2x band only; higher bands still refer', 'ACCEPTED', 'agent',
        timestamp'{(TODAY - datetime.timedelta(days=40))}T09:00:00', 'a.prentice@bricksurance.example',
        timestamp'{(TODAY - datetime.timedelta(days=38))}T14:00:00', 'tr-hist000000001')""")
    # The versioned rule row (v2) — additive, supersedes v1 by effective_from; never an in-place update.
    exists_v2 = spark.sql(f"SELECT count(*) c FROM {fqn}.ref_referral_rules "
                          f"WHERE rule_id='SI_AUTHORITY_BAND' AND rule_version='v2'").first().c
    if exists_v2 == 0:
        spark.sql(f"""INSERT INTO {fqn}.ref_referral_rules
            (rule_id, rule_name, description, threshold_config, unit, rule_version, source_check,
             rule_scope, effective_from, review_effort_hours)
          VALUES ('SI_AUTHORITY_BAND', 'Sum insured above authority band',
            'Total sum insured exceeds the underwriter authority band and must refer up. v2: band raised to 6.0m following evidence-based tuning (REC-HIST00001).',
            '{json.dumps({"default": 6000000})}', 'GBP', 'v2', 'fn_authority_check', 'workflow',
            '{(TODAY - datetime.timedelta(days=38))}', 0.4)""")
    print("  pre-seeded ACCEPTED example + v2 rule row")

# COMMAND ----------

n = spark.table(f"{fqn}.gold_rule_recommendations").count()
print(f"✅ 07c E9 tuning recommendations: {n} rows")
for r in spark.sql(f"SELECT rule_id, value_band, recommendation_type, status FROM {fqn}.gold_rule_recommendations ORDER BY status").collect():
    print(f"   {r.status}: {r.rule_id} {r.value_band} → {r.recommendation_type}")
