# Databricks notebook source
# MAGIC %md
# MAGIC # 07e · Referral Control governance — change ledger + SCD2 write-path + predicted-vs-realised
# MAGIC
# MAGIC The governance loop: discover → recommend → emulate → **approve** → monitor → reverse. This
# MAGIC notebook owns `gold_rule_changes` (one row per proposed change, full lifecycle) and the SCD2
# MAGIC write-path that a HUMAN approval runs: close the current rule version (`valid_to`) and append
# MAGIC the next version to `ref_referral_rules` (escalate-not-bind — the engine never auto-applies).
# MAGIC
# MAGIC It seeds a populated ledger so the demo opens with history: two APPROVED+monitored changes
# MAGIC (one on-track, one diverging → drift flag) and one PROPOSED change (the live convert beat). The
# MAGIC two approved changes bump analytics rules to v2 — giving the registry the SCD2 version history
# MAGIC the Rulebook as-of scrubber shows (and NOT touching any rule a hero fires, so heroes stay
# MAGIC byte-identical). Runs after `00f` (telemetry) and before `07d` (detection reads current rules).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
import hashlib
TODAY = datetime.date.today()
CHANGES = f"{fqn}.gold_rule_changes"
RULES = f"{fqn}.ref_referral_rules"
TELE = f"{fqn}.gold_referral_telemetry"
LOADED_HOURLY_COST = 95.0

# COMMAND ----------

# MAGIC %md ## `gold_rule_changes` — the change ledger (create empty with the full lifecycle schema)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CHANGES} (
  change_id STRING, rule_id STRING, action STRING, proposed_by STRING, proposed_at TIMESTAMP,
  status STRING, approved_by STRING, approved_at TIMESTAMP, effective_date DATE,
  from_version STRING, to_version STRING,
  predicted_referrals_released BIGINT, predicted_hours_released DOUBLE, predicted_gwp_delta DOUBLE,
  predicted_lr_delta DOUBLE,
  realised_referrals_released BIGINT, realised_hours_saved DOUBLE, realised_gwp_effect DOUBLE,
  realised_lr_delta DOUBLE, drift_flag BOOLEAN, drift_note STRING, rationale STRING
) USING DELTA
""")
spark.sql(f"ALTER TABLE {CHANGES} SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench','lane'='referral_control')")
spark.sql(f"COMMENT ON TABLE {CHANGES} IS 'Referral-rule change ledger: one row per proposed change with its frozen predicted impact pack, lifecycle status (proposed -> approved -> live -> monitored -> reversed/retired), and realised-vs-predicted tracking (divergence past tolerance sets drift_flag). Approval writes a new SCD2 version to ref_referral_rules.'")
print("  gold_rule_changes created")

# COMMAND ----------

# MAGIC %md ## Predicted impact from telemetry (emulate-equivalent, no ML dependency)

# COMMAND ----------

def predicted_impact(rule_id, action, as_of):
    """The frozen predicted numbers for a change — computed directly from telemetry so this notebook
    (setup path) has no dependency on the 05e functions (ml path)."""
    r = spark.sql(f"""
        SELECT count_if(fired) fires,
               sum(CASE WHEN fired THEN gwp ELSE 0 END) gwp_bound,
               sum(CASE WHEN would_fire THEN technical_premium ELSE 0 END) stake,
               avg(CASE WHEN fired AND loss_ratio_pct IS NOT NULL THEN loss_ratio_pct END) avg_lr
        FROM {TELE} WHERE rule_id='{rule_id}' AND as_of_date <= DATE'{as_of}'""").first()
    effort = spark.sql(f"SELECT review_effort_hours e FROM {RULES} WHERE rule_id='{rule_id}' AND valid_to IS NULL").first().e or 1.0
    fires = r.fires or 0
    if action in ("remove", "convert_to_auto_decline", "auto_apply_clause"):
        ref_rel, hrs = fires, fires * effort
    elif action == "re_threshold":
        ref_rel, hrs = fires // 2, fires * effort * 0.5
    else:
        ref_rel, hrs = 0, 0.0
    gwp_delta = (-(r.gwp_bound or 0) if action == "convert_to_auto_decline"
                 else (r.stake or 0) if action == "reopen_to_referral" else 0.0)
    lr_delta = round((55.0 - (r.avg_lr or 50)) * 0.02, 2) if action == "convert_to_auto_decline" else 0.0
    return int(ref_rel), float(round(hrs, 0)), float(round(gwp_delta, 0)), float(lr_delta)


def cur_version(rule_id):
    return spark.sql(f"SELECT rule_version v FROM {RULES} WHERE rule_id='{rule_id}' AND valid_to IS NULL").first().v


def apply_scd2_change(rule_id, prev_version, new_params_json, new_disposition, new_version,
                      effective_date, approved_by, change_id):
    """The SCD2 write-path a human approval runs: close the current version, append the next.
    (The app replicates this SQL in the approve route.)"""
    spark.sql(f"UPDATE {RULES} SET valid_to = DATE'{effective_date}', is_current = false "
              f"WHERE rule_id = '{rule_id}' AND valid_to IS NULL")
    spark.sql(f"""
        INSERT INTO {RULES}
        SELECT rule_id, rule_name, category, description, '{new_params_json}' AS threshold_config,
               '{new_disposition}' AS disposition, compliance_lock, rule_scope, source_check, unit,
               review_effort_hours, '{new_version}' AS rule_version, DATE'{effective_date}' AS valid_from,
               CAST(NULL AS DATE) AS valid_to, '{approved_by}' AS approved_by, '{change_id}' AS change_id,
               true AS is_current, '{effective_date}' AS effective_from
        FROM {RULES} WHERE rule_id = '{rule_id}' AND rule_version = '{prev_version}'
    """)

# COMMAND ----------

# MAGIC %md ## Seed the ledger: 2 approved+monitored (one on-track, one drifting) + 1 proposed

# COMMAND ----------

import json
cur_version_for_copy = {}
change_rows = []


def cid(seed):
    return "chg-" + hashlib.sha256(seed.encode()).hexdigest()[:12]


# ---- Change 1: PRICE_BELOW_TECHNICAL_FLOOR re_threshold, approved 180d ago, ON TRACK ----------
rid1 = "PRICE_BELOW_TECHNICAL_FLOOR"; eff1 = TODAY - datetime.timedelta(days=180)
cur_version_for_copy[rid1] = cur_version(rid1)
c1 = cid(rid1 + "re_threshold")
pr1 = predicted_impact(rid1, "re_threshold", eff1)
apply_scd2_change(rid1, cur_version_for_copy[rid1], json.dumps({"default": 0.88}), "refer", "v2", eff1, "head_of_underwriting", c1)
change_rows.append((c1, rid1, "re_threshold", "engine",
                    datetime.datetime.combine(eff1 - datetime.timedelta(days=7), datetime.time(10)),
                    "monitored", "head_of_underwriting",
                    datetime.datetime.combine(eff1, datetime.time(14)), eff1, "v1", "v2",
                    pr1[0], pr1[1], pr1[2], pr1[3],
                    int(pr1[0] * 0.95), round(pr1[1] * 0.96, 0), round(pr1[2] * 0.98, 0), round(pr1[3] * 0.9, 2),
                    False, "Realised within tolerance of prediction (<10% variance).",
                    "Referral noise on marginal-price cases; raised the floor to 0.88 of technical."))

# ---- Change 2: MAX_TURNOVER re_threshold, approved 120d ago, DIVERGING → drift flag ----------
rid2 = "MAX_TURNOVER"; eff2 = TODAY - datetime.timedelta(days=120)
cur_version_for_copy[rid2] = cur_version(rid2)
c2 = cid(rid2 + "re_threshold")
pr2 = predicted_impact(rid2, "re_threshold", eff2)
apply_scd2_change(rid2, cur_version_for_copy[rid2], json.dumps({"default": 25_000_000}), "refer", "v2", eff2, "head_of_underwriting", c2)
change_rows.append((c2, rid2, "re_threshold", "engine",
                    datetime.datetime.combine(eff2 - datetime.timedelta(days=6), datetime.time(11)),
                    "monitored", "head_of_underwriting",
                    datetime.datetime.combine(eff2, datetime.time(15)), eff2, "v1", "v2",
                    pr2[0], pr2[1], pr2[2], pr2[3],
                    int(pr2[0] * 0.55), round(pr2[1] * 0.52, 0), round(pr2[2], 0), round(pr2[3], 2),
                    True, "Realised referral reduction is ~half the prediction — a new binder channel "
                          "kept turnover referrals higher than modelled; re-review the threshold.",
                    "Raised the turnover authority band to GBP 25m to cut referrals on large but clean risks."))

# ---- Change 3: HAZARDOUS_ACTIVITY_HEIGHT convert_to_auto_decline, PROPOSED (the live beat) --------
rid3 = "HAZARDOUS_ACTIVITY_HEIGHT"
c3 = cid(rid3 + "convert")
pr3 = predicted_impact(rid3, "convert_to_auto_decline", TODAY)
change_rows.append((c3, rid3, "convert_to_auto_decline", "engine",
                    datetime.datetime.combine(TODAY, datetime.time(9)), "proposed", None, None, None,
                    cur_version(rid3), None,
                    pr3[0], pr3[1], pr3[2], pr3[3], None, None, None, None,
                    False, None,
                    "95% of these referrals are declined or walk on price; the few that bind run a poor "
                    "loss ratio. Convert to auto-decline — the tail exhibit shows nothing of value is lost."))

spark.createDataFrame(change_rows, spark.table(CHANGES).schema).write.mode("append").saveAsTable(CHANGES)
print(f"  gold_rule_changes seeded: {spark.table(CHANGES).count()} changes "
      f"(2 monitored incl. 1 drift, 1 proposed)")

# COMMAND ----------

# MAGIC %md ## Verification — SCD2 history + ledger + drift

# COMMAND ----------

# The two approved rules now have TWO versions; current is v2.
for rid in (rid1, rid2):
    vers = [(r.rule_version, str(r.valid_from), str(r.valid_to)) for r in spark.sql(
        f"SELECT rule_version, valid_from, valid_to FROM {RULES} WHERE rule_id='{rid}' ORDER BY valid_from").collect()]
    print(f"  {rid}: {vers}")
    assert len(vers) == 2, f"{rid} should have 2 SCD2 versions"
    assert spark.sql(f"SELECT count(*) c FROM {RULES} WHERE rule_id='{rid}' AND valid_to IS NULL").first().c == 1

# Exactly one current version per rule (no double-open SCD2 rows).
dbl = spark.sql(f"""SELECT count(*) c FROM (SELECT rule_id, count_if(valid_to IS NULL) n
                    FROM {RULES} GROUP BY rule_id HAVING n <> 1)""").first().c
assert dbl == 0, f"{dbl} rules have <>1 current version — SCD2 write-path bug"

# As-of scrub: the rulebook 1 year ago differs from today (MAX_TURNOVER threshold).
def threshold_as_of(rid, as_of):
    return spark.sql(f"""SELECT get_json_object(threshold_config,'$.default') t FROM {RULES}
        WHERE rule_id='{rid}' AND valid_from <= DATE'{as_of}'
          AND (valid_to IS NULL OR valid_to > DATE'{as_of}')""").first()
now_t = threshold_as_of(rid2, TODAY.isoformat()).t
old_t = threshold_as_of(rid2, (TODAY - datetime.timedelta(days=365)).isoformat()).t
print(f"  MAX_TURNOVER threshold: today={now_t} vs 1y-ago={old_t}")
assert now_t != old_t, "as-of scrub: rulebook should differ a year ago"

# Ledger: one proposed, drift flag present.
assert spark.sql(f"SELECT count(*) c FROM {CHANGES} WHERE status='proposed'").first().c >= 1
assert spark.sql(f"SELECT count(*) c FROM {CHANGES} WHERE drift_flag").first().c >= 1
print("✅ 07e Referral Control governance — SCD2 write-path + change ledger + drift verified")
