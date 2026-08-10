# Databricks notebook source
# MAGIC %md
# MAGIC # 02d · Referral conformance — messy source feeds → one conformed event table (DLT)
# MAGIC
# MAGIC The lineage beat behind the referral analytics. Three raw systems speak three dialects
# MAGIC (`raw_pas_referrals` nested CDC JSON with opaque `UW_REF_nnn` codes · `raw_etrade_referrals`
# MAGIC flat JSON with verbose codes and string amounts · `raw_case_outcomes` daily CSV keyed on a
# MAGIC `case_ref` that needs a bridge). This Lakeflow Declarative Pipeline lands each as bronze, then
# MAGIC conforms them — via the **`ref_rule_code_map`** demo artefact and the case bridge — into one
# MAGIC `silver_referral_events_conformed` table with the SAME shape as the generated canonical events.
# MAGIC
# MAGIC Expectations-as-code (house pattern): orphan case rows are **quarantined, not dropped**; the
# MAGIC authority-matrix threshold that disagrees with the PAS rating config is **flagged** to a
# MAGIC drift table (not silently resolved). `03c_gold_referral_events` re-sources the generated
# MAGIC history from this conformed output — the smoke test proves the Lane E rows are byte-identical.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CAT = spark.conf.get("source_catalog")
SCH = spark.conf.get("source_schema")
SRC = f"{CAT}.{SCH}"
PROPS = {"layer": "conformance", "demo": "underwriting_workbench", "lane": "referral_discretion"}

# COMMAND ----------

# MAGIC %md ## Bronze — land the three raw feeds as-is (parse the dialects)

# COMMAND ----------

@dlt.table(name="bronze_pas_referrals", comment="PAS/rating-engine referrals — nested CDC JSON parsed.",
           table_properties=PROPS)
def bronze_pas_referrals():
    schema = "op STRING, seq STRING, payload STRUCT<referral: STRUCT<policy_txn_ref: STRING, " \
             "rule: STRUCT<code: STRING, version: STRING>, observed_value: DOUBLE, " \
             "raised_ts_local: STRING, underwriter: STRING>>"
    p = (spark.read.table(f"{SRC}.raw_pas_referrals")
         .withColumn("j", F.from_json("raw_record", schema)))
    r = p.select(F.col("j.payload.referral.*"))
    return (r.select(
        F.col("policy_txn_ref").alias("transaction_id"),
        F.col("rule.code").alias("source_code"),
        F.col("rule.version").alias("rule_version"),
        F.col("observed_value").alias("triggering_value"),
        F.col("raised_ts_local").alias("fired_at"),
        F.col("underwriter").alias("decided_by"),
        F.lit("pas_rating").alias("source_system")))


@dlt.table(name="bronze_etrade_referrals", comment="E-trade/portal referrals — flat JSON, string amounts normalised.",
           table_properties=PROPS)
def bronze_etrade_referrals():
    schema = "quoteRef STRING, reasonCode STRING, triggerVal STRING, raisedAt STRING, uw STRING, " \
             "indicativePremium STRING"
    p = (spark.read.table(f"{SRC}.raw_etrade_referrals")
         .withColumn("j", F.from_json("raw_record", schema)).select("j.*"))
    return (p.select(
        F.col("quoteRef").alias("transaction_id"),
        F.col("reasonCode").alias("source_code"),
        F.lit("v1").alias("rule_version"),
        F.col("triggerVal").cast("double").alias("triggering_value"),
        F.col("raisedAt").alias("fired_at"),
        F.col("uw").alias("decided_by"),
        # "£1,234" → 1234.0
        F.regexp_replace(F.col("indicativePremium"), "[£,]", "").cast("double").alias("charged_premium"),
        F.lit("etrade_portal").alias("source_system")))


@dlt.table(name="bronze_case_outcomes", comment="Underwriter case-tool outcomes — daily CSV drops.",
           table_properties=PROPS)
def bronze_case_outcomes():
    return (spark.read.table(f"{SRC}.raw_case_outcomes")
            .withColumn("charged_premium", F.col("charged_premium_str").cast("double")))

# COMMAND ----------

# MAGIC %md ## Silver — conform: map source codes → rule_id, bridge case_ref → transaction, join outcomes
# MAGIC The union of the two referral feeds is the event grain. Codes resolve through `ref_rule_code_map`;
# MAGIC outcomes/charged come from the case tool via the bridge. Rows whose code has no mapping fail an
# MAGIC expectation (would signal an unmapped source code — the map must cover every code).

# COMMAND ----------

@dlt.table(name="silver_referral_events_conformed",
           comment="Conformed referral events from all source systems: source codes resolved via ref_rule_code_map, outcomes joined from the case tool via the bridge. Same shape as the generated canonical events.",
           table_properties=PROPS)
@dlt.expect("code_mapped", "rule_id IS NOT NULL")
@dlt.expect("has_transaction", "transaction_id IS NOT NULL")
def silver_referral_events_conformed():
    pas = dlt.read("bronze_pas_referrals").select(
        "transaction_id", "source_system", "source_code", "rule_version",
        "triggering_value", "fired_at", "decided_by")
    et = dlt.read("bronze_etrade_referrals").select(
        "transaction_id", "source_system", "source_code", "rule_version",
        "triggering_value", "fired_at", "decided_by")
    events = pas.unionByName(et)

    code_map = spark.read.table(f"{SRC}.ref_rule_code_map").select(
        "source_system", "source_code", "rule_id")
    # bridge + outcomes from the case tool — event-grain key (transaction_id + rule_id) so co-fires
    # (one txn, several rules) each resolve to their own outcome without a fan-out.
    bridge = spark.read.table(f"{SRC}.raw_case_bridge")
    cases = (spark.read.table(f"{SRC}.raw_case_outcomes")
             .withColumn("charged_premium", F.col("charged_premium_str").cast("double"))
             .join(bridge, "case_ref", "inner")
             .select("transaction_id", "rule_id", "resolved_ts", "outcome", "charged_premium"))

    out = (events.join(code_map, ["source_system", "source_code"], "left")
           .join(cases, ["transaction_id", "rule_id"], "left"))
    return (out
            .withColumn("referral_event_id",
                        F.sha2(F.concat_ws("|", "transaction_id", "rule_id", "fired_at"), 256).substr(1, 32))
            .withColumn("unit", F.lit("unit"))
            .withColumn("threshold_value", F.lit(0.0))
            .withColumn("time_to_decision_hours",
                        F.round((F.unix_timestamp("resolved_ts") - F.unix_timestamp("fired_at")) / 3600.0, 1))
            .withColumn("event_source", F.lit("conformed"))
            .select("referral_event_id", "transaction_id", "rule_id", "rule_version",
                    "triggering_value", "threshold_value", "unit", "fired_at", "resolved_ts",
                    "decided_by", "outcome", "time_to_decision_hours", "event_source",
                    "source_system", "charged_premium"))


@dlt.table(name="silver_referral_orphans",
           comment="Case-tool rows whose case_ref has no linked transaction (system drift / late policy creation). Quarantined, not dropped.",
           table_properties=PROPS)
def silver_referral_orphans():
    bridge = spark.read.table(f"{SRC}.raw_case_bridge")
    cases = spark.read.table(f"{SRC}.raw_case_outcomes")
    return (cases.join(bridge, "case_ref", "left_anti")
            .withColumn("quarantine_reason", F.lit("orphan_case_no_transaction")))

# COMMAND ----------

# MAGIC %md ## Silver — authority-matrix drift flag
# MAGIC The licence spreadsheet's thresholds vs the rating-engine config (`ref_referral_rules`). Any
# MAGIC band that DISAGREES is surfaced here (and to the DQ scorecard) — never silently reconciled.

# COMMAND ----------

@dlt.table(name="silver_authority_drift",
           comment="Threshold drift: authority-matrix (licence spreadsheet) bands that disagree with the rating-engine config in ref_referral_rules. The governance beat — surfaced, not silently resolved.",
           table_properties=PROPS)
@dlt.expect("known_rule", "rule_id IS NOT NULL")
def silver_authority_drift():
    am = spark.read.table(f"{SRC}.raw_authority_matrix")
    rr = (spark.read.table(f"{SRC}.ref_referral_rules")
          .select("rule_id",
                  F.get_json_object("threshold_config", "$.etrade").cast("long").alias("cfg_etrade"),
                  F.get_json_object("threshold_config", "$.standard").cast("long").alias("cfg_standard"),
                  F.get_json_object("threshold_config", "$.senior").cast("long").alias("cfg_senior"),
                  F.get_json_object("threshold_config", "$.default").cast("long").alias("cfg_default")))
    j = am.join(rr, "rule_id", "left")
    cfg = (F.when(F.col("authority_band") == "etrade", F.col("cfg_etrade"))
           .when(F.col("authority_band") == "standard", F.col("cfg_standard"))
           .when(F.col("authority_band") == "senior", F.col("cfg_senior"))
           .otherwise(F.coalesce(F.col("cfg_default"), F.col("cfg_standard"))))
    return (j.withColumn("config_threshold", cfg)
            .withColumn("drift", F.col("threshold_value") != F.col("config_threshold"))
            .filter("drift = true")
            .select("rule_id", "authority_band",
                    F.col("threshold_value").alias("matrix_threshold"),
                    "config_threshold", "note"))
