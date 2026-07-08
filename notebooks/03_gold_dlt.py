# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — the head-of-underwriting marts
# MAGIC
# MAGIC Funnel (by channel — the e-trade vs manual straight-through story), portfolio position
# MAGIC vs plan, **accumulation vs district capacity** (the HX7 87% beat), broker scorecard,
# MAGIC rate adequacy, renewals (retention % + rate change), underinsurance flags, and the
# MAGIC live lifecycle/SLA mart.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CAT = spark.conf.get("source_catalog")
SCH = spark.conf.get("source_schema")
SRC = f"{CAT}.{SCH}"
GOLD_PROPS = {"quality": "gold", "layer": "gold", "demo": "underwriting_workbench"}

# COMMAND ----------

@dlt.table(name="gold_pipeline_funnel",
           comment="Submission funnel by month × channel: received → quoted → bound, with NTU distinct from lost, and hours-to-quote. Straight-through rate by channel is the headline.",
           table_properties=GOLD_PROPS)
def gold_pipeline_funnel():
    s = dlt.read("silver_submissions")
    return (s.withColumn("month", F.date_format(F.to_timestamp("received_ts"), "yyyy-MM"))
            .withColumn("hours_to_quote",
                        (F.unix_timestamp(F.to_timestamp("quote_ts")) - F.unix_timestamp(F.to_timestamp("received_ts"))) / 3600)
            .groupBy("month", "channel")
            .agg(F.count("*").alias("received"),
                 F.sum(F.when(F.col("quoted_premium").isNotNull(), 1).otherwise(0)).alias("quoted"),
                 F.sum(F.when(F.col("outcome") == "bound", 1).otherwise(0)).alias("bound"),
                 F.sum(F.when(F.col("outcome") == "declined", 1).otherwise(0)).alias("declined"),
                 F.sum(F.when(F.col("outcome") == "ntu", 1).otherwise(0)).alias("ntu"),
                 F.sum(F.when(F.col("outcome") == "lost", 1).otherwise(0)).alias("lost"),
                 F.sum(F.when(F.col("outcome") == "quote_expired", 1).otherwise(0)).alias("quote_expired"),
                 F.sum(F.when(F.col("outcome") == "withdrawn", 1).otherwise(0)).alias("withdrawn"),
                 F.sum(F.when(F.col("lifecycle_state") != "closed", 1).otherwise(0)).alias("open_now"),
                 F.round(F.avg("hours_to_quote"), 1).alias("avg_hours_to_quote"),
                 F.sum(F.when(F.col("outcome") == "bound", F.col("quoted_premium")).otherwise(0)).alias("gwp_bound")))

# COMMAND ----------

@dlt.table(name="gold_portfolio_position",
           comment="In-force book by trade group: GWP vs plan (plan = prior GWP +7%, labelled derived), 3-year loss ratio, appetite status.",
           table_properties=GOLD_PROPS)
def gold_portfolio_position():
    pol = dlt.read("bronze_pas_policies").filter("policy_status = 'in_force'")
    clm = (dlt.read("bronze_pas_claims")
           .join(pol.select("policy_number", "trade_group"), "policy_number"))
    losses = clm.groupBy("trade_group").agg(F.sum("incurred").alias("incurred_3y"))
    app = spark.read.table(f"{SRC}.ref_appetite").select("trade_group", "appetite_status", "hazard_grade")
    return (pol.groupBy("trade_group")
            .agg(F.count("*").alias("policies"),
                 F.sum("gross_premium").alias("gwp"),
                 F.round(F.avg("rate_change_pct") * 100, 1).alias("avg_rate_change_pct"),
                 F.sum(F.col("buildings_si") + F.col("contents_si") + F.col("stock_si")).alias("property_si"))
            .join(losses, "trade_group", "left")
            .join(app, "trade_group", "left")
            .withColumn("plan_gwp", F.round(F.col("gwp") * 1.07, 0))
            .withColumn("loss_ratio_3y_pct",
                        F.round(F.coalesce(F.col("incurred_3y"), F.lit(0)) / 3 / F.col("gwp") * 100, 1)))

# COMMAND ----------

@dlt.table(name="gold_accumulation",
           comment="Property accumulation by postcode district vs capacity appetite. Utilisation >80% = referral territory (amber), >100% = breach (red). HX7 sits at 67% before hero 900002's marginal £5m.",
           table_properties=GOLD_PROPS)
def gold_accumulation():
    pol = dlt.read("bronze_pas_policies").filter("policy_status = 'in_force'")
    cap = spark.read.table(f"{SRC}.ref_district_capacity")
    flood = spark.read.table(f"{SRC}.ref_flood_open").select("postcode_district", "flood_band", "rivers")
    cent = spark.read.table(f"{SRC}.ref_postcode_centroid")
    acc = (pol.groupBy("postcode_district")
           .agg(F.sum(F.col("buildings_si") + F.col("contents_si") + F.col("stock_si")).alias("in_force_property_si"),
                F.count("*").alias("policies"),
                F.sum("gross_premium").alias("gwp")))
    return (acc.join(cap, "postcode_district")
            .join(flood, "postcode_district", "left")
            .join(cent, "postcode_district", "left")
            .withColumn("utilisation_pct", F.round(F.col("in_force_property_si") / F.col("property_capacity_gbp") * 100, 1))
            .withColumn("headroom_gbp", F.col("property_capacity_gbp") - F.col("in_force_property_si"))
            .withColumn("rag", F.when(F.col("utilisation_pct") >= 100, "red")
                        .when(F.col("utilisation_pct") >= 80, "amber").otherwise("green")))

# COMMAND ----------

@dlt.table(name="gold_broker_scorecard",
           comment="Broker performance: submission volume, quote rate, hit ratio (bound/quoted), speed, data quality (complete submissions), GWP bound.",
           table_properties=GOLD_PROPS)
def gold_broker_scorecard():
    s = dlt.read("silver_submissions")
    br = spark.read.table(f"{SRC}.ref_broker")
    return (s.groupBy("broker_id")
            .agg(F.count("*").alias("submissions_12m"),
                 F.round(F.avg(F.when(F.col("quoted_premium").isNotNull(), 1.0).otherwise(0.0)) * 100, 1).alias("quote_rate_pct"),
                 F.round(F.sum(F.when(F.col("outcome") == "bound", 1).otherwise(0))
                         / F.greatest(F.sum(F.when(F.col("quoted_premium").isNotNull(), 1).otherwise(0)), F.lit(1)) * 100, 1).alias("hit_ratio_pct"),
                 F.round(F.avg((F.unix_timestamp(F.to_timestamp("quote_ts"))
                                - F.unix_timestamp(F.to_timestamp("received_ts"))) / 3600), 1).alias("avg_hours_to_quote"),
                 F.round(F.avg(F.when(F.col("data_complete"), 1.0).otherwise(0.0)) * 100, 1).alias("data_complete_pct"),
                 F.sum(F.when(F.col("outcome") == "bound", F.col("quoted_premium")).otherwise(0)).alias("gwp_bound"),
                 F.round(F.avg(F.when(F.col("outcome") == "ntu", 1.0).otherwise(0.0)) * 100, 1).alias("ntu_rate_pct"))
            .join(br, "broker_id", "left"))

# COMMAND ----------

@dlt.table(name="gold_rate_adequacy",
           comment="Rate adequacy by trade group: quoted premium vs the rate-guide technical base (adequacy % <100 = under-priced vs guide), with renewal rate movement from the PAS book.",
           table_properties=GOLD_PROPS)
def gold_rate_adequacy():
    s = dlt.read("silver_submissions").filter("quoted_premium IS NOT NULL")
    pol = dlt.read("bronze_pas_policies").filter("policy_status = 'in_force'")
    ren = pol.groupBy("trade_group").agg(F.round(F.avg("rate_change_pct") * 100, 1).alias("renewal_rate_change_pct"))
    return (s.groupBy("trade_group")
            .agg(F.count("*").alias("quotes_12m"),
                 F.round(F.avg(F.col("quoted_premium") / F.col("technical_base_premium")) * 100, 1).alias("adequacy_pct"),
                 F.round(F.avg("technical_base_premium"), 0).alias("avg_technical_premium"),
                 F.round(F.avg("quoted_premium"), 0).alias("avg_quoted_premium"),
                 F.round(F.avg("cohort_loss_ratio_pct"), 1).alias("loss_ratio_3y_pct"))
            .join(ren, "trade_group", "left"))

# COMMAND ----------

@dlt.table(name="gold_renewals",
           comment="Retention and rate change by month: in-force renewed vs lapsed (retention %), average rate change — the head-of-underwriting vital signs.",
           table_properties=GOLD_PROPS)
def gold_renewals():
    pol = dlt.read("bronze_pas_policies")
    return (pol.withColumn("month", F.date_format(F.to_date("inception_date"), "yyyy-MM"))
            .groupBy("month", "trade_group")
            .agg(F.sum(F.when(F.col("policy_status") == "in_force", 1).otherwise(0)).alias("in_force"),
                 F.sum(F.when(F.col("policy_status") == "lapsed", 1).otherwise(0)).alias("lapsed"),
                 F.round(F.avg("rate_change_pct") * 100, 1).alias("avg_rate_change_pct"),
                 F.sum(F.when(F.col("policy_status") == "in_force", F.col("gross_premium")).otherwise(0)).alias("gwp")))

# COMMAND ----------

@dlt.table(name="gold_underinsurance",
           comment="Underinsurance flags on open submissions: declared buildings SI vs floor-area × rebuild benchmark (<85% = flag), and BI indemnity-period adequacy for manufacturing trades (24 months is the market push).",
           table_properties=GOLD_PROPS)
def gold_underinsurance():
    locs = (dlt.read("silver_locations_enriched")
            .filter("rebuild_benchmark_gbp IS NOT NULL")
            .groupBy("submission_public_id")
            .agg(F.sum("buildings_si").alias("declared_buildings_si"),
                 F.sum("rebuild_benchmark_gbp").alias("rebuild_benchmark_gbp")))
    s = dlt.read("silver_submissions").filter("lifecycle_state != 'closed'")
    return (s.join(locs, "submission_public_id", "left")
            .withColumn("buildings_adequacy_pct",
                        F.when(F.col("rebuild_benchmark_gbp") > 0,
                               F.round(F.col("declared_buildings_si") / F.col("rebuild_benchmark_gbp") * 100, 0)))
            .withColumn("underinsured_flag", F.col("buildings_adequacy_pct") < 85)
            .withColumn("bi_indemnity_flag",
                        F.col("trade_group").isin("food_manufacturing", "light_manufacturing", "metal_engineering")
                        & (F.col("bi_indemnity_months") < 24))
            .select("submission_public_id", "company_name", "trade_group", "declared_buildings_si",
                    "rebuild_benchmark_gbp", "buildings_adequacy_pct", "underinsured_flag",
                    "bi_indemnity_months", "bi_indemnity_flag"))

# COMMAND ----------

@dlt.table(name="gold_submission_lifecycle",
           comment="The live pipeline: every open submission with its lifecycle state, SLA clock and breach status. The app reads this for the inbox and the funnel drill.",
           table_properties=GOLD_PROPS)
def gold_submission_lifecycle():
    s = dlt.read("silver_submissions").filter("lifecycle_state != 'closed'")
    return (s.withColumn("hours_since_received",
                         F.round((F.unix_timestamp(F.current_timestamp())
                                  - F.unix_timestamp(F.to_timestamp("received_ts"))) / 3600, 1))
            .withColumn("sla_status", F.when(F.col("hours_since_received") > F.col("sla_target_hours"), "breached")
                        .when(F.col("hours_since_received") > F.col("sla_target_hours") * 0.75, "at_risk")
                        .otherwise("on_track"))
            .select("submission_public_id", "company_name", "trade_group", "segment", "channel", "broker_id",
                    "postcode_district", "lifecycle_state", "received_ts", "hours_since_received",
                    "sla_target_hours", "sla_status", "total_si", "total_property_si", "target_premium",
                    "appetite_status", "hazard_grade", "flood_band", "turnover_mismatch_ratio",
                    "technical_base_premium", "data_complete", "n_documents"))
