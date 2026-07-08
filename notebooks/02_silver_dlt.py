# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — the enriched submission record
# MAGIC
# MAGIC One row per submission with everything an underwriter swivel-chairs for today joined on:
# MAGIC company profile (turnover mismatch, SIC mismatch, accounts overdue), flood band + EA
# MAGIC evidence, crime counts (with the GMP data-gap flag), EPC/MEES mix, trade cohort loss
# MAGIC experience (burning cost), document-extraction signals, appetite status, and the
# MAGIC rate-guide technical base premium. Plus per-location enrichment for schedule risks.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CAT = spark.conf.get("source_catalog")
SCH = spark.conf.get("source_schema")
SRC = f"{CAT}.{SCH}"
SILVER_PROPS = {"quality": "silver", "layer": "silver", "demo": "underwriting_workbench"}

SLA_HOURS = "CASE channel WHEN 'etrade' THEN 4 WHEN 'portal' THEN 24 ELSE 48 END"

# COMMAND ----------

@dlt.table(name="silver_submissions",
           comment="Enriched submission record — one row per submission; the single view an underwriter used to assemble from six systems.",
           table_properties=SILVER_PROPS)
@dlt.expect("has_trade", "trade_group IS NOT NULL")
@dlt.expect("has_district", "postcode_district IS NOT NULL")
def silver_submissions():
    subs = dlt.read("bronze_submissions")
    prof = (dlt.read("bronze_company_profiles")
            .select(F.col("company_number"),
                    F.col("sic_code").alias("sic_code_registered"),
                    "company_status", "accounts_overdue", "filed_turnover",
                    F.col("directors_json"), "incorporation_date"))
    flood = spark.read.table(f"{SRC}.ref_flood_open").select(
        "postcode_district", "flood_band", "named_area_count", "rivers")
    crime = spark.read.table(f"{SRC}.ref_crime_open").select(
        "postcode_district", F.col("effective_count").alias("crime_count"),
        F.col("imputed").alias("crime_imputed"))
    epc = spark.read.table(f"{SRC}.ref_epc_mix_open").select(
        "postcode_district", (F.col("pct_e") + F.col("pct_f") + F.col("pct_g")).alias("epc_efg_pct"))
    app = spark.read.table(f"{SRC}.ref_appetite").select(
        "trade_group", "hazard_grade", "appetite_status", "decline_code", "guide_section", "appetite_note")
    rg = spark.read.table(f"{SRC}.ref_rate_guide")

    # Trade cohort loss experience from the PAS book (burning-cost basis)
    pol = dlt.read("bronze_pas_policies").select("policy_number", "trade_group",
                                                 (F.col("buildings_si") + F.col("contents_si") + F.col("stock_si")).alias("prop_si"),
                                                 "gross_premium")
    clm = dlt.read("bronze_pas_claims").join(pol, "policy_number")
    cohort = (clm.groupBy("trade_group")
              .agg(F.sum("incurred").alias("cohort_incurred_3y"), F.count("*").alias("cohort_claims_3y")))
    book = pol.groupBy("trade_group").agg(F.sum("prop_si").alias("cohort_prop_si"),
                                          F.sum("gross_premium").alias("cohort_gwp"),
                                          F.count("*").alias("cohort_policies"))
    cohort_rates = (book.join(cohort, "trade_group", "left")
                    .select("trade_group",
                            (F.coalesce(F.col("cohort_incurred_3y"), F.lit(0)) / 3 / F.col("cohort_gwp") * 100)
                            .alias("cohort_loss_ratio_pct"),
                            (F.coalesce(F.col("cohort_claims_3y"), F.lit(0)) / 3 / F.col("cohort_policies"))
                            .alias("cohort_claim_freq")))

    # Document-extraction signals (heroes + any doc-carrying submission)
    docs = (dlt.read("bronze_doc_extractions")
            .groupBy("submission_public_id")
            .agg(F.count("*").alias("n_documents"),
                 F.max(F.col("doc_type") == "proposal_form").alias("has_proposal_form"),
                 F.max(F.col("doc_type") == "loss_run").alias("has_loss_run"),
                 F.max("flood_disclosed").alias("flood_disclosed"),
                 F.flatten(F.collect_list(F.from_json("key_hazards_json", "ARRAY<STRING>"))).alias("doc_hazards"),
                 F.max("turnover_stated_gbp").alias("doc_turnover_stated")))

    out = (subs
           .join(prof, "company_number", "left")
           .join(flood, "postcode_district", "left")
           .join(crime, "postcode_district", "left")
           .join(epc, "postcode_district", "left")
           .join(app, "trade_group", "left")
           .join(rg, "trade_group", "left")
           .join(cohort_rates, "trade_group", "left")
           .join(docs, "submission_public_id", "left"))

    return (out
            .withColumn("total_property_si", F.col("buildings_si") + F.col("plant_si") + F.col("contents_si") + F.col("stock_si"))
            .withColumn("total_si", F.col("total_property_si") + F.col("bi_si"))
            .withColumn("turnover_mismatch_ratio",
                        F.when(F.col("filed_turnover").isNotNull() & (F.col("turnover_stated") > 0),
                               F.round(F.col("filed_turnover") / F.col("turnover_stated"), 2)))
            .withColumn("sic_mismatch", F.col("sic_code_registered").isNotNull() &
                        (F.col("sic_code_registered") != F.col("sic_code_declared")))
            .withColumn("technical_base_premium", F.greatest(F.col("min_premium").cast("double"), F.round(
                (F.col("total_property_si") * F.col("property_rate_permille")
                 + F.col("bi_si") * F.col("bi_rate_permille")) / 1000
                + F.col("employees") * F.col("el_rate_per_employee")
                + F.coalesce(F.col("turnover_stated"), F.lit(0)) / 1000 * F.col("pl_rate_per_1k_turnover"), 0)))
            .withColumn("sla_target_hours", F.expr(SLA_HOURS))
            .withColumn("data_complete", F.col("turnover_stated").isNotNull() & (F.col("employees") > 0)
                        & F.col("company_number").isNotNull())
            .drop("property_rate_permille", "bi_rate_permille", "el_rate_per_employee",
                  "pl_rate_per_1k_turnover", "min_premium"))

# COMMAND ----------

@dlt.table(name="silver_locations_enriched",
           comment="Per-location risk view: schedule locations (multi-site risks) + primary premises for single-site submissions, enriched with flood band, EA river evidence and crime.",
           table_properties=SILVER_PROPS)
@dlt.expect("has_district", "postcode_district IS NOT NULL")
def silver_locations_enriched():
    sched = (dlt.read("bronze_schedule_locations")
             .select(F.col("submission_ref").alias("submission_public_id"), "loc_no", "site_name",
                     "postcode_district", "site_type", "construction_type", "year_built",
                     "floor_area_m2", "buildings_si", "plant_si", "stock_si")
             .dropDuplicates(["submission_public_id", "loc_no"]))
    sched_ids = sched.select("submission_public_id").distinct()
    single = (dlt.read("bronze_submissions")
              .join(sched_ids, "submission_public_id", "left_anti")
              .select("submission_public_id", F.lit(1).alias("loc_no"),
                      F.lit("Primary premises").alias("site_name"), "postcode_district",
                      F.lit("primary").alias("site_type"), "construction_type", "year_built",
                      "floor_area_m2", "buildings_si", "plant_si", "stock_si"))
    locs = sched.unionByName(single)
    flood = spark.read.table(f"{SRC}.ref_flood_open").select("postcode_district", "flood_band",
                                                             "named_area_count", "rivers")
    crime = spark.read.table(f"{SRC}.ref_crime_open").select("postcode_district",
                                                             F.col("effective_count").alias("crime_count"),
                                                             F.col("imputed").alias("crime_imputed"))
    cent = spark.read.table(f"{SRC}.ref_postcode_centroid")
    reb = spark.read.table(f"{SRC}.ref_rebuild_benchmark")
    return (locs.join(flood, "postcode_district", "left")
            .join(crime, "postcode_district", "left")
            .join(cent, "postcode_district", "left")
            .join(reb, "construction_type", "left")
            .withColumn("property_si", F.col("buildings_si") + F.coalesce(F.col("plant_si"), F.lit(0))
                        + F.coalesce(F.col("stock_si"), F.lit(0)))
            .withColumn("rebuild_benchmark_gbp",
                        F.when(F.col("buildings_si") > 0, F.col("floor_area_m2") * F.col("rebuild_cost_per_m2"))))
