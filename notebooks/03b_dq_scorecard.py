# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · DQ scorecard + ingestion source map
# MAGIC
# MAGIC Parses the pipeline **event log** (published to `medallion_event_log`) into
# MAGIC `gold_dq_scorecard` — one row per expectation with pass/fail counts — and builds
# MAGIC `gold_ingestion_sources`, the Source-Assets map the app's Ingestion page renders
# MAGIC (live sources carry real row counts; roadmap rows are labelled).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## DQ scorecard from the DLT event log

# COMMAND ----------

ev = spark.table(f"{fqn}.medallion_event_log")
flow = (ev.filter("event_type = 'flow_progress'")
        .select(F.col("origin.flow_name").alias("dataset"),
                F.explode(F.expr("details:flow_progress:data_quality:expectations::array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>")).alias("e"),
                F.col("timestamp")))
score = (flow.groupBy(F.col("e.dataset").alias("dataset"), F.col("e.name").alias("expectation"))
         .agg(F.max("timestamp").alias("last_seen"),
              F.sum("e.passed_records").alias("passed"),
              F.sum("e.failed_records").alias("failed"))
         .withColumn("pass_pct", F.round(F.col("passed") / F.greatest(F.col("passed") + F.col("failed"), F.lit(1)) * 100, 2))
         .withColumn("action", F.when(F.col("expectation").isin(
             "el_meets_statutory_minimum", "valid_submission_id", "standard_template", "extraction_confident"),
             "drop_to_quarantine").otherwise("track_and_retain")))
score.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_dq_scorecard")
spark.sql(f"ALTER TABLE {fqn}.gold_dq_scorecard SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")
display(spark.table(f"{fqn}.gold_dq_scorecard").orderBy("dataset", "expectation"))

# COMMAND ----------

# MAGIC %md ## Ingestion source map (live counts + honest roadmap rows)

# COMMAND ----------

def cnt(t):
    try:
        return spark.table(f"{fqn}.{t}").count()
    except Exception:
        return None

SRC_MAP = [
    # group, source, system, format, cadence, databricks_tool, table, status, note
    ("Broker submissions", "E-trade gateway", "Broker portals (Acturis-shaped)", "structured JSON", "real-time",
     "Lakeflow Declarative Pipelines", "bronze_submissions", "live", "55% of SME flow — straight-through eligible"),
    ("Broker submissions", "Broker portal feed", "Bricksurance broker extranet", "structured", "real-time",
     "Lakeflow Declarative Pipelines", "bronze_submissions", "live", "Structured but manual-keyed"),
    ("Broker submissions", "Email inbox", "newbusiness@ mailbox", "unstructured text", "continuous",
     "Auto Loader on UC Volume", "bronze_documents", "live", "The messy 20% that eats underwriter time"),
    ("Broker submissions", "Proposal forms & loss runs (PDF)", "Broker attachments", "scanned PDF", "continuous",
     "ai_parse_document + ai_query", "bronze_doc_extractions", "live", "Document AI with a 0.6 confidence gate"),
    ("Broker submissions", "Risk schedules / SOVs", "Broker BMS exports", "CSV/XLSX, schema drifts", "continuous",
     "Auto Loader rescuedDataColumn", "bronze_schedule_locations", "live", "Drifted exports quarantine — nothing silently lost"),
    ("System of record", "Policy admin system (book)", "PAS", "Delta", "daily",
     "Lakeflow Declarative Pipelines", "bronze_pas_policies", "live", "In-force + lapsed (retention)"),
    ("System of record", "Claims history", "Claims system", "Delta", "daily",
     "Lakeflow Declarative Pipelines", "bronze_pas_claims", "live", "Burning-cost basis for technical price"),
    ("Enrichment — open data", "Companies House profiles", "Synthetic (live API = notebook 91)", "API JSON", "on submission",
     "Simulated API call (labelled)", "bronze_company_profiles", "live", "Incorporation, SIC, accounts status, directors, filed turnover"),
    ("Enrichment — open data", "OFSI consolidated sanctions list", "HM Treasury OFSI (REAL, OGL)", "CSV", "monthly refresh",
     "Bundled real extract", "ref_sanctions_ofsi", "live", "12k primary names — screened at point of quote"),
    ("Enrichment — open data", "EA flood areas (RoFRS evidence)", "Environment Agency (REAL, OGL)", "API/CSV", "quarterly",
     "Bundled real extract", "ref_flood_open", "live", "England only; banding curated from RoFRS statistics"),
    ("Enrichment — open data", "police.uk crime counts", "Home Office police.uk (REAL, OGL)", "API/CSV", "monthly",
     "Bundled real extract", "ref_crime_open", "live", "GMP data gap flagged + imputed — honest sourcing"),
    ("Enrichment — open data", "EPC band mix (MEES lens)", "MHCLG EPC statistics (curated)", "CSV", "quarterly",
     "Bundled curated extract", "ref_epc_mix_open", "live", "MEES letting-ban/ESG lens — never a rating factor"),
    ("Enrichment — roadmap", "Full RoFRS property-level flood", "Environment Agency", "GIS", "quarterly", "—", None, "roadmap", "Property-level flood banding + surface water"),
    ("Enrichment — roadmap", "Live Companies House API", "Companies House", "REST API", "on submission", "—", None, "roadmap", "Real API demo in notebook 91 (off critical path)"),
    ("Enrichment — roadmap", "Credit reference (Experian/D&B)", "Credit bureau", "API", "on submission", "—", None, "roadmap", "Financial resilience score"),
    ("Enrichment — roadmap", "Perils/cat aggregation feed", "Cat modeller", "YELT/Delta", "quarterly", "—", None, "roadmap", "See reinsurance workbench for the treaty view"),
    ("Enrichment — roadmap", "Survey & risk engineering reports", "Survey panel", "PDF", "post-quote", "—", None, "roadmap", "Post-bind obligations tracking"),
]
rows = [(g, s, sys, fmt, cad, tool, tbl, st, note, cnt(tbl) if st == "live" and tbl else None)
        for g, s, sys, fmt, cad, tool, tbl, st, note in SRC_MAP]
df = spark.createDataFrame(rows, "source_group string, source string, system string, format string, cadence string, "
                                 "databricks_tool string, table_name string, status string, note string, row_count long")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_ingestion_sources")
spark.sql(f"ALTER TABLE {fqn}.gold_ingestion_sources SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")
live_rows = df.filter("status='live'").count()
assert spark.table(f"{fqn}.gold_dq_scorecard").count() >= 8, "expected ≥8 expectations in the scorecard"
print(f"✅ 03b complete — {live_rows} live sources mapped, scorecard written")
