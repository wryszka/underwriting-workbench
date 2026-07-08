# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — governed front door (Lakeflow Declarative Pipeline)
# MAGIC
# MAGIC Every inbound source lands through expectations. Failures are never silently lost:
# MAGIC track-and-retain rules are measured in the event log; drop rules land the offending
# MAGIC rows in a **quarantine mirror** with the reason. The drifted risk schedule (hero
# MAGIC 900002's v1) is caught here via `rescuedDataColumn`.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CAT = spark.conf.get("source_catalog")
SCH = spark.conf.get("source_schema")
SRC = f"{CAT}.{SCH}"
INBOX = f"/Volumes/{CAT}/{SCH}/submission_inbox"
CKPT = f"/Volumes/{CAT}/{SCH}/ingest_checkpoints"

BRONZE_PROPS = {"quality": "bronze", "layer": "bronze", "demo": "underwriting_workbench"}

# COMMAND ----------

# MAGIC %md ## Submissions feed (structured broker-portal/e-trade payloads)

# COMMAND ----------

@dlt.table(name="bronze_submissions",
           comment="Broker submissions — governed bronze. EL below the £5m statutory minimum is dropped to quarantine; channel/turnover issues are tracked and retained.",
           table_properties=BRONZE_PROPS)
@dlt.expect_or_drop("valid_submission_id", "submission_public_id RLIKE '^sub:[0-9]+$'")
@dlt.expect_or_drop("el_meets_statutory_minimum", "el_limit >= 5000000")
@dlt.expect("valid_channel", "channel IN ('etrade','portal','email')")
@dlt.expect("turnover_stated_present", "turnover_stated IS NOT NULL")
@dlt.expect("non_negative_sums", "buildings_si >= 0 AND contents_si >= 0 AND stock_si >= 0 AND bi_si >= 0")
def bronze_submissions():
    return (spark.read.table(f"{SRC}.landing_submissions_feed")
            .withColumn("_bronze_ingested_at", F.current_timestamp()))


@dlt.table(name="bronze_quarantine_submissions",
           comment="Quarantine mirror: submissions that failed a DROP rule, with the reason. Nothing is silently lost.",
           table_properties=BRONZE_PROPS)
def bronze_quarantine_submissions():
    src = spark.read.table(f"{SRC}.landing_submissions_feed")
    bad_el = (src.filter("el_limit < 5000000 OR el_limit IS NULL")
              .withColumn("quarantine_reason", F.lit("el_below_statutory_minimum")))
    bad_id = (src.filter("NOT submission_public_id RLIKE '^sub:[0-9]+$'")
              .withColumn("quarantine_reason", F.lit("invalid_submission_id")))
    return bad_el.unionByName(bad_id).withColumn("_quarantined_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md ## Risk schedules (SOV) — Auto Loader CSV with schema-drift rescue

# COMMAND ----------

SCHEDULE_SCHEMA = ("submission_ref string, loc_no int, site_name string, postcode_district string, "
                   "site_type string, construction_type string, year_built int, floor_area_m2 int, "
                   "buildings_si long, plant_si long, stock_si long")


def _schedule_stream():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.schemaLocation", f"{CKPT}/schedules_schema")
            .schema(SCHEDULE_SCHEMA + ", _rescued_data string")
            .load(f"{INBOX}/*_schedule_*.csv")
            .withColumn("source_file", F.col("_metadata.file_name")))


@dlt.table(name="bronze_schedule_locations",
           comment="Risk schedules (schedules of premises / SOVs) in the standard broker template. Non-standard exports fail the template rule and quarantine.",
           table_properties=BRONZE_PROPS)
@dlt.expect_or_drop("standard_template", "submission_ref IS NOT NULL AND buildings_si IS NOT NULL")
@dlt.expect("known_district", "postcode_district IS NOT NULL")
def bronze_schedule_locations():
    return _schedule_stream().withColumn("_bronze_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_quarantine_schedules",
           comment="Quarantine mirror: schedule rows that arrived with non-standard (drifted) columns — the raw values are preserved in _rescued_data for re-mapping.",
           table_properties=BRONZE_PROPS)
def bronze_quarantine_schedules():
    return (_schedule_stream()
            .filter("submission_ref IS NULL OR buildings_si IS NULL")
            .withColumn("quarantine_reason", F.lit("schema_drift_nonstandard_columns"))
            .withColumn("_quarantined_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Document registry (Auto Loader binaryFile) + Document AI extractions

# COMMAND ----------

@dlt.table(name="bronze_documents",
           comment="Registry of every document that arrived in the submission inbox (emails, PDFs, schedules) via Auto Loader.",
           table_properties=BRONZE_PROPS)
def bronze_documents():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.schemaLocation", f"{CKPT}/documents_schema")
            .load(f"{INBOX}/*")
            .select(F.col("path"), F.col("length").alias("bytes"), F.col("modificationTime"),
                    F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
                    F.regexp_extract(F.element_at(F.split(F.col("path"), "/"), -1), r"sub-(\d+)", 1).alias("submission_ref_no"),
                    F.current_timestamp().alias("_bronze_ingested_at")))


@dlt.table(name="bronze_doc_extractions",
           comment="Document AI extractions (ai_parse_document + ai_query in notebook 01c). Low-confidence extractions are dropped to quarantine for human review.",
           table_properties=BRONZE_PROPS)
@dlt.expect_or_drop("extraction_confident", "extraction_confidence >= 0.6")
def bronze_doc_extractions():
    return (spark.read.table(f"{SRC}.landing_doc_extractions")
            .withColumn("_bronze_ingested_at", F.current_timestamp()))


@dlt.table(name="bronze_quarantine_extractions",
           comment="Quarantine mirror: document extractions below the 0.6 confidence gate — routed to human review, never silently used.",
           table_properties=BRONZE_PROPS)
def bronze_quarantine_extractions():
    return (spark.read.table(f"{SRC}.landing_doc_extractions")
            .filter("extraction_confidence < 0.6 OR extraction_confidence IS NULL")
            .withColumn("quarantine_reason", F.lit("extraction_confidence_below_gate"))
            .withColumn("_quarantined_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## PAS book + company profiles (system-of-record copies, governed)

# COMMAND ----------

@dlt.table(name="bronze_pas_policies",
           comment="Policy admin system book — governed copy.",
           table_properties=BRONZE_PROPS)
@dlt.expect("valid_policy_dates", "expiry_date > inception_date")
@dlt.expect("premium_positive", "gross_premium > 0")
def bronze_pas_policies():
    return spark.read.table(f"{SRC}.landing_pas_policies").withColumn("_bronze_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_pas_claims",
           comment="PAS claims history — burning-cost basis.",
           table_properties=BRONZE_PROPS)
@dlt.expect("non_negative_amounts", "paid >= 0 AND incurred >= 0")
def bronze_pas_claims():
    return spark.read.table(f"{SRC}.landing_pas_claims").withColumn("_bronze_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_company_profiles",
           comment="Companies-House-shaped profiles (synthetic; live API demo in notebook 91).",
           table_properties=BRONZE_PROPS)
@dlt.expect("has_company_number", "company_number IS NOT NULL")
def bronze_company_profiles():
    return spark.read.table(f"{SRC}.landing_company_profiles").withColumn("_bronze_ingested_at", F.current_timestamp())
