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
EXP_SCHEMA = "array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>"
flow = (ev.filter("event_type = 'flow_progress'")
        .select(F.col("origin.flow_name").alias("dataset"),
                F.explode(F.from_json(F.expr("details:flow_progress:data_quality:expectations"), EXP_SCHEMA)).alias("e"),
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
    # group, source, system, format, cadence, databricks_tool, table, status, note, production_connector
    ("Broker submissions", "E-trade gateway", "Broker portals (Acturis-shaped)", "structured JSON", "real-time",
     "Lakeflow Declarative Pipelines", "bronze_submissions", "live", "55% of SME flow — straight-through eligible",
     "SIMULATED here as a landing table. Production: software-house gateway (Acturis/Applied/CDL) webhook or file drop → Lakeflow Connect / Volume; same pipeline from there."),
    ("Broker submissions", "Broker portal feed", "Bricksurance broker extranet", "structured", "real-time",
     "Lakeflow Declarative Pipelines", "bronze_submissions", "live", "Structured but manual-keyed",
     "SIMULATED as a landing table. Production: portal backend emits JSON to a Volume/queue; identical bronze contract."),
    ("Broker submissions", "Email inbox", "newbusiness@ mailbox", "unstructured text", "continuous",
     "Auto Loader on UC Volume", "bronze_documents", "live", "The messy 20% that eats underwriter time",
     "SIMULATED as files in the Volume. Production: M365 Graph API or Power Automate rule drops the mailbox to the same Volume — everything you see runs from the drop onward."),
    ("Broker submissions", "Proposal forms & loss runs (PDF)", "Broker attachments", "scanned PDF", "continuous",
     "ai_parse_document + ai_query", "bronze_doc_extractions", "live", "Document AI with a 0.6 confidence gate",
     "REAL Document AI on real PDF bytes. 'Will it misread a form?' — below-gate extractions quarantine for human review; nothing low-confidence is silently used."),
    ("Broker submissions", "Risk schedules / SOVs", "Broker BMS exports", "CSV/XLSX, schema drifts", "continuous",
     "Auto Loader rescuedDataColumn", "bronze_schedule_locations", "live", "Drifted exports quarantine — nothing silently lost",
     "REAL file ingestion incl. the drift handling. Production: same Volume drop from email attachments or SFTP."),
    ("Broker submissions", "Loss runs — 5 carriers, 5 formats", "RSA PDF · Aviva CSV · Zurich pipe · broker export · 1 ambiguous scan", "mixed", "on submission",
     "ai_parse_document + ai_query normalisation", "landing_lossrun_claims", "live", "The gauntlet: varying deductibles + windows → ONE canonical claims table; the ambiguous scan is HELD OUT, never guessed",
     "REAL multi-format parsing on real file bytes. Production: same pipeline; add carriers by dropping their format in the folder."),
    ("Broker submissions", "Call transcripts", "Call platform export", "text transcripts", "continuous",
     "Auto Loader + ai_query insights", "bronze_doc_extractions", "live", "Material facts said on calls that never reach a form",
     "SIMULATED transcripts in the Volume. Production: NICE/Genesys/Teams transcript export → same folder drop — a NEW SOURCE IS A FOLDER DROP + ONE EXTRACTION PROMPT."),
    ("System of record", "Policy admin system (book)", "PAS", "Delta", "daily",
     "Lakeflow Declarative Pipelines", "bronze_pas_policies", "live", "In-force + lapsed (retention)",
     "SIMULATED book. Production: CDC from the PAS (Guidewire CDA-style / JDBC) via Lakeflow Connect. Quote/bind WRITE-BACK to the PAS is deliberately roadmap — this overlays, it does not replace."),
    ("System of record", "Claims history", "Claims system", "Delta", "daily",
     "Lakeflow Declarative Pipelines", "bronze_pas_claims", "live", "Burning-cost basis for technical price",
     "SIMULATED claims. Production: same CDC pattern — see the sibling Claims Workbench for the full Guidewire CDA landing."),
    ("Enrichment — open data", "Companies House profiles", "Synthetic (live API = notebook 91)", "API JSON", "on submission",
     "Simulated API call (labelled)", "bronze_company_profiles", "live", "Incorporation, SIC, accounts status, directors, filed turnover", "SIMULATED profiles (synthetic insureds). Production: live Companies House REST API — working example in notebook 91 (free API key)."),
    ("Enrichment — open data", "OFSI consolidated sanctions list", "HM Treasury OFSI (REAL, OGL)", "CSV", "monthly refresh",
     "Bundled real extract", "ref_sanctions_ofsi", "live", "12k primary names — screened at point of quote", "REAL OFSI consolidated list (bundled monthly). Does NOT replace WorldCheck/LexisNexis — it puts screening evidence + resolutions INTO the decision record; a vendor feed is just another source."),
    ("Enrichment — open data", "EA flood areas (RoFRS evidence)", "Environment Agency (REAL, OGL)", "API/CSV", "quarterly",
     "Bundled real extract", "ref_flood_open", "live", "England only; banding curated from RoFRS statistics", "REAL EA register extract (bundled quarterly). Production: full property-level RoFRS + surface water via the EA APIs."),
    ("Enrichment — open data", "police.uk crime counts", "Home Office police.uk (REAL, OGL)", "API/CSV", "monthly",
     "Bundled real extract", "ref_crime_open", "live", "GMP data gap flagged + imputed — honest sourcing", "REAL police.uk counts (bundled monthly). Production: scheduled refresh job against data.police.uk."),
    ("Enrichment — open data", "EPC band mix (MEES lens)", "MHCLG EPC statistics (curated)", "CSV", "quarterly",
     "Bundled curated extract", "ref_epc_mix_open", "live", "MEES letting-ban/ESG lens — never a rating factor", "CURATED from MHCLG statistics. Production: EPC register bulk download (free registration)."),
    ("Enrichment — roadmap", "Full RoFRS property-level flood", "Environment Agency", "GIS", "quarterly", "—", None, "roadmap", "Property-level flood banding + surface water"),
    ("Enrichment — roadmap", "Live Companies House API", "Companies House", "REST API", "on submission", "—", None, "roadmap", "Real API demo in notebook 91 (off critical path)"),
    ("Enrichment — roadmap", "Credit reference (Experian/D&B)", "Credit bureau", "API", "on submission", "—", None, "roadmap", "Financial resilience score"),
    ("Enrichment — roadmap", "Perils/cat aggregation feed", "Cat modeller", "YELT/Delta", "quarterly", "—", None, "roadmap", "See reinsurance workbench for the treaty view"),
    ("Enrichment — roadmap", "Survey & risk engineering reports", "Survey panel", "PDF", "post-quote", "—", None, "roadmap", "Post-bind obligations tracking"),
]
rows = []
for item in SRC_MAP:
    g, src, sys_, fmt, cad, tool, tbl, st, note = item[:9]
    connector = item[9] if len(item) > 9 else "Roadmap — connector named per source when built."
    rows.append((g, src, sys_, fmt, cad, tool, tbl, st, note, connector,
                 cnt(tbl) if st == "live" and tbl else None))
df = spark.createDataFrame(rows, "source_group string, source string, system string, format string, cadence string, "
                                 "databricks_tool string, table_name string, status string, note string, "
                                 "production_connector string, row_count long")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_ingestion_sources")

# Canonical claims experience (the loss-run gauntlet's output) + per-document reconciliation
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gold_claims_experience
COMMENT 'Canonical claims experience normalised from multi-carrier loss runs (narrative PDF, CSVs, pipe-delimited, broker exports) by Document AI — one row per claim with carrier, deductible and period. Ambiguous documents are held out, never guessed.' AS
SELECT submission_public_id, file_name, carrier, period_note, loss_date, peril,
       paid_gbp, outstanding_gbp, deductible_gbp, status, extraction_confidence
FROM {fqn}.landing_lossrun_claims
WHERE extraction_confidence >= 0.6 AND peril IS NOT NULL""")
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gold_lossrun_recon
COMMENT 'Reconciliation per loss-run document: claims extracted, totals, confidence, held-out flag.' AS
SELECT file_name, any_value(submission_public_id) submission_public_id, any_value(carrier) carrier,
       count(CASE WHEN peril IS NOT NULL THEN 1 END) claims_extracted,
       sum(coalesce(paid_gbp, 0)) total_paid, sum(coalesce(outstanding_gbp, 0)) total_outstanding,
       any_value(extraction_confidence) confidence,
       any_value(extraction_confidence) < 0.6 AS held_out
FROM {fqn}.landing_lossrun_claims GROUP BY file_name""")
print("gold_claims_experience + gold_lossrun_recon views created")
spark.sql(f"ALTER TABLE {fqn}.gold_ingestion_sources SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")
live_rows = df.filter("status='live'").count()
assert spark.table(f"{fqn}.gold_dq_scorecard").count() >= 8, "expected ≥8 expectations in the scorecard"
print(f"✅ 03b complete — {live_rows} live sources mapped, scorecard written")
