# Databricks notebook source
# MAGIC %md
# MAGIC # 01c · Document AI — extraction from the submission inbox
# MAGIC
# MAGIC PDFs go through **`ai_parse_document`** (layout-aware parsing) and emails/text through
# MAGIC direct read; both are then entity-extracted with **`ai_query`** (Claude via the
# MAGIC Foundation Model API) into `landing_doc_extractions` with a self-reported confidence.
# MAGIC The bronze pipeline gates on confidence ≥ 0.6 — low-confidence extractions quarantine
# MAGIC for human review (the fax scan `sub-108150` lands there by design).
# MAGIC
# MAGIC Runs BEFORE the medallion pipeline in the ingest job (bronze consumes its output).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"
INBOX = f"/Volumes/{catalog}/{schema}/submission_inbox"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1 · Parse — PDFs via ai_parse_document, text files directly

# COMMAND ----------

pdfs = (spark.read.format("binaryFile").load(f"{INBOX}/*.pdf")
        .select(F.element_at(F.split("path", "/"), -1).alias("file_name"), "content"))
pdfs.createOrReplaceTempView("_pdf_docs")
parsed_pdfs = spark.sql("""
  SELECT file_name,
         concat_ws('\n',
           transform(
             try_cast(ai_parse_document(content):document:elements AS ARRAY<STRUCT<content:STRING>>),
             x -> x.content)) AS raw_text
  FROM _pdf_docs""")

texts = (spark.read.format("text").option("wholetext", "true")
         .load([f"{INBOX}/*.txt", f"{INBOX}/*.csv"])
         .select(F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).alias("file_name"),
                 F.col("value").alias("raw_text"))
         .filter("file_name NOT LIKE '%_schedule_%'"))

docs = parsed_pdfs.unionByName(texts).filter(F.length("raw_text") > 20)
docs.createOrReplaceTempView("_docs_raw")
print(f"parsed {docs.count()} documents")

# COMMAND ----------

# MAGIC %md ## 2 · Extract — ai_query entity extraction with self-reported confidence

# COMMAND ----------

PROMPT = (
    "You are a commercial insurance submission analyst. Extract facts from the document below. "
    "Respond with ONLY a JSON object, no prose, with these keys: "
    "company_name (string|null), company_number (string|null), trade_description (string|null), "
    "turnover_stated_gbp (number|null), employees (number|null), n_locations (number|null), "
    "buildings_si_gbp (number|null), plant_si_gbp (number|null), stock_si_gbp (number|null), "
    "bi_si_gbp (number|null), bi_indemnity_months (number|null), el_limit_gbp (number|null), "
    "pl_limit_gbp (number|null), target_premium_gbp (number|null), incumbent_insurer (string|null), "
    "directors (array of strings), key_hazards (array of strings - e.g. composite panels unconfirmed, "
    "frying line suppression unconfirmed, ammonia plant, flood exposure disclosed, waste transfer), "
    "prior_material_losses (array of strings), flood_disclosed (boolean), "
    "confidence (number 0-1: your confidence the document was legible and the extraction is faithful). "
    "If the document is garbled or illegible, set confidence below 0.5. Document follows:\n\n"
).replace("'", "''")

extracted = spark.sql(f"""
  SELECT file_name, raw_text,
         ai_query('{FM}', concat('{PROMPT}', substring(raw_text, 1, 12000))) AS extraction_raw
  FROM _docs_raw""")
extracted.createOrReplaceTempView("_docs_extracted")

SCHEMA_JSON = ("STRUCT<company_name:STRING, company_number:STRING, trade_description:STRING, "
               "turnover_stated_gbp:DOUBLE, employees:DOUBLE, n_locations:DOUBLE, buildings_si_gbp:DOUBLE, "
               "plant_si_gbp:DOUBLE, stock_si_gbp:DOUBLE, bi_si_gbp:DOUBLE, bi_indemnity_months:DOUBLE, "
               "el_limit_gbp:DOUBLE, pl_limit_gbp:DOUBLE, target_premium_gbp:DOUBLE, incumbent_insurer:STRING, "
               "directors:ARRAY<STRING>, key_hazards:ARRAY<STRING>, prior_material_losses:ARRAY<STRING>, "
               "flood_disclosed:BOOLEAN, confidence:DOUBLE>")

final = spark.sql(f"""
  WITH j AS (
    SELECT file_name, raw_text,
           from_json(regexp_extract(extraction_raw, '(?s)\\\\{{.*\\\\}}', 0), '{SCHEMA_JSON}') AS x
    FROM _docs_extracted)
  SELECT file_name,
         CASE WHEN regexp_extract(file_name, 'sub-([0-9]+)', 1) != ''
              THEN concat('sub:', regexp_extract(file_name, 'sub-([0-9]+)', 1)) END AS submission_public_id,
         CASE WHEN file_name LIKE '%call%' THEN 'call_transcript'
              WHEN file_name LIKE '%proposal%' THEN 'proposal_form'
              WHEN file_name LIKE '%loss_run%' OR file_name LIKE '%lossrun%' THEN 'loss_run'
              WHEN file_name LIKE '%schedule%' THEN 'risk_schedule'
              WHEN file_name LIKE '%presentation%' THEN 'risk_presentation'
              WHEN file_name LIKE '%fax%' THEN 'fax_scan'
              WHEN file_name LIKE '%receipt%' THEN 'etrade_receipt'
              ELSE 'email' END AS doc_type,
         x.company_name, x.company_number, x.trade_description,
         cast(x.turnover_stated_gbp AS LONG) AS turnover_stated_gbp,
         cast(x.employees AS INT) AS employees, cast(x.n_locations AS INT) AS n_locations,
         cast(x.buildings_si_gbp AS LONG) AS buildings_si_gbp, cast(x.plant_si_gbp AS LONG) AS plant_si_gbp,
         cast(x.stock_si_gbp AS LONG) AS stock_si_gbp, cast(x.bi_si_gbp AS LONG) AS bi_si_gbp,
         cast(x.bi_indemnity_months AS INT) AS bi_indemnity_months,
         cast(x.el_limit_gbp AS LONG) AS el_limit_gbp, cast(x.pl_limit_gbp AS LONG) AS pl_limit_gbp,
         cast(x.target_premium_gbp AS LONG) AS target_premium_gbp, x.incumbent_insurer,
         to_json(x.directors) AS directors_json, to_json(x.key_hazards) AS key_hazards_json,
         to_json(x.prior_material_losses) AS prior_losses_json, x.flood_disclosed,
         coalesce(x.confidence, 0.0) AS extraction_confidence,
         substring(raw_text, 1, 2000) AS raw_text_excerpt,
         'ai_parse_document + ai_query ({FM})' AS source_tool,
         current_timestamp() AS extracted_at
  FROM j""")

final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.landing_doc_extractions")
spark.sql(f"ALTER TABLE {fqn}.landing_doc_extractions SET TBLPROPERTIES ('layer'='landing','demo'='underwriting_workbench')")

# COMMAND ----------

# MAGIC %md ## 3 · The loss-run gauntlet — normalise five carriers/formats into ONE canonical table
# MAGIC Narrative PDF, clean CSV, pipe-delimited with total rows, broker export with junk headers
# MAGIC and £-formatted amounts — each becomes structured claim rows with carrier, deductible and
# MAGIC period. The ambiguous scan self-reports low confidence and QUARANTINES instead of guessing.

# COMMAND ----------

LR_PROMPT = (
    "You are normalising an insurance claims experience document (a loss run / bordereau). "
    "Extract EVERY individual claim as structured data. Respond ONLY with JSON: "
    "{carrier (string|null - the insurer that paid, from the document), "
    "period_note (string|null - the experience window as stated), "
    "claims (array of {loss_date (ISO yyyy-mm-dd or null), peril (string), paid_gbp (number|null), "
    "outstanding_gbp (number|null), deductible_gbp (number|null), status (string|null)}), "
    "confidence (0-1: legibility + whether amounts/currency/periods are unambiguous)}. "
    "Ignore total/summary rows. Convert formatted amounts (pound signs, commas) to numbers. "
    "If periods overlap ambiguously or currency is unclear, set confidence below 0.5 and claims to []. "
    "Document follows:\n\n"
).replace("'", "''")

LR_SCHEMA = ("STRUCT<carrier:STRING, period_note:STRING, "
             "claims:ARRAY<STRUCT<loss_date:STRING, peril:STRING, paid_gbp:DOUBLE, "
             "outstanding_gbp:DOUBLE, deductible_gbp:DOUBLE, status:STRING>>, confidence:DOUBLE>")

lr = spark.sql(f"""
  WITH lr_docs AS (
    SELECT file_name, raw_text FROM _docs_raw
    WHERE file_name LIKE '%loss_run%' OR file_name LIKE '%lossrun%'),
  ext AS (
    SELECT file_name,
           from_json(regexp_extract(ai_query('{FM}', concat('{LR_PROMPT}', substring(raw_text, 1, 10000))),
                     '(?s)\\\\{{.*\\\\}}', 0), '{LR_SCHEMA}') AS x
    FROM lr_docs)
  SELECT CASE WHEN regexp_extract(file_name, 'sub-([0-9]+)', 1) != ''
              THEN concat('sub:', regexp_extract(file_name, 'sub-([0-9]+)', 1)) END AS submission_public_id,
         file_name, x.carrier, x.period_note,
         c.loss_date, c.peril, c.paid_gbp, c.outstanding_gbp, c.deductible_gbp, c.status,
         coalesce(x.confidence, 0.0) AS extraction_confidence,
         current_timestamp() AS extracted_at
  FROM ext LATERAL VIEW OUTER explode(x.claims) c AS c""")
lr.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.landing_lossrun_claims")
spark.sql(f"ALTER TABLE {fqn}.landing_lossrun_claims SET TBLPROPERTIES ('layer'='landing','demo'='underwriting_workbench')")
lrt = spark.table(f"{fqn}.landing_lossrun_claims")
n_ok = lrt.filter("extraction_confidence >= 0.6 AND peril IS NOT NULL").count()
n_ambig = lrt.filter("extraction_confidence < 0.6").select("file_name").distinct().count()
print(f"loss-run gauntlet: {n_ok} claims normalised from {lrt.select('file_name').distinct().count()} documents · {n_ambig} ambiguous doc(s) held out")
assert n_ok >= 12, f"expected >=12 normalised claims across carriers, got {n_ok}"
assert n_ambig >= 1, "the ambiguous scan must be held out, not guessed"

# COMMAND ----------

out = spark.table(f"{fqn}.landing_doc_extractions")
display(out.select("file_name", "doc_type", "company_name", "turnover_stated_gbp", "extraction_confidence"))
n_low = out.filter("extraction_confidence < 0.6").count()
h2 = out.filter("file_name = 'sub-900002_proposal_form.pdf'").first()
assert h2 is not None and h2.extraction_confidence >= 0.6, "hero proposal must extract confidently"
assert n_low >= 1, "the fax scan should land below the confidence gate"
print(f"✅ 01c complete — {out.count()} extractions, {n_low} below gate")
