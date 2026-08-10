# Databricks notebook source
# MAGIC %md
# MAGIC # 01d · Wageroll extraction — Document AI for the EL rating basis (ADDITIVE)
# MAGIC
# MAGIC Wageroll (annual payroll) is the Employers' Liability rating basis and is rarely a clean
# MAGIC structured field — it arrives in a broker's covering note or a payroll schedule. This
# MAGIC notebook demonstrates the **same `ai_query` confidence-gate pattern** as `01c`, applied to
# MAGIC the Lane E hero `sub:900004`: write its wageroll statement to the submission inbox volume,
# MAGIC extract the declared figure with a self-reported confidence, and MERGE it into
# MAGIC `silver_submission_wageroll` (the sidecar `00c` seeds for the whole book).
# MAGIC
# MAGIC Additive: new notebook, new landing file, MERGE into the Lane E sidecar only. `00c` already
# MAGIC seeded the hero value so the table is reset-safe even if this notebook is skipped; here the
# MAGIC figure is *earned* from the document. Runs in the ingest path after `doc_extraction`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md ## 1 · Write the hero's wageroll statement to the inbox volume
# MAGIC A short broker covering note stating payroll — the kind of unstructured text a wageroll
# MAGIC figure actually hides in. (Deterministic content; no RNG.)

# COMMAND ----------

vol_dir = f"/Volumes/{catalog}/{schema}/submission_inbox"
note = (
    "From: Fiona Slate, Harborough & Slate\n"
    "Re: New business - Harwood & Vane Scaffolding Ltd (LS10)\n\n"
    "Please find the scaffolding contractor risk for your consideration. Trade: commercial and "
    "industrial scaffolding, no work above 30m, hot-work permit system in place.\n\n"
    "Employers' Liability rating basis - the client confirms an annual wageroll (gross payroll "
    "including subcontract labour treated as employees) of GBP 6,800,000 across 178 operatives. "
    "Turnover for the year was GBP 14.8m.\n\n"
    "Limits sought: EL GBP 10m, PL GBP 5m. Target premium around GBP 72,000.\n"
)
dbutils.fs.put(f"{vol_dir}/sub-900004_wageroll_statement.txt", note, overwrite=True)
print("wrote sub-900004_wageroll_statement.txt")

# COMMAND ----------

# MAGIC %md ## 2 · Extract declared wageroll with ai_query (self-reported confidence)

# COMMAND ----------

PROMPT = (
    "You are an insurance data-extraction assistant. From the broker note below, extract the "
    "declared Employers Liability wageroll (annual gross payroll used as the EL rating basis). "
    "Return STRICT JSON only, no prose: "
    "{declared_wageroll_gbp:(number, GBP, null if absent), "
    "confidence:(0-1, your confidence the figure is legible and unambiguous)}. Note follows:\n\n"
)

raw = spark.read.text(f"{vol_dir}/sub-900004_wageroll_statement.txt", wholetext=True) \
          .withColumnRenamed("value", "raw_text")
raw.createOrReplaceTempView("_wr_raw")

extracted = spark.sql(f"""
  WITH q AS (
    SELECT ai_query('{FM}', concat('{PROMPT}', substring(raw_text, 1, 8000))) AS out FROM _wr_raw
  )
  SELECT x.declared_wageroll_gbp, x.confidence
  FROM q, LATERAL (SELECT from_json(regexp_extract(out, '(?s)\\\\{{.*\\\\}}', 0),
                          'STRUCT<declared_wageroll_gbp:DOUBLE, confidence:DOUBLE>') AS x) t
""").first()

wr_value = int(extracted["declared_wageroll_gbp"]) if extracted and extracted["declared_wageroll_gbp"] else None
wr_conf = float(extracted["confidence"]) if extracted and extracted["confidence"] is not None else 0.0
print(f"extracted wageroll={wr_value} confidence={wr_conf}")

# COMMAND ----------

# MAGIC %md ## 3 · MERGE into the sidecar (confidence-gated)
# MAGIC Only overwrite the seeded value if the extraction is confident (≥0.6); otherwise keep the
# MAGIC `00c` seed. Additive MERGE — no other row touched.

# COMMAND ----------

if wr_value and wr_conf >= 0.6:
    spark.sql(f"""
      MERGE INTO {fqn}.silver_submission_wageroll t
      USING (SELECT 'sub:900004' AS sid, {wr_value}L AS wr,
                    'ai_parse_document + ai_query ({FM})' AS src, {wr_conf}D AS conf) s
      ON t.submission_public_id = s.sid
      WHEN MATCHED THEN UPDATE SET declared_wageroll = s.wr, source = s.src, extraction_confidence = s.conf
      WHEN NOT MATCHED THEN INSERT (submission_public_id, declared_wageroll, source, extraction_confidence)
                          VALUES (s.sid, s.wr, s.src, s.conf)
    """)
    print("MERGED extracted wageroll for sub:900004")
else:
    print("extraction below confidence gate — keeping 00c seed (reset-safe)")

# COMMAND ----------

row = spark.sql(f"SELECT declared_wageroll, source, extraction_confidence "
                f"FROM {fqn}.silver_submission_wageroll WHERE submission_public_id='sub:900004'").first()
assert row.declared_wageroll == 6_800_000, f"hero wageroll must be £6.8m, got {row.declared_wageroll}"
print(f"✅ 01d wageroll extraction — sub:900004 = £{row.declared_wageroll:,} (via {row.source})")
