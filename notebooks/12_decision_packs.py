# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Decision audit packs — one PDF per decided submission
# MAGIC
# MAGIC For every decision in `gold_decision_audit`, compile the audit pack PDF (the dossier
# MAGIC as-at decision time, the checks, the price with IPT, terms & subjectivities, and the
# MAGIC external/internal reason separation) into the governed **`comms_out` Volume**, named by
# MAGIC submission number, and register it in `gold_decision_packs`.
# MAGIC
# MAGIC Decisions recorded in the app carry their `decision_evidence` snapshot; seed decisions
# MAGIC (no snapshot) are rendered from the live crux functions. Shares the exact renderer with
# MAGIC the app (`app/server/packs.py`).

# COMMAND ----------

# MAGIC %pip install fpdf2 --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"
VOL = f"/Volumes/{catalog}/{schema}/comms_out"

import json
import os
import sys

from pyspark.sql import functions as F

# Shared renderer: bundle files layout is files/notebooks + files/app/server
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "app", "server")))
from packs import build_pack  # noqa: E402

# COMMAND ----------

audits = [r.asDict() for r in spark.sql(f"""
    SELECT * FROM (
      SELECT *, row_number() OVER (PARTITION BY submission_public_id ORDER BY decision_ts DESC) rn
      FROM {fqn}.gold_decision_audit) WHERE rn = 1""").collect()]
print(f"{len(audits)} decided submissions")


def evidence_for(a):
    if a.get("decision_evidence"):
        try:
            return json.loads(a["decision_evidence"])
        except Exception:  # noqa: BLE001
            pass
    sid = a["submission_public_id"]
    ev = {}
    for key, fn in (("dossier", "fn_extract_summary"), ("appetite", "fn_appetite_check"),
                    ("authority", "fn_authority_check"), ("accumulation", "fn_accumulation_impact"),
                    ("price", "fn_technical_price"), ("screening", "fn_sanctions_screen"),
                    ("recommendation", "fn_recommendation")):
        ev[key] = json.loads(spark.sql(f"SELECT to_json({fqn}.{fn}('{sid}')) r").first().r)
    return ev


rows = []
for a in audits:
    sid = a["submission_public_id"]
    fname = sid.replace(":", "-") + "_decision_pack.pdf"
    pdf = build_pack(a, evidence_for(a))
    with open(f"{VOL}/{fname}", "wb") as f:
        f.write(pdf)
    rows.append((sid, a["decision_id"], fname, f"{VOL}/{fname}", len(pdf)))
    print(f"  📦 {fname} ({len(pdf):,} bytes)")

(spark.createDataFrame(rows, "submission_public_id string, decision_id string, file_name string, path string, bytes long")
 .withColumn("generated_at", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_decision_packs"))
spark.sql(f"ALTER TABLE {fqn}.gold_decision_packs SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")

n = spark.table(f"{fqn}.gold_decision_packs").count()
assert n >= 3, f"expected packs for at least the 3 heroes, got {n}"
h = next(r for r in rows if r[0] == "sub:900003")
assert h[4] > 1500, "decline pack suspiciously small"
print(f"✅ 12 complete — {n} audit packs in {VOL}")
