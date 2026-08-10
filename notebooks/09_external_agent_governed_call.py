# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Builder beat — an EXTERNAL agent runtime calling governed UC functions (ADDITIVE)
# MAGIC
# MAGIC The positioning proof: Databricks is the neutral **data + decision-logic + governance** layer
# MAGIC that other platforms stand on. A model-agnostic agent runtime on a policy core (e.g. an
# MAGIC MCP-native framework) doesn't need this workbench's app — it can call the very same governed
# MAGIC Unity Catalog functions as tools, and **the column masks and lineage hold regardless of who
# MAGIC calls**.
# MAGIC
# MAGIC This notebook stands in for that external runtime: it calls the crux decision functions
# MAGIC exactly as a foreign agent would (name-qualified UC functions over SQL), then proves the
# MAGIC conduct mask still redacts watchlist detail for a non-privileged caller. No app, no bespoke
# MAGIC integration — just the governed catalog surface.
# MAGIC
# MAGIC Additive: read-only demonstration; creates nothing, modifies nothing. Not in the reset path.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import json

# COMMAND ----------

# MAGIC %md ## 1 · The "tools" an external agent would bind
# MAGIC A foreign runtime discovers these as callable functions (they carry COMMENTs = tool specs).
# MAGIC It calls them by fully-qualified name — no knowledge of our app, our SPA or our server code.

# COMMAND ----------

TOOLS = ["fn_appetite_check", "fn_authority_check", "fn_accumulation_impact",
         "fn_technical_price", "fn_recommendation", "fn_wageroll_check"]
for t in TOOLS:
    c = spark.sql(f"DESCRIBE FUNCTION EXTENDED {fqn}.{t}").collect()
    comment = next((r.function_desc for r in c if "Comment" in (r.function_desc or "")), "")
    print(f"• {t}  {comment[:90]}")

# COMMAND ----------

# MAGIC %md ## 2 · The external agent calls a governed decision tool
# MAGIC Exactly what an MCP tool call resolves to server-side: a scalar UC-function invocation. The
# MAGIC agent gets a structured result it can reason over — never our prose, never our UI.

# COMMAND ----------

sid = "sub:900004"   # the Lane E wageroll hero
appetite = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_appetite_check('{sid}')) r").first().r)
wageroll = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_wageroll_check('{sid}')) r").first().r)
rec = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_recommendation('{sid}')) r").first().r)
print("external-agent tool results for", sid)
print("  appetite:", appetite.get("appetite_status"), "| in_appetite:", appetite.get("in_appetite"))
print("  wageroll:", wageroll.get("declared_wageroll"), "fires:", wageroll.get("fires"),
      "→ refer to", wageroll.get("required_grade"))
print("  recommendation:", rec.get("action"), "| reasons:", (rec.get("reasons") or [])[:1])
assert appetite.get("appetite_status") and rec.get("action"), "governed tools returned no structured result"
print("✅ external runtime reasoned over governed UC functions — no app involved")

# COMMAND ----------

# MAGIC %md ## 3 · The governance holds — the conduct mask redacts for a non-privileged caller
# MAGIC The external agent reads the same governed view an internal caller would. Because this
# MAGIC identity is NOT in `underwriting_conduct_readers`, Unity Catalog itself redacts the watchlist
# MAGIC reason — the mask is enforced at the data layer, not in any application. A foreign runtime
# MAGIC cannot route around it.

# COMMAND ----------

rows = spark.sql(f"SELECT watchlist_id, reason FROM {fqn}.gov_watchlist_secure").collect()
for r in rows[:5]:
    print(f"  {r.watchlist_id}: {r.reason}")
masked = [r for r in rows if "restricted" in (r.reason or "")]
privileged = spark.sql("SELECT is_account_group_member('underwriting_conduct_readers') m").first().m
if privileged:
    print("NOTE: this run identity IS in underwriting_conduct_readers — it legitimately sees detail. "
          "Run as the app service principal (or any non-conduct identity) to see the redaction.")
else:
    assert masked, "expected the conduct mask to redact watchlist reasons for this non-privileged caller"
    print("✅ conduct mask enforced by Unity Catalog for the external caller — watchlist reason redacted")

# COMMAND ----------

# MAGIC %md ## The point
# MAGIC The decision logic (appetite, authority, accumulation, price, wageroll referral) is a governed
# MAGIC asset the carrier owns — callable by this workbench, by a pricing engine, or by a foreign
# MAGIC agent runtime alike. Whoever calls, the same masks, lineage and audit apply. Own the decision;
# MAGIC let best-of-breed apps rent the plumbing.
