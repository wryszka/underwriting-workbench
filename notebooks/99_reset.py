# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset — cache clear + hero determinism check
# MAGIC
# MAGIC The reset JOB re-runs: data gen (seed=42, dates re-anchor to today) → landing files →
# MAGIC doc extraction → medallion full refresh → features → inbox re-score + governance re-seed →
# MAGIC this notebook (cache clear + verification). Models are NOT retrained and the crux
# MAGIC functions are NOT recreated (grant-revocation gotcha). The app re-warms the cache after
# MAGIC the job succeeds (POST /api/warm-cache).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fqn}.cache_agent_responses
              (cache_key STRING, endpoint STRING, response STRING, created_ts TIMESTAMP) USING DELTA""")
spark.sql(f"TRUNCATE TABLE {fqn}.cache_agent_responses")
print("cache cleared")

# COMMAND ----------

# Hero determinism after re-anchor: same actions, same key numbers
import json

for sid, want in (("sub:900001", "quote"), ("sub:900002", "refer"), ("sub:900003", "decline")):
    r = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_recommendation('{sid}')) AS r").first().r)
    assert r["action"] == want, f"{sid}: {r['action']} != {want}"
    print(sid, "→", r["action"], "✓")
acc = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_accumulation_impact('sub:900002')) AS r").first().r)
hx7 = [d for d in acc["districts"] if d["postcode_district"] == "HX7"][0]
assert 85.0 <= hx7["post_util_pct"] <= 89.0, hx7
print(f"HX7 post-bind {hx7['post_util_pct']}% ✓")
print("✅ reset verification complete — heroes deterministic")
