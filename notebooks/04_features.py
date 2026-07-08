# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Feature engineering — UC Feature Store
# MAGIC
# MAGIC `feature_submission` (PK `submission_public_id`): the deterministic feature vector both
# MAGIC models and the serving path share. Encodings are persisted in `ref_feature_encodings`
# MAGIC (fixed maps, unseen → -1) so train and serve encode identically — the claims_workbench
# MAGIC feature-vector contract (no online store; UC fns pre-fetch the vector by key).

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# COMMAND ----------

# Persisted deterministic encodings (train/serve contract)
ENC = {
    "channel": {"etrade": 0, "portal": 1, "email": 2},
    "segment": {"sme": 0, "mid_market": 1},
    "appetite_status": {"core": 0, "selective": 1, "excluded": 2},
    "flood_band": {"Low": 0, "Medium": 1, "High": 2},
    "construction_type": {"brick_traditional": 0, "steel_frame_clad": 1, "concrete_frame": 2,
                          "timber_frame": 3, "composite_panel_clad": 4, "listed_heritage": 5},
}
enc_rows = [(feat, k, v) for feat, m in ENC.items() for k, v in m.items()]
(spark.createDataFrame(enc_rows, "feature string, value string, code int")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ref_feature_encodings"))
spark.sql(f"ALTER TABLE {fqn}.ref_feature_encodings SET TBLPROPERTIES ('layer'='feature','demo'='underwriting_workbench')")


def enc_col(col, feat):
    m = ENC[feat]
    expr = F.lit(-1)
    for k, v in m.items():
        expr = F.when(F.col(col) == k, v).otherwise(expr)
    return expr.cast("int")

# COMMAND ----------

s = spark.table(f"{fqn}.silver_submissions")
feat = (s.select(
    "submission_public_id",
    enc_col("channel", "channel").alias("channel_e"),
    enc_col("segment", "segment").alias("segment_e"),
    enc_col("appetite_status", "appetite_status").alias("appetite_e"),
    enc_col("flood_band", "flood_band").alias("flood_e"),
    enc_col("construction_type", "construction_type").alias("construction_e"),
    F.coalesce(F.col("hazard_grade"), F.lit(3)).cast("int").alias("hazard_grade"),
    F.round(F.log1p(F.col("total_si")), 4).alias("log_total_si"),
    F.round(F.log1p(F.coalesce(F.col("turnover_stated"), F.lit(0))), 4).alias("log_turnover"),
    F.coalesce(F.col("employees"), F.lit(0)).cast("int").alias("employees"),
    F.coalesce(F.col("n_locations"), F.lit(1)).cast("int").alias("n_locations"),
    F.coalesce(F.col("crime_count"), F.lit(0)).cast("int").alias("crime_count"),
    F.coalesce(F.col("year_built"), F.lit(1985)).cast("int").alias("year_built"),
    F.round(F.coalesce(F.col("target_premium") / F.col("technical_base_premium"), F.lit(1.0)), 4).alias("target_vs_technical"),
    F.round(F.coalesce(F.col("cohort_loss_ratio_pct"), F.lit(50.0)), 2).alias("cohort_loss_ratio_pct"),
    F.col("data_complete").cast("int").alias("data_complete"),
    F.coalesce(F.col("turnover_mismatch_ratio"), F.lit(1.0)).alias("turnover_mismatch_ratio"),
).dropDuplicates(["submission_public_id"]))

FEAT_TABLE = f"{fqn}.feature_submission"
if not spark.catalog.tableExists(FEAT_TABLE):
    fe.create_table(name=FEAT_TABLE, primary_keys=["submission_public_id"], df=feat,
                    description="Submission feature vector (triage priority + risk quality) — feature-vector serving contract")
else:
    fe.write_table(name=FEAT_TABLE, df=feat, mode="merge")
spark.sql(f"ALTER TABLE {FEAT_TABLE} SET TBLPROPERTIES ('layer'='feature','demo'='underwriting_workbench')")

n = spark.table(FEAT_TABLE).count()
d = spark.table(FEAT_TABLE).select("submission_public_id").distinct().count()
assert n == d, "feature table PK must be unique"
for sid in ("sub:900001", "sub:900002", "sub:900003"):
    assert spark.table(FEAT_TABLE).filter(f"submission_public_id='{sid}'").count() == 1
print(f"✅ 04 complete — feature_submission {n} rows (PK unique, heroes present)")
