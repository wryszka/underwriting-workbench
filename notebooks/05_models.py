# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Models — triage priority + risk quality (LightGBM, UC registry, serving)
# MAGIC
# MAGIC **Model A `model_triage_priority`** — P(bind | quoted) on 12 months of closed
# MAGIC submissions via a Feature Store training set (real FS→model lineage). Ranks the inbox.
# MAGIC **Model B `model_risk_quality`** — P(large loss ≥ £25k within 3y) trained on the PAS
# MAGIC book's actual claims experience; feeds rate adequacy and referral triggers.
# MAGIC
# MAGIC Both logged as **pyfunc probability wrappers** (predict → P(positive) as double —
# MAGIC avoids the class-label-serving gotcha), registered @champion, served scale-to-zero on
# MAGIC endpoints `underwriting-triage` / `underwriting-risk` (created imperatively here —
# MAGIC never version-pinned in DAB, so deploys can't silently revert them).

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering lightgbm --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import mlflow
import pandas as pd
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()
w = WorkspaceClient()

FEATS_TRIAGE = ["channel_e", "segment_e", "appetite_e", "flood_e", "hazard_grade", "log_total_si",
                "log_turnover", "employees", "n_locations", "target_vs_technical",
                "cohort_loss_ratio_pct", "data_complete"]
FEATS_RISK = ["hazard_grade", "construction_e", "flood_e", "crime_count", "year_built",
              "log_total_si", "log_turnover", "employees", "cohort_loss_ratio_pct"]


class ProbaModel(mlflow.pyfunc.PythonModel):
    """Serve P(positive class) as a plain double (ai_query returnType 'DOUBLE')."""

    def __init__(self, model, feats):
        self.model, self.feats = model, feats

    def predict(self, context, model_input: pd.DataFrame):
        return self.model.predict_proba(model_input[self.feats])[:, 1]


def log_and_champion(model, feats, name, run_name, metrics, training_set=None, example=None):
    # Feature-VECTOR contract (claims_workbench gotcha): fe.log_model-packaged models do a
    # feature lookup BY KEY at serving time, which requires an ONLINE store — absent here, the
    # endpoint fails to build. So models are always logged as plain pyfunc taking the feature
    # vector; the training run still records the Feature Store training set for lineage.
    with mlflow.start_run(run_name=run_name):
        mlflow.log_metrics(metrics)
        if training_set is not None:
            mlflow.log_param("feature_store_training_set", f"{fqn}.feature_submission")
        sig = mlflow.models.infer_signature(example[feats], pd.Series([0.5]))
        info = mlflow.pyfunc.log_model(artifact_path="model", python_model=ProbaModel(model, feats),
                                       registered_model_name=f"{fqn}.{name}",
                                       signature=sig, input_example=example[feats].head(2),
                                       pip_requirements=["lightgbm", "pandas", "scikit-learn"])
    v = (info.registered_model_version if getattr(info, "registered_model_version", None)
         else max(int(m.version) for m in w.model_versions.list(f"{fqn}.{name}")))
    w.registered_models.set_alias(full_name=f"{fqn}.{name}", alias="champion", version_num=int(v))
    print(f"  {name} v{v} @champion  {metrics}")
    return int(v)

# COMMAND ----------

# MAGIC %md ## Model A — triage priority (FS training set on closed submissions)

# COMMAND ----------

closed = (spark.table(f"{fqn}.silver_submissions")
          .filter("lifecycle_state = 'closed' AND outcome IS NOT NULL AND appetite_status != 'excluded'")
          .select("submission_public_id", (F.col("outcome") == "bound").cast("int").alias("label")))
ts = fe.create_training_set(df=closed,
                            feature_lookups=[FeatureLookup(table_name=f"{fqn}.feature_submission",
                                                           lookup_key="submission_public_id")],
                            label="label", exclude_columns=["submission_public_id"])
df_a = ts.load_df().toPandas()
Xa, ya = df_a[FEATS_TRIAGE], df_a["label"]
Xtr, Xte, ytr, yte = train_test_split(Xa, ya, test_size=0.25, random_state=42, stratify=ya)
m_a = LGBMClassifier(n_estimators=300, learning_rate=0.06, max_depth=5, random_state=42, verbose=-1)
m_a.fit(Xtr, ytr)
auc_a = roc_auc_score(yte, m_a.predict_proba(Xte)[:, 1])
v_a = log_and_champion(m_a, FEATS_TRIAGE, "model_triage_priority", "triage_priority_lgbm",
                       {"auc": round(auc_a, 4), "n_train": len(Xtr)}, training_set=ts, example=Xa)

# COMMAND ----------

# MAGIC %md ## Model B — risk quality (trained on the PAS book's claims experience)

# COMMAND ----------

pol = spark.table(f"{fqn}.bronze_pas_policies")
large = (spark.table(f"{fqn}.bronze_pas_claims").filter("incurred >= 25000")
         .select("policy_number").distinct().withColumn("label", F.lit(1)))
flood = spark.table(f"{fqn}.ref_flood_open").select("postcode_district", "flood_band")
crime = spark.table(f"{fqn}.ref_crime_open").select("postcode_district", F.col("effective_count").alias("crime_count"))
cohort = (spark.table(f"{fqn}.silver_submissions")
          .groupBy("trade_group").agg(F.avg("cohort_loss_ratio_pct").alias("cohort_loss_ratio_pct")))
enc = {r["value"]: r["code"] for r in spark.table(f"{fqn}.ref_feature_encodings")
       .filter("feature='construction_type'").collect()}
enc_f = {r["value"]: r["code"] for r in spark.table(f"{fqn}.ref_feature_encodings")
         .filter("feature='flood_band'").collect()}
app = spark.table(f"{fqn}.ref_appetite").select("trade_group", "hazard_grade")

df_b = (pol.join(large, "policy_number", "left").fillna({"label": 0})
        .join(flood, "postcode_district", "left").join(crime, "postcode_district", "left")
        .join(cohort, "trade_group", "left").join(app, "trade_group", "left")
        .select("label", "construction_type", "flood_band", "hazard_grade",
                F.coalesce("crime_count", F.lit(0)).alias("crime_count"), "year_built",
                F.round(F.log1p(F.col("buildings_si") + F.col("contents_si") + F.col("stock_si") + F.col("bi_si")), 4).alias("log_total_si"),
                F.round(F.log1p(F.col("turnover")), 4).alias("log_turnover"),
                "employees", F.coalesce("cohort_loss_ratio_pct", F.lit(50.0)).alias("cohort_loss_ratio_pct"))
        ).toPandas()
df_b["construction_e"] = df_b["construction_type"].map(enc).fillna(-1).astype(int)
df_b["flood_e"] = df_b["flood_band"].map(enc_f).fillna(-1).astype(int)
Xb, yb = df_b[FEATS_RISK], df_b["label"]
Xtr, Xte, ytr, yte = train_test_split(Xb, yb, test_size=0.25, random_state=42, stratify=yb)
m_b = LGBMClassifier(n_estimators=300, learning_rate=0.06, max_depth=5, random_state=42, verbose=-1)
m_b.fit(Xtr, ytr)
auc_b = roc_auc_score(yte, m_b.predict_proba(Xte)[:, 1])
v_b = log_and_champion(m_b, FEATS_RISK, "model_risk_quality", "risk_quality_lgbm",
                       {"auc": round(auc_b, 4), "base_rate": round(float(yb.mean()), 4), "n_train": len(Xtr)},
                       example=Xb)

# COMMAND ----------

# MAGIC %md ## Serving — scale-to-zero endpoints (imperative; champion version)

# COMMAND ----------

def ensure_endpoint(ep_name, model_name, version):
    cfg = EndpointCoreConfigInput(name=ep_name, served_entities=[ServedEntityInput(
        entity_name=f"{fqn}.{model_name}", entity_version=str(version),
        scale_to_zero_enabled=True, workload_size="Small", name=model_name.replace("model_", "")[:20])])
    existing = {e.name for e in w.serving_endpoints.list()}
    if ep_name in existing:
        w.serving_endpoints.update_config(name=ep_name, served_entities=cfg.served_entities)
        print(f"  endpoint {ep_name} → v{version} (updated)")
    else:
        w.serving_endpoints.create(name=ep_name, config=cfg)
        print(f"  endpoint {ep_name} → v{version} (created)")


ensure_endpoint("underwriting-triage", "model_triage_priority", v_a)
ensure_endpoint("underwriting-risk", "model_risk_quality", v_b)
print(f"✅ 05 complete — triage AUC {auc_a:.3f} v{v_a} · risk AUC {auc_b:.3f} v{v_b} (endpoints reconciling; readiness checked in smoke)")
