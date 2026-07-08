# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Governance — audit, inventory, masking, conduct
# MAGIC
# MAGIC (1) `gold_decision_audit` — every quote/refer/decline/query recorded; hero rows seeded
# MAGIC FROM the live crux functions so the trail reconciles with what the app computes.
# MAGIC (2) `gov_data_inventory` — collect once, surface many ways by role (incl. open-data
# MAGIC provenance). (3) UC **dynamic masking** on watchlist reasons — enforced by Unity
# MAGIC Catalog, not the app (the app SP is outside the readers group and sees the mask).
# MAGIC (4) `gold_ai_activity` — the explainability record per subject. (5) `gold_comms_drafts`
# MAGIC — HITL letter audit shell. Lineage is queried live from `system.access.table_lineage`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import json

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Decision audit — seeded from the LIVE functions (reconciles by construction)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gold_decision_audit (
  decision_id STRING, submission_public_id STRING, action STRING, refer_to_grade STRING,
  suggested_underwriter STRING, reasons ARRAY<STRING>, terms ARRAY<STRING>,
  subjectivities ARRAY<STRING>, decline_code_external STRING, external_reason STRING,
  internal_notes ARRAY<STRING>, quoted_premium DOUBLE, straight_through BOOLEAN,
  decided_by STRING, decided_via STRING, decision_ts TIMESTAMP,
  decision_evidence STRING)
TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')
""")
# Reproducibility (the auditor's question: "show me exactly what the underwriter saw"):
# the app stores the full as-at panel JSON per decision. Migrate older installs in place.
if "decision_evidence" not in [c.name for c in spark.table(f"{fqn}.gold_decision_audit").schema.fields]:
    spark.sql(f"ALTER TABLE {fqn}.gold_decision_audit ADD COLUMN decision_evidence STRING")

# Idempotent hero seed: delete + re-derive from fn_recommendation + fn_technical_price
spark.sql(f"DELETE FROM {fqn}.gold_decision_audit WHERE decided_via = 'seed'")
spark.sql(f"""
INSERT INTO {fqn}.gold_decision_audit
SELECT concat('UW-', substr(sha2(sid, 256), 1, 10)), sid,
       r.action, r.refer_to_grade, r.suggested_underwriter, r.reasons, r.terms, r.subjectivities,
       r.decline_code_external, r.external_reason, r.internal_notes,
       p.technical_premium, r.straight_through,
       'system_recommendation', 'seed', current_timestamp(), CAST(NULL AS STRING)
FROM (SELECT explode(array('sub:900001', 'sub:900002', 'sub:900003')) AS sid) s
CROSS JOIN LATERAL (SELECT {fqn}.fn_recommendation(s.sid) AS r)
CROSS JOIN LATERAL (SELECT {fqn}.fn_technical_price(s.sid) AS p)
""")
print("decision audit seeded:", spark.table(f"{fqn}.gold_decision_audit").count(), "rows")

# COMMAND ----------

# MAGIC %md ## Comms drafts audit shell (HITL: drafted → approved; app appends)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gold_comms_drafts (
  draft_id STRING, submission_public_id STRING, letter_type STRING, content STRING,
  status STRING, drafted_by STRING, approved_by STRING, drafted_ts TIMESTAMP, approved_ts TIMESTAMP)
TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')
""")
print("gold_comms_drafts ready")

# COMMAND ----------

# MAGIC %md ## Guide-change audit — the appetite & rate committee's register
# MAGIC The underwriting guide is DATA here (ref_appetite / ref_rate_guide), so changing it is a
# MAGIC governed, attributable event with the projected impact captured at proposal time.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gov_guide_changes (
  proposal_id STRING, trade_group STRING, change_type STRING,
  current_value STRING, proposed_value STRING, rationale STRING,
  impact_json STRING, status STRING, proposed_by STRING,
  proposed_ts TIMESTAMP, applied_ts TIMESTAMP)
TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')
""")
print("gov_guide_changes ready")

# COMMAND ----------

# MAGIC %md ## Data inventory — collect once, surface many ways by role

# COMMAND ----------

INVENTORY = [
    # asset, tier, pii, retention, used_for, surfaced_as, audience
    ("bronze_submissions", "Internal", False, "7y", "Triage, pricing, funnel", "Inbox, Work-a-submission, funnel marts", "Underwriters, Head of UW"),
    ("bronze_documents / bronze_doc_extractions", "Confidential", True, "7y", "Document AI extraction", "Dossier document panel, Ingestion spotlight", "Underwriters"),
    ("bronze_pas_policies / bronze_pas_claims", "Internal", False, "10y", "Burning cost, accumulation, renewals", "Control Tower, rate adequacy, accumulation", "Head of UW, Underwriters"),
    ("bronze_company_profiles", "Public-derived", False, "refresh on submission", "Fair presentation check, screening subjects", "Dossier company panel", "Underwriters"),
    ("ref_sanctions_ofsi (REAL OFSI list)", "Public", False, "monthly refresh", "Point-of-quote screening", "Screening panel (resolutions logged)", "Underwriters, Compliance"),
    ("ref_internal_watchlist", "Restricted", True, "review annually", "Screening — INTERNAL ONLY", "Masked view gov_watchlist_secure; reasons never broker-facing", "Compliance, Senior UW"),
    ("ref_flood_open / ref_crime_open / ref_epc_mix_open", "Public", False, "quarterly refresh", "Flood banding, theft loading, MEES lens", "Enrichment panels, price build-up, One Book ESG", "Underwriters, Head of UW"),
    ("gold_decision_audit", "Confidential", False, "10y", "Conduct, consistency, regulator defence", "Governance decisions tab, Submission track", "Compliance, Audit, Head of UW"),
    ("gold_comms_drafts", "Confidential", True, "7y", "HITL letter approval trail", "Work-a-submission comms card", "Underwriters, Compliance"),
    ("cache_agent_responses", "Internal", False, "cleared on reset", "LLM narration cache (never decisions)", "AI mode toggle (cached/live)", "Demo operators"),
]
(spark.createDataFrame(INVENTORY,
  "asset string, sensitivity_tier string, contains_personal_data boolean, retention string, "
  "used_for string, surfaced_as string, audience string")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gov_data_inventory"))
spark.sql(f"ALTER TABLE {fqn}.gov_data_inventory SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")
print("gov_data_inventory:", len(INVENTORY), "assets")

# COMMAND ----------

# MAGIC %md ## UC dynamic masking — watchlist reasons (enforced by UC, not the app)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.mask_watchlist(v STRING)
RETURNS STRING
COMMENT 'Dynamic mask: watchlist detail visible only to the underwriting_conduct_readers group; everyone else (including the app service principal) sees a redaction marker enforced by Unity Catalog.'
RETURN CASE WHEN is_account_group_member('underwriting_conduct_readers') THEN v
            ELSE '*** restricted — underwriting_conduct_readers only ***' END
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gov_watchlist_secure
COMMENT 'Screening watchlist through the UC dynamic mask: names visible for matching transparency; reasons/sources masked outside the conduct-readers group. Proves governance is enforced in the platform, not the app.'
AS SELECT watchlist_id, name, subject_type,
          {fqn}.mask_watchlist(reason) AS reason,
          {fqn}.mask_watchlist(source) AS source
   FROM {fqn}.ref_internal_watchlist
""")
print("gov_watchlist_secure view created (UC-enforced mask)")

# COMMAND ----------

# MAGIC %md ## AI activity record — deterministic explainability log per hero

# COMMAND ----------

rows = []
for sid in ("sub:900001", "sub:900002", "sub:900003"):
    rec = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_recommendation('{sid}')) AS r").first().r)
    scr = json.loads(spark.sql(f"SELECT to_json({fqn}.fn_sanctions_screen('{sid}')) AS r").first().r)
    rows += [
        (sid, "rules_engine", "decision_recommendation",
         "fn_appetite_check, fn_authority_check, fn_accumulation_impact, fn_technical_price, fn_sanctions_screen, fn_underinsurance_check",
         rec["action"], "; ".join(rec["reasons"])[:900]),
        (sid, "screening", "sanctions_watchlist_screen", "fn_sanctions_screen (OFSI + internal watchlist)",
         scr["status"], (scr.get("guidance") or "")[:900]),
    ]
(spark.createDataFrame(rows, "submission_public_id string, agent string, activity string, tools_used string, signal string, reasoning string")
 .withColumn("recorded_at", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_ai_activity"))
spark.sql(f"ALTER TABLE {fqn}.gold_ai_activity SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench')")
print("gold_ai_activity:", len(rows), "rows")

# COMMAND ----------

# Conduct view: declinature consistency (coded reasons only, external vs internal separation)
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gov_conduct_declines
COMMENT 'Declinature consistency: every decline carries a coded external reason citing the underwriting guide; internal notes are separate. The FCA-conduct half of decline-with-dignity.'
AS SELECT decline_code_external, count(*) AS declines,
          array_join(collect_set(substr(external_reason, 1, 120)), ' | ') AS external_wordings,
          sum(CASE WHEN size(internal_notes) > 0 THEN 1 ELSE 0 END) AS with_internal_notes
   FROM {fqn}.gold_decision_audit WHERE action = 'decline'
   GROUP BY decline_code_external
""")
audit = spark.table(f"{fqn}.gold_decision_audit")
assert audit.filter("submission_public_id='sub:900003' AND action='decline'").count() == 1
h3 = audit.filter("submission_public_id='sub:900003'").first()
assert "watchlist" not in (h3.external_reason or "").lower(), "external decline reason must NEVER mention the watchlist"
assert any("watchlist" in (n or "").lower() for n in (h3.internal_notes or [])), "internal notes must carry the watchlist record"
print("✅ 07 governance complete — external/internal reason separation verified")
