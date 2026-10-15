# Databricks notebook source
# MAGIC %md
# MAGIC # 05f · Disaster-zone check at quote/bind (cross-workbench, additive)
# MAGIC
# MAGIC The **Exposure & Event Response** workbench publishes where catastrophes are happening right now
# MAGIC (`exposure_response.mv_active_zones`, read through its governed `fn_zone_check`). It ingests live
# MAGIC **Environment Agency flood warnings** for England, plus the European perils it tracks. This notebook wires that
# MAGIC published surface into underwriting:
# MAGIC
# MAGIC - **`fn_disaster_zone_check(postcode_district)`** — the district's centroid is checked against the active
# MAGIC   zones. Inside (within 250 m) → **REFER**, with a plain reason: *"Property is in an active Environment Agency
# MAGIC   Flood Warning area — River Calder and Hebden Water at Hebden Bridge, issued …; refer to underwriter."*
# MAGIC - **Referral Control rule `CAT_ACTIVE_ZONE`** — the control is registered in the governed rule registry
# MAGIC   (workflow scope, compliance-locked, effective today). It is added **after** the registry rebuild and the
# MAGIC   telemetry generation, so no existing rule's history or the heroes' outcomes change.
# MAGIC - The app runs the check for every location on a submission and blocks **Quote** while a location is in an
# MAGIC   active zone (Refer / Decline stay available; an override is recorded).
# MAGIC
# MAGIC Geocoding is at postcode-district level (`ref_postcode_centroid`) — the same grain the workbench already
# MAGIC uses for flood bands and accumulation. Additive: hero functions are not recreated.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("exposure_schema", "exposure_response")
dbutils.widgets.text("warehouse_id", "a3b61648ea4809e3")
dbutils.widgets.text("app_sp", "81589066-5ed3-49c3-8093-f723a711cc2d")
dbutils.widgets.text("buffer_m", "250")

import time, datetime
from databricks.sdk import WorkspaceClient

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
exp = f"{catalog}.{dbutils.widgets.get('exposure_schema')}"
warehouse_id = dbutils.widgets.get("warehouse_id")
app_sp = dbutils.widgets.get("app_sp").strip()
buffer_m = int(dbutils.widgets.get("buffer_m"))
fqn = f"{catalog}.{schema}"
_w = WorkspaceClient()


def run_wh(stmt, label="", max_wait=300):
    r = _w.statement_execution.execute_statement(
        statement=stmt, warehouse_id=warehouse_id, catalog=catalog, schema=schema, wait_timeout="50s")
    waited = 0
    while r.status.state.value in ("PENDING", "RUNNING") and waited < max_wait:
        time.sleep(3); waited += 3
        r = _w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"{label}: {r.status.state.value} — {r.status.error.message if r.status.error else '?'}")
    return r

# COMMAND ----------
# MAGIC %md ### The governed check — calls the Exposure workbench's published `fn_zone_check`

# COMMAND ----------

# Same rule as exposure_response.fn_zone_check (nearest active zone, true metres in EPSG:3035, within
# buffer_m), applied over the PUBLISHED surface mv_active_zones. (A SQL table function cannot pass per-row /
# parameter-correlated values into another table function, so the rule is applied over the view; the smoke
# check below proves both give the same answer.)
FN = f"""CREATE OR REPLACE FUNCTION {fqn}.fn_disaster_zone_check(p_postcode_district STRING)
RETURNS TABLE(postcode_district STRING, lat DOUBLE, lon DOUBLE, in_zone BOOLEAN, decision STRING, reason STRING,
              zone_id STRING, event_id STRING, area_name STRING, peril_code STRING, source STRING, severity STRING,
              is_live BOOLEAN, distance_m DOUBLE, active_from TIMESTAMP)
COMMENT 'Disaster-zone check at quote/bind: is the postcode district (centroid) inside an ACTIVE catastrophe zone published by the Exposure & Event Response workbench (live Environment Agency flood warnings + tracked European perils)? Within {buffer_m} m -> REFER with a plain reason; otherwise CLEAR. Same rule as exposure_response.fn_zone_check, over exposure_response.mv_active_zones.'
RETURN WITH c AS (
  SELECT postcode_district AS pd, lat, lon FROM {fqn}.ref_postcode_centroid
  WHERE postcode_district = upper(trim(p_postcode_district))),
zz AS (
  SELECT c.pd, c.lat, c.lon, a.zone_id, a.event_id, a.peril_code, a.source, a.severity, a.is_live, a.active_from,
         coalesce(nullif(regexp_extract(a.source_detail, 'area: ([^·]+) ·', 1), ''), a.event_name) AS area_name,
         ST_Distance(ST_Transform(ST_SetSRID(ST_Point(c.lon, c.lat), 4326), 3035),
                     ST_Transform(ST_SetSRID(ST_GeomFromText(a.footprint_wkt), 4326), 3035)) AS d
  FROM c CROSS JOIN {exp}.mv_active_zones a),
best AS (SELECT * FROM zz ORDER BY d ASC LIMIT 1),
one AS (SELECT upper(trim(p_postcode_district)) AS pd)
SELECT one.pd, c.lat, c.lon,
       coalesce(best.d <= {buffer_m}, false) AS in_zone,
       CASE WHEN c.lat IS NULL THEN 'UNKNOWN' WHEN best.d <= {buffer_m} THEN 'REFER' ELSE 'CLEAR' END AS decision,
       CASE WHEN c.lat IS NULL THEN concat('Postcode district ', one.pd, ' is not geocoded — check manually.')
            WHEN best.d <= {buffer_m} THEN concat('Property is in an active ',
                 CASE WHEN best.source = 'EA' THEN 'Environment Agency ' ELSE '' END, best.severity, ' area — ',
                 trim(best.area_name), ', issued ', date_format(best.active_from, 'd MMM yyyy HH:mm'), ' UTC',
                 CASE WHEN best.is_live THEN '' ELSE ' (frozen demo sample)' END,
                 '; refer to underwriter. Cover cannot be bound inside an active warning area.')
            ELSE concat('No active catastrophe zone within {buffer_m} m (nearest active zone ',
                 coalesce(round(best.d / 1000, 1), 0), ' km away).') END AS reason,
       best.zone_id, best.event_id, trim(best.area_name), best.peril_code, best.source, best.severity, best.is_live,
       best.d AS distance_m, best.active_from
FROM one LEFT JOIN c ON c.pd = one.pd LEFT JOIN best ON true"""
run_wh(FN, "fn_disaster_zone_check")
print("fn_disaster_zone_check ready")

# COMMAND ----------
# MAGIC %md ### Register the control in Referral Control (additive — after registry + telemetry rebuild)

# COMMAND ----------

today = datetime.date.today().isoformat()
RULE = f"""MERGE INTO {fqn}.ref_referral_rules t
USING (SELECT 'CAT_ACTIVE_ZONE' rule_id, 'Property in an active catastrophe zone' rule_name,
              'risk_selection' category,
              'A risk location sits inside an ACTIVE catastrophe zone published by the Exposure & Event Response workbench (e.g. a live Environment Agency flood warning area). Cover cannot be bound while the warning is in force: refer to a senior underwriter. Fires live at quote/bind via fn_disaster_zone_check.' description,
              '{{"default": {buffer_m}, "unit_note": "metres from the active zone"}}' threshold_config,
              'refer' disposition, true compliance_lock, 'workflow' rule_scope,
              'fn_disaster_zone_check' source_check, 'metres' unit, CAST(0.3 AS DOUBLE) review_effort_hours,
              'v1' rule_version, DATE'{today}' valid_from, CAST(NULL AS DATE) valid_to,
              'underwriting_governance_committee' approved_by, 'chg-cat-active-zone' change_id,
              true is_current, '{today}' effective_from) s
ON t.rule_id = s.rule_id AND t.valid_to IS NULL
WHEN MATCHED THEN UPDATE SET description = s.description, threshold_config = s.threshold_config,
  source_check = s.source_check
WHEN NOT MATCHED THEN INSERT *"""
run_wh(RULE, "register CAT_ACTIVE_ZONE")
r = run_wh(f"SELECT count(*) FROM {fqn}.ref_referral_rules WHERE valid_to IS NULL", "count")
print("current rules:", r.result.data_array[0][0])

# COMMAND ----------
# MAGIC %md ### Grants — UC SQL functions run with invoker rights

# COMMAND ----------

grantees = ["`account users`"] + ([f"`{app_sp}`"] if app_sp else [])
for who in grantees:
    for stmt in (
        f"GRANT EXECUTE ON FUNCTION {fqn}.fn_disaster_zone_check TO {who}",
        f"GRANT SELECT ON TABLE {fqn}.ref_postcode_centroid TO {who}",
        f"GRANT USE SCHEMA ON SCHEMA {exp} TO {who}",
        f"GRANT EXECUTE ON FUNCTION {exp}.fn_zone_check TO {who}",
        f"GRANT SELECT ON VIEW {exp}.mv_active_zones TO {who}",
        f"GRANT SELECT ON TABLE {exp}.`2_event_footprint` TO {who}",
    ):
        run_wh(stmt, f"grant {who}")
print("grants applied:", grantees)

# COMMAND ----------
# MAGIC %md ### Verify — the Calder Valley hero district vs a clear one

# COMMAND ----------

for pd in ("HX7", "WA14"):
    r = run_wh(f"SELECT decision, reason, zone_id, round(distance_m) FROM {fqn}.fn_disaster_zone_check('{pd}')", pd)
    mine = r.result.data_array[0]
    r2 = run_wh(f"""SELECT in_zone, zone_id, round(distance_m) FROM {exp}.fn_zone_check(
        (SELECT lat FROM {fqn}.ref_postcode_centroid WHERE postcode_district='{pd}'),
        (SELECT lon FROM {fqn}.ref_postcode_centroid WHERE postcode_district='{pd}'), {buffer_m})""", pd + " parity")
    ref = r2.result.data_array[0]
    assert (mine[0] == "REFER") == (ref[0] == "true") and mine[2] == ref[1] and mine[3] == ref[2], (mine, ref)
    print(pd, mine[0], "|", mine[1], "| parity with fn_zone_check OK")
