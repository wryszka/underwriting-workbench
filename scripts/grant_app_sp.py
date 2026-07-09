#!/usr/bin/env python3
"""Grant the app service principal everything it needs — one command after (re)creating the app.

Usage: python3 scripts/grant_app_sp.py [profile] [catalog] [schema] [warehouse_id] [genie_space_id] [dashboard_id]
Auto-discovers the app SP id from the app, all underwriting serving endpoints, and the reset job.
Idempotent. This is the single source for the grant list (mirrored in docs/DEPLOY.md §8).
"""
import sys

from databricks.sdk import WorkspaceClient

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
cat = sys.argv[2] if len(sys.argv) > 2 else "lr_dev_aws_us_catalog"
sch = sys.argv[3] if len(sys.argv) > 3 else "underwriting_workbench"
wh = sys.argv[4] if len(sys.argv) > 4 else "a3b61648ea4809e3"
genie = sys.argv[5] if len(sys.argv) > 5 else "01f17afafb6d1309bcff27506395be54"
dash = sys.argv[6] if len(sys.argv) > 6 else "01f17afbb9bc192f993f9a26d2343bdb"

w = WorkspaceClient(profile=prof)
app = w.apps.get("underwriting-workbench")
sp = app.service_principal_client_id
print(f"app SP: {sp}")


def sql(stmt):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh, wait_timeout="50s")
    st = r.status.state.value if r.status else "?"
    print(("✓" if st == "SUCCEEDED" else f"✗ {st}"), stmt[:90])


sql(f"GRANT USE CATALOG ON CATALOG {cat} TO `{sp}`")
sql(f"GRANT USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE ON SCHEMA {cat}.{sch} TO `{sp}`")
sql(f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {cat}.{sch}.comms_out TO `{sp}`")

eps = [e for e in w.serving_endpoints.list()
       if "underwriting" in e.name or (e.name.startswith("agents_") and sch in e.name)]
for e in eps:
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/serving-endpoints/{w.serving_endpoints.get(e.name).id}",
                        body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_QUERY"}]})
        print("✓ CAN_QUERY", e.name)
    except Exception as ex:  # noqa: BLE001
        print("✗", e.name, str(ex)[:80])

job = next((j for j in w.jobs.list(limit=100) if "underwriting_99_reset" in (j.settings.name or "")), None)
if job:
    w.api_client.do("PATCH", f"/api/2.0/permissions/jobs/{job.job_id}",
                    body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_MANAGE_RUN"}]})
    print("✓ CAN_MANAGE_RUN reset job", job.job_id)

for obj, oid, lvl in (("genie", genie, "CAN_RUN"), ("dashboards", dash, "CAN_READ")):
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/{obj}/{oid}",
                        body={"access_control_list": [{"service_principal_name": sp, "permission_level": lvl}]})
        print(f"✓ {lvl} {obj} {oid}")
    except Exception as ex:  # noqa: BLE001
        print("✗", obj, str(ex)[:80])
print("done — restart the app once so endpoint-name caches refresh")
