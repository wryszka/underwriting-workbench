#!/usr/bin/env python3
"""Dev helper: apply the 05b/05c CREATE FUNCTION statements straight to the warehouse and
smoke the heroes — a fast iteration loop for crux SQL without a job round-trip.
Usage: python3 scripts/dev_apply_crux.py [profile] [warehouse] [catalog] [schema]"""
import json
import re
import subprocess
import sys

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
wh = sys.argv[2] if len(sys.argv) > 2 else "a3b61648ea4809e3"
cat = sys.argv[3] if len(sys.argv) > 3 else "lr_dev_aws_us_catalog"
sch = sys.argv[4] if len(sys.argv) > 4 else "underwriting_workbench"
fqn = f"{cat}.{sch}"


def run_sql(stmt):
    payload = json.dumps({"warehouse_id": wh, "statement": stmt, "wait_timeout": "50s"})
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "-p", prof,
                          "--json", payload], capture_output=True, text=True)
    d = json.loads(out.stdout or "{}")
    state = ((d.get("status") or {}).get("state"))
    if state != "SUCCEEDED":
        err = ((d.get("status") or {}).get("error") or {}).get("message", out.stderr)
        raise RuntimeError(err[:500])
    return d


def extract_creates(path):
    src = open(path).read()
    return [m.format(F=fqn) for m in re.findall(r'create_fn\("""(.*?)"""\)', src, re.S)] or \
           [m.format(fqn=fqn) for m in re.findall(r'spark\.sql\(f"""(CREATE OR REPLACE FUNCTION.*?)"""\)', src, re.S)]


for path in ("notebooks/05b_crux.py", "notebooks/05c_whatif.py"):
    for stmt in extract_creates(path):
        name = stmt.split("FUNCTION")[1].split("(")[0].strip()
        try:
            run_sql(stmt)
            print("✓", name)
        except Exception as e:  # noqa: BLE001
            print("✗", name, "→", e)
            sys.exit(1)

for sid, want in (("sub:900001", "quote"), ("sub:900002", "refer"), ("sub:900003", "decline")):
    d = run_sql(f"SELECT to_json({fqn}.fn_recommendation('{sid}')) AS r")
    r = json.loads(d["result"]["data_array"][0][0])
    ok = "✓" if r["action"] == want else "✗"
    print(f"{ok} {sid} → {r['action']} (want {want}) · reasons: {r['reasons'][:2]}")
    if r["action"] != want:
        print(json.dumps(r, indent=1)[:1200])
        sys.exit(1)
print("heroes OK")
