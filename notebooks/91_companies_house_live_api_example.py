# Databricks notebook source
# MAGIC %md
# MAGIC # 91 · Companies House — LIVE API example (off the critical path)
# MAGIC
# MAGIC The demo's company profiles are synthetic + bundled (reliability by design: the demo
# MAGIC never depends on an external API at show time). This notebook demonstrates that the
# MAGIC **real integration is a small step**: the same profile shape fetched live from the
# MAGIC Companies House Public Data API.
# MAGIC
# MAGIC Needs a free API key from https://developer.company-information.service.gov.uk —
# MAGIC store it as a Databricks secret (`ch_api/key`) and pass the scope/key names as widgets.
# MAGIC Nothing in the workbench reads this notebook's output; it writes to a `_live_demo`
# MAGIC suffixed table only.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("secret_scope", "ch_api")
dbutils.widgets.text("secret_key", "key")
dbutils.widgets.text("company_numbers", "00000006,01471587")  # any real numbers to demo
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import json

import requests

try:
    API_KEY = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key"))
except Exception:
    API_KEY = None
    print("No API key secret found — notebook will print the request shape and exit. "
          "Create a free key + secret to run live.")

# COMMAND ----------

def fetch_profile(company_number: str):
    """GET /company/{number} + /company/{number}/officers — the live twin of landing_company_profiles."""
    base = "https://api.company-information.service.gov.uk"
    auth = (API_KEY, "")
    prof = requests.get(f"{base}/company/{company_number}", auth=auth, timeout=30).json()
    officers = requests.get(f"{base}/company/{company_number}/officers", auth=auth, timeout=30).json()
    return {
        "company_number": prof.get("company_number"),
        "company_name": prof.get("company_name"),
        "incorporation_date": prof.get("date_of_creation"),
        "sic_code": (prof.get("sic_codes") or [None])[0],
        "company_status": prof.get("company_status"),
        "accounts_overdue": bool((prof.get("accounts") or {}).get("overdue")),
        "directors_json": json.dumps([o.get("name") for o in (officers.get("items") or [])
                                      if "director" in (o.get("officer_role") or "")]),
        "registered_postcode": (prof.get("registered_office_address") or {}).get("postal_code"),
    }


if API_KEY:
    rows = [fetch_profile(n.strip()) for n in dbutils.widgets.get("company_numbers").split(",") if n.strip()]
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.company_profiles_live_demo")
    display(df)
    print(f"✅ live profiles written to {fqn}.company_profiles_live_demo — same shape as landing_company_profiles")
else:
    print("Request shape: GET https://api.company-information.service.gov.uk/company/{number} (basic auth, key as user)")
    print("Fields consumed: company_name, date_of_creation, sic_codes[0], company_status, accounts.overdue, officers")
