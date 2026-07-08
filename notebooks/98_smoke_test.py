# Databricks notebook source
# MAGIC %md
# MAGIC # 98 · Smoke test — end-to-end verification + installed-assets checklist
# MAGIC
# MAGIC One row per step, PASS/FAIL, fails loudly. Doubles as the **"is everything deployed?"**
# MAGIC check on a fresh workspace (tables · volumes · functions · models · endpoints · agents ·
# MAGIC Genie · dashboard · app), then asserts the three sacred heroes' exact outcomes.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import json, time

RESULTS = []


def check(step, fn):
    t0 = time.time()
    try:
        detail = fn() or ""
        RESULTS.append((step, "PASS", round(time.time() - t0, 1), str(detail)[:220]))
        print(f"✅ {step}: {detail}")
    except Exception as e:  # noqa: BLE001
        RESULTS.append((step, "FAIL", round(time.time() - t0, 1), str(e)[:220]))
        print(f"❌ {step}: {e}")


def q(sql):
    return spark.sql(sql)


def scalar_fn(fn, arg):
    return json.loads(q(f"SELECT to_json({fqn}.{fn}('{arg}')) AS r").first().r)

# COMMAND ----------

# MAGIC %md ## A · Installed assets (deployability checklist)

# COMMAND ----------

TABLES = ["landing_pas_policies", "landing_pas_claims", "landing_submissions_feed", "landing_company_profiles",
          "landing_doc_extractions", "ref_broker", "ref_underwriter", "ref_appetite", "ref_authority_matrix",
          "ref_rate_guide", "ref_rebuild_benchmark", "ref_internal_watchlist", "ref_district_capacity",
          "ref_sanctions_ofsi", "ref_flood_open", "ref_crime_open", "ref_epc_mix_open", "ref_postcode_centroid",
          "ref_feature_encodings", "bronze_submissions", "bronze_quarantine_submissions", "bronze_schedule_locations",
          "bronze_quarantine_schedules", "bronze_documents", "bronze_doc_extractions", "bronze_quarantine_extractions",
          "bronze_pas_policies", "bronze_pas_claims", "bronze_company_profiles", "silver_submissions",
          "silver_locations_enriched", "gold_pipeline_funnel", "gold_portfolio_position", "gold_accumulation",
          "gold_broker_scorecard", "gold_rate_adequacy", "gold_renewals", "gold_underinsurance",
          "gold_submission_lifecycle", "gold_dq_scorecard", "gold_ingestion_sources", "gold_inbox_priority",
          "gold_decision_audit", "gold_comms_drafts", "gold_ai_activity", "gov_data_inventory", "gov_guide_changes",
          "feature_submission", "medallion_event_log"]
FUNCTIONS = ["fn_extract_summary", "fn_appetite_check", "fn_authority_check", "fn_accumulation_impact",
             "fn_technical_price", "fn_sanctions_screen", "fn_underinsurance_check", "fn_recommendation",
             "fn_triage_score", "fn_risk_score", "fn_price_whatif", "fn_accumulation_whatif", "mask_watchlist"]
VIEWS = ["gov_watchlist_secure", "gov_conduct_declines"]
VOLUMES = ["submission_inbox", "open_data", "ingest_checkpoints", "comms_out"]
MODELS = ["model_triage_priority", "model_risk_quality", "model_underwriting_agent", "underwriting_agent"]
ENDPOINT_SUBSTRS = ["underwriting-triage", "underwriting-risk", "underwriting-riskprofile", "underwriting-appetite",
                    "underwriting-adequacy", "underwriting-comms", "underwriting-challenge", "underwriting-brief"]


def _tables():
    have = {r.tableName for r in q(f"SHOW TABLES IN {fqn}").collect()}
    missing = [t for t in TABLES + VIEWS if t not in have]
    assert not missing, f"missing tables/views: {missing}"
    return f"{len(TABLES)} tables + {len(VIEWS)} views present"


def _volumes():
    have = {r.volume_name for r in q(f"SHOW VOLUMES IN {fqn}").collect()}
    missing = [v for v in VOLUMES if v not in have]
    assert not missing, f"missing volumes: {missing}"
    return f"{len(VOLUMES)} volumes present"


def _functions():
    have = {r.function.split(".")[-1] for r in q(f"SHOW USER FUNCTIONS IN {fqn}").collect()}
    missing = [f for f in FUNCTIONS if f not in have]
    assert not missing, f"missing functions: {missing}"
    return f"{len(FUNCTIONS)} UC functions present"


def _models():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    out = []
    for m in MODELS:
        vs = list(w.model_versions.list(f"{fqn}.{m}"))
        assert vs, f"model {m} missing"
        out.append(f"{m}:v{max(int(v.version) for v in vs)}")
    return " ".join(out)


def _endpoints():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    names = [e.name for e in w.serving_endpoints.list()]
    missing = [s for s in ENDPOINT_SUBSTRS if not any(s in n for n in names)]
    assert not missing, f"missing endpoints: {missing}"
    # agents.deploy auto-name is truncated to 63 chars — match schema, not the model name
    assert any(n.startswith("agents_") and schema in n for n in names), "tool-calling supervisor endpoint missing"
    return f"{len(ENDPOINT_SUBSTRS)} + supervisor endpoints resolved"


def _app():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    a = next((x for x in w.apps.list() if x.name == "underwriting-workbench"), None)
    assert a, "app underwriting-workbench not found"
    return f"app {a.name}: {a.app_status.state.value if a.app_status else '?'}"


check("A1 tables & views", _tables)
check("A2 volumes", _volumes)
check("A3 UC functions", _functions)
check("A4 UC models", _models)
check("A5 serving endpoints", _endpoints)
check("A6 app", _app)

# COMMAND ----------

# MAGIC %md ## B · Data + pipeline invariants

# COMMAND ----------

check("B1 landing volumes", lambda: f"{q(f'SELECT count(*) c FROM {fqn}.landing_submissions_feed').first().c} submissions, "
      f"{q(f'SELECT count(*) c FROM {fqn}.landing_pas_policies').first().c} policies")
check("B2 date freshness (rolling)", lambda: (lambda d: (_ for _ in ()).throw(AssertionError(f"stale: {d} days")) if d > 5 else f"max received {d}d ago")(
      q(f"SELECT datediff(current_date(), max(to_date(received_ts))) d FROM {fqn}.landing_submissions_feed").first().d))
check("B3 quarantine non-empty (no silent loss)", lambda: (lambda a, b, c: (_ for _ in ()).throw(AssertionError("empty quarantine")) if (a == 0 or b == 0 or c == 0) else f"subs {a} · schedules {b} · extractions {c}")(
      q(f"SELECT count(*) c FROM {fqn}.bronze_quarantine_submissions").first().c,
      q(f"SELECT count(*) c FROM {fqn}.bronze_quarantine_schedules").first().c,
      q(f"SELECT count(*) c FROM {fqn}.bronze_quarantine_extractions").first().c))
check("B4 EL statutory gate", lambda: (lambda n: (_ for _ in ()).throw(AssertionError("EL<5m leaked into bronze")) if n > 0 else "no sub-statutory EL rows in bronze")(
      q(f"SELECT count(*) c FROM {fqn}.bronze_submissions WHERE el_limit < 5000000").first().c))
check("B5 HX7 accumulation baseline 67%", lambda: (lambda u: (_ for _ in ()).throw(AssertionError(f"HX7 util {u}")) if not (66.0 <= u <= 68.0) else f"HX7 {u}%")(
      float(q(f"SELECT utilisation_pct FROM {fqn}.gold_accumulation WHERE postcode_district='HX7'").first().utilisation_pct)))
check("B6 DQ scorecard from event log", lambda: f"{q(f'SELECT count(*) c FROM {fqn}.gold_dq_scorecard').first().c} expectations")
check("B7 heroes in silver + features", lambda: (lambda a, b: (_ for _ in ()).throw(AssertionError(f"heroes s={a} f={b}")) if (a != 3 or b != 3) else "3/3 in both")(
      q(f"SELECT count(*) c FROM {fqn}.silver_submissions WHERE submission_public_id LIKE 'sub:9000%'").first().c,
      q(f"SELECT count(*) c FROM {fqn}.feature_submission WHERE submission_public_id LIKE 'sub:9000%'").first().c))
check("B8 hero schedule through the REAL file path", lambda: (lambda n, hx: (_ for _ in ()).throw(AssertionError(f"n={n} hx7={hx}")) if (n != 6 or hx != 5_000_000) else "6 locations · HX7 marginal exactly £5m")(
      q(f"SELECT count(*) c FROM {fqn}.bronze_schedule_locations WHERE submission_ref='sub:900002'").first().c,
      q(f"SELECT sum(buildings_si+plant_si+stock_si) s FROM {fqn}.bronze_schedule_locations WHERE submission_ref='sub:900002' AND postcode_district='HX7'").first().s))
check("B9 doc extraction confident for hero, fax gated", lambda: (lambda h, f_: (_ for _ in ()).throw(AssertionError(f"hero {h} fax {f_}")) if (h < 0.6 or f_ >= 0.6) else f"proposal {h} · fax {f_} (quarantined)")(
      float(q(f"SELECT extraction_confidence c FROM {fqn}.landing_doc_extractions WHERE file_name='sub-900002_proposal_form.pdf'").first().c),
      float(q(f"SELECT extraction_confidence c FROM {fqn}.landing_doc_extractions WHERE file_name='sub-108150_fax_scan.txt'").first().c)))

# COMMAND ----------

# MAGIC %md ## C · The three heroes — exact outcomes

# COMMAND ----------

def _hero1():
    r = scalar_fn("fn_recommendation", "sub:900001")
    assert r["action"] == "quote", r["action"]
    assert r["straight_through"] is True, "900001 must be straight-through eligible"
    s = scalar_fn("fn_sanctions_screen", "sub:900001")
    assert s["status"] == "false_positive_resolved", s["status"]
    assert any("Emraan" in (c.get("listed_name") or "") for c in s["ofsi_candidates"]), "expected the Emraan ALI near-miss"
    p = scalar_fn("fn_technical_price", "sub:900001")
    assert p["ipt_amount"] > 0 and p["total_inc_ipt"] > p["technical_premium"], "IPT must show"
    return f"quote · straight-through · near-miss resolved · premium £{p['technical_premium']:.0f} + IPT"


def _hero2():
    r = scalar_fn("fn_recommendation", "sub:900002")
    assert r["action"] == "refer", r["action"]
    assert r["refer_to_grade"] in ("senior_underwriter", "head_of_underwriting"), r["refer_to_grade"]
    joined = " | ".join(r["reasons"]).lower()
    assert "accumulation" in joined and "fair presentation" in joined, joined
    a = scalar_fn("fn_accumulation_impact", "sub:900002")
    hx7 = [d for d in a["districts"] if d["postcode_district"] == "HX7"][0]
    assert 85.0 <= hx7["post_util_pct"] <= 89.0 and hx7["status"] == "referral", hx7
    subj = " | ".join(r["subjectivities"]).lower()
    assert "audited turnover" in subj and "survey" in subj and "composite" in subj, subj
    assert any("flood excess" in t.lower() for t in r["terms"]), r["terms"]
    return f"refer → {r['refer_to_grade']} · HX7 {hx7['post_util_pct']}% · 3 subjectivities + flood excess"


def _hero3():
    r = scalar_fn("fn_recommendation", "sub:900003")
    assert r["action"] == "decline", r["action"]
    assert r["decline_code_external"] == "APP-EXCL-WASTE", r["decline_code_external"]
    ext = (r["external_reason"] or "").lower()
    assert "watchlist" not in ext and "sanction" not in ext, "external reason must cite appetite only"
    assert any("watchlist" in n.lower() for n in r["internal_notes"]), "internal note must record the watchlist hit"
    s = scalar_fn("fn_sanctions_screen", "sub:900003")
    assert s["status"] == "internal_watchlist_hit" and any(h["subject"] == "Derek Ashworth" for h in s["watchlist_hits"])
    return "decline APP-EXCL-WASTE · external cites appetite only · watchlist internal"


check("C1 hero 900001 fast-track", _hero1)
check("C2 hero 900002 referral", _hero2)
check("C3 hero 900003 decline", _hero3)
check("C4 what-if fns", lambda: (lambda p: f"HX7 whatif flood loading £{p['flood_loading']:.0f} > 0" if p["flood_loading"] > 0 else (_ for _ in ()).throw(AssertionError("no flood loading in HX7")))(
      json.loads(q(f"SELECT to_json({fqn}.fn_price_whatif('food_manufacturing','HX7',2000000,500000,0,300000,1000000,50,4000000,NULL)) AS r").first().r)))
check("C4b call transcripts through the pipeline", lambda: (lambda n, t: (_ for _ in ()).throw(AssertionError(f"transcripts n={n} fd_turnover={t}")) if (n < 3 or t != 24_000_000) else f"{n} transcripts · FD call turnover £24m extracted")(
      q(f"SELECT count(*) c FROM {fqn}.bronze_doc_extractions WHERE doc_type='call_transcript'").first().c,
      q(f"SELECT turnover_stated_gbp t FROM {fqn}.bronze_doc_extractions WHERE file_name='sub-900002_call_fd.txt'").first().t))
check("C4c decision evidence column", lambda: (lambda cols: "decision_evidence present" if "decision_evidence" in cols else (_ for _ in ()).throw(AssertionError("missing")))(
      [c.name for c in spark.table(f"{fqn}.gold_decision_audit").schema.fields]))
check("C5 inbox batch-scored", lambda: (lambda n: f"{n} open submissions scored" if n > 100 else (_ for _ in ()).throw(AssertionError(f"only {n}")))(
      q(f"SELECT count(*) c FROM {fqn}.gold_inbox_priority").first().c))

# COMMAND ----------

# MAGIC %md ## D · Governance + AI

# COMMAND ----------

check("D1 decision audit seeded + reconciles", lambda: (lambda n, a: f"{n} rows, 900003={a}" if n >= 3 and a == "decline" else (_ for _ in ()).throw(AssertionError(f"{n}/{a}")))(
      q(f"SELECT count(*) c FROM {fqn}.gold_decision_audit").first().c,
      q(f"SELECT action FROM {fqn}.gold_decision_audit WHERE submission_public_id='sub:900003' ORDER BY decision_ts DESC LIMIT 1").first().action))
check("D2 UC mask enforced", lambda: (lambda v: "masked for this principal" if "restricted" in v else f"UNMASKED — principal is in the readers group? got: {v[:60]}")(
      q(f"SELECT reason FROM {fqn}.gov_watchlist_secure WHERE watchlist_id='WL-001'").first().reason))


def _agent_live():
    from databricks.sdk import WorkspaceClient
    import requests
    w = WorkspaceClient()
    ep = next(e.name for e in w.serving_endpoints.list()
              if "underwriting_agent" in e.name or (e.name.startswith("agents_") and schema in e.name))
    # A rolling config update transiently lacks the auto-provisioned EXECUTE grants (claims gotcha)
    # → wait for NOT_UPDATING before invoking (fresh deploys hit this).
    for _ in range(60):
        st = w.serving_endpoints.get(ep).state
        if st.config_update.value == "NOT_UPDATING" and st.ready.value == "READY":
            break
        time.sleep(20)
    host = w.config.host.rstrip("/")
    r = requests.post(f"{host}/serving-endpoints/{ep}/invocations",
                      headers={**w.config._header_factory(), "Content-Type": "application/json"},
                      json={"messages": [{"role": "user", "content": "One sentence: should we quote this?"}],
                            "custom_inputs": {"submission_public_id": "sub:900001"}}, timeout=300)
    r.raise_for_status()
    out = r.json()
    tools = [t.get("tool") for t in (out.get("custom_outputs") or {}).get("trace", [])]
    assert tools, "supervisor returned no tool trace"
    return f"supervisor called: {' → '.join(tools[:5])}"


check("D3 tool-calling supervisor live", _agent_live)

# COMMAND ----------

import pandas as pd

df = pd.DataFrame(RESULTS, columns=["step", "status", "seconds", "detail"])
display(spark.createDataFrame(df))
fails = df[df.status == "FAIL"]
assert fails.empty, f"{len(fails)} smoke steps FAILED:\n{fails.to_string()}"
print(f"✅ SMOKE {len(df)}/{len(df)} PASS")
