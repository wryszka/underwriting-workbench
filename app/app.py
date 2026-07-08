"""Underwriting Workbench — thin FastAPI backend. Presentation only: every panel calls a
real UC function / serving endpoint / Genie / SQL. No underwriting logic lives here."""
import json
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse

from server import agents, config, sql

app = FastAPI(title="Underwriting Workbench — Bricksurance SE")

F = config.fqn


def _struct(fn, arg):
    row = sql.query_one(f"SELECT to_json({F(fn)}('{sql.esc(arg)}')) AS r")
    return json.loads(row["r"]) if row and row.get("r") else {}


def _cache_flag(cache):
    return config.USE_CACHE if cache is None else bool(int(cache))


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/api/config")
def get_config():
    host = config.workspace_host()
    return {
        "catalog": config.CATALOG, "schema": config.SCHEMA, "entity": "Bricksurance SE",
        "use_cache": config.USE_CACHE, "workspace_host": host,
        "genie_space_id": config.GENIE_SPACE_ID,
        "genie_embed_url": f"{host}/embed/genie/rooms/{config.GENIE_SPACE_ID}" if config.GENIE_SPACE_ID else "",
        "dashboard_embed_url": f"{host}/embed/dashboardsv3/{config.DASHBOARD_ID}" if config.DASHBOARD_ID else "",
        "pricing_workbench_url": "https://github.com/wryszka/pricing-workbench",
        "supervisor_endpoint": config.resolve_endpoint(config.EP_AGENT_SUBSTR),
    }


# ---------------------------------------------------------------- control tower
@app.get("/api/control-tower")
def control_tower():
    q = sql.query_many({
        "gwp": f"""SELECT sum(gwp) gwp, sum(plan_gwp) plan_gwp, sum(policies) policies,
                          round(sum(incurred_3y)/3/sum(gwp)*100,1) loss_ratio
                   FROM {F('gold_portfolio_position')}""",
        "retention": f"""SELECT round(sum(in_force)/(sum(in_force)+sum(lapsed))*100,1) retention_pct,
                                round(avg(avg_rate_change_pct),1) rate_change_pct
                         FROM {F('gold_renewals')}""",
        "funnel": f"""SELECT channel, sum(received) received, sum(quoted) quoted, sum(bound) bound,
                             round(avg(avg_hours_to_quote),1) avg_hours_to_quote
                      FROM {F('gold_pipeline_funnel')} GROUP BY channel ORDER BY channel""",
        "accum": f"""SELECT rag, count(*) districts FROM {F('gold_accumulation')} GROUP BY rag""",
        "accum_hot": f"""SELECT postcode_district, utilisation_pct, rag, flood_band
                         FROM {F('gold_accumulation')} ORDER BY utilisation_pct DESC LIMIT 5""",
        "adequacy": f"""SELECT round(avg(adequacy_pct),1) adequacy_pct FROM {F('gold_rate_adequacy')}""",
        "pipeline": f"""SELECT count(*) open_subs,
                               sum(CASE WHEN sla_status='breached' THEN 1 ELSE 0 END) sla_breached,
                               sum(CASE WHEN lifecycle_state='referred' THEN 1 ELSE 0 END) referred,
                               sum(total_si) open_si
                        FROM {F('gold_submission_lifecycle')}""",
        "meta": f"""SELECT current_timestamp() queried_at,
                           (SELECT max(scored_at) FROM {F('gold_inbox_priority')}) scored_at""",
    })
    return {k: (v[0] if k not in ("funnel", "accum", "accum_hot") else v) for k, v in q.items()}


DRILLS = {
    "gwp": f"SELECT trade_group, policies, gwp, plan_gwp, loss_ratio_3y_pct, avg_rate_change_pct, appetite_status FROM {{t}} ORDER BY gwp DESC",
    "retention": "SELECT month, sum(in_force) in_force, sum(lapsed) lapsed, round(sum(in_force)/(sum(in_force)+sum(lapsed))*100,1) retention_pct, round(avg(avg_rate_change_pct),1) rate_change_pct FROM {t} GROUP BY month ORDER BY month DESC LIMIT 14",
    "accumulation": "SELECT postcode_district, in_force_property_si, property_capacity_gbp, utilisation_pct, rag, flood_band, rivers FROM {t} ORDER BY utilisation_pct DESC",
    "adequacy": "SELECT trade_group, quotes_12m, adequacy_pct, avg_technical_premium, avg_quoted_premium, loss_ratio_3y_pct, renewal_rate_change_pct FROM {t} ORDER BY adequacy_pct ASC",
    "funnel": "SELECT month, channel, received, quoted, bound, declined, ntu, lost, quote_expired, avg_hours_to_quote FROM {t} ORDER BY month DESC, channel LIMIT 36",
}
DRILL_TABLE = {"gwp": "gold_portfolio_position", "retention": "gold_renewals",
               "accumulation": "gold_accumulation", "adequacy": "gold_rate_adequacy",
               "funnel": "gold_pipeline_funnel"}


@app.get("/api/control-tower/drill")
def ct_drill(key: str):
    if key not in DRILLS:
        return JSONResponse({"error": "unknown drill"}, status_code=400)
    stmt = DRILLS[key].format(t=F(DRILL_TABLE[key]))
    return {"rows": sql.query(stmt), "sql": stmt, "table": F(DRILL_TABLE[key])}


# ---------------------------------------------------------------- inbox
@app.get("/api/inbox")
def inbox():
    rows = sql.query(f"""
        SELECT l.*, p.bind_propensity_pct, p.large_loss_propensity_pct
        FROM {F('gold_submission_lifecycle')} l
        LEFT JOIN {F('gold_inbox_priority')} p USING (submission_public_id)
        ORDER BY CASE WHEN l.submission_public_id LIKE 'sub:9000%' THEN 0 ELSE 1 END,
                 p.bind_propensity_pct DESC NULLS LAST
        LIMIT 120""")
    return {"rows": rows, "note": "priority = batch-scored bind propensity (model_triage_priority); heroes pinned"}


# ---------------------------------------------------------------- work a submission
PANEL_FNS = {"dossier": "fn_extract_summary", "appetite": "fn_appetite_check",
             "authority": "fn_authority_check", "accumulation": "fn_accumulation_impact",
             "price": "fn_technical_price", "screening": "fn_sanctions_screen",
             "underinsurance": "fn_underinsurance_check", "recommendation": "fn_recommendation"}


@app.get("/api/submission/{sid:path}/panels")
def submission_panels(sid: str):
    sid = sql.esc(sid)
    stmts = {k: f"SELECT to_json({F(fn)}('{sid}')) AS r" for k, fn in PANEL_FNS.items()}
    stmts["scores"] = f"""SELECT bind_propensity_pct, large_loss_propensity_pct
                          FROM {F('gold_inbox_priority')} WHERE submission_public_id='{sid}'"""
    stmts["locations"] = f"""SELECT loc_no, site_name, postcode_district, site_type, construction_type,
                                    year_built, floor_area_m2, buildings_si, plant_si, stock_si, property_si,
                                    flood_band, rivers, crime_count, crime_imputed, lat, lon
                             FROM {F('silver_locations_enriched')} WHERE submission_public_id='{sid}' ORDER BY loc_no"""
    stmts["documents"] = f"""SELECT file_name, doc_type, extraction_confidence, key_hazards_json,
                                    prior_losses_json, turnover_stated_gbp
                             FROM {F('bronze_doc_extractions')} WHERE submission_public_id='{sid}'"""
    out = sql.query_many(stmts)
    res = {k: (json.loads(out[k][0]["r"]) if out.get(k) and out[k][0].get("r") else {}) for k in PANEL_FNS}
    res["scores"] = out["scores"][0] if out.get("scores") else {}
    res["locations"] = out.get("locations", [])
    res["documents"] = out.get("documents", [])
    res["fns"] = {k: f"{F(v)}('{sid}')" for k, v in PANEL_FNS.items()}
    return res


@app.get("/api/submission/{sid:path}/narrate")
def submission_narrate(sid: str, role: str = "challenge", cache: int = None):
    data = {k: _struct(fn, sid) for k, fn in
            (("recommendation", "fn_recommendation"), ("price", "fn_technical_price"),
             ("dossier", "fn_extract_summary"))}
    q = {"risk_profile": "Write the risk profile for this submission.",
         "appetite": "Explain the appetite and portfolio fit for this submission.",
         "pricing_adequacy": "Is the broker target achievable? Where is the negotiation room?",
         "challenge": "Second opinion: argue the other side of this recommendation."}.get(role, "Comment.")
    if role in ("risk_profile",):
        data["screening"] = _struct("fn_sanctions_screen", sid)
    return agents.narrate(role, f"{q} ({sid})", data, use_cache=_cache_flag(cache))


@app.get("/api/submission/{sid:path}/comms")
def submission_comms(sid: str, letter_type: str = "quote", cache: int = None):
    data = {"recommendation": _struct("fn_recommendation", sid),
            "price": _struct("fn_technical_price", sid),
            "dossier": _struct("fn_extract_summary", sid)}
    r = agents.narrate("broker_comms", f"Draft the {letter_type} letter for {sid}.", data,
                       use_cache=_cache_flag(cache))
    return r


@app.post("/api/comms/record")
async def comms_record(req: Request):
    b = await req.json()
    did = "CD-" + uuid.uuid4().hex[:10]
    approver = req.headers.get("x-forwarded-email", "demo-user")
    sql.query(f"""INSERT INTO {F('gold_comms_drafts')} VALUES (
        '{did}', '{sql.esc(b.get("sid", ""))}', '{sql.esc(b.get("letter_type", ""))}',
        '{sql.esc(b.get("content", "")[:12000])}', '{sql.esc(b.get("status", "drafted"))}',
        'broker_comms_agent', '{sql.esc(approver)}', current_timestamp(),
        {"current_timestamp()" if b.get("status") == "approved" else "NULL"})""")
    return {"draft_id": did, "status": b.get("status", "drafted"), "approver": approver}


@app.post("/api/decision")
async def decision(req: Request):
    b = await req.json()
    sid = sql.esc(b.get("sid", ""))
    who = sql.esc(req.headers.get("x-forwarded-email", "demo-user"))
    did = "UW-" + uuid.uuid4().hex[:10]

    def arr(key):
        return "array(" + ",".join(f"'{sql.esc(x)}'" for x in b.get(key, [])) + ")"

    sql.query(f"""INSERT INTO {F('gold_decision_audit')} VALUES (
        '{did}', '{sid}', '{sql.esc(b.get("action", ""))}', {f"'{sql.esc(b['refer_to'])}'" if b.get("refer_to") else "NULL"},
        {f"'{sql.esc(b['underwriter'])}'" if b.get("underwriter") else "NULL"},
        {arr("reasons")}, {arr("terms")}, {arr("subjectivities")},
        {f"'{sql.esc(b['decline_code'])}'" if b.get("decline_code") else "NULL"},
        {f"'{sql.esc(b['external_reason'])}'" if b.get("external_reason") else "NULL"},
        {arr("internal_notes")}, {b.get("quoted_premium") or "NULL"},
        {str(bool(b.get("straight_through"))).lower()}, '{who}', 'app', current_timestamp())""")
    return {"decision_id": did, "recorded_by": who}


@app.get("/api/decisions")
def decisions(sid: str = None):
    where = f"WHERE submission_public_id='{sql.esc(sid)}'" if sid else ""
    return {"rows": sql.query(f"""SELECT * FROM {F('gold_decision_audit')} {where}
                                  ORDER BY decision_ts DESC LIMIT 50""")}


# ---------------------------------------------------------------- try a submission (what-if)
@app.post("/api/whatif")
async def whatif(req: Request):
    b = await req.json()
    t = sql.esc(b.get("trade_group", "retail_shop"))
    d = sql.esc(b.get("postcode_district", "M1"))
    nums = {k: int(b.get(k) or 0) for k in
            ("buildings_si", "plant_si", "contents_si", "stock_si", "bi_si", "employees", "turnover")}
    target = int(b["target_premium"]) if b.get("target_premium") else "NULL"
    q = sql.query_many({
        "price": f"""SELECT to_json({F('fn_price_whatif')}('{t}','{d}',{nums['buildings_si']},{nums['plant_si']},
                     {nums['contents_si']},{nums['stock_si']},{nums['bi_si']},{nums['employees']},
                     {nums['turnover']},{target})) AS r""",
        "accum": f"""SELECT to_json({F('fn_accumulation_whatif')}('{d}',
                     {nums['buildings_si'] + nums['plant_si'] + nums['contents_si'] + nums['stock_si']})) AS r""",
    })
    return {k: (json.loads(v[0]["r"]) if v and v[0].get("r") else {}) for k, v in q.items()}


@app.get("/api/whatif/options")
def whatif_options():
    q = sql.query_many({
        "trades": f"SELECT trade_group, appetite_status, hazard_grade FROM {F('ref_appetite')} ORDER BY trade_group",
        "districts": f"SELECT postcode_district, flood_band FROM {F('ref_flood_open')} ORDER BY postcode_district",
    })
    return q


# ---------------------------------------------------------------- supervisor + genie
@app.post("/api/agent/ask")
async def agent_ask(req: Request, cache: int = None):
    b = await req.json()
    ci = {}
    if b.get("sid"):
        ci["submission_public_id"] = b["sid"]
    return agents.ask_agent(b.get("question", ""), ci, use_cache=_cache_flag(cache))


@app.post("/api/genie/ask")
async def genie_ask(req: Request):
    b = await req.json()
    if not config.GENIE_SPACE_ID:
        return {"error": "Genie space not configured"}
    try:
        w = config.get_workspace_client()
        m = w.genie.start_conversation_and_wait(space_id=config.GENIE_SPACE_ID, content=b.get("question", ""))
        out = {"answer": None, "query": None, "rows": []}
        for att in (m.attachments or []):
            if att.text and att.text.content:
                out["answer"] = att.text.content
            if att.query and att.query.query:
                out["query"] = att.query.query
                try:
                    qr = w.genie.get_message_attachment_query_result(
                        config.GENIE_SPACE_ID, m.conversation_id, m.message_id, att.attachment_id)
                    st = qr.statement_response
                    if st and st.manifest and st.result:
                        cols = [c.name for c in st.manifest.schema.columns]
                        out["rows"] = [dict(zip(cols, r)) for r in (st.result.data_array or [])][:20]
                except Exception:
                    pass
        return out
    except Exception as e:
        return {"error": f"genie unavailable: {str(e)[:180]}"}


@app.get("/api/genie/examples")
def genie_examples():
    return {"examples": [
        "Which channel converts fastest and what share is straight-through?",
        "Which districts are over 80% of property capacity?",
        "Rank brokers by hit ratio and data quality",
        "Where is quoted premium furthest below technical?",
        "What is retention and rate change by month this year?"]}


# ---------------------------------------------------------------- ingestion
@app.get("/api/ingestion/assets")
def ingestion_assets():
    q = sql.query_many({
        "sources": f"SELECT * FROM {F('gold_ingestion_sources')} ORDER BY status, source_group, source",
        "scorecard": f"SELECT * FROM {F('gold_dq_scorecard')} ORDER BY dataset, expectation",
        "quarantine": f"""SELECT 'submissions' src, quarantine_reason, count(*) n FROM {F('bronze_quarantine_submissions')} GROUP BY 2
                          UNION ALL SELECT 'schedules', quarantine_reason, count(*) FROM {F('bronze_quarantine_schedules')} GROUP BY 2
                          UNION ALL SELECT 'extractions', quarantine_reason, count(*) FROM {F('bronze_quarantine_extractions')} GROUP BY 2""",
        "docs": f"""SELECT file_name, doc_type, submission_public_id, extraction_confidence, source_tool
                    FROM {F('bronze_doc_extractions')} ORDER BY file_name""",
    })
    return q


@app.get("/api/ingestion/quarantine")
def ingestion_quarantine(src: str = "schedules"):
    tbl = {"schedules": "bronze_quarantine_schedules", "submissions": "bronze_quarantine_submissions",
           "extractions": "bronze_quarantine_extractions"}.get(src)
    if not tbl:
        return JSONResponse({"error": "unknown quarantine"}, status_code=400)
    return {"rows": sql.query(f"SELECT * FROM {F(tbl)} LIMIT 25"), "table": F(tbl)}


@app.get("/api/ingestion/sample")
def ingestion_sample(table: str):
    allowed = {"bronze_submissions", "bronze_schedule_locations", "bronze_documents", "bronze_doc_extractions",
               "bronze_pas_policies", "bronze_pas_claims", "bronze_company_profiles", "ref_sanctions_ofsi",
               "ref_flood_open", "ref_crime_open", "ref_epc_mix_open"}
    if table not in allowed:
        return JSONResponse({"error": "table not inspectable"}, status_code=400)
    return {"rows": sql.query(f"SELECT * FROM {F(table)} LIMIT 8"), "table": F(table)}


# ---------------------------------------------------------------- governance
@app.get("/api/governance/inventory")
def gov_inventory():
    return {"rows": sql.query(f"SELECT * FROM {F('gov_data_inventory')} ORDER BY sensitivity_tier, asset")}


@app.get("/api/governance/decisions")
def gov_decisions():
    q = sql.query_many({
        "audit": f"SELECT * FROM {F('gold_decision_audit')} ORDER BY decision_ts DESC LIMIT 40",
        "conduct": f"SELECT * FROM {F('gov_conduct_declines')}",
        "comms": f"SELECT draft_id, submission_public_id, letter_type, status, approved_by, drafted_ts FROM {F('gold_comms_drafts')} ORDER BY drafted_ts DESC LIMIT 20",
    })
    return q


@app.get("/api/governance/masking")
def gov_masking():
    return {"rows": sql.query(f"SELECT * FROM {F('gov_watchlist_secure')}"),
            "note": "This app's service principal is OUTSIDE underwriting_conduct_readers — the redaction you see is enforced by Unity Catalog, not the app.",
            "view": F("gov_watchlist_secure")}


@app.get("/api/governance/models")
def gov_models():
    w = config.get_workspace_client()
    out = []
    for m in ("model_triage_priority", "model_risk_quality", "model_underwriting_agent", "underwriting_agent"):
        full = f"{config.CATALOG}.{config.SCHEMA}.{m}"
        try:
            versions = list(w.model_versions.list(full))
            champ = None
            try:
                champ = w.model_versions.get_by_alias(full, "champion").version
            except Exception:
                pass
            out.append({"model": full, "versions": len(versions),
                        "latest": max((int(v.version) for v in versions), default=0), "champion": champ})
        except Exception:
            continue
    return {"models": out}


@app.get("/api/governance/ai-activity")
def gov_ai_activity(sid: str = None):
    where = f"WHERE submission_public_id='{sql.esc(sid)}'" if sid else ""
    return {"rows": sql.query(f"SELECT * FROM {F('gold_ai_activity')} {where} ORDER BY submission_public_id, agent")}


@app.get("/api/governance/lineage")
def gov_lineage():
    rows = sql.query(f"""
        SELECT source_table_full_name AS src, target_table_full_name AS tgt, count(*) reads
        FROM system.access.table_lineage
        WHERE target_table_catalog = '{config.CATALOG}' AND target_table_schema = '{config.SCHEMA}'
          AND source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 60""")
    return {"rows": rows, "source": "system.access.table_lineage (real UC lineage)"}


# ---------------------------------------------------------------- brokers / renewals / one book
@app.get("/api/brokers")
def brokers():
    return {"rows": sql.query(f"SELECT * FROM {F('gold_broker_scorecard')} ORDER BY gwp_bound DESC")}


@app.get("/api/renewals")
def renewals():
    return {"rows": sql.query(f"""SELECT month, sum(in_force) in_force, sum(lapsed) lapsed,
                                         round(sum(in_force)/(sum(in_force)+sum(lapsed))*100,1) retention_pct,
                                         round(avg(avg_rate_change_pct),1) rate_change_pct, sum(gwp) gwp
                                  FROM {F('gold_renewals')} GROUP BY month ORDER BY month DESC LIMIT 14""")}


@app.get("/api/agents")
def agent_roster():
    host = config.workspace_host()

    def ep(substr):
        n = config.resolve_endpoint(substr)
        return {"endpoint": n, "url": f"{host}/ml/endpoints/{n}"}

    return {"supervisor": ep(config.EP_AGENT_SUBSTR),
            "roles": {r: ep(s) for r, s in config.ROLE_SUBSTR.items()},
            "models": {m: ep(m) for m in ("underwriting-triage", "underwriting-risk")}}


# ---------------------------------------------------------------- reset
@app.post("/api/reset")
def reset():
    w = config.get_workspace_client()
    job = next((j for j in w.jobs.list(limit=100, expand_tasks=False)
                if config.RESET_JOB_SUBSTR in ((j.settings.name if j.settings else "") or "")), None)
    if not job:
        return JSONResponse({"error": "reset job not found"}, status_code=404)
    run = w.jobs.run_now(job_id=job.job_id)
    return {"run_id": run.run_id}


@app.get("/api/reset/status")
def reset_status(run_id: int):
    w = config.get_workspace_client()
    r = w.jobs.get_run(run_id)
    state = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "UNKNOWN"
    result = r.state.result_state.value if r.state and r.state.result_state else None
    return {"life_cycle": state, "result": result}


@app.post("/api/warm-cache")
def warm_cache():
    warmed = []
    for sid in ("sub:900001", "sub:900002", "sub:900003"):
        for role in ("risk_profile", "appetite", "pricing_adequacy", "challenge"):
            submission_narrate(sid, role=role, cache=1)
            warmed.append(f"{sid}:{role}")
    agents.ask_agent("Should we quote this? Give the call, terms and who signs.",
                     {"submission_public_id": "sub:900002"}, use_cache=True)
    return {"warmed": len(warmed) + 1}


# ---------------------------------------------------------------- static SPA
@app.get("/")
def root():
    return FileResponse("dist/index.html")


@app.get("/{path:path}")
def spa(path: str):
    return FileResponse("dist/index.html")
