"""Underwriting Workbench — thin FastAPI backend. Presentation only: every panel calls a
real UC function / serving endpoint / Genie / SQL. No underwriting logic lives here."""
import json
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse

from server import agents, config, sql
from server.packs import build_pack

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
        "claims_app_url": config.CLAIMS_APP_URL,
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
                               sum(CASE WHEN l.sla_status='breached' THEN 1 ELSE 0 END) sla_breached,
                               sum(CASE WHEN l.lifecycle_state='referred' THEN 1 ELSE 0 END) referred,
                               sum(l.total_si) open_si
                        FROM {F('gold_submission_lifecycle')} l
                        LEFT ANTI JOIN {F('gold_auto_bound')} z USING (submission_public_id)""",
        "zerotouch": f"""SELECT count(*) bound, sum(premium) premium FROM {F('gold_auto_bound')}""",
        "diary": f"""SELECT sum(CASE WHEN status='open' AND due_date < current_date() THEN 1 ELSE 0 END) overdue,
                            sum(CASE WHEN status='open' AND datediff(due_date, current_date()) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) due_soon
                     FROM {F('gold_subjectivity_tracker')}""",
        "forecast": f"""SELECT round(sum(coalesce(l.target_premium, l.technical_base_premium)
                                          * coalesce(p.bind_propensity_pct, 25) / 100), 0) expected_gwp,
                               count(*) open_subs,
                               sum(coalesce(l.target_premium, l.technical_base_premium)) max_gwp
                        FROM {F('gold_submission_lifecycle')} l
                        LEFT JOIN {F('gold_inbox_priority')} p USING (submission_public_id)
                        LEFT ANTI JOIN {F('gold_auto_bound')} z USING (submission_public_id)""",
        "declines": f"""SELECT coalesce(decline_code, 'APP-REF-RISK') code, count(*) n
                         FROM {F('silver_submissions')} WHERE outcome = 'declined'
                         GROUP BY 1 ORDER BY n DESC""",
        "broker_conc": f"""SELECT round(sum(CASE WHEN rk <= 3 THEN gwp ELSE 0 END) / sum(gwp) * 100, 1) top3_pct
                           FROM (SELECT broker_id, sum(gross_premium) gwp,
                                        row_number() OVER (ORDER BY sum(gross_premium) DESC) rk
                                 FROM {F('bronze_pas_policies')} WHERE policy_status = 'in_force'
                                   AND broker_id != 'DIRECT' GROUP BY broker_id)""",
        "trade_acc": f"""SELECT trade_group,
                                round(property_si / sum(property_si) OVER () * 100, 1) share_pct
                         FROM {F('gold_portfolio_position')}
                         ORDER BY share_pct DESC LIMIT 1""",
        "meta": f"""SELECT current_timestamp() queried_at,
                           (SELECT max(scored_at) FROM {F('gold_inbox_priority')}) scored_at""",
    })
    return {k: (v if k in ("funnel", "accum", "accum_hot", "declines") else (v[0] if v else {})) for k, v in q.items()}


DRILLS = {
    "gwp": f"SELECT trade_group, policies, gwp, plan_gwp, loss_ratio_3y_pct, avg_rate_change_pct, appetite_status FROM {{t}} ORDER BY gwp DESC",
    "retention": "SELECT month, sum(in_force) in_force, sum(lapsed) lapsed, round(sum(in_force)/(sum(in_force)+sum(lapsed))*100,1) retention_pct, round(avg(avg_rate_change_pct),1) rate_change_pct FROM {t} GROUP BY month ORDER BY month DESC LIMIT 14",
    "accumulation": "SELECT postcode_district, in_force_property_si, property_capacity_gbp, utilisation_pct, rag, flood_band, rivers FROM {t} ORDER BY utilisation_pct DESC",
    "adequacy": "SELECT trade_group, quotes_12m, adequacy_pct, avg_technical_premium, avg_quoted_premium, loss_ratio_3y_pct, renewal_rate_change_pct FROM {t} ORDER BY adequacy_pct ASC",
    "funnel": "SELECT month, channel, received, quoted, bound, declined, ntu, lost, quote_expired, avg_hours_to_quote FROM {t} ORDER BY month DESC, channel LIMIT 36",
    "forecast": ("SELECT l.submission_public_id, l.company_name, l.trade_group, l.lifecycle_state, "
                 "coalesce(l.target_premium, l.technical_base_premium) premium, p.bind_propensity_pct, "
                 "round(coalesce(l.target_premium, l.technical_base_premium) * coalesce(p.bind_propensity_pct, 25) / 100, 0) expected_value "
                 "FROM {t} l LEFT JOIN {p} p USING (submission_public_id) ORDER BY expected_value DESC LIMIT 60"),
    "declines": ("SELECT coalesce(decline_code,'APP-REF-RISK') code, count(*) declines, "
                 "min(decided_ts) first_seen, max(decided_ts) last_seen "
                 "FROM {t} WHERE outcome = 'declined' GROUP BY 1 ORDER BY declines DESC"),
    "brokers": ("SELECT broker_id, count(*) policies, sum(gross_premium) gwp, "
                "round(sum(gross_premium) / (SELECT sum(gross_premium) FROM {t} WHERE policy_status='in_force') * 100, 1) share_pct "
                "FROM {t} WHERE policy_status = 'in_force' GROUP BY broker_id ORDER BY gwp DESC"),
    "zerotouch": ("SELECT submission_public_id, company_name, trade_group, broker_id, premium, "
                  "array_join(rules_passed, ' + ') AS rules_passed, bound_at FROM {t} ORDER BY premium DESC LIMIT 60"),
    "trades": ("SELECT trade_group, appetite_status, policies, gwp, property_si, "
               "round(property_si / (SELECT sum(property_si) FROM {t}) * 100, 1) property_share_pct, loss_ratio_3y_pct "
               "FROM {t} ORDER BY property_share_pct DESC"),
}
DRILL_TABLE = {"gwp": "gold_portfolio_position", "retention": "gold_renewals",
               "accumulation": "gold_accumulation", "adequacy": "gold_rate_adequacy",
               "funnel": "gold_pipeline_funnel", "forecast": "gold_submission_lifecycle",
               "declines": "silver_submissions", "brokers": "bronze_pas_policies", "zerotouch": "gold_auto_bound",
               "trades": "gold_portfolio_position"}


@app.get("/api/control-tower/drill")
def ct_drill(key: str):
    if key not in DRILLS:
        return JSONResponse({"error": "unknown drill"}, status_code=400)
    stmt = DRILLS[key].format(t=F(DRILL_TABLE[key]), p=F("gold_inbox_priority"))
    return {"rows": sql.query(stmt), "sql": stmt, "table": F(DRILL_TABLE[key])}


@app.get("/api/worth")
def worth():
    """The business case in £, computed live from the book's own funnel (labelled illustrative)."""
    ch = {r["channel"]: r for r in sql.query(f"""
        SELECT channel, sum(received) received, sum(quoted) quoted, sum(bound) bound,
               sum(gwp_bound) gwp_bound, round(avg(avg_hours_to_quote),1) hours
        FROM {F('gold_pipeline_funnel')} WHERE channel IN ('etrade','portal','email') GROUP BY channel""")}
    et, em, po = ch.get("etrade", {}), ch.get("email", {}), ch.get("portal", {})

    def g(d, k):
        try:
            return float(d.get(k) or 0)
        except (TypeError, ValueError):
            return 0.0

    etr_qr = g(et, "quoted") / max(g(et, "received"), 1)
    bound_total = g(et, "bound") + g(em, "bound") + g(po, "bound")
    quoted_total = g(et, "quoted") + g(em, "quoted") + g(po, "quoted")
    hit = bound_total / max(quoted_total, 1)
    avg_prem = (g(et, "gwp_bound") + g(em, "gwp_bound") + g(po, "gwp_bound")) / max(bound_total, 1)
    extra_quotes = sum(max(etr_qr * g(d, "received") - g(d, "quoted"), 0) for d in (em, po))
    uplift = extra_quotes * hit * avg_prem
    manual_recv = g(em, "received") + g(po, "received")
    hours_saved = manual_recv * 40 / 60  # 40 min rekeying/assembly per manual submission (assumption)
    headline = (f"Lift the manual channels' quote rate to e-trade's {etr_qr*100:.0f}% at today's "
                f"{hit*100:.0f}% hit ratio and £{avg_prem:,.0f} average premium.")
    lines = [
        ["E-trade quote rate (the benchmark)", f"{etr_qr*100:.0f}%"],
        ["Email quote rate today", f"{g(em,'quoted')/max(g(em,'received'),1)*100:.0f}% of {g(em,'received'):.0f} submissions"],
        ["Portal quote rate today", f"{g(po,'quoted')/max(g(po,'received'),1)*100:.0f}% of {g(po,'received'):.0f} submissions"],
        ["Additional submissions quoted/yr", f"{extra_quotes:,.0f}"],
        ["× hit ratio", f"{hit*100:.0f}%"],
        ["× average bound premium", f"£{avg_prem:,.0f}"],
        ["= GWP potential", f"£{uplift:,.0f}/yr"],
        ["Manual submissions/yr × 40 min assembly saved", f"{hours_saved:,.0f} underwriter hours"],
        ["Time-to-quote today", f"e-trade {g(et,'hours'):.1f}h · portal {g(po,'hours'):.1f}h · email {g(em,'hours'):.1f}h"],
    ]
    return {"gwp_uplift_per_year": round(uplift), "hours_saved_per_year": round(hours_saved),
            "headline": headline, "lines": lines}


@app.get("/api/brief")
def brief(cache: int = None):
    """CUO morning brief — the leader persona's daily wow: the control-tower numbers narrated."""
    ct = control_tower()
    data = {
        "forecast": ct.get("forecast"), "gwp": ct.get("gwp"), "retention": ct.get("retention"),
        "adequacy": ct.get("adequacy"), "pipeline": ct.get("pipeline"),
        "hot_districts": ct.get("accum_hot"), "funnel_by_channel": ct.get("funnel"),
    }
    return agents.narrate("cuo_brief", "Write today's morning brief for the Head of Underwriting.",
                          data, use_cache=_cache_flag(cache))


# ---------------------------------------------------------------- inbox
@app.get("/api/inbox")
def inbox():
    # Expected value = premium × P(bind): work the WINNABLE-AND-WORTH-IT business first
    # (P(bind) alone drives a desk to small easy risks). Risk quality shown alongside.
    rows = sql.query(f"""
        SELECT l.*, p.bind_propensity_pct, p.large_loss_propensity_pct,
               coalesce(cc.cnt, 1) - 1 AS prior_contacts,
               bs.broker_trust_score,
               round(coalesce(l.target_premium, l.technical_base_premium)
                     * coalesce(p.bind_propensity_pct, 25) / 100
                     * (0.7 + 0.3 * coalesce(bs.broker_trust_score, 70) / 100), 0) AS expected_value_gbp,
               CASE WHEN coalesce(l.target_premium, l.technical_base_premium) >= 50000 THEN 'size'
                    WHEN coalesce(p.bind_propensity_pct, 0) >= 50 THEN 'winnable'
                    WHEN p.large_loss_propensity_pct <= 15 THEN 'quality'
                    ELSE 'balance' END AS ev_driver
        FROM {F('gold_submission_lifecycle')} l
        LEFT JOIN {F('gold_inbox_priority')} p USING (submission_public_id)
        LEFT JOIN {F('silver_submissions')} ss USING (submission_public_id)
        LEFT JOIN (SELECT company_number, count(*) cnt FROM {F('silver_submissions')}
                   GROUP BY company_number) cc ON cc.company_number = ss.company_number
        LEFT JOIN {F('gold_broker_scorecard')} bs ON bs.broker_id = l.broker_id
        LEFT ANTI JOIN {F('gold_auto_bound')} z USING (submission_public_id)
        ORDER BY CASE WHEN l.submission_public_id LIKE 'sub:9000%' THEN 0 ELSE 1 END,
                 expected_value_gbp DESC NULLS LAST
        LIMIT 120""")
    return {"rows": rows,
            "note": ("ranked by EXPECTED VALUE (premium × batch-scored P(bind)) — winnable AND worth it; "
                     "risk quality shown alongside; heroes pinned")}


# ---------------------------------------------------------------- work a submission
PANEL_FNS = {"dossier": "fn_extract_summary", "appetite": "fn_appetite_check",
             "authority": "fn_authority_check", "accumulation": "fn_accumulation_impact",
             "price": "fn_technical_price", "screening": "fn_sanctions_screen",
             "underinsurance": "fn_underinsurance_check", "treaty": "fn_treaty_check",
             "recommendation": "fn_recommendation"}


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
    stmts["claims_experience"] = f"""SELECT carrier, loss_date, peril, paid_gbp, outstanding_gbp,
                                            deductible_gbp, status, file_name
                                     FROM {F('gold_claims_experience')}
                                     WHERE submission_public_id='{sid}' ORDER BY loss_date DESC LIMIT 12"""
    stmts["account"] = f"""SELECT p.policy_number, p.product_line, p.gross_premium, p.policy_status,
                                  p.inception_date, c.client_id, c.client_since,
                                  coalesce(cl.incurred_3y, 0) AS incurred_3y
                           FROM {F('ref_client')} c
                           JOIN {F('silver_submissions')} ss ON ss.company_number = c.company_number
                            AND ss.submission_public_id = '{sid}'
                           JOIN {F('landing_pas_policies')} p ON p.client_id = c.client_id
                           LEFT JOIN (SELECT policy_number, sum(incurred) incurred_3y
                                      FROM {F('landing_pas_claims')} GROUP BY policy_number) cl
                             ON cl.policy_number = p.policy_number
                           ORDER BY p.gross_premium DESC LIMIT 8"""
    stmts["broker"] = f"""SELECT bs.broker_id, bs.broker_name, bs.broker_trust_score, bs.hit_ratio_pct,
                                 bs.data_complete_pct, bs.fact_discrepancy_pct, bs.ntu_rate_pct
                          FROM {F('gold_broker_scorecard')} bs
                          JOIN {F('silver_submissions')} ss ON ss.broker_id = bs.broker_id
                          WHERE ss.submission_public_id = '{sid}'"""
    stmts["auto_bound"] = f"""SELECT b.bound_at, a.decision_id FROM {F('gold_auto_bound')} b
                              LEFT JOIN {F('gold_decision_audit')} a
                                ON a.submission_public_id = b.submission_public_id AND a.decided_via='system_etrade'
                              WHERE b.submission_public_id='{sid}'"""
    stmts["client_history"] = f"""SELECT h.submission_public_id, h.received_ts, h.trade_group,
                                         h.lifecycle_state, h.outcome, h.quoted_premium, h.channel
                                  FROM {F('silver_submissions')} h
                                  JOIN {F('silver_submissions')} c
                                    ON h.company_number = c.company_number
                                   AND c.submission_public_id = '{sid}'
                                   AND h.submission_public_id != '{sid}'
                                  ORDER BY h.received_ts DESC LIMIT 8"""
    out = sql.query_many(stmts)
    res = {k: (json.loads(out[k][0]["r"]) if out.get(k) and out[k][0].get("r") else {}) for k in PANEL_FNS}
    res["scores"] = out["scores"][0] if out.get("scores") else {}
    res["locations"] = out.get("locations", [])
    res["documents"] = out.get("documents", [])
    res["client_history"] = out.get("client_history", [])
    res["auto_bound"] = (out.get("auto_bound") or [None])[0]
    res["broker"] = (out.get("broker") or [None])[0]
    res["account"] = out.get("account", [])
    res["claims_experience"] = out.get("claims_experience", [])
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

    evidence = json.dumps(b.get("evidence")) if b.get("evidence") else None
    ev_sql = f"'{sql.esc(evidence[:180000])}'" if evidence else "NULL"
    sql.query(f"""INSERT INTO {F('gold_decision_audit')} VALUES (
        '{did}', '{sid}', '{sql.esc(b.get("action", ""))}', {f"'{sql.esc(b['refer_to'])}'" if b.get("refer_to") else "NULL"},
        {f"'{sql.esc(b['underwriter'])}'" if b.get("underwriter") else "NULL"},
        {arr("reasons")}, {arr("terms")}, {arr("subjectivities")},
        {f"'{sql.esc(b['decline_code'])}'" if b.get("decline_code") else "NULL"},
        {f"'{sql.esc(b['external_reason'])}'" if b.get("external_reason") else "NULL"},
        {arr("internal_notes")}, {b.get("quoted_premium") or "NULL"},
        {str(bool(b.get("straight_through"))).lower()}, '{who}', 'app', current_timestamp(), {ev_sql})""")
    for i, subj in enumerate(b.get("subjectivities") or []):
        try:
            import re as _re
            m = _re.search("within ([0-9]+) days", subj)
            days = int(m.group(1)) if m else 30
        except Exception:
            days = 30
        sql.query(f"""MERGE INTO {F('gold_subjectivity_tracker')} t
            USING (SELECT '{did}-S{i}' k) s ON t.tracker_id = s.k
            WHEN NOT MATCHED THEN INSERT VALUES ('{did}-S{i}', '{sid}', '{did}',
                '{sql.esc(subj)}', date_add(current_date(), {days}), 'open', NULL, NULL, current_timestamp())""")
    pack = _write_pack({**b, "decision_id": did, "submission_public_id": b.get("sid", ""),
                        "decided_by": who, "decided_via": "app", "decision_ts": "now"},
                       b.get("evidence") or {})
    return {"decision_id": did, "recorded_by": who, "evidence_recorded": bool(evidence), "pack": pack}


def _write_pack(audit_like: dict, evidence: dict):
    """Compile the audit-pack PDF into the comms_out Volume + register it. Best-effort."""
    try:
        sid = audit_like.get("submission_public_id", "")
        fname = sid.replace(":", "-") + "_decision_pack.pdf"
        path = f"/Volumes/{config.CATALOG}/{config.SCHEMA}/comms_out/{fname}"
        pdf = build_pack(audit_like, evidence)
        import io

        config.get_workspace_client().files.upload(path, io.BytesIO(pdf), overwrite=True)
        sql.query(f"""CREATE TABLE IF NOT EXISTS {F('gold_decision_packs')} (
            submission_public_id STRING, decision_id STRING, file_name STRING,
            path STRING, bytes BIGINT, generated_at TIMESTAMP) USING DELTA""")
        sql.query(f"""MERGE INTO {F('gold_decision_packs')} t
            USING (SELECT '{sql.esc(sid)}' sid) s ON t.submission_public_id = s.sid
            WHEN MATCHED THEN UPDATE SET decision_id='{sql.esc(audit_like.get("decision_id", ""))}',
                 file_name='{fname}', path='{path}', bytes={len(pdf)}, generated_at=current_timestamp()
            WHEN NOT MATCHED THEN INSERT VALUES ('{sql.esc(sid)}', '{sql.esc(audit_like.get("decision_id", ""))}',
                 '{fname}', '{path}', {len(pdf)}, current_timestamp())""")
        return {"file_name": fname, "bytes": len(pdf)}
    except Exception as e:  # noqa: BLE001 — the decision record must never fail on the pack
        return {"error": str(e)[:150]}


@app.get("/api/decisions")
def decisions(sid: str = None):
    where = f"WHERE submission_public_id='{sql.esc(sid)}'" if sid else ""
    return {"rows": sql.query(f"""SELECT decision_id, submission_public_id, action, refer_to_grade,
                                         suggested_underwriter, reasons, terms, subjectivities,
                                         decline_code_external, quoted_premium, straight_through,
                                         decided_by, decided_via, decision_ts,
                                         (decision_evidence IS NOT NULL) AS has_evidence
                                  FROM {F('gold_decision_audit')} {where}
                                  ORDER BY decision_ts DESC LIMIT 50""")}


@app.get("/api/decision/pack")
def decision_pack(sid: str):
    """Stream the audit-pack PDF for a decided submission from the comms_out Volume."""
    from fastapi.responses import Response
    row = sql.query_one(f"""SELECT path, file_name FROM {F('gold_decision_packs')}
                            WHERE submission_public_id='{sql.esc(sid)}' LIMIT 1""")
    if not row:
        return JSONResponse({"error": "no pack for this submission yet"}, status_code=404)
    data = config.get_workspace_client().files.download(row["path"]).contents.read()
    return Response(content=data, media_type="application/pdf",
                    headers={"Content-Disposition": f"inline; filename={row['file_name']}"})


@app.get("/api/decision/evidence")
def decision_evidence(decision_id: str):
    """The auditor's question answered: the exact dossier as-at decision time."""
    row = sql.query_one(f"""SELECT decision_id, submission_public_id, decided_by, decision_ts,
                                   decision_evidence
                            FROM {F('gold_decision_audit')}
                            WHERE decision_id = '{sql.esc(decision_id)}' LIMIT 1""")
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        row["decision_evidence"] = json.loads(row["decision_evidence"]) if row.get("decision_evidence") else None
    except Exception:
        pass
    return row


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


# ---------------------------------------------------------------- MTAs / endorsements
@app.get("/api/mtas")
def mtas():
    rows = sql.query(f"""
        SELECT m.*, p.trade_group, p.gross_premium, p.client_id
        FROM {F('landing_mta_feed')} m
        JOIN {F('landing_pas_policies')} p USING (policy_number)
        WHERE m.status = 'open'
        ORDER BY CASE WHEN m.mta_id LIKE 'mta:9000%' THEN 0 ELSE 1 END,
                 (m.delta_buildings_si + m.delta_contents_si) DESC
        LIMIT 60""")
    kpi = sql.query_one(f"""
        SELECT count(*) open_mtas, sum(delta_buildings_si + delta_contents_si) delta_si
        FROM {F('landing_mta_feed')} WHERE status = 'open'""")
    return {"rows": rows, "kpi": kpi,
            "note": "Read from the landing feed (bronze governance for MTAs = roadmap); delta checks are live UC-function calls."}


@app.get("/api/mta/{mid:path}")
def mta_detail(mid: str):
    return _struct("fn_mta_check", mid)


@app.post("/api/mta/decide")
async def mta_decide(req: Request):
    b = await req.json()
    mid = sql.esc(b.get("mta_id", ""))
    who = sql.esc(req.headers.get("x-forwarded-email", "demo-user"))
    did = "ME-" + uuid.uuid4().hex[:10]
    chk = _struct("fn_mta_check", mid)
    sql.query(f"""INSERT INTO {F('gold_decision_audit')} VALUES (
        '{did}', '{mid}', 'mta_{sql.esc(b.get("action", "approve"))}',
        {f"'{sql.esc(chk.get('required_grade', ''))}'" if b.get("action") == "refer" else "NULL"}, NULL,
        array({','.join(chr(39) + sql.esc(r) + chr(39) for r in chk.get('reasons', []))}),
        array(), array(), NULL, NULL, array('endorsement on {sql.esc(chk.get("policy_number", ""))}'),
        {chk.get('pro_rata_additional_premium') or 'NULL'}, false, '{who}', 'app',
        current_timestamp(), '{sql.esc(json.dumps({"mta_check": chk}))[:180000]}')""")
    return {"decision_id": did, "recorded_by": who, "check": chk}


# ---------------------------------------------------------------- subjectivity diary
@app.get("/api/diary")
def diary():
    rows = sql.query(f"""
        SELECT t.*, datediff(t.due_date, current_date()) AS days_left,
               CASE WHEN t.status != 'open' THEN t.status
                    WHEN t.due_date < current_date() THEN 'overdue'
                    WHEN datediff(t.due_date, current_date()) <= 3 THEN 'due_soon'
                    ELSE 'on_track' END AS bucket
        FROM {F('gold_subjectivity_tracker')} t
        ORDER BY t.due_date LIMIT 100""")
    kpi = sql.query_one(f"""
        SELECT sum(CASE WHEN status='open' AND due_date < current_date() THEN 1 ELSE 0 END) overdue,
               sum(CASE WHEN status='open' AND due_date >= current_date()
                         AND datediff(due_date, current_date()) <= 3 THEN 1 ELSE 0 END) due_soon,
               sum(CASE WHEN status='chased' THEN 1 ELSE 0 END) chased,
               count(*) total
        FROM {F('gold_subjectivity_tracker')}""")
    return {"rows": rows, "kpi": kpi}


@app.post("/api/diary/chase")
async def diary_chase(req: Request, cache: int = None):
    """The system drafts the broker chaser; a human approves — underwriters are not diary managers."""
    b = await req.json()
    tid = sql.esc(b.get("tracker_id", ""))
    row = sql.query_one(f"SELECT * FROM {F('gold_subjectivity_tracker')} WHERE tracker_id='{tid}'")
    if not row:
        return JSONResponse({"error": "tracker row not found"}, status_code=404)
    sid = row["submission_public_id"]
    data = {"dossier": _struct("fn_extract_summary", sid),
            "subjectivity": row["subjectivity"], "due_date": row["due_date"],
            "days_overdue_or_left": row.get("due_date")}
    r = agents.narrate("broker_comms",
                       f"Draft a polite but firm chaser to the broker for {sid}: the subjectivity below "
                       f"falls due on {row['due_date']} and remains outstanding. Ask for the item or a date.",
                       data, use_cache=_cache_flag(cache))
    sql.query(f"""UPDATE {F('gold_subjectivity_tracker')}
                  SET status='chased', chased_ts=current_timestamp() WHERE tracker_id='{tid}'""")
    return {"text": r.get("text"), "cache": r.get("cache"), "tracker_id": tid, "sid": sid}


# ---------------------------------------------------------------- appetite & rate committee
@app.get("/api/committee")
def committee():
    q = sql.query_many({
        "guide": f"""SELECT a.trade_group, a.appetite_status, a.hazard_grade, a.guide_section,
                            r.property_rate_permille, r.bi_rate_permille, r.min_premium,
                            p.gwp, p.policies, p.loss_ratio_3y_pct, ra.adequacy_pct
                     FROM {F('ref_appetite')} a
                     JOIN {F('ref_rate_guide')} r USING (trade_group)
                     LEFT JOIN {F('gold_portfolio_position')} p USING (trade_group)
                     LEFT JOIN {F('gold_rate_adequacy')} ra USING (trade_group)
                     ORDER BY p.gwp DESC NULLS LAST""",
        "proposals": f"""SELECT * FROM {F('gov_guide_changes')} ORDER BY proposed_ts DESC LIMIT 25""",
    })
    return q


@app.post("/api/committee/preview")
async def committee_preview(req: Request):
    """Projected impact of a rate change on the OPEN pipeline — property component repriced."""
    b = await req.json()
    t = sql.esc(b.get("trade_group", ""))
    new_rate = float(b.get("new_property_rate_permille", 0))
    row = sql.query_one(f"""
        WITH cur AS (SELECT property_rate_permille FROM {F('ref_rate_guide')} WHERE trade_group='{t}')
        SELECT count(*) open_subs,
               round(avg(l.technical_base_premium), 0) avg_tech_now,
               round(avg(l.technical_base_premium
                         + l.total_property_si * ({new_rate} - cur.property_rate_permille) / 1000), 0) avg_tech_new,
               round(sum(l.total_property_si * ({new_rate} - cur.property_rate_permille) / 1000), 0) pipeline_premium_delta,
               round(avg(l.target_premium / nullif(l.technical_base_premium
                         + l.total_property_si * ({new_rate} - cur.property_rate_permille) / 1000, 0)) * 100, 1) new_adequacy_pct,
               any_value(cur.property_rate_permille) current_rate
        FROM {F('gold_submission_lifecycle')} l CROSS JOIN cur
        WHERE l.trade_group = '{t}'""")
    inforce = sql.query_one(f"""SELECT gwp, policies FROM {F('gold_portfolio_position')} WHERE trade_group='{t}'""")
    return {"impact": row, "in_force": inforce,
            "note": "Property component repriced on the OPEN pipeline (BI/EL/PL unchanged); book marts refresh on the next pipeline run."}


@app.post("/api/committee/propose")
async def committee_propose(req: Request):
    b = await req.json()
    pid = "GC-" + uuid.uuid4().hex[:8]
    who = sql.esc(req.headers.get("x-forwarded-email", "demo-user"))
    sql.query(f"""INSERT INTO {F('gov_guide_changes')} VALUES (
        '{pid}', '{sql.esc(b.get("trade_group", ""))}', '{sql.esc(b.get("change_type", "rate"))}',
        '{sql.esc(str(b.get("current_value", "")))}', '{sql.esc(str(b.get("proposed_value", "")))}',
        '{sql.esc(b.get("rationale", ""))[:800]}', '{sql.esc(json.dumps(b.get("impact") or {}))[:4000]}',
        'proposed', '{who}', current_timestamp(), NULL)""")
    return {"proposal_id": pid, "status": "proposed", "by": who}


@app.post("/api/committee/apply")
async def committee_apply(req: Request):
    """Apply a rate proposal: the guide IS data — the change is one governed UPDATE."""
    b = await req.json()
    pid = sql.esc(b.get("proposal_id", ""))
    row = sql.query_one(f"SELECT * FROM {F('gov_guide_changes')} WHERE proposal_id='{pid}'")
    if not row:
        return JSONResponse({"error": "proposal not found"}, status_code=404)
    if row["change_type"] == "rate":
        sql.query(f"""UPDATE {F('ref_rate_guide')} SET property_rate_permille = {float(row['proposed_value'])}
                      WHERE trade_group = '{sql.esc(row['trade_group'])}'""")
    else:
        sql.query(f"""UPDATE {F('ref_appetite')} SET appetite_status = '{sql.esc(row['proposed_value'])}'
                      WHERE trade_group = '{sql.esc(row['trade_group'])}'""")
    sql.query(f"""UPDATE {F('gov_guide_changes')} SET status='applied', applied_ts=current_timestamp()
                  WHERE proposal_id='{pid}'""")
    return {"proposal_id": pid, "status": "applied",
            "note": "New quotes price on the new guide immediately (the crux functions read the ref tables live); book marts refresh on the next pipeline run."}


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
               "ref_flood_open", "ref_crime_open", "ref_epc_mix_open", "gold_lossrun_recon", "gold_claims_experience"}
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
        "audit": f"""SELECT decision_id, submission_public_id, action, refer_to_grade, decided_by,
                            decided_via, decline_code_external, straight_through, decision_ts,
                            (decision_evidence IS NOT NULL) AS has_evidence
                     FROM {F('gold_decision_audit')} ORDER BY decision_ts DESC LIMIT 40""",
        "conduct": f"SELECT * FROM {F('gov_conduct_declines')}",
        "comms": f"SELECT draft_id, submission_public_id, letter_type, status, approved_by, drafted_ts FROM {F('gold_comms_drafts')} ORDER BY drafted_ts DESC LIMIT 20",
    })
    return q


@app.get("/api/governance/masking")
def gov_masking():
    return {"rows": sql.query(f"SELECT * FROM {F('gov_watchlist_secure')}"),
            "note": "This app's service principal is OUTSIDE underwriting_conduct_readers — the redaction you see is enforced by Unity Catalog, not the app.",
            "view": F("gov_watchlist_secure")}


MODEL_REGISTER = {
    # SS1/23-shaped model risk register metadata (owner/purpose/validation/monitoring)
    "model_triage_priority": {
        "tier": "Tier 2 — decision-support", "owner": "Head of Underwriting Operations",
        "purpose": "Ranks the submission inbox by P(bind); never accepts or declines risk",
        "training_data": "12 months closed submissions via UC Feature Store (feature_submission)",
        "validation": "Challenger review pending; AUC tracked per training run in MLflow",
        "monitoring": "Batch-rescored on every pipeline run; smoke-tested on every reset"},
    "model_risk_quality": {
        "tier": "Tier 2 — decision-support", "owner": "Portfolio Underwriting Manager",
        "purpose": "Large-loss propensity feeding rate adequacy and referral judgement",
        "training_data": "PAS book claims experience (3y, large-loss >= GBP 25k label)",
        "validation": "Challenger review pending; AUC + base rate logged per run",
        "monitoring": "Batch-rescored on every pipeline run; smoke-tested on every reset"},
    "model_underwriting_agent": {
        "tier": "Tier 3 — narrative only", "owner": "Chief Underwriting Officer (accountable executive)",
        "purpose": "Narrate-only role agents (risk profile, appetite, adequacy, comms, challenge, brief)",
        "training_data": "None — FM-backed; receives structured findings, never decides",
        "validation": "Hard prompt rules (decline letters cite appetite only) asserted in the smoke test",
        "monitoring": "Every interaction logged to gold_ai_activity; HITL approval on all letters"},
    "underwriting_agent": {
        "tier": "Tier 3 — advisory with tool audit", "owner": "Chief Underwriting Officer (accountable executive)",
        "purpose": "Tool-calling supervisor over the governed UC decision functions",
        "training_data": "None — FM-backed; every tool call traced and returned",
        "validation": "Tool-trace asserted live in the smoke test",
        "monitoring": "Tool trace + answer logged to gold_ai_activity per interaction"},
}


@app.get("/api/governance/models")
def gov_models():
    w = config.get_workspace_client()
    out = []
    for m, meta in MODEL_REGISTER.items():
        full = f"{config.CATALOG}.{config.SCHEMA}.{m}"
        try:
            versions = list(w.model_versions.list(full))
            champ = None
            try:
                champ = w.model_versions.get_by_alias(full, "champion").version
            except Exception:
                pass
            latest = max(versions, key=lambda v: int(v.version), default=None)
            out.append({"model": full, "versions": len(versions),
                        "latest": int(latest.version) if latest else 0,
                        "last_updated": str(getattr(latest, "created_at", "") or "")[:10] if latest else "",
                        "champion": champ, **meta})
        except Exception:
            continue
    return {"models": out,
            "note": "Register shaped on PRA SS1/23 (model risk management): tier, accountable owner, purpose, data, validation, monitoring — read live from the UC registry."}


@app.get("/api/governance/ai-activity")
def gov_ai_activity(sid: str = None):
    where = f"WHERE submission_public_id='{sql.esc(sid)}'" if sid else ""
    return {"rows": sql.query(f"SELECT * FROM {F('gold_ai_activity')} {where} ORDER BY recorded_at DESC LIMIT 60")}


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


@app.get("/api/renewals/due")
def renewals_due():
    # The desk's real book: renewals due in the next 90 days with claims experience,
    # last rate movement, adequacy-informed stance and a retention-risk flag.
    rows = sql.query(f"""
        WITH clm AS (
          SELECT policy_number, count(*) claims_3y, sum(incurred) incurred_3y
          FROM {F('bronze_pas_claims')} GROUP BY policy_number)
        SELECT p.policy_number, p.trade_group, p.segment, p.postcode_district, p.broker_id,
               p.expiry_date, datediff(p.expiry_date, current_date()) AS days_to_expiry,
               p.gross_premium, round(p.rate_change_pct * 100, 1) AS last_rate_change_pct,
               coalesce(c.claims_3y, 0) AS claims_3y, coalesce(c.incurred_3y, 0) AS incurred_3y,
               round(coalesce(c.incurred_3y, 0) / 3 / p.gross_premium * 100, 0) AS loss_ratio_pct,
               a.adequacy_pct AS trade_adequacy_pct,
               CASE WHEN coalesce(c.incurred_3y,0) / 3 / p.gross_premium > 0.8 THEN 'increase_or_restructure'
                    WHEN a.adequacy_pct < 90 THEN 'increase_to_technical'
                    WHEN coalesce(c.claims_3y,0) = 0 AND a.adequacy_pct >= 100 THEN 'hold_and_retain'
                    ELSE 'review' END AS suggested_stance,
               (coalesce(c.incurred_3y,0) / 3 / p.gross_premium > 0.5 OR a.adequacy_pct < 85
                OR p.rate_change_pct > 0.08) AS retention_risk
        FROM {F('bronze_pas_policies')} p
        LEFT JOIN clm c USING (policy_number)
        LEFT JOIN {F('gold_rate_adequacy')} a USING (trade_group)
        WHERE p.policy_status = 'in_force'
          AND p.expiry_date BETWEEN current_date() AND date_add(current_date(), 90)
        ORDER BY p.expiry_date, p.gross_premium DESC
        LIMIT 150""")
    kpi = sql.query_one(f"""
        SELECT count(*) due, sum(gross_premium) gwp_due,
               sum(CASE WHEN expiry_date <= date_add(current_date(), 30) THEN 1 ELSE 0 END) due_30d
        FROM {F('bronze_pas_policies')}
        WHERE policy_status = 'in_force'
          AND expiry_date BETWEEN current_date() AND date_add(current_date(), 90)""")
    return {"rows": rows, "kpi": kpi}


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
def warm_cache(scope: str = None):
    """Warm the narration cache. Chunked via ?scope=sub:NNN|book so each call stays inside
    the Apps gateway timeout (a full warm after reset used to exceed it)."""
    warmed = 0
    sids = [scope] if scope and scope.startswith("sub:") else         ([] if scope == "book" else ["sub:900001", "sub:900002", "sub:900003"])
    for sid in sids:
        for role in ("risk_profile", "appetite", "pricing_adequacy", "challenge"):
            submission_narrate(sid, role=role, cache=1)
            warmed += 1
    if scope in (None, "book"):
        brief(cache=1)
        agents.ask_agent("Should we quote this? Give the call, terms and who signs.",
                         {"submission_public_id": "sub:900002"}, use_cache=True)
        warmed += 2
    return {"warmed": warmed, "scope": scope or "all"}


# ---------------------------------------------------------------- static SPA
@app.get("/")
def root():
    return FileResponse("dist/index.html")


@app.get("/{path:path}")
def spa(path: str):
    return FileResponse("dist/index.html")
