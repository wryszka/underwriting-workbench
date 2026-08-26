"""MCP server — the Underwriting Workbench (main app) exposed as callable tools.

This is the STANDARD /api/mcp read/ops surface for the underwriting workbench, so
the Bricksurance control tower speaks to it with the same JSON-RPC transport as
the other estate nodes. It sits ALONGSIDE the separate `uw-mcp` FastMCP app,
which remains the mutating broker/referral surface (submit_risk, propose_rule_change,
…) under its own trust boundary — the manifest lists both servers for this node.

Every tool DELEGATES to the app's own endpoint function (sync or async — handled
uniformly), reusing the exact logic AND any server-side gate. Reads are idempotent;
[action] tools write through the governed handler via a Request shim so the
handler's own body/identity handling is unchanged. A 401/403 → {"ok":False,"gated":True}.

Transport: JSON-RPC 2.0 over one POST + a GET manifest. MUST be mounted before the
SPA catch-all route so /api/mcp is not swallowed by it.
"""
from __future__ import annotations

import inspect
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/mcp", tags=["mcp"])

PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "bricksurance-underwriting-workbench", "version": "1.0.0"}


def _mk(name, desc, props=None, required=None):
    return {"name": name, "description": desc,
            "inputSchema": {"type": "object", "properties": props or {}, "required": required or []}}


async def _call(fn, *args, **kwargs) -> dict:
    """Call an app endpoint (sync or async); normalise to {"ok": ...}."""
    try:
        r = fn(*args, **kwargs)
        if inspect.isawaitable(r):
            r = await r
    except HTTPException as e:
        gated = e.status_code in (401, 403)
        return {"ok": False, **({"gated": True} if gated else {}), "error": f"{e.status_code}: {e.detail}"}
    except Exception as e:
        logger.warning("mcp underwriting delegate %s failed: %s", getattr(fn, "__name__", "?"), str(e)[:200])
        return {"ok": False, "error": str(e)[:200]}
    return r if isinstance(r, dict) else {"ok": True, "data": r}


class _ActionReq:
    """Request shim for POST handlers that do `await req.json()` and read headers.
    Carries the agent args as the body and attributes the action to the agent."""
    def __init__(self, body: dict, agent_id: str):
        self._body = body or {}
        self.headers = {"x-forwarded-email": f"agent:{agent_id}"}

    async def json(self):
        return self._body


_SID = {"sid": {"type": "string", "description": "Submission id"}}


def _reg(app):
    # --- referral control (the SCD2 rulebook + fire-vector telemetry) ---
    async def _ref_discipline(a, ag):  return await _call(app.referral_discipline)
    async def _ref_findings(a, ag):    return await _call(app.referral_findings, a.get("as_of"))
    async def _ref_rule(a, ag):        return await _call(app.referral_rule, str(a.get("rule_id") or ""), a.get("as_of"))
    async def _ref_emulate(a, ag):     return await _call(app.referral_emulate, str(a.get("rule_id") or ""), str(a.get("action") or ""), a.get("as_of"))
    async def _ref_rulebook(a, ag):    return await _call(app.referral_rulebook, a.get("as_of"))
    async def _ref_changes(a, ag):     return await _call(app.referral_changes)
    async def _ref_decision(a, ag):    return await _call(app.referral_decision, str(a.get("transaction_id") or ""))
    async def _ref_reviewer(a, ag):    return await _call(app.referral_reviewer, str(a.get("sid") or ""))
    async def _mcp_traffic(a, ag):     return await _call(app.mcp_traffic)
    async def _ref_propose(a, ag):     return await _call(app.referral_propose, _ActionReq(a, ag))
    async def _ref_approve(a, ag):     return await _call(app.referral_approve, _ActionReq(a, ag))
    async def _ref_advisor(a, ag):     return await _call(app.referral_advisor, _ActionReq(a, ag))
    # --- desk: control tower, inbox, submissions, decisions, whatif, MTA ---
    async def _ct(a, ag):              return await _call(app.control_tower)
    async def _ct_drill(a, ag):        return await _call(app.ct_drill, str(a.get("key") or ""))
    async def _worth(a, ag):           return await _call(app.worth)
    async def _brief(a, ag):           return await _call(app.brief)
    async def _inbox(a, ag):           return await _call(app.inbox)
    async def _panels(a, ag):          return await _call(app.submission_panels, str(a.get("sid") or ""))
    async def _narrate(a, ag):         return await _call(app.submission_narrate, str(a.get("sid") or ""), str(a.get("role") or "challenge"))
    async def _comms(a, ag):           return await _call(app.submission_comms, str(a.get("sid") or ""), str(a.get("letter_type") or "quote"))
    async def _adj_reasons(a, ag):     return await _call(app.adjustment_reasons)
    async def _decisions(a, ag):       return await _call(app.decisions, a.get("sid"))
    async def _decision_pack(a, ag):   return await _call(app.decision_pack, str(a.get("sid") or ""))
    async def _decision_evidence(a, ag): return await _call(app.decision_evidence, str(a.get("decision_id") or ""))
    async def _whatif_options(a, ag):  return await _call(app.whatif_options)
    async def _mtas(a, ag):            return await _call(app.mtas)
    async def _mta_detail(a, ag):      return await _call(app.mta_detail, str(a.get("mid") or ""))
    async def _diary(a, ag):           return await _call(app.diary)
    async def _committee(a, ag):       return await _call(app.committee)
    # actions
    async def _comms_record(a, ag):    return await _call(app.comms_record, _ActionReq(a, ag))
    async def _decision(a, ag):        return await _call(app.decision, _ActionReq(a, ag))
    async def _whatif(a, ag):          return await _call(app.whatif, _ActionReq(a, ag))
    async def _mta_decide(a, ag):      return await _call(app.mta_decide, _ActionReq(a, ag))
    async def _diary_chase(a, ag):     return await _call(app.diary_chase, _ActionReq(a, ag))
    async def _cm_preview(a, ag):      return await _call(app.committee_preview, _ActionReq(a, ag))
    async def _cm_propose(a, ag):      return await _call(app.committee_propose, _ActionReq(a, ag))
    async def _cm_apply(a, ag):        return await _call(app.committee_apply, _ActionReq(a, ag))
    # --- ingestion / governance / AI / book ---
    async def _ing_assets(a, ag):      return await _call(app.ingestion_assets)
    async def _ing_quarantine(a, ag):  return await _call(app.ingestion_quarantine, str(a.get("src") or "schedules"))
    async def _ing_sample(a, ag):      return await _call(app.ingestion_sample, str(a.get("table") or ""))
    async def _gov_inventory(a, ag):   return await _call(app.gov_inventory)
    async def _gov_decisions(a, ag):   return await _call(app.gov_decisions)
    async def _gov_masking(a, ag):     return await _call(app.gov_masking)
    async def _gov_models(a, ag):      return await _call(app.gov_models)
    async def _gov_ai_activity(a, ag): return await _call(app.gov_ai_activity, a.get("sid"))
    async def _gov_lineage(a, ag):     return await _call(app.gov_lineage)
    async def _brokers(a, ag):         return await _call(app.brokers)
    async def _renewals_due(a, ag):    return await _call(app.renewals_due)
    async def _renewals(a, ag):        return await _call(app.renewals)
    async def _agents(a, ag):          return await _call(app.agent_roster)
    async def _agent_ask(a, ag):       return await _call(app.agent_ask, _ActionReq(a, ag))
    async def _genie_ask(a, ag):       return await _call(app.genie_ask, _ActionReq(a, ag))
    async def _genie_examples(a, ag):  return await _call(app.genie_examples)

    return {
        "ref_discipline": _ref_discipline, "ref_findings": _ref_findings, "ref_rule": _ref_rule,
        "ref_emulate": _ref_emulate, "ref_rulebook": _ref_rulebook, "ref_changes": _ref_changes,
        "ref_decision": _ref_decision, "ref_reviewer": _ref_reviewer, "mcp_traffic": _mcp_traffic,
        "ref_propose": _ref_propose, "ref_approve": _ref_approve, "ref_advisor": _ref_advisor,
        "control_tower": _ct, "ct_drill": _ct_drill, "worth": _worth, "brief": _brief, "inbox": _inbox,
        "submission_panels": _panels, "submission_narrate": _narrate, "submission_comms": _comms,
        "adjustment_reasons": _adj_reasons, "decisions": _decisions, "decision_pack": _decision_pack,
        "decision_evidence": _decision_evidence, "whatif_options": _whatif_options, "mtas": _mtas,
        "mta_detail": _mta_detail, "diary": _diary, "committee": _committee,
        "comms_record": _comms_record, "decision": _decision, "whatif": _whatif, "mta_decide": _mta_decide,
        "diary_chase": _diary_chase, "committee_preview": _cm_preview, "committee_propose": _cm_propose,
        "committee_apply": _cm_apply,
        "ingestion_assets": _ing_assets, "ingestion_quarantine": _ing_quarantine, "ingestion_sample": _ing_sample,
        "gov_inventory": _gov_inventory, "gov_decisions": _gov_decisions, "gov_masking": _gov_masking,
        "gov_models": _gov_models, "gov_ai_activity": _gov_ai_activity, "gov_lineage": _gov_lineage,
        "brokers": _brokers, "renewals_due": _renewals_due, "renewals": _renewals, "agents": _agents,
        "agent_ask": _agent_ask, "genie_ask": _genie_ask, "genie_examples": _genie_examples,
    }


TOOL_SCHEMAS: list[dict[str, Any]] = [
    _mk("ref_discipline", "Referral-discipline overview — is the underwriting authority being respected (fire-vector telemetry)."),
    _mk("ref_findings", "Referral findings as of a date (rules fired vs suppressed).", {"as_of": {"type": "string"}}),
    _mk("ref_rule", "One referral rule (SCD2-versioned) as of a date.", {"rule_id": {"type": "string"}, "as_of": {"type": "string"}}, ["rule_id"]),
    _mk("ref_emulate", "Emulate a rule action to see what it would have caught (detection/emulation).", {"rule_id": {"type": "string"}, "action": {"type": "string"}, "as_of": {"type": "string"}}, ["rule_id", "action"]),
    _mk("ref_rulebook", "The referral rulebook as of a date.", {"as_of": {"type": "string"}}),
    _mk("ref_changes", "Rulebook change history."),
    _mk("ref_decision", "The referral decision for a transaction.", {"transaction_id": {"type": "string"}}, ["transaction_id"]),
    _mk("ref_reviewer", "The referral reviewer view for a submission.", _SID, ["sid"]),
    _mk("mcp_traffic", "Agent-traffic card — MCP tool calls across all client types (incl. refusals) from gold_mcp_activity."),
    _mk("ref_propose", "[action] Propose a referral-rule change (escalate-not-bind; refuses compliance-locked). Writes a pending proposal.", {"rule_id": {"type": "string"}, "change": {"type": "object"}, "rationale": {"type": "string"}}),
    _mk("ref_approve", "[gated] Approve a pending referral-rule change (maker/checker).", {"proposal_id": {"type": "string"}}),
    _mk("ref_advisor", "Ask the referral advisor about a rule/finding (grounded).", {"question": {"type": "string"}}),
    _mk("control_tower", "The underwriting control tower — portfolio KPIs, queues, appetite posture."),
    _mk("ct_drill", "Drill into a control-tower metric.", {"key": {"type": "string"}}, ["key"]),
    _mk("worth", "Portfolio worth / value view."),
    _mk("brief", "The underwriter's brief for the day."),
    _mk("inbox", "The submission inbox (what's arriving)."),
    _mk("submission_panels", "The full underwriting desk view for one submission.", _SID, ["sid"]),
    _mk("submission_narrate", "Narrate a submission in plain language for a role (quote / refer / decline).", {**_SID, "role": {"type": "string"}}, ["sid"]),
    _mk("submission_comms", "Draft communication for a submission.", {**_SID, "letter_type": {"type": "string"}}, ["sid"]),
    _mk("adjustment_reasons", "The valid adjustment reasons."),
    _mk("decisions", "Underwriting decisions (optionally for one submission).", {"sid": {"type": "string"}}),
    _mk("decision_pack", "The assembled decision pack for a submission.", _SID, ["sid"]),
    _mk("decision_evidence", "The evidence behind a decision.", {"decision_id": {"type": "string"}}, ["decision_id"]),
    _mk("whatif_options", "Available what-if levers."),
    _mk("mtas", "Mid-term adjustments in flight."),
    _mk("mta_detail", "Detail of one mid-term adjustment.", {"mid": {"type": "string"}}, ["mid"]),
    _mk("diary", "The underwriting diary (chases, follow-ups)."),
    _mk("committee", "The referrals/committee view."),
    _mk("comms_record", "[action] Record that a communication was sent for a submission (audited).", {"sid": {"type": "string"}, "letter_type": {"type": "string"}}, ["sid"]),
    _mk("decision", "[action] Record an underwriting decision (quote / refer / decline) on a submission — audited.", {"sid": {"type": "string"}, "action": {"type": "string"}, "reason": {"type": "string"}}, ["sid", "action"]),
    _mk("whatif", "Run an underwriting what-if (re-rate under changed terms). Does not bind.", {"sid": {"type": "string"}, "changes": {"type": "object"}}, ["sid"]),
    _mk("mta_decide", "[action] Decide a mid-term adjustment.", {"mid": {"type": "string"}, "action": {"type": "string"}}, ["mid", "action"]),
    _mk("diary_chase", "[action] Send a diary chase for an outstanding item.", {"sid": {"type": "string"}}),
    _mk("committee_preview", "Preview a committee proposal.", {"proposal": {"type": "object"}}),
    _mk("committee_propose", "[action] Propose to the referrals committee.", {"proposal": {"type": "object"}}),
    _mk("committee_apply", "[gated] Apply a committee decision.", {"proposal_id": {"type": "string"}}),
    _mk("ingestion_assets", "The labelled ingestion assets (SOVs, loss runs, questionnaires)."),
    _mk("ingestion_quarantine", "Quarantined inbound records for a source.", {"src": {"type": "string"}}),
    _mk("ingestion_sample", "Sample rows from an ingestion table.", {"table": {"type": "string"}}, ["table"]),
    _mk("gov_inventory", "Governed-asset inventory for underwriting."),
    _mk("gov_decisions", "The governed decision log."),
    _mk("gov_masking", "PII masking / access-control view."),
    _mk("gov_models", "Registered models under governance."),
    _mk("gov_ai_activity", "AI-activity log (optionally for one submission).", {"sid": {"type": "string"}}),
    _mk("gov_lineage", "Lineage (submission → decision → bind)."),
    _mk("brokers", "The broker book."),
    _mk("renewals_due", "Renewals due."),
    _mk("renewals", "The renewals book."),
    _mk("agents", "The AI agent roster."),
    _mk("agent_ask", "Ask the grounded underwriting assistant a question.", {"question": {"type": "string"}, "sid": {"type": "string"}}, ["question"]),
    _mk("genie_ask", "Ask AI/BI Genie a data question over the underwriting space.", {"q": {"type": "string"}}, ["q"]),
    _mk("genie_examples", "Example Genie data questions."),
]


def _ok(rpc_id, result):  return {"jsonrpc": "2.0", "id": rpc_id, "result": result}
def _err(rpc_id, code, m): return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": m}}


def register(app_module):
    impls = _reg(app_module)

    @router.post("")
    async def jsonrpc(request: Request):
        try:
            body = await request.json()
        except Exception:
            return _err(None, -32700, "Parse error: body is not valid JSON")
        rpc_id = body.get("id"); method = body.get("method"); params = body.get("params") or {}
        agent_id = request.headers.get("user-agent", "unknown-agent")[:120]
        if method == "initialize":
            return _ok(rpc_id, {
                "protocolVersion": PROTOCOL_VERSION, "serverInfo": SERVER_INFO,
                "capabilities": {"tools": {}},
                "instructions": ("Underwriting workbench (main app) — submission-to-bind desk, referral control, "
                                 "MTAs, committee, ingestion and governance. Standard read/ops surface; the mutating "
                                 "broker surface is the separate uw-mcp FastMCP server. Actions write through the same "
                                 "governed handlers as the UI. Never invent a figure.")})
        if method in ("notifications/initialized", "notifications/cancelled"):
            return _ok(rpc_id, {})
        if method == "tools/list":
            return _ok(rpc_id, {"tools": TOOL_SCHEMAS})
        if method == "tools/call":
            name = params.get("name"); args = params.get("arguments") or {}
            impl = impls.get(name)
            if impl is None:
                return _err(rpc_id, -32601, f"Unknown tool: {name}")
            try:
                payload = await impl(args, agent_id)
            except Exception as e:
                logger.exception("mcp tool %s failed", name)
                return _err(rpc_id, -32603, f"Tool execution failed: {str(e)[:200]}")
            return _ok(rpc_id, {
                "content": [{"type": "text", "text": json.dumps(payload, default=str)}],
                "structuredContent": payload,
                "isError": isinstance(payload, dict) and payload.get("ok") is False})
        return _err(rpc_id, -32601, f"Method not found: {method}")

    @router.get("/manifest")
    async def manifest():
        return {"server": SERVER_INFO, "protocol_version": PROTOCOL_VERSION,
                "tools": [{"name": t["name"], "description": t["description"]} for t in TOOL_SCHEMAS]}

    return router
