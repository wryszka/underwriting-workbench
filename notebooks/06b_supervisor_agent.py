# Databricks notebook source
# MAGIC %md
# MAGIC # 06b · Underwriting AI supervisor — a REAL tool-calling agent
# MAGIC
# MAGIC A `ChatAgent` with a genuine Claude tool-use loop: the LLM autonomously decides which UC
# MAGIC functions to call (dossier, appetite, authority, accumulation, price, sanctions,
# MAGIC underinsurance, recommendation, ML scores, Genie), executes them via the Statement
# MAGIC Execution API, and returns a grounded answer **plus the tool-call trace** (the proof of
# MAGIC real tool use the app surfaces). Deployed via `databricks.agents.deploy` with the UC
# MAGIC functions declared as resources. Escalate-not-bind.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5")
dbutils.widgets.text("warehouse_id", "a3b61648ea4809e3")
dbutils.widgets.text("genie_space_id", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fm_endpoint = dbutils.widgets.get("fm_endpoint")
warehouse_id = dbutils.widgets.get("warehouse_id")
genie_space_id = dbutils.widgets.get("genie_space_id")
fqn = f"{catalog}.{schema}"

import json, os, uuid

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse

mlflow.set_registry_uri("databricks-uc")

SYSTEM = (
    "You are the Underwriting AI supervisor at Bricksurance SE, a UK commercial insurer. You help underwriters "
    "and the head of underwriting decide on broker submissions. You have tools that call real Unity Catalog "
    "functions — USE THEM to ground every claim in numbers; never invent figures. For a submission, typically "
    "call get_dossier, get_appetite, get_authority, get_accumulation, get_price, get_screening and "
    "get_recommendation, then give the call (quote / refer / decline / request information) with 2-3 quantified "
    "reasons, the terms and subjectivities, and who should sign it. COMPLIANCE RULES: screening/watchlist "
    "findings are INTERNAL — if asked to word anything broker-facing, decline reasons cite appetite only; a true "
    "OFSI match means freeze and escalate to compliance. You advise and escalate; humans quote, refer, decline "
    "and bind — never say you have bound or issued anything.")

TOOLS = {
    "get_dossier": ("Dossier header for a submission: company, trade, channel, broker, sums insured, turnover mismatch vs filed accounts (Insurance Act 2015 fair presentation), flood band + EA river evidence, crime counts, document-extraction hazards.", "submission_public_id"),
    "get_appetite": ("Appetite check: core/selective/excluded per the underwriting guide, hazard grade, coded decline reason + guide citation.", "submission_public_id"),
    "get_authority": ("Authority check: minimum grade whose limits cover this submission, e-trade eligibility, referral route, suggested named underwriter, triggers.", "submission_public_id"),
    "get_accumulation": ("Marginal property accumulation per district vs capacity appetite before/after binding (>=80% referral, >=100% breach).", "submission_public_id"),
    "get_price": ("Technical price build-up: components + named loadings (crime-derived theft, flood, claims experience), IPT, broker target, adequacy %, verdict.", "submission_public_id"),
    "get_screening": ("Sanctions & watchlist screening vs the REAL OFSI consolidated list + internal watchlist, with false-positive resolutions. Findings are INTERNAL ONLY.", "submission_public_id"),
    "get_underinsurance": ("Underinsurance check: declared buildings SI vs rebuild benchmark, BI indemnity-period adequacy.", "submission_public_id"),
    "get_recommendation": ("The composed rules recommendation: quote/refer/decline/request_information with reasons, terms, subjectivities, external vs internal reasons.", "submission_public_id"),
    "get_triage_score": ("ML bind-propensity score (P(bind), priority band) from model_triage_priority.", "submission_public_id"),
    "get_risk_score": ("ML large-loss propensity (risk quality band) from model_risk_quality.", "submission_public_id"),
    "ask_the_book": ("Ask a natural-language analytics question over the underwriting marts (funnel, accumulation, brokers, renewals) via AI/BI Genie.", "question"),
}


def _run_sql(sql):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient()
    wid = os.environ.get("AGENT_WAREHOUSE_ID", warehouse_id)
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wid, wait_timeout="50s")
    if r.status and r.status.state == StatementState.FAILED:
        raise RuntimeError(r.status.error.message if r.status.error else "SQL failed")
    if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def _genie_ask(space_id, question):
    from databricks.sdk import WorkspaceClient
    if not space_id or not question:
        return {"note": "Genie space not configured"}
    try:
        w = WorkspaceClient()
        m = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
        out = {"answer": None, "query": None}
        for att in (m.attachments or []):
            if att.text and att.text.content:
                out["answer"] = att.text.content[:1500]
            if att.query and att.query.query:
                out["query"] = att.query.query[:600]
        return out
    except Exception as e:  # noqa: BLE001
        return {"error": f"genie unavailable: {e}"}


def _call_fm(endpoint, messages, tools):
    import requests
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    hdr = w.config._header_factory()
    r = requests.post(f"{host}/serving-endpoints/{endpoint}/invocations",
                      headers={**hdr, "Content-Type": "application/json"},
                      json={"messages": messages, "tools": tools, "tool_choice": "auto",
                            "max_tokens": 1400, "temperature": 0.1}, timeout=120)
    r.raise_for_status()
    return r.json()


FN_BY_TOOL = {
    "get_dossier": "fn_extract_summary", "get_appetite": "fn_appetite_check",
    "get_authority": "fn_authority_check", "get_accumulation": "fn_accumulation_impact",
    "get_price": "fn_technical_price", "get_screening": "fn_sanctions_screen",
    "get_underinsurance": "fn_underinsurance_check", "get_recommendation": "fn_recommendation",
    "get_triage_score": "fn_triage_score", "get_risk_score": "fn_risk_score",
}


class UnderwritingSupervisor(ChatAgent):
    def __init__(self, catalog, schema, fm_endpoint, genie_space_id):
        self.fqn = f"{catalog}.{schema}"
        self.fm = fm_endpoint
        self.genie = genie_space_id

    def _scalar(self, fn, arg):
        rows = _run_sql(f"SELECT to_json({self.fqn}.{fn}('{arg}')) AS r")
        return json.loads(rows[0]["r"]) if rows and rows[0].get("r") else {"error": "no row"}

    def _tool(self, name, args):
        a = args or {}
        if name == "ask_the_book":
            return _genie_ask(self.genie, a.get("question", ""))
        fn = FN_BY_TOOL.get(name)
        if not fn:
            return {"error": f"unknown tool {name}"}
        return self._scalar(fn, a.get("submission_public_id", "").replace("'", ""))

    def predict(self, messages, context=None, custom_inputs=None) -> ChatAgentResponse:
        ci = custom_inputs or {}
        hint = ""
        if ci.get("submission_public_id"):
            hint = (f"\nThe submission under review is submission_public_id='{ci['submission_public_id']}'. "
                    f"Pass this id to the submission tools.")
        full = [{"role": "system", "content": SYSTEM + hint}]
        for m in messages:
            full.append({"role": m.role, "content": m.content or ""})
        tools = [{"type": "function", "function": {"name": n, "description": TOOLS[n][0],
                  "parameters": {"type": "object", "properties": {TOOLS[n][1]: {"type": "string"}},
                                 "required": [TOOLS[n][1]]}}} for n in TOOLS]
        trace, final = [], ""
        for _hop in range(8):
            resp = _call_fm(self.fm, full, tools)
            choices = resp.get("choices") or []
            if not choices:
                break
            msg = choices[0].get("message") or {}
            tcs = msg.get("tool_calls") or []
            if tcs:
                full.append({"role": "assistant", "content": msg.get("content") or "", "tool_calls": tcs})
                for tc in tcs:
                    fnm = (tc.get("function") or {}).get("name")
                    raw = (tc.get("function") or {}).get("arguments") or "{}"
                    try:
                        a = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception:  # noqa: BLE001
                        a = {}
                    res = self._tool(fnm, a)
                    trace.append({"tool": fnm, "args": a})
                    full.append({"role": "tool", "tool_call_id": tc.get("id") or fnm,
                                 "content": json.dumps(res, default=str)[:8000]})
                continue
            final = msg.get("content") or ""
            break
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=final, id=str(uuid.uuid4()))],
            custom_outputs={"trace": trace, "model": self.fm})

# COMMAND ----------

# Local smoke before logging — proves the tool loop calls real UC functions.
_local = UnderwritingSupervisor(catalog, schema, fm_endpoint, genie_space_id)
_r = _local.predict([ChatAgentMessage(role="user", content="Should we quote this? Give the call, terms and who signs.", id="u1")],
                    custom_inputs={"submission_public_id": "sub:900002"})
print("LOCAL:", _r.messages[0].content[:600])
print("TOOLS CALLED:", [t["tool"] for t in (_r.custom_outputs or {}).get("trace", [])])
assert (_r.custom_outputs or {}).get("trace"), "supervisor must call real tools"

# COMMAND ----------

# MAGIC %md ## Log + register + deploy

# COMMAND ----------

from mlflow.models.resources import (DatabricksFunction, DatabricksGenieSpace,
                                     DatabricksServingEndpoint, DatabricksSQLWarehouse, DatabricksTable)
from databricks.sdk import WorkspaceClient

FNS = ["fn_extract_summary", "fn_appetite_check", "fn_authority_check", "fn_accumulation_impact",
       "fn_technical_price", "fn_sanctions_screen", "fn_underinsurance_check", "fn_recommendation",
       "fn_triage_score", "fn_risk_score"]
resources = [DatabricksServingEndpoint(endpoint_name=fm_endpoint),
             DatabricksSQLWarehouse(warehouse_id=warehouse_id)]
# fn_triage_score / fn_risk_score wrap ai_query on the model endpoints — the agent SP needs
# CAN_QUERY on those too (the nested ai_query is a separate resource).
_w = WorkspaceClient()
for ep in ["underwriting-triage", "underwriting-risk"]:
    try:
        nm = next(e.name for e in _w.serving_endpoints.list() if ep in e.name and "profile" not in e.name)
        resources.append(DatabricksServingEndpoint(endpoint_name=nm))
    except StopIteration:
        print("skip ep resource", ep)
resources += [DatabricksFunction(function_name=f"{fqn}.{fn}") for fn in FNS]
for t in ["silver_submissions", "silver_locations_enriched", "gold_accumulation", "gold_underinsurance",
          "feature_submission", "ref_appetite", "ref_authority_matrix", "ref_underwriter", "ref_rate_guide",
          "ref_sanctions_ofsi", "ref_internal_watchlist", "ref_crime_open", "ref_flood_open"]:
    resources.append(DatabricksTable(table_name=f"{fqn}.{t}"))
if genie_space_id:
    resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))

agent_uc_name = f"{fqn}.underwriting_agent"
input_example = {"messages": [{"role": "user", "content": "Should we quote this? Give the call."}],
                 "custom_inputs": {"submission_public_id": "sub:900002"}}
with mlflow.start_run(run_name="underwriting_supervisor_agent"):
    mi = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=UnderwritingSupervisor(catalog, schema, fm_endpoint, genie_space_id),
        resources=resources, input_example=input_example,
        registered_model_name=agent_uc_name,
        pip_requirements=["mlflow", "databricks-sdk>=0.30.0", "requests"])
    print("logged:", mi.model_uri)

from mlflow.tracking import MlflowClient

mc = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in mc.search_model_versions(f"name='{agent_uc_name}'"))

from databricks import agents

dep = agents.deploy(model_name=agent_uc_name, model_version=version, scale_to_zero=True,
                    environment_vars={"AGENT_WAREHOUSE_ID": warehouse_id},
                    tags={"project": "underwriting_workbench", "layer": "agent"})
ep_name = getattr(dep, "endpoint_name", None) or getattr(dep, "endpoint", None)
print("agents.deploy →", ep_name)
dbutils.notebook.exit(json.dumps({"endpoint_name": str(ep_name), "version": version}))
