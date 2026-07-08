# Databricks notebook source
# MAGIC %md
# MAGIC # 06a · Role agents — Underwriting AI (narrate-only)
# MAGIC
# MAGIC One pyfunc model, five serving endpoints differentiated by `AGENT_ROLE`. Each receives
# MAGIC the structured findings the app computed from the UC functions (`data_json`) and writes
# MAGIC prose. **Escalate-not-bind**: agents advise, draft and challenge; a named underwriter
# MAGIC acts. The broker-comms role is hard-constrained: decline letters cite appetite ONLY —
# MAGIC screening/watchlist reasons never leave the audit trail.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"

import mlflow
import pandas as pd
from mlflow.models.signature import infer_signature
from mlflow.models.resources import DatabricksServingEndpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

class UnderwritingAgent(mlflow.pyfunc.PythonModel):
    """Narrate-only FM-backed agent. Input columns: role, question, data_json → prose per row."""

    def load_context(self, context):
        import os
        from mlflow.deployments import get_deploy_client
        self.client = get_deploy_client("databricks")
        self.fm = os.environ.get("FM_ENDPOINT", "databricks-claude-sonnet-4-5")
        self.default_role = os.environ.get("AGENT_ROLE", "challenge")
        self.systems = {
            "risk_profile": (
                "You are the risk analyst on a UK commercial underwriting desk at Bricksurance SE. From the "
                "structured findings (company profile, trade, enrichment: flood band with EA river evidence, "
                "crime counts, EPC/MEES lens, claims cohort), write a tight 3-4 sentence risk profile an "
                "underwriter can read in ten seconds. Note any fair-presentation concern (Insurance Act 2015) "
                "factually. You inform; you never bind."),
            "appetite": (
                "You are the appetite and portfolio-fit advisor at Bricksurance SE. From the structured findings "
                "(appetite status, guide section, accumulation utilisation, authority triggers), explain in 2-3 "
                "sentences why this risk is in/out of appetite and what would need to change. Cite the guide "
                "section code. You advise; you never bind."),
            "pricing_adequacy": (
                "You are the pricing adequacy reviewer at Bricksurance SE. From the technical price build-up "
                "(components, named loadings incl. crime-derived theft and flood, IPT, broker target, adequacy %), "
                "give a 2-3 sentence view on whether the target is achievable and where the negotiation room is. "
                "The full GLM engine is the Pricing Workbench; this is the desk view. You advise; you never bind."),
            "broker_comms": (
                "You draft broker communications for Bricksurance SE underwriters. From the structured decision "
                "findings, draft the requested letter (quote / decline / request for information) as a short, "
                "professional UK commercial insurance letter. HARD RULES: (1) a DECLINE letter cites appetite and "
                "the underwriting guide ONLY — NEVER mention screening, sanctions, watchlists or internal "
                "intelligence, whatever the findings contain; (2) a QUOTE letter must show premium, IPT at 12%, "
                "total payable, commission basis, and list every term and subjectivity verbatim; (3) an "
                "INFORMATION REQUEST lists the missing items as bullets with a 14-day response window. "
                "End 'This letter is a draft for underwriter review and approval.' A human approves and sends."),
            "challenge": (
                "You are the challenge / second-pair-of-eyes underwriter at Bricksurance SE, with a conduct lens. "
                "Argue the OTHER side of the recommendation in 3-4 quantified sentences: if it says quote, surface "
                "the residual risk; if it says decline/refer, note mitigants and whether the customer outcome is "
                "fair and consistent (coded declinature, no cherry-picking). You challenge and escalate; you never bind."),
        }

    def _one(self, role, question, data_json):
        system = self.systems.get(role or self.default_role, self.systems["challenge"])
        user = (f"Request: {question}\n\nStructured findings (already computed by Databricks UC functions — "
                f"narrate, never recompute or invent figures):\n{data_json}")
        try:
            resp = self.client.predict(endpoint=self.fm, inputs={
                "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}],
                "max_tokens": 700, "temperature": 0.2})
            return resp["choices"][0]["message"]["content"]
        except Exception as e:  # noqa: BLE001 — surface, never crash the endpoint
            return f"[narration unavailable: {str(e)[:120]}]"

    def predict(self, context, model_input):
        return [self._one(r.get("role"), r.get("question", ""), r.get("data_json", ""))
                for _, r in model_input.iterrows()]

# COMMAND ----------

example = pd.DataFrame([{"role": "challenge", "question": "Second opinion on sub:900002?",
                         "data_json": '{"action":"refer","hx7_post_util_pct":87.0}'}])
sig = infer_signature(example, ["..."])
with mlflow.start_run(run_name="underwriting_agent"):
    mi = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=UnderwritingAgent(),
        signature=sig, input_example=example,
        pip_requirements=["mlflow", "pandas"],
        resources=[DatabricksServingEndpoint(endpoint_name=FM)],
        registered_model_name=f"{fqn}.model_underwriting_agent")
ver = mi.registered_model_version
print("agent model v", ver)

# COMMAND ----------

w = WorkspaceClient()


def deploy_agent(endpoint, role):
    entity = ServedEntityInput(name="agent", entity_name=f"{fqn}.model_underwriting_agent",
                               entity_version=ver, workload_size="Small", scale_to_zero_enabled=True,
                               environment_vars={"AGENT_ROLE": role, "FM_ENDPOINT": FM})
    existing = [e.name for e in w.serving_endpoints.list()]
    if endpoint in existing:
        w.serving_endpoints.update_config(name=endpoint, served_entities=[entity])
    else:
        w.serving_endpoints.create(name=endpoint, config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity]))
    print("deploying", endpoint, "(role:", role + ")")


deploy_agent("underwriting-riskprofile", "risk_profile")
deploy_agent("underwriting-appetite", "appetite")
deploy_agent("underwriting-adequacy", "pricing_adequacy")
deploy_agent("underwriting-comms", "broker_comms")
deploy_agent("underwriting-challenge", "challenge")
print("✅ 06a — 5 role agents deploying (non-blocking)")
