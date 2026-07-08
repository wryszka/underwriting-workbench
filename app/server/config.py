"""Config — all portability via env vars (set in app.yaml). No hardcoded catalog/schema/IDs."""
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient


def _flag(name, default=True):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


CATALOG = os.getenv("CATALOG_NAME", "lr_dev_aws_us_catalog")
SCHEMA = os.getenv("SCHEMA_NAME", "underwriting_workbench")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "a3b61648ea4809e3")
USE_CACHE = _flag("USE_CACHE", True)
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
DASHBOARD_ID = os.getenv("DASHBOARD_ID", "")
FM_ENDPOINT = os.getenv("FM_ENDPOINT", "databricks-claude-sonnet-4-5")
RESET_JOB_SUBSTR = os.getenv("RESET_JOB_SUBSTR", "underwriting_99_reset")

# Agent / model endpoints are resolved by substring at runtime (dev-prefix safe).
EP_RISKPROFILE_SUBSTR = os.getenv("EP_RISKPROFILE_SUBSTR", "underwriting-riskprofile")
EP_APPETITE_SUBSTR = os.getenv("EP_APPETITE_SUBSTR", "underwriting-appetite")
EP_ADEQUACY_SUBSTR = os.getenv("EP_ADEQUACY_SUBSTR", "underwriting-adequacy")
EP_COMMS_SUBSTR = os.getenv("EP_COMMS_SUBSTR", "underwriting-comms")
EP_CHALLENGE_SUBSTR = os.getenv("EP_CHALLENGE_SUBSTR", "underwriting-challenge")
EP_AGENT_SUBSTR = os.getenv("EP_AGENT_SUBSTR", "underwriting_agent")  # the REAL tool-calling supervisor

CACHE_TABLE = f"{CATALOG}.{SCHEMA}.cache_agent_responses"

ROLE_SUBSTR = {
    "risk_profile": EP_RISKPROFILE_SUBSTR,
    "appetite": EP_APPETITE_SUBSTR,
    "pricing_adequacy": EP_ADEQUACY_SUBSTR,
    "broker_comms": EP_COMMS_SUBSTR,
    "challenge": EP_CHALLENGE_SUBSTR,
}


def fqn(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


@lru_cache(maxsize=16)
def resolve_endpoint(substr: str) -> str:
    try:
        names = [e.name for e in get_workspace_client().serving_endpoints.list()]
        for n in names:
            if substr in n:
                return n
        if substr == EP_AGENT_SUBSTR:
            # agents.deploy auto-names `agents_<catalog>-<schema>-<model>` TRUNCATED to 63 chars —
            # the model name may be cut (e.g. "...-underwritin"), so match on the schema instead.
            for n in names:
                if n.startswith("agents_") and SCHEMA in n:
                    return n
    except Exception:
        pass
    return substr


def workspace_host() -> str:
    h = os.getenv("DATABRICKS_HOST", "")
    if not h:
        try:
            h = get_workspace_client().config.host or ""
        except Exception:
            h = ""
    h = h.rstrip("/")
    if h and not h.startswith("http"):
        h = "https://" + h
    return h
