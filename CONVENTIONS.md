# CONVENTIONS.md — Underwriting Workbench

Mirrors `reinsurance_workbench` (the framework template), `claims_workbench` (data-universe +
ingestion UI patterns), `pricing-workbench` (app shell origin), and `agentic-underwriting-v2`
(`ai_parse_document` pattern) — all in `~/vibe`. Single source of conventions. **Mirror, don't
invent.** Domain spec + sacred heroes live in `UNDERWRITING_WORKBENCH_BUILD_BRIEF.md`.

## Deploy target
- **Workspace (dev):** `fevm-lr-dev-aws-us`, CLI profile `DEV`.
- **Catalog:** variable `catalog` (default `lr_dev_aws_us_catalog`). Portable — change one variable.
- **Schema (fixed):** `underwriting_workbench`.
- **Warehouse:** `a3b61648ea4809e3` (Serverless Starter).
- **FM endpoint:** `databricks-claude-sonnet-4-5` (Claude via FM API; batch-ai_query-capable — NOT sonnet-5).
- All compute serverless: DLT `serverless: true`; jobs `environment_key` + `client: "5"`, no clusters.

## DAB layout
```
databricks.yml            # bundle + variables + targets (dev default, shared stub) + include resources/*.yml
resources/*.yml           # one file per pipeline / job / app
notebooks/                # numbered: 00_setup_and_data_generation, 00b_landing_files, 01_bronze_dlt,
                          #   01b_file_ingest, 01c_doc_extraction, 02_silver_dlt, 03_gold_dlt,
                          #   03b_dq_scorecard, 04_features, 05_models, 05b_crux, 05c_whatif,
                          #   06_agent_tools, 06a_agents, 06b_supervisor_agent, 07_governance,
                          #   91_companies_house_live_api_example, 98_smoke_test, 99_reset
app/                      # thin FastAPI + self-contained dist/index.html (no npm egress)
data/open/                # bundled real open-data extracts (OGL) → copied to Volume open_data
dashboards/               # Lakeview JSON
docs/ scripts/
```
- Notebook paths in `resources/*.yml` are relative to `resources/` → `../notebooks/...`.
- Deployed notebook references have **no `.py` extension**.

## Portability
- `${var.catalog}` → job/pipeline `base_parameters`/`configuration` → `dbutils.widgets.get(...)`
  → `fqn = f"{catalog}.{schema}"`. **Never hardcode** catalog/schema/IDs.
- DLT notebooks read `spark.conf.get("source_catalog"/"source_schema")` from pipeline `configuration`.
- App reads env vars only (`app.yaml`): CATALOG_NAME, SCHEMA_NAME, WAREHOUSE_ID (valueFrom
  sql_warehouse), USE_CACHE, GENIE_SPACE_ID, DASHBOARD_ID, FM_ENDPOINT. Helper `fqn(table)`.
- **Deployability is a feature** (user req): DEPLOY.md carries the full asset inventory; the smoke
  test doubles as the installed-assets check on a fresh workspace.

## DLT style (mirror reinsurance 01_bronze_dlt_pipeline)
- `@dlt.table(..., table_properties={"quality":"bronze","layer":"bronze"})`.
- `@dlt.expect(...)` track+retain, `@dlt.expect_or_drop(...)` drop.
- **Quarantine** = mirror table reading the landing source, filtering the failing predicate,
  `quarantine_reason` + `_quarantined_at`. One seeded drifted risk schedule (`rescuedDataColumn`).
- `_bronze_ingested_at = current_timestamp()`.

## UC functions
- `CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_*` RETURNS STRUCT, rich COMMENT (supervisor
  routes off it). Scorers: pre-aggregate the feature row (`any_value`) then
  `ai_query('{EP}', named_struct(...), 'ARRAY<DOUBLE>')` — scalar UDF bodies must be provably
  one row (MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY gotcha; denormalize onto silver).
- Resolve endpoint names at fn-creation time by substring (dev-prefix + truncation safe).
- **GOTCHA:** `CREATE OR REPLACE FUNCTION` revokes EXECUTE grants → reset never recreates fns;
  re-deploy agents after recreating fns.
- ai_query prompt strings: SQL-escape single quotes (`' → ''`).

## Models + Feature Store
- `fe.create_training_set(FeatureLookup(feature_submission, lookup_key='submission_public_id'))`.
- Log with native flavor (lightgbm — NOT mlflow.sklearn, dep-omission gotcha), signature +
  input_example; `registered_model_name = {catalog}.{schema}.model_*`; alias `champion`.
- Serving: `resources`-declared or imperative; scale-to-zero Small. Feature-vector contract (no
  online store): UC fn pre-fetches features and passes the struct.

## Agents
- Narrate-only role agents: ONE pyfunc model (`model_underwriting_agent`), one endpoint per role
  via `AGENT_ROLE` env (risk_profile, appetite, pricing_adequacy, broker_comms, challenge).
  FM endpoint declared as `DatabricksServingEndpoint` resource at log time (serving-time creds).
- ONE real tool-calling supervisor: `mlflow.pyfunc.ChatAgent` Claude tool-use loop over the crux
  UC fns + Genie; `databricks.agents.deploy` with DatabricksFunction/Table/GenieSpace/SQLWarehouse
  resources; tool-call trace surfaced in the app.
- **Escalate-not-bind.** Endpoint names resolved by substring everywhere.

## Cache wrapper (mirror reinsurance agents.py)
- Delta `cache_agent_responses`; key sha256(endpoint+canonical_json)[:32]; MERGE upsert; NO TTL —
  cached mode fills on miss, live mode overwrites. Narration ONLY. `USE_CACHE` env default,
  per-request `?cache=1/0`. Reset clears + re-warms all three heroes.

## App (thin)
- FastAPI `app.py` + `server/{config,sql,agents}.py`; `sql.py` = Statement Execution API,
  INLINE disposition, `query_many` ThreadPool concurrency; values come back as strings → cast.
- Frontend: self-contained `app/dist/index.html` (vanilla JS, hash routing, render-fn map).
  Theme = reinsurance `:root` vars verbatim (slate #1e293b/#0f172a sidebar gradient, blue
  #2563eb/#60a5fa, white 12px-radius cards, tone-* agent tiles, Learn tile, cache toggle amber
  CACHED / emerald LIVE, Reset button, About-this-demo disclaimer).
- **Genie + dashboard EMBEDDED, not linked** (user req): Genie answered in-app via
  `w.genie.start_conversation_and_wait` + embed iframe; dashboard published with
  `embed_credentials=True` and rendered in an iframe.
- Every panel calls a real UC fn / endpoint / Genie / SQL. No logic in the app.
- App SP grants post-first-create (imperative): schema USE/SELECT/EXECUTE/MODIFY/CREATE TABLE ·
  warehouse CAN_USE · CAN_QUERY every serving/agent endpoint · reset job CAN_MANAGE_RUN ·
  Genie space CAN_RUN · dashboard CAN_READ.

## Reset + smoke
- Reset job: data_gen (seed=42, rolling dates) → landing files → medallion full_refresh →
  features → governance re-seed + cache clear/re-warm. retrain=false; fns NOT recreated.
- Smoke (P10): one-row-per-step PASS/FAIL, fails loudly; includes the **installed-assets
  checklist** (tables, volumes, fns, models, endpoints, Genie, dashboard, app) so it verifies a
  fresh deployment end-to-end.

## Disclaimer + hygiene
- "About this demo" box on landing + sidebar: synthetic insurer, real Databricks objects, open
  data provenance labelled, illustrative elements labelled. **No "WOW" branding.** Single schema,
  numbered layers. Push to `wryszka/underwriting-workbench` (public) after each phase.
