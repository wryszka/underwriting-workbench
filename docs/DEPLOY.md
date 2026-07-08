# DEPLOY.md — fresh-workspace runbook + asset inventory

Deployable to any Databricks workspace with: Unity Catalog (a catalog you can `CREATE SCHEMA`
in), serverless jobs/DLT/SQL warehouse, Mosaic AI Model Serving + Agent Framework, and a
batch-capable Claude FM endpoint (`databricks-claude-sonnet-4-5`). **Catalog is the single
portability anchor** — override one variable.

## What gets installed (the asset inventory)

| Kind | Names |
|---|---|
| Schema | `<catalog>.underwriting_workbench` (everything lives here) |
| Volumes (4) | `submission_inbox` · `open_data` · `ingest_checkpoints` · `comms_out` |
| Landing tables | `landing_pas_policies` · `landing_pas_claims` · `landing_submissions_feed` · `landing_company_profiles` · `landing_doc_extractions` |
| Reference tables | `ref_broker` · `ref_underwriter` · `ref_appetite` · `ref_authority_matrix` · `ref_rate_guide` · `ref_rebuild_benchmark` · `ref_internal_watchlist` · `ref_district_capacity` · `ref_feature_encodings` + open data: `ref_sanctions_ofsi` · `ref_flood_open` · `ref_crime_open` · `ref_epc_mix_open` · `ref_postcode_centroid` |
| Bronze (DLT) | `bronze_submissions` · `bronze_schedule_locations` · `bronze_documents` · `bronze_doc_extractions` · `bronze_pas_*` · `bronze_company_profiles` + 3 quarantine mirrors |
| Silver (DLT) | `silver_submissions` · `silver_locations_enriched` |
| Gold | `gold_pipeline_funnel` · `gold_portfolio_position` · `gold_accumulation` · `gold_broker_scorecard` · `gold_rate_adequacy` · `gold_renewals` · `gold_underinsurance` · `gold_submission_lifecycle` · `gold_dq_scorecard` · `gold_ingestion_sources` · `gold_inbox_priority` · `gold_decision_audit` · `gold_comms_drafts` · `gold_ai_activity` · `gov_data_inventory` + views `gov_watchlist_secure` · `gov_conduct_declines` · event log `medallion_event_log` |
| Feature Store | `feature_submission` (PK submission_public_id) |
| UC functions (13) | crux: `fn_extract_summary` `fn_appetite_check` `fn_authority_check` `fn_accumulation_impact` `fn_technical_price` `fn_sanctions_screen` `fn_underinsurance_check` `fn_recommendation` · ML: `fn_triage_score` `fn_risk_score` · what-if: `fn_price_whatif` `fn_accumulation_whatif` · mask: `mask_watchlist` |
| UC models (4) | `model_triage_priority` · `model_risk_quality` (@champion) · `model_underwriting_agent` · `underwriting_agent` |
| Serving endpoints (8) | `underwriting-triage` · `underwriting-risk` · `underwriting-riskprofile` · `underwriting-appetite` · `underwriting-adequacy` · `underwriting-comms` · `underwriting-challenge` · `agents_…underwriting_agent` (auto-named — resolve by substring) |
| Pipeline | `underwriting_medallion` (serverless DLT) |
| Jobs (6) | `underwriting_00_setup` · `underwriting_01_ingest` · `underwriting_05_ml` · `underwriting_06_ai` · `underwriting_06b_agent` · `underwriting_99_reset` · `underwriting_98_smoke_test` |
| Genie space | "Underwriting — Ask the Book (Bricksurance SE)" |
| Dashboard | "Underwriting Portfolio" (Lakeview, published with embedded credentials) |
| App | `underwriting-workbench` (FastAPI + self-contained SPA) |

**Verification is automated:** `underwriting_98_smoke_test` step group A checks every asset
above exists, then verifies data invariants and the three heroes end-to-end. On a fresh
install, all steps PASS or the job fails loudly.

## Deploy steps (~45–60 min, mostly job runtime)

```bash
# 0. Point the bundle (edit databricks.yml): target host/profile + vars catalog / warehouse_id.
rm -rf .databricks && databricks bundle validate -t dev

# 1. First deploy (jobs + pipeline; app resource may lag until later — fine)
databricks bundle deploy -t dev

# 2. Data + ingest
databricks bundle run underwriting_00_setup -t dev
databricks bundle run underwriting_01_ingest -t dev      # landing files → Document AI → medallion → DQ

# 3. Models + crux (creates the 2 model endpoints imperatively)
databricks bundle run underwriting_05_ml -t dev

# 4. Genie space → capture the id
python3 scripts/create_genie_space.py <PROFILE> <WAREHOUSE_ID> <CATALOG> underwriting_workbench
#    → set genie_space_id in databricks.yml vars AND app/app.yaml GENIE_SPACE_ID

# 5. Agents (role agents + governance, then the tool-calling supervisor)
databricks bundle run underwriting_06_ai -t dev
databricks bundle run underwriting_06b_agent -t dev

# 6. Dashboard: python3 scripts/create_dashboard.py … → publish with embed_credentials=True
#    → set dashboard_id in databricks.yml vars AND app/app.yaml DASHBOARD_ID

# 7. App (second deploy picks up app.yaml changes)
databricks bundle deploy -t dev
databricks apps deploy underwriting-workbench --source-code-path \
  /Workspace/Users/<you>/.bundle/underwriting-workbench/dev/files/app

# 8. GRANT the app service principal (capture SP id from the app page):
#    schema: USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE (cache) on <catalog>.underwriting_workbench
#    warehouse: CAN_USE (declared in app.yml resource)
#    serving: CAN_QUERY on all 8 endpoints (incl. the agents_… supervisor)
#    reset job: CAN_MANAGE_RUN · Genie space: CAN_RUN · dashboard: CAN_READ

# 9. Warm + verify
databricks bundle run underwriting_99_reset -t dev
databricks bundle run underwriting_98_smoke_test -t dev   # expect ALL PASS
```

## Known gotchas

- `bundle deploy` from a sandboxed shell can fail with keychain "exit status 45" — retry unsandboxed.
- Serving/agent endpoint names carry DAB/agents.deploy prefixes per workspace → everything resolves by **substring**.
- `CREATE OR REPLACE FUNCTION` revokes agent EXECUTE grants → reset never recreates fns (06_agent_tools runs `mode=score_only`); if you change fns, re-run `underwriting_06b_agent`.
- `ai_query` needs a batch-inference-capable FM endpoint (sonnet-4-5 ✓, sonnet-5 ✗ on this estate).
- Statement Execution API returns all values as strings — the app casts.
- The open-data files ship in `data/open/` (see PROVENANCE.md); re-fetch with `scripts/fetch_open_data.py` at build time only.
- The app caches endpoint-name resolution (`lru_cache`). If the app started BEFORE the agents were
  deployed, it will 404 on agent calls — **restart the app once after step 5** (`databricks apps stop/start`).
