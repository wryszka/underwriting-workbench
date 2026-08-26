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
| Reference tables | `ref_broker` · `ref_underwriter` · `ref_appetite` · `ref_authority_matrix` · `ref_rate_guide` · `ref_rebuild_benchmark` · `ref_internal_watchlist` · `ref_district_capacity` · `ref_feature_encodings` + open data: `ref_sanctions_ofsi` · `ref_flood_open` · `ref_crime_open` · `ref_epc_mix_open` · `ref_postcode_centroid` · `ref_treaty_structure` |
| Bronze (DLT) | `bronze_submissions` · `bronze_schedule_locations` · `bronze_documents` · `bronze_doc_extractions` · `bronze_pas_*` · `bronze_company_profiles` + 3 quarantine mirrors |
| Silver (DLT) | `silver_submissions` · `silver_locations_enriched` |
| Gold | `gold_pipeline_funnel` · `gold_portfolio_position` · `gold_accumulation` · `gold_broker_scorecard` · `gold_rate_adequacy` · `gold_renewals` · `gold_underinsurance` · `gold_submission_lifecycle` · `gold_dq_scorecard` · `gold_ingestion_sources` · `gold_inbox_priority` · `gold_decision_audit` · `gold_comms_drafts` · `gold_ai_activity` · `gov_data_inventory` · `gold_subjectivity_tracker` · `gold_auto_bound` · `gold_decision_packs` · `gov_guide_changes` + views `gov_watchlist_secure` · `gov_conduct_declines` · event log `medallion_event_log` |
| Lane E tables | referral & discretion analytics: `ref_referral_rules` (+ `rule_scope`, `review_effort_hours`) · `ref_underwriter_persona` · `ref_adjustment_reasons` · `silver_submission_wageroll` · `gold_referral_events` · `gold_transactions` · `gold_premium_components` · `landing_referral_events_generated` |
| Lane E.1 source feeds | `raw_pas_referrals` · `raw_etrade_referrals` · `raw_case_outcomes` · `raw_case_bridge` · `raw_authority_matrix` · `ref_rule_code_map` (code→rule demo artefact) → DLT `underwriting_referral_conformance` → `silver_referral_events_conformed` · `silver_referral_orphans` (quarantine) · `silver_authority_drift` (threshold drift) |
| Referral Control | SCD2 `ref_referral_rules` (versioned; + `disposition` / `compliance_lock` / `review_effort_hours`) · `gold_referral_telemetry` (fire-vector, no short-circuit, ~3yr backfill + 90d future book, `as_of_date` time-travel) · `gold_rule_effectiveness` (recomputed from telemetry) · `gold_referral_findings` (4 `as_of` snapshots) · `gold_rule_cofire_partners` · `gold_rule_changes` (change ledger + SCD2 write-path + predicted-vs-realised + `drift_flag`) — **supersedes the E8/E9 `gold_rule_recommendations`** |
| Lane E metric view | `mv_underwriting_discipline` (UC Metric View — the semantic trunk; extended with rubber_stamp/decline/changed-answer + value_band) + base view `gold_discipline_base` |
| Lane E / Referral Control notebooks | `00c_lane_e_setup` · `00d_source_feed_simulation` · `00e_referral_registry` · `00f_referral_telemetry` · `01d_wageroll_extraction` · `05d_lane_e_crux` · `05e_referral_control_fns` · `03c_gold_referral_events` · `02d_referral_conformance` (DLT) · `07d_referral_detection` · `07e_referral_governance` · `08_metric_views` · `09_external_agent_governed_call` — (`07b_rule_effectiveness` / `07c_rule_tuning_agent` RETIRED) |
| Feature Store | `feature_submission` (PK submission_public_id) |
| UC functions (24) | crux: `fn_extract_summary` `fn_appetite_check` `fn_authority_check` `fn_accumulation_impact` `fn_technical_price` `fn_sanctions_screen` `fn_underinsurance_check` `fn_treaty_check` `fn_recommendation` `fn_rule_threshold` `fn_rule_version` · Lane E: `fn_wageroll_check` `fn_referral_events_from_checks` · Referral Control: `fn_rule_metrics` `fn_isolation_analysis` `fn_recommend_action` `fn_emulate_rule_change` · ML: `fn_triage_score` `fn_risk_score` · what-if: `fn_price_whatif` `fn_accumulation_whatif` `fn_mta_check` · mask: `mask_watchlist` |
| UC models (4) | `model_triage_priority` · `model_risk_quality` (@champion) · `model_underwriting_agent` · `underwriting_agent` |
| Serving endpoints (11) | `underwriting-triage` · `underwriting-risk` · `underwriting-riskprofile` · `underwriting-appetite` · `underwriting-adequacy` · `underwriting-comms` · `underwriting-challenge` · `underwriting-brief` · `underwriting-portfolio` (Referral Control advisor) · `underwriting-reviewer` (Referral Control reviewer) · `agents_…underwriting_agent` (auto-named — resolve by substring) |
| Pipeline | `underwriting_medallion` (serverless DLT) |
| Jobs (6) | `underwriting_00_setup` · `underwriting_01_ingest` · `underwriting_05_ml` · `underwriting_06_ai` · `underwriting_06b_agent` · `underwriting_99_reset` · `underwriting_98_smoke_test` |
| Genie spaces | "Underwriting — Ask the Book (Bricksurance SE)" (metric view + facts) · "Referral Control — Ask the Rulebook" (`space_id 01f1a14c775e12458d4998f97c00cd59` — telemetry / findings / rules / changes) |
| Dashboard | "Underwriting Portfolio" (Lakeview, published with embedded credentials) |
| Builder-beat notebook | `09_external_agent_governed_call` — read-only demo: a foreign agent runtime calls governed UC functions and the conduct mask holds (positioning proof; not in reset) |
| App | `underwriting-workbench` (FastAPI + self-contained SPA) |
| MCP — managed servers (Lane F) | `/api/2.0/mcp/functions/<catalog>/underwriting_workbench` (F2/F3/F4 read UC-function tools; OBO — caller is the principal) · `/api/2.0/mcp/genie/<space_id>` (both Genie spaces). Trust boundary = per-principal EXECUTE grants |
| MCP — custom server (Lane F) | Databricks App `uw-mcp` (FastMCP) — the mutating tools (submit_risk/upload_document/get_submission_status/get_quote/respond_to_subjectivity + propose_rule_change); all return `pending_human_approval`; row-filtered, rate-limited, structured refusals, per-call audit |
| MCP — objects/principals | `gold_mcp_activity` (audit) · `gold_mcp_proposals` (pending) · `ref_mcp_broker_identity` · `mcp_broker_submissions` (UC row-filter view) · SPs `uw_broker_agent` + `uw_governance_agent` (read-only). Read fns in `05f_mcp_functions`. Harness `scripts/mcp_demo_harness.py`; seed `92_mcp_seed` (hostile doc). Docs: MCP_ARCHITECTURE / MCP_TOOL_CONTRACTS / MCP_DEMO_RUN / MCP_GOVERNANCE_NOTES |

**Verification is automated:** `underwriting_98_smoke_test` step group A checks every asset
above exists, then verifies data invariants and the three heroes end-to-end. On a fresh
install, all steps PASS or the job fails loudly.

## Where things live (workspace legibility)

Production deployment root: **`/Workspace/Shared/underwriting_workbench`** (notebooks under
`files/notebooks`, app under `files/app`) — any workspace user can find and reuse them. Jobs
and the pipeline carry clean `underwriting_*` names + descriptions and `project` tags; every
table carries `layer`/`demo` UC tags and the schema has a self-describing comment (set by
notebook 07). Serving endpoints are `underwriting-*`.

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
#    To ADD Lane E assets to an EXISTING space in place (keeps the embedded id), pass the id as arg 5:
#    python3 scripts/create_genie_space.py <PROFILE> <WH> <CATALOG> underwriting_workbench <SPACE_ID>

# 5. Agents (role agents + governance, then the tool-calling supervisor)
databricks bundle run underwriting_06_ai -t dev
databricks bundle run underwriting_06b_agent -t dev

# 6. Dashboard: python3 scripts/create_dashboard.py … → publish with embed_credentials=True
#    → set dashboard_id in databricks.yml vars AND app/app.yaml DASHBOARD_ID

# 7. App (second deploy picks up app.yaml changes)
databricks bundle deploy -t dev
databricks apps deploy underwriting-workbench --source-code-path \
  /Workspace/Users/<you>/.bundle/underwriting-workbench/dev/files/app

# 8. GRANT the app service principal — one command (auto-discovers SP, endpoints, reset job):
python3 scripts/grant_app_sp.py <PROFILE> <CATALOG> underwriting_workbench <WAREHOUSE_ID> <GENIE_ID> <DASHBOARD_ID>
#    (grants: catalog/schema incl. CREATE TABLE for the cache · comms_out volume R/W ·
#     CAN_QUERY on every underwriting + supervisor endpoint · reset job CAN_MANAGE_RUN ·
#     Genie CAN_RUN · dashboard CAN_READ — then restart the app once)

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
