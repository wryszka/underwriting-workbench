# ARCHITECTURE.md — asset map & design decisions

```
data/open/*.csv (REAL OGL extracts, bundled at build time)
        │                                    scripts/fetch_open_data.py (build-time only)
        ▼
00_setup_and_data_generation ──► landing_* + ref_* tables · Volumes (open_data, submission_inbox…)
00b_landing_files ─────────────► emails / PDFs (fpdf2) / schedules incl. drifted v1 → submission_inbox
01c_doc_extraction ────────────► ai_parse_document + ai_query → landing_doc_extractions (confidence)
underwriting_medallion (DLT) ──► 01 bronze (+expectations, 3 quarantine mirrors)
                                 02 silver (enriched submission record + per-location)
                                 03 gold (funnel · portfolio · accumulation · brokers · adequacy ·
                                          renewals · underinsurance · lifecycle)
03b_dq_scorecard ──────────────► gold_dq_scorecard (event log) + gold_ingestion_sources
04_features ───────────────────► feature_submission (UC Feature Store, PK submission_public_id)
05_models ─────────────────────► model_triage_priority + model_risk_quality (@champion)
                                 endpoints underwriting-triage / underwriting-risk (imperative, s2z)
05b_crux ──────────────────────► the DECISION ENGINE: fn_appetite_check · fn_authority_check ·
                                 fn_accumulation_impact · fn_technical_price · fn_sanctions_screen ·
                                 fn_underinsurance_check · fn_recommendation (+ fn_extract_summary)
05c_whatif ────────────────────► fn_price_whatif · fn_accumulation_whatif
06_agent_tools ────────────────► fn_triage_score / fn_risk_score (ai_query wrappers) +
                                 gold_inbox_priority (BATCH scoring — no interactive cold starts)
06a_agents ────────────────────► model_underwriting_agent → 5 narrate-only endpoints by AGENT_ROLE
06b_supervisor_agent ──────────► underwriting_agent (ChatAgent tool loop) via agents.deploy
07_governance ─────────────────► gold_decision_audit (hero seed reconciles with live fns) ·
                                 gov_data_inventory · gold_ai_activity · UC mask gov_watchlist_secure
app/ ──────────────────────────► FastAPI (server/{config,sql,agents}.py) + dist/index.html SPA
99_reset / 98_smoke_test ──────► reset chain (no retrain, no fn recreation) · smoke = asset checklist
                                 + invariants + the three heroes' exact outcomes
```

## Design decisions worth knowing

- **Decision engine = deterministic UC SQL functions**, not ML — an underwriter must defend
  every check line-by-line. ML advises (priority, risk quality) and is served separately.
- **Speed model** (claims_workbench lesson): interactive pages never hit scale-to-zero model
  endpoints. The inbox is batch-scored (`gold_inbox_priority`) in the pipeline; live model
  calls exist only behind explicit user actions.
- **Scalar UDF bodies aggregate** (`any_value`, `collect_list`) so they are provably one row —
  avoids MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY.
- **Cache wraps narration only.** Structured checks/prices are always live. sha256 key, MERGE
  upsert, no TTL; reset clears and the app re-warms all three heroes.
- **External vs internal reasons** are separate columns end-to-end; the broker-comms agent has
  a hard rule and the smoke test asserts the separation (hero 900003).
- **Open data is real and bundled** — fetched once at build time, never at demo time; every
  dataset carries a provenance label in the UI (incl. the honest GMP crime-data gap).
- **Genie + dashboard are embedded** in Insight (embed URLs + in-app Genie API), not linked out.
- **Deployability is tested**: smoke group A is the installed-assets checklist for any workspace.
