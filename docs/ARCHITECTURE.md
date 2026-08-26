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
── Lane E · referral & pricing-discretion analytics (ADDITIVE) ─────────────────────────────────
00c_lane_e_setup ──────────────► ref_referral_rules · ref_underwriter_persona · ref_adjustment_reasons ·
                                 silver_submission_wageroll · gold_transactions (NB/RN/MTA facts) ·
                                 gold_premium_components · landing_referral_events_generated
                                 (isolated RNG 4242; hero sub:900004 append + scoped-checksum guard)
01d_wageroll_extraction ───────► ai_query wageroll from a broker note → silver_submission_wageroll (MERGE)
05d_lane_e_crux ───────────────► fn_wageroll_check + fn_referral_events_from_checks (COMPOSE the crux)
03c_gold_referral_events ──────► gold_referral_events (live-feed events ∪ generated 2025 history)
08_metric_views ───────────────► gold_discipline_base → mv_underwriting_discipline (UC Metric View =
                                 the SEMANTIC TRUNK; Genie + Control Tower tile consume its MEASUREs)
app/ ──────────────────────────► FastAPI (server/{config,sql,agents}.py) + dist/index.html SPA
99_reset / 98_smoke_test ──────► reset chain (no retrain, no fn recreation) · smoke = asset checklist
                                 + invariants + the three heroes' exact outcomes + Lane E (E1–E5)
```

## Referral & discretion analytics (Lane E)

Client-driven lane answering a real practitioner question — *"every transaction where the Max
Wageroll referral triggered, split NB/RN/MTA, technical vs charged"* — generalised to
referral-effectiveness and pricing-discretion analytics. **Strictly additive**: new tables join on
keys, the crux is composed (never modified), and the only write to an existing table is one appended
hero (`sub:900004`) guarded by a scoped checksum. All synthetic generation uses an **isolated
`Random(4242)`** so the seed=42 book is byte-identical.

- **Rule-grain fact.** `gold_referral_events` — one row per referral rule that fires (SI band,
  flood-High, fair-presentation, accumulation, MAX_WAGEROLL). Live-feed events derive from the crux
  via `fn_referral_events_from_checks`; ~400 generated 2025 MAX_WAGEROLL fires give history on day one.
- **Wageroll.** The EL rating basis, extracted with `ai_query` into `silver_submission_wageroll`;
  `fn_wageroll_check` refers above the authority band. Hero `sub:900004` is a clean single-trigger.
- **Transactions + discretion.** `gold_transactions` (NB/RN/MTA facts) + `gold_premium_components`
  (reason-coded discounts/loads) capture charged-vs-technical. A planted, discoverable signal —
  renewal give-away > MTA > NB, generous persona leans on the pen — survives a GROUP BY without
  screaming from a scatter.
- **Semantic trunk.** `mv_underwriting_discipline` is a UC Metric View; *all* metric logic
  (referral_rate, discretion_ratio, avg_giveaway_pts, rate_adequacy…) lives there. The Genie space,
  the Control-Tower tile and its drill all consume the same MEASUREs — one definition, no divergence.

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

## Referral Control (Lane RC)

- **Rulebook as SCD2 data.** `ref_referral_rules` is Type-2 (current = `valid_to IS NULL`). The crux
  reads it via `fn_rule_threshold` / `fn_rule_version` instead of hardcoded literals — seeds equal
  the old constants, so heroes stay byte-identical (smoke hero-gate). A governed change writes a new
  version; decisions replay under the rulebook in force at their date.
- **Fire-vector telemetry.** `gold_referral_telemetry` logs every rule that fired (no short-circuit)
  plus `would_fire` shadow rows for auto-declined rules, with the outcome joined. Read through an
  `as_of_date` filter — the time-travel mechanic (backfill + ~90d future book, RNG 4245, isolated
  from the seed=42 and Lane E streams).
- **Deterministic engine, agents narrate.** `fn_rule_metrics` / `fn_isolation_analysis` /
  `fn_recommend_action` (closed action set; compliance-locked ⇒ keep, enforced in the function) /
  `fn_emulate_rule_change` (mandatory tail exhibit) compute; `portfolio_advisor` + `reviewer`
  narrate what they produce (invariant 9). SQL-UDF params are threaded via a one-row param relation
  JOINed to the tables (no correlated-aggregate).
- **Governance is escalate-not-bind.** `gold_rule_changes` is the lifecycle ledger with
  predicted-vs-realised + drift; approval runs the SCD2 write-path (close current, append next),
  refused for compliance-locked rules at both the function and the app route. Detection
  (`07d`) supersedes the E8/E9 tuning tables.
