# Underwriting Workbench — Build Brief

Bricksurance SE **Underwriting Workbench**: the fifth Bricksurance workbench (claims, reinsurance,
pricing, solvency are siblings). It showcases Databricks as the platform for the *underwriting
decision layer* — the seam every sibling deliberately leaves open: pricing-workbench computes a
price but never accept/refer/decline; claims is post-bind; reinsurance is treaty.

Audience: **insurance experts and practitioners**. Everything implemented for real on synthetic
data, enriched with **bundled real UK open data**. This brief is the working spec; conventions
live in `CONVENTIONS.md` ("mirror, don't invent" — the framework is `reinsurance_workbench`).

## Scope decision (user)

- **Commercial SME / mid-market first**, with visible growth provisions for other lines: a
  consolidation story ("One Book") + clearly-labelled placeholders for what is not built yet.
- Open data **bundled in-repo** for reliability. API integration shown via **simulated calls**
  (labelled) + one **real-API example notebook (91)** off the critical path. The demo must never
  depend on an external API at run time.
- Demo doc REQUIRED: step-by-step demo in Google Docs (+ repo markdown mirror).
- Learn section as in other workbenches.
- Deployable in other workspaces as easily as possible: DEPLOY.md asset inventory + the smoke
  test doubles as an installed-assets verification.
- Genie + dashboard **embedded in the app, not linked**.

## The story

**Pain** (practitioner-recognised): an SME/mid-market desk drowns in broker submissions — email +
PDF proposal forms + risk schedules in every format; ~40% never quoted; underwriters spend the day
rekeying and swivel-chairing between Companies House, flood maps, sanctions tools, the PAS and the
claims system; appetite is tribal knowledge; referrals are untracked; first-quote-wins means
slow = lose.

**Resolution**: every submission lands in one governed lakehouse, is extracted, enriched, triaged,
checked against appetite/authority/accumulation, technically priced, and lands in the right
underwriter's queue with a complete dossier — minutes not days, every step recorded in Unity
Catalog.

**Lifecycle state model** (process-management pillar):
`received → extracted → triaged → enriched → assessed → referred → quoted → awaiting_broker /
quote_expired → bound / declined / ntu / lost`.
NTU (not taken up) is distinct from lost-to-competitor — it is a reported KPI. Funnel + SLA clocks
+ "Submission track" auditor timeline.

## SACRED HEROES (deterministic, seed=42, rolling dates, survive every reset — never mutate)

### `sub:900001` — clean fast-track SME package
*Fenwick & Moss Homewares Ltd*, homeware retailer, Altrincham **WA14**, tenant (no buildings).
Shop package: contents & stock £120,000 · BI (gross profit, **12-month indemnity**) £180,000 ·
money £2,500 · glass · **EL £10m** (compulsory cover; £10m market standard) · PL £2m.
Premium **~£1,850 + IPT 12%** (live-verified £1,856), commission 22.5%.
Straight-through because: in-appetite trade, under the e-trade threshold, clean 3-year loss run,
RoFRS low, sanctions clear — including a **resolved near-miss/false-positive** on the director
screen (false-positive handling is where real screening lives).

### `sub:900002` — the referral (mid-market Commercial Combined)
*Calder Valley Fine Foods Ltd*, chilled/ambient food manufacturer, **6 locations** — 2
manufacturing sites in **Hebden Bridge HX7 / Mytholmroyd** (famously EA-flood-prone, England),
3 distribution warehouses, 1 office. Messy submission: email + scanned PDF proposal + drifted-
columns risk schedule (SOV) → the quarantine beat.
Sums: buildings £14m · plant £6m · stock £3.5m · **BI £10m gross profit, 24-month indemnity** ·
EL £10m · **Products/PL £5m** · GIT £250k · **deterioration of stock £500k** (chilled trade
signature). Broker states turnover **£8m**; Companies-House-style full accounts show **£24m** →
**Insurance Act 2015 duty-of-fair-presentation issue** corrupting the EL/PL rating basis and BI
sums. Referral triggers: SI above the £10m authority band · RoFRS **high** at 2 locations ·
fair-presentation turnover mismatch · postcode-sector accumulation **87% of £25m capacity**.
Technical price **~£192k vs broker target £140k** (adequacy ~73% — live-verified 72.9%).
Senior underwriter quotes with terms: **£100k flood excess** at the 2 affected sites;
subjectivities — (1) audited turnover + revised EL/PL/BI figures within 14 days, (2) satisfactory
**risk survey** of both manufacturing sites within 60 days of inception, (3) confirmation of
**composite-panel percentage** and frying/cooking protections (the defining UK food-manufacturing
hazards — extracted as proposal-form fields); risk improvement — flood resilience plan for HX7.

### `sub:900003` — decline with dignity
*Midland Metal & Waste Recycling Ltd*, SIC **38320**, waste transfer + metal recycling, Walsall
**WS2**. Buildings/plant £4m · EL £10m · PL £5m.
Appetite check → excluded trade, coded reason **`APP-EXCL-WASTE`** citing the underwriting-guide
section. Screening → **OFSI clear; internal watchlist hit on a director** (synthetic: prior policy
avoided for non-disclosure — NEVER a real-entity match).
**Compliance-correct comms**: the agent-drafted broker letter cites appetite ONLY; the watchlist
reason stays internal in `gold_decision_audit`. The external-vs-internal reason separation is
surfaced in the UI as a conduct feature. Sanctions panel copy states the true-OFSI-match path:
freeze, no letter, escalate to compliance (potential OFSI report). Redirect wording at most:
"this trade sits with specialist waste markets."

## Sacred invariants (beyond CONVENTIONS.md)

1. **Escalate-not-bind** — agents advise/draft/flag; a human quotes, refers, declines.
2. **No GLM duplication** — `fn_technical_price` is burning cost + rate guide + named loadings;
   the full frequency/severity GLM engine is pricing-workbench's job (cross-link, never rebuild).
3. **EPC is never a rating factor** — surfaced only as MEES letting-ban/unoccupancy risk,
   building-age proxy, portfolio ESG lens.
4. **Crime data appears only as a named theft & malicious-damage loading inside the price
   build-up** — never a standalone "crime: high" chip.
5. **Decline letters cite appetite only**; screening/watchlist reasons are internal-only audit
   records. A true OFSI match = freeze + escalate, never a letter.
6. **Quote outputs always show IPT (12%) and commission %** — a UK quote without IPT is visibly
   fake.
7. **EA flood data labelled RoFRS (Risk of Flooding from Rivers and Sea), England only**; hero
   flood sites are in England (Calder Valley). Surface water = separate dataset (roadmap note).
8. **UK vocabulary**: "risk schedule / schedule of premises" alongside "SOV"; SOVs live on the
   mid-market lane only (SME arrives via e-trade/portal/proposal form).
9. **Structured panels never parse LLM prose** — decisions/prices/checks call UC fns directly;
   agents narrate in separate boxes. Cache wraps narration only.
10. **Heroes are byte-identical after every reset** (seed=42, rolling dates re-anchor).

## Open data (bundled, OGL-licensed, Volume `open_data`)

| Dataset | Use | Guard rails |
|---|---|---|
| EA flood risk (RoFRS-style, by district) | flood band → excess/exclusion/referral + accumulation | England only; label precisely |
| police.uk crime aggregates (by district) | **named theft/mal-damage loading in `fn_technical_price`** | never a standalone chip |
| EPC band aggregates | MEES letting-ban / building-age / portfolio ESG | never in per-risk price |
| OFSI consolidated sanctions list (real extract) | fuzzy screen entity + directors at quote | hero hit lives on the synthetic internal watchlist, never a real entity |
| ONS district centroids | maps + accumulation grain | reuse claims' 30-district file |
| Companies-House-style profiles (simulated API + bundled table) | incorporation, SIC, overdue accounts, directors, filed turnover (the mismatch) | no CCJs/credit scores; micro-entities file without turnover; bonus: declared-trade vs SIC mismatch check |

## Build phases (continuous P0→P10; single end-to-end verification at P10; halt only on genuine blocker)

- **P0** scaffold: repo, brief, conventions, databricks.yml, README, GitHub.
- **P1** `00_setup_and_data_generation` + `data/open/` extracts → Volume.
- **P2** `00b_landing_files`: emails, PDFs (fpdf2), schedules incl. one drifted file.
- **P3** medallion: `01_bronze_dlt`, `01b_file_ingest`, `01c_doc_extraction`
  (`ai_parse_document` + `ai_query`, confidence gate), `02_silver_dlt`, `03_gold_dlt`,
  `03b_dq_scorecard` (event log). Pipeline + ingest job.
- **P4** `04_features` (UC FS `feature_submission`) + `05_models`
  (`model_triage_priority`, `model_risk_quality`; LightGBM, @champion, scale-to-zero serving).
- **P5** `05b_crux`: fn_extract_summary · fn_appetite_check · fn_authority_check ·
  fn_accumulation_impact · fn_technical_price · fn_sanctions_screen · fn_underinsurance_check ·
  fn_recommendation (quote / refer / decline / request-information). `05c_whatif`.
- **P6** `06_agent_tools` + `06a_agents` (roles: risk_profile, appetite, pricing_adequacy,
  broker_comms, challenge) + `06b_supervisor_agent` (real tool-calling ChatAgent) +
  `scripts/create_genie_space.py` ("Underwriting — Ask the Book").
- **P7** `07_governance`: decision audit, inventory, AI activity, UC masking, lineage, conduct.
- **P8** Lakeview dashboard (embedded; symbol-map widget version 2).
- **P9** app: FastAPI + `dist/index.html` (reinsurance theme verbatim), app.yml, SP grants,
  live verification.
- **P10** `99_reset` + `98_smoke_test` (~20 loud asserts incl. hero outcomes + full
  installed-assets checklist) + docs (README, DEPLOY.md asset inventory, ARCHITECTURE.md,
  demo doc repo + Google Doc) + practitioner gate + full E2E verify + final push.

## App screens (persona lanes)

Overview · **Head of Underwriting**: Portfolio Control Tower (GWP vs plan, retention % +
rate-change-on-renewal, appetite utilisation RAG, funnel by channel, rate adequacy, broker
scorecard — every tile drills to reconciling rows + show-the-SQL), Insight (EMBEDDED Lakeview +
Genie in-app) · **Underwriter**: Submission Inbox (priority-ranked, SLA clocks, why-chips),
Work a submission (the centrepiece — extraction, enrichment incl. location map + underinsurance,
appetite/authority/accumulation/sanctions checks, price build-up with named loadings + IPT +
commission, recommendation, HITL quote/refer/decline/request-info → audit write, supervisor
narration, Submission track), Try a submission (live what-if) · **Distribution**: Broker view
(scorecard + portal roadmap tile) · **Data & Governance**: Ingestion (Source-Assets accordion,
quarantine drill, Document AI spotlight, open-data provenance, simulated-API card + real-API
pointer), Governance (What we collect / Decisions & audit / Models & AI activity / Authority &
conduct incl. fair-value + premium-finance tile) · **Platform**: Underwriting AI (supervisor
ask-box + agent bench + Genie), Learn (Underwriting-101 deck + demo run) · **One Book**
(consolidation: live commercial lane + labelled placeholders — Personal Motor/Home ×claims,
Specialty/London Market, MGA binders, Renewals workbench, MTAs, post-bind obligations,
Reinsurance outward ×reinsurance-workbench; benefits panel).

## Deploy

Dev target `fevm-lr-dev-aws-us`, profile `DEV`, catalog var default `lr_dev_aws_us_catalog`,
schema fixed `underwriting_workbench`, warehouse `a3b61648ea4809e3`, FM endpoint
`databricks-claude-sonnet-4-5` (sonnet-5 fails batch ai_query on this estate). All serverless.
GitHub `wryszka/underwriting-workbench` (public), push after every phase.
