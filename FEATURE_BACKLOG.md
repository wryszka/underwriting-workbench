# FEATURE_BACKLOG.md — living register (workshops and reviews append here)

Status: compiled 2026-07-09 after the CUO review round (F1–F15 shipped), the multi-policy
analysis (docs/MULTI_POLICY_PLAN.md) and the practitioner critique round 2. Sizes are focused
build-days. Nothing below starts without an explicit go.

**2026-07-09 update — lane A COMPLETE (A1–A6 all shipped and smoke-asserted, incl. A4 MTAs —
live: landing_mta_feed + fn_mta_check + /api/mtas + hero mta:900010), B1 positioning shipped,
C1 shipped, D5 shipped (/Workspace/Shared migration). Open: C2–C5, D1–D4, D6, B1 preview-
enrollment follow-up.**

**2026-08-09 update — Lane E added (client-driven referral & pricing-discretion analytics,
below). Competitive-hardening tasks #37–#40 (One Book positioning card, hyperoperator/Guidewire
Q&A armour, PAS-SoR + channel honesty lines, external-agent builder beat) queued from the
hx/Guidewire review, not yet started.**

## Lane A — Practitioner critique round 2 (credibility fixes) — ✅ ALL SHIPPED 2026-07-09

| # | Feature | The point | Size |
|---|---|---|---|
| A1 | **Treaty check on referrals** — net vs gross line + facultative flag | £23.5m property on a £5m net retention: the referral panel must show what the surplus treaty absorbs and flag "facultative required" above treaty capacity. `ref_treaty_structure` + a `fn_treaty_check` in the checks row; cross-links the Reinsurance Workbench. | 0.5–1d |
| A2 | **Multi-carrier loss-run gauntlet** — prove the parsing claim | Today we parse ONE clean loss run. Seed 4–5 genuinely awkward ones (different carriers, formats, 5-year windows, varying deductibles, one ambiguous → quarantine) → normalised claims-experience table + reconciliation view. The "prove your OCR" answer. | 1–1.5d |
| A3 | **E-trade zero-touch auto-bind** — clean STP business never reaches a human | If it clears every rule inside e-trade authority: bind automatically (decided_via=system_etrade, audit row + pack), EXCLUDE from the inbox, show a "zero-touch today: N bound · £X" ledger with drill. Claims auto-close pattern. Hero 900001 becomes the auto-bind beat; smoke asserts updated. | 1d |
| A4 | ✅ SHIPPED 2026-07-09 — **MTAs / endorsements lane** — 70% of operational leakage | Endorsement intake on in-force policies (SI uplift, add location, add cover) → DELTA checks (accumulation delta, authority on the delta, pro-rata additional premium) → endorsement decision + audit + pack. New lifecycle branch + inbox type chip. LIVE: `landing_mta_feed`, `fn_mta_check`, `/api/mtas` + `/api/mta/{id}` + `/api/mta/decide`, hero `mta:900010` (HX7 +£4m → refer, 67→83%). | 1.5–2d |
| A5 | **Broker Trust Score** | Compose hit ratio + data-completeness + **fact-discrepancy rate** (turnover-mismatch incidence per broker — computable today) + NTU into a trust score; show on the dossier screening row; penalise triage EV. | 0.5–1d |
| A6 | **Auto-chased subjectivities** — underwriters are not diary managers | Subjectivity tracker (due dates parsed from terms) + scheduled check → comms agent drafts the day-12 chaser → HITL approve/send; diary view + Control Tower "subjectivities at risk" tile. Also closes the parked post-bind-obligations item. | 1–1.5d |

## Lane B — CustomerLake (CRM / customer-360) positioning

| # | Feature | The point | Size |
|---|---|---|---|
| B1 | **CustomerLake alignment for the client master** | CustomerLake (announced DAIS Jun-2026, Private Preview) = agentic CDP in Databricks: Profile Agents do identity resolution → governed golden Customer 360 profiles in UC. Its Profile-Agent half IS our G1 client-master problem for a REAL estate (messy party data across PAS/claims/CRM, no shared key). Action now: build M1's `ref_client` field-compatible with CustomerLake profile outputs + a positioning card on One Book ("client master runs on CustomerLake Profile Agents when GA/preview reachable"); request preview enrollment for the dev workspace; if granted, a real Profile-Agent resolution beat replaces the synthetic ref_client build. Campaign-Agent half = marketing-oriented; maps loosely to M4 cross-sell only. Keep OFF the demo's critical path (same reliability rule as live APIs). | 0.5d positioning now; preview-dependent later |

## Lane C — Multi-policy / account underwriting (docs/MULTI_POLICY_PLAN.md)

| # | Phase | Size |
|---|---|---|
| C1 | ✅ SHIPPED 2026-07-09 — M1 client master + product lines (fleet/cyber/D&O; Calder Valley holds fleet+D&O — hero uplift). Built CustomerLake-compatible (B1). | 1d |
| C2 | M2 Client 360 page + gold_client_book + top-accounts tile | 1d |
| C3 | M3 client-level accumulation + per-client line in the crux | 0.5d |
| C4 | M4 cross-sell gaps + account renewal alignment | 1d |
| C5 | M5 group structures + account pricing lens (optional) | 1d |

## Lane D — Parked family-convention items

| # | Feature | Size |
|---|---|---|
| D1 | Learn in-app "demo run" tab (RUN_STEPS + persona stories) | 0.25d |
| D2 | Live model scores in Try-a-submission (the one deliberate live-endpoint beat) | 0.25d |
| D3 | Dashboard page 2 (funnel trend, retention trend, adequacy by trade) | 0.5d |
| D4 | Joined cross-workbench Genie ("broker 360" over underwriting + claims gold) | 0.5d + grants |
| D5 | ✅ SHIPPED 2026-07-09 — `/Workspace/Shared` migration (production mode, any-SA redeploy) | 0.5d |
| D6 | REDEPLOYABILITY_AUDIT.md (B1–B6 checklist, reinsurance pattern) | 0.25d |

## Lane E — Referral & pricing-discretion analytics (client-driven, 2026-08)

Client-demand origin: real practitioner workflow question (Max Wageroll referral, NB/RN/MTA
split, technical vs charged). Generalised to referral-effectiveness analytics. A4 already
shipped, so its "pull-forward" in E3b collapses to a delta (see note).

| #   | Feature | The point | Size |
| --- | ------- | --------- | ---- |
| E1  | `gold_referral_events` — rule-grain referral fact table | Persist every crux check that FIRES as an event row (rule_id, value, threshold, outcome, latency). Makes EVERY referral rule analysable, not just wageroll. Non-fires = aggregate counter in DQ scorecard, not rows. | 0.5d |
| E2  | `MAX_WAGEROLL` rule + hero `sub:900004` | Wageroll = EL rating basis; threshold per authority band, config-driven via `ref_referral_rules`. New hero *Harwood & Vane Scaffolding* (£6.8m wageroll → single-trigger refer). 900001–900003 untouched. | 0.5d |
| E3a | Transaction facts NB/RN/MTA (`gold_transactions`) + renewal generator | The dimension the practitioner question splits on. Analytics-grade facts with planted discretion signal (renewal give-away > MTA > NB). Facts only — renewal workbench stays a One Book placeholder. | 1d |
| E3b | Reconcile A4 → `gold_transactions` (**delta only**, A4 already live) | Existing `/api/mta/decide` also writes a `gold_transactions` MTA row so workflow + analytics reconcile. NOT a rebuild of A4 (shipped). Ships after E5. | 0.25d |
| E4  | Charged-vs-technical capture (`gold_premium_components`, `ref_adjustment_reasons`) | Named discounts/loads with reason codes at the HITL quote step; discretion_ratio = charged/technical. `discretion_ratio` nests in the existing audit `decision_evidence` JSON (no schema change). | 0.5d |
| E5  | `mv_underwriting_discipline` Metric View + Genie wiring + Control Tower tile | Single semantic trunk for referral_rate, discretion_ratio, rate_adequacy. The practitioner question answered verbatim in Genie is the demo beat. **Depends on UC Metric View being available on this workspace — verify at build; stop-and-flag if not.** | 0.5d |

**Lane E deviations from the brief (confirmed with owner 2026-08-09):**
- Persona table named **`ref_underwriter_persona`** (joins existing `ref_underwriter` by
  `underwriter_id`) — NOT `ref_underwriters`, which would collide with the existing singular
  `ref_underwriter` table and confuse the next SA.
- Hero `sub:900004` uses **append + scoped checksum**: `00c` appends ONLY the 900004 row to
  the shared `landing_submissions_feed` (rng 4242, after existing gen); the constraint-3
  "existing-book unchanged" checksum is computed over submissions EXCLUDING lane-E ids, so it
  still guards every pre-existing row while letting 900004 flow through the live screen.
- E3b is a delta write, not an A4 build (A4 shipped 2026-07-09).
- Metric-view notebook named `08_metric_views` (no `08` exists; `08b` implied a missing `08`).

## Suggested sequencing (pending go)

1. **A3 zero-touch auto-bind** (changes the inbox story; do before more GUI review)
2. **A5 broker trust score** (cheap, feeds triage + screening panel)
3. **A1 treaty check** (cheap, big credibility, sibling cross-link)
4. **A6 subjectivity auto-chase** (kills a parked item too)
5. **C1 client master (+ B1 positioning card)** → then C2/C3
6. **A2 loss-run gauntlet** · **A4 MTAs** (the two bigger builds)
7. D-lane items woven in during GUI review; D5 last.
