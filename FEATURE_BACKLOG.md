# FEATURE_BACKLOG.md — living register (workshops and reviews append here)

Status: compiled 2026-07-09 after the CUO review round (F1–F15 shipped), the multi-policy
analysis (docs/MULTI_POLICY_PLAN.md) and the practitioner critique round 2. Sizes are focused
build-days. Nothing below starts without an explicit go.

## Lane A — Practitioner critique round 2 (credibility fixes)

| # | Feature | The point | Size |
|---|---|---|---|
| A1 | **Treaty check on referrals** — net vs gross line + facultative flag | £23.5m property on a £5m net retention: the referral panel must show what the surplus treaty absorbs and flag "facultative required" above treaty capacity. `ref_treaty_structure` + a `fn_treaty_check` in the checks row; cross-links the Reinsurance Workbench. | 0.5–1d |
| A2 | **Multi-carrier loss-run gauntlet** — prove the parsing claim | Today we parse ONE clean loss run. Seed 4–5 genuinely awkward ones (different carriers, formats, 5-year windows, varying deductibles, one ambiguous → quarantine) → normalised claims-experience table + reconciliation view. The "prove your OCR" answer. | 1–1.5d |
| A3 | **E-trade zero-touch auto-bind** — clean STP business never reaches a human | If it clears every rule inside e-trade authority: bind automatically (decided_via=system_etrade, audit row + pack), EXCLUDE from the inbox, show a "zero-touch today: N bound · £X" ledger with drill. Claims auto-close pattern. Hero 900001 becomes the auto-bind beat; smoke asserts updated. | 1d |
| A4 | **MTAs / endorsements lane** — 70% of operational leakage | Endorsement intake on in-force policies (SI uplift, add location, add cover) → DELTA checks (accumulation delta, authority on the delta, pro-rata additional premium) → endorsement decision + audit + pack. New lifecycle branch + inbox type chip. | 1.5–2d |
| A5 | **Broker Trust Score** | Compose hit ratio + data-completeness + **fact-discrepancy rate** (turnover-mismatch incidence per broker — computable today) + NTU into a trust score; show on the dossier screening row; penalise triage EV. | 0.5–1d |
| A6 | **Auto-chased subjectivities** — underwriters are not diary managers | Subjectivity tracker (due dates parsed from terms) + scheduled check → comms agent drafts the day-12 chaser → HITL approve/send; diary view + Control Tower "subjectivities at risk" tile. Also closes the parked post-bind-obligations item. | 1–1.5d |

## Lane B — CustomerLake (CRM / customer-360) positioning

| # | Feature | The point | Size |
|---|---|---|---|
| B1 | **CustomerLake alignment for the client master** | CustomerLake (announced DAIS Jun-2026, Private Preview) = agentic CDP in Databricks: Profile Agents do identity resolution → governed golden Customer 360 profiles in UC. Its Profile-Agent half IS our G1 client-master problem for a REAL estate (messy party data across PAS/claims/CRM, no shared key). Action now: build M1's `ref_client` field-compatible with CustomerLake profile outputs + a positioning card on One Book ("client master runs on CustomerLake Profile Agents when GA/preview reachable"); request preview enrollment for the dev workspace; if granted, a real Profile-Agent resolution beat replaces the synthetic ref_client build. Campaign-Agent half = marketing-oriented; maps loosely to M4 cross-sell only. Keep OFF the demo's critical path (same reliability rule as live APIs). | 0.5d positioning now; preview-dependent later |

## Lane C — Multi-policy / account underwriting (docs/MULTI_POLICY_PLAN.md)

| # | Phase | Size |
|---|---|---|
| C1 | M1 client master + product lines (fleet/cyber/D&O; Calder Valley holds fleet+D&O — hero uplift). Build CustomerLake-compatible (B1). | 1d |
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
| D5 | `/Workspace/Shared` migration (production mode, any-SA redeploy) — after GUI review settles | 0.5d |
| D6 | REDEPLOYABILITY_AUDIT.md (B1–B6 checklist, reinsurance pattern) | 0.25d |

## Suggested sequencing (pending go)

1. **A3 zero-touch auto-bind** (changes the inbox story; do before more GUI review)
2. **A5 broker trust score** (cheap, feeds triage + screening panel)
3. **A1 treaty check** (cheap, big credibility, sibling cross-link)
4. **A6 subjectivity auto-chase** (kills a parked item too)
5. **C1 client master (+ B1 positioning card)** → then C2/C3
6. **A2 loss-run gauntlet** · **A4 MTAs** (the two bigger builds)
7. D-lane items woven in during GUI review; D5 last.
