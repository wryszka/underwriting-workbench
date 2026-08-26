# Referral Control — as-built spec

**Referral Control** is the Underwriting Workbench capability that turns the referral rulebook into
governed, effective-dated data and runs a continuous loop over it: *discover → recommend → emulate →
approve → monitor → reverse*. Real desks tackle referral triggers one workshop at a time, each a
hand-built Excel exhibit stale the day it is presented. This shows the platform doing it continuously —
every number drilling to its reconciling rows and its SQL. Agents narrate; deterministic functions
compute (invariant 9).

Origin: a practitioner question about referral effectiveness and pricing discretion (client-driven). It
is **strictly additive** — heroes `900001-900004` stay byte-identical (invariant 10), and all new random
data uses an isolated `Random(4245)` stream (never the seed=42 book or the Lane E 4242/4243/4244 streams).

## Three deterministic layers + one narration layer

### Layer 1 — Rules as data (`ref_referral_rules`, SCD2)
The rulebook is a Type-2 slowly-changing table (`notebooks/00e_referral_registry.py`). One row per rule
*version*; the CURRENT version has `valid_to IS NULL` / `is_current = true`. ~24 rules across the
taxonomy — risk-selection, exposure size, cross-cover, question-set design, distribution, lifecycle,
pricing-adjacent, and **compliance (locked)**. Columns add `category`, `disposition`
(`refer` / `auto_decline` / `auto_apply_clause` / `accept`), `compliance_lock`, `review_effort_hours`,
and the SCD2 fields (`valid_from`, `valid_to`, `approved_by`, `change_id`).

The crux reads the registry, not hardcoded literals: `fn_rule_threshold(rule_id, key)` and
`fn_rule_version(rule_id)` (in `05b_crux`) return the CURRENT version's values, so a governed rule change
flows into every check with no SQL edit. The seed values equal the old hardcoded constants, so the heroes
reproduce byte-for-byte (proven by the smoke hero-gate). `fn_authority_check`, `fn_accumulation_impact`,
`fn_recommendation` (05b) and `fn_referral_events_from_checks` / `fn_wageroll_check` (05d) all read the
registry.

### Layer 2 — Fire-vector telemetry (`gold_referral_telemetry`)
`notebooks/00f_referral_telemetry.py` (RNG 4245) generates the substrate: **the full fire-vector with no
short-circuit** — one row per (case, rule) that fired, plus `would_fire` SHADOW rows for
auto_decline/accept rules (the "GWP-at-stake" signal), with the eventual outcome joined
(`declined` / `price_walked` / `bound_clean` / `bound_with_terms`), the decider, terms applied, GWP,
technical price, loading, downstream loss ratio, and `co_fire_count` (0 = the rule fired in isolation).
It is the substrate for isolation, co-fire, drift, emulation and the reviewer agent.

**Time-travel.** Cases span `[epoch (≈3y ago) .. today + 90d]`. Every Referral Control surface reads
through a single `as_of_date` filter (`as_of_date <= chosen date`): "advance one month" just moves the
parameter over pre-generated data, and scrubbing to ANY date works.

### Layer 3 — Detection, scoring, emulation, governance
Deterministic UC functions (`notebooks/05e_referral_control_fns.py`), all `as_of`-parameterised:
- `fn_rule_metrics(rule_id, as_of)` — the metric tuple: fires, approval / decline+walk / no-adjustment
  rates, isolation, loss ratio (overall / isolated / co-fire), GWP bound, shadow GWP-at-stake, dominant
  clause, touch cost (fires × review-effort-hours × £95/hr), and recent/prior 90-day windows for drift.
- `fn_isolation_analysis(rule_id, as_of)` — what uniquely fires on the rule vs what else catches the
  rest (co-fire partners materialised in `gold_rule_cofire_partners`).
- `fn_recommend_action(rule_id, as_of)` — deterministic EV over the tuple → ONE action from the closed
  set, with the arithmetic as structured evidence and **two currencies** (portfolio £ + operational
  hours). **Compliance-locked ⇒ `keep`, enforced in the function** (not just the UI).
- `fn_emulate_rule_change(rule_id, action, as_of)` — replays the book with a change: referrals/hours
  released, GWP delta, predicted LR delta, and the **mandatory surviving tail exhibit** (the named
  policies decided differently) — the defence when a practitioner asks "what did we lose?".

Closed action set: `remove` · `re_threshold` · `auto_apply_clause` · `convert_to_auto_decline` ·
`reopen_to_referral` · `reprice_instead_of_refer` · `split_question` · `keep`.

Detection pass (`notebooks/07d_referral_detection.py`) materialises `gold_rule_effectiveness` (the
league table at the anchor) and `gold_referral_findings` (the ranked feed at four monthly `as_of`
snapshots for the dashboard trend; the app computes live for free-scrub). It **supersedes the old E8/E9**
`07b`/`07c`.

Governance (`notebooks/07e_referral_governance.py`): `gold_rule_changes` is the change ledger — one row
per proposal with its frozen predicted-impact pack, lifecycle status (`proposed → approved → live →
monitored → reversed/retired`), and realised-vs-predicted tracking (divergence past tolerance sets
`drift_flag`). The SCD2 write-path a human approval runs: close the current rule version (`valid_to`),
append the next to `ref_referral_rules`. **Escalate-not-bind — the engine never auto-applies.**

### Narration layer — two agents (narrate-only, `06a_agents`)
- **Portfolio advisor** (`portfolio_advisor`): makes the case for a finding's recommended action and
  drafts the one-line change proposal for human approval. Every number comes from the functions.
- **Reviewer** (`reviewer`): beside a LIVE referral in Work-a-submission — the historical outcome
  distribution for the exact fire-pattern, likely terms from precedent, and a consistency challenge if
  the proposed decision diverges. Advisory only, logged to audit.

## Seeded storylines (deterministic, DISCOVERED by 07d — never hardcoded in the UI)
1. `HAZARDOUS_ACTIVITY_HEIGHT` — niche hazard, ~95% declined/price-walked, the few that bind are small
   GWP + poor LR → **convert_to_auto_decline** (tail exhibit proves nothing of value lost).
2. **Drift + reversal** — a future new broker channel (`BRK-NEWCO`) pushes growing volume in that
   segment with better risk; the shadow GWP-at-stake grows ~4× → **reopen_to_referral** (never straight
   to accept; probation), citing the mix shift. Surfaces once S1 is approved.
3. `EVENT_ATTENDANCE_LIMIT` — high volume, the same clause (`CROWD_MGMT_CLAUSE_07`) every time →
   **auto_apply_clause** + re_threshold.
4. `DUAL_TRADE_DECLARED` — high approval, high no-adjustment, ~zero isolation (caught by others) →
   **remove**.
5. `NEW_VENTURE_TRADING_HISTORY` — healthy overall BUT its isolated fires carry a materially worse loss
   ratio → **keep** (it is catching real risk).
6. `SANCTIONS_SCREEN_HIT` — high fire count, **compliance-locked** → keep (computed, never changed).
7. `RENEWAL_UNCHANGED_RISK` — the biggest raw volume, unchanged risks → **re_threshold** on materiality.

## App — Referral Control panel (Head of Underwriting lane), five tabs
1. **Today** — ranked findings feed; each card drills metric → reconciling telemetry rows + show-the-SQL;
   tail exhibit one click away; portfolio-advisor narration; propose → HITL approve. The time-travel
   scrubber lives here and drives every tab.
2. **Rulebook** — the governed rulebook as of the selected date (SCD2 as-of) + the change ledger with
   predicted-vs-realised and drift.
3. **Investigate** — the what-if bench (pick a rule + action → emulate → tail exhibit → propose) + an
   embedded Genie box scoped to the referral tables.
4. **Decisions** — replay a decision under its contemporaneous rulebook (fire-vector, rule versions).
5. **How it works** — the no-black-box walkthrough: telemetry → detection SQL → EV formula → closed
   action set → what the agents do and do NOT do → the governance loop diagram. Boundaries stated: the
   engine never auto-applies; compliance-locked rules are untouchable; auto-declined gates reopen to
   referral, never straight to accept.

A reviewer advisory box lives inside Work-a-submission; one Control Tower tile drills into the panel.

## Genie
Scoped space **"Referral Control — Ask the Rulebook"** (`space_id 01f1a14c775e12458d4998f97c00cd59`) over
`gold_referral_findings`, `gold_referral_telemetry`, `gold_rule_changes`, `ref_referral_rules`, with
instructions on the fire-vector / shadow / approval / compliance-lock semantics.

## Sacred invariants preserved
Escalate-not-bind (human approves every change); compliance-locked rules never recommended for
change (enforced in `fn_recommend_action`); structured panels never parse LLM prose (agents narrate in
separate boxes); heroes byte-identical after reset (registry seeds the exact old thresholds); runs on any
serverless workspace via the bundle; fully deterministic (seed=42 book + isolated 4245 stream + `as_of`
filter, never runtime randomness).
