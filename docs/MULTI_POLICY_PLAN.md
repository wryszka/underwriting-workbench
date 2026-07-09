# MULTI_POLICY_PLAN.md — account-level (multi-policy) underwriting: analysis + delivery plan

## What "multi-policy" means for this desk

Today the workbench underwrites **submissions** — one risk, one decision. Real commercial
relationships are **accounts**: one client holding a package *and* a fleet *and* cyber *and*
D&O, often across group companies, ideally renewing as one conversation. Account-level
underwriting changes the questions:

- *"Is this £192k Commercial Combined worth writing?"* becomes *"what does the **whole
  relationship** earn us, and what does this policy do to it?"*
- Terms flex on account profitability, not policy profitability.
- Exposure aggregates per **client**, not just per postcode district.
- Cross-sell is an underwriting signal, not just a sales one.
- The renewal is one date, one pack, one negotiation.

## Gap analysis — what the current build is missing

| # | Gap | Why it blocks multi-policy | Severity |
|---|---|---|---|
| G1 | **No client master.** `landing_pas_policies` has no client identifier; only submissions carry `company_number`. Policies, submissions and claims cannot be grouped by insured. | The keystone. Everything below depends on one governed party id across all three. | Blocker |
| G2 | **One product line.** The PAS book is property/liability packages only — no fleet/cyber/D&O/PI dimension, so "multiple policies" would all look the same. | Multi-policy needs product variety to mean anything. | Blocker |
| G3 | **No account marts.** No per-client GWP / whole-account loss ratio / tenure / products-held. | The account-profitability lens. | High |
| G4 | **Accumulation is geographic only.** No per-client exposure roll-up or per-client line capacity in the crux. | One insured across 5 policies can exceed any sensible line without tripping a check. | High |
| G5 | **Screening is per-submission.** The same directors get re-screened per policy; no client-level screening record reused (and no consistency check between them). | Efficiency + conduct consistency. | Medium |
| G6 | **No group structures.** Companies-House-style profiles have no parent/subsidiary links. | Group accounts (the mid-market norm) invisible. | Medium |
| G7 | **No cross-sell signal.** Nothing computes "peers in this trade hold products this client doesn't". | The commercial upside of consolidation. | Medium |
| G8 | **Renewals are per-policy.** No common-renewal-date alignment view per client. | The account renewal conversation. | Low |
| G9 | **Heroes are single-policy.** No story beat showing the account view changing a decision. | Demo needs its multi-policy moment. | Demo |

**Already in place (the seeds):** `company_number` on every submission joined to profiles;
"this client's history with us" card on the dossier + "returning client" inbox chip (live,
matched on company number); the broker 360 cross-link; the One Book consolidation frame; and
the future alignment with **bricksurance-data-core** (its ACORD-aligned party model is exactly
the client master G1 asks for — build G1 compatibly and the ontology migration is free).

## Delivery plan (each phase demoable on its own)

### M1 — Client master + product lines (the keystone) ~1 day
- `ref_client` (client_id `CL-NNNNNN`, legal name, company_number, trade, segment, since-date)
  generated in 00; **client_id added to** `landing_pas_policies`, `landing_submissions_feed`,
  `landing_pas_claims` (claims inherit via policy).
- Deliberate multi-policy shape: ~25% of clients hold **2–5 policies** across a new
  `product_line` dimension (package / commercial_combined / fleet / cyber / directors_officers),
  each with simple per-line rate entries in `ref_rate_guide`.
- **Hero uplift:** Calder Valley Fine Foods already holds a **fleet policy (28 vehicles) and a
  D&O policy** with us — profitable, 4 years tenure. The 900002 referral conversation changes:
  "the account earns £Xk at 38% LR; hold the flood excess but flex the premium."
- Silver/gold pick up client_id; smoke asserts hero account shape.

### M2 — Client 360 + account marts ~1 day
- `gold_client_book`: per client — policies by product line, account GWP, whole-account 3y loss
  ratio, tenure, open submissions, claims count, products-held bitmap.
- **Client 360 page** (route `#client/CL-…`): the account header (GWP, LR, tenure), every
  policy + submission + claim in one timeline, screening record once at client level.
- Dossier "Client & account" card upgrades from history-only to the full account view;
  inbox "returning client" chip deep-links to it.
- Control Tower: "Top 10 accounts" + account-concentration tile (top-10 clients % of GWP).

### M3 — Client-level accumulation + account line ~0.5 day
- `fn_client_exposure(client_id)`: total property SI + total EL/PL limits across ALL the
  client's policies + the open submission's marginal → vs a per-client line appetite
  (`ref_client_line` or a segment default), status ok/referral/breach.
- Wired into `fn_recommendation` as an additional referral trigger and into the dossier checks
  row. The demo beat: a modest submission refers because **the account** is already at line.

### M4 — Cross-sell + account renewal alignment ~1 day
- `gold_cross_sell`: per trade_group, product-line penetration ("83% of food manufacturers
  ≥100 staff hold D&O") → per-client gap list with estimated premium; surfaces on Client 360,
  the broker page (per-broker cross-sell list), and as a One Book benefit made live.
- Renewals desk gains a **by-client grouping**: policies renewing within 60 days of each other
  for the same client flagged "align to one date"; the account renewal pack (reuses the
  audit-pack renderer) as the artefact.
- Optional ML: cross-sell propensity (same LightGBM/feature-store pattern as triage).

### M5 — Group structures + account pricing lens ~1 day, optional
- `parent_company_number` on profiles (+ 91-notebook shows the real CH link source);
  group roll-up in Client 360 and client-level accumulation.
- Committee page gains an account-pricing lens: account-level discount/loading as a governed,
  audited adjustment (same gov_guide_changes register pattern).

**Sequencing note:** M1 is the only disruptive change (data-gen + pipeline + feature/scoring
rerun end-to-end); do it in one pass with a full reset + smoke extension. M2–M5 are additive.
Total ≈ 4–5 focused days. Everything reuses existing patterns: marts + drills, crux fns with
rich comments, agent tools gaining `get_client_account`, pack renderer, present-strips.

## What NOT to do
- Don't fake the account view off company_name string-matching — wrong identities are worse
  than none; wait for the client master (G1).
- Don't build a CRM. The client master is an underwriting party record, not a sales pipeline.
- Don't duplicate bricksurance-data-core's party ontology — align field names with it now so
  this schema migrates into the central model later.
