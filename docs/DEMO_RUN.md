# DEMO_RUN.md — step-by-step demo (≈25 min + optional beats)

**Canonical presenter copy (Google Doc):** https://docs.google.com/document/d/1-J6OfcRAekJUEwmA3kWD3GpBZx7OoNT0LbDLA7j-jRY/edit

Audience: insurance practitioners (underwriters, heads of underwriting, COO/ops, data leaders).
Presenter prep: app RUNNING · smoke test all-PASS · AI toggle on **CACHED** (flip to LIVE to prove
it) · this doc mirrors the Google Doc (canonical for presenters).

> One-line story: *"Every submission — email, scanned PDF, drifting spreadsheet, portal — lands in
> one governed lakehouse and comes out as a complete underwriting dossier with a defensible
> recommendation, in minutes. Humans decide; everything is audited."*

## Beat 0 · Overview (1 min)
Open the app → landing page. Read the pain paragraph. Point at the 7-stage lifecycle strip —
"this is the whole demo in one line". Point at the **About this demo** honesty box (synthetic
insurer, real Databricks services, real OGL open data).

## Beat 1 · Portfolio Control Tower (4 min) — the Head of Underwriting
- Vital signs: GWP written vs plan, **retention % + rate change** (a real book is 70–80% renewals),
  rate adequacy, accumulation hot districts, declinature mix, broker & trade concentration.
- **Zero-touch today** tile: N policies bound with **no human touch** (clean e-trade under authority)
  and the premium they carried — drill to the ledger, every one has an audit row and a pack.
- **Subjectivities at risk** tile: outstanding post-bind conditions approaching their due dates —
  "underwriters are not diary managers"; the system chases (Beat 7).
- **☕ Morning brief** — the agent narrates today's numbers into three actions (cached; flip to LIVE to prove it).
- **Pipeline forecast (weighted)** — Σ premium × P(bind) = a live month-end GWP forecast. Drill it.
- **💷 What this is worth** — the business case computed from THIS book's funnel (click "how this is computed").
- **Prove it's real**: click the accumulation tile → reconciling district rows → "show the SQL".
- Funnel by channel: e-trade quotes in hours (4.5h), email in days (124h) — "the 40% never quoted problem".

## Beat 2 · Zero-touch + the inbox (3 min)
- **★ sub:900001 Fenwick & Moss is NOT in the inbox — that's the beat.** It cleared every rule
  inside the e-trade authority and **bound itself** (decided_via `system_etrade`): audit row, quote
  with **IPT 12%** and commission, decision pack in the volume. Open it from the zero-touch ledger —
  the dossier carries the auto-bound banner; the amber screening card shows a REAL resolved
  near-miss (director "Emran Ali" vs OFSI-listed "Emraan ALI", cleared on DOB/nationality, logged).
  *Clean STP business never reaches a human; false-positive handling is where screening lives.*
- The inbox holds what's LEFT: ranked by **expected value** — premium × batch-scored P(bind) ×
  a **Broker Trust Score** factor (hit ratio + data completeness + fact-discrepancy rate + NTU) —
  with SLA clocks and why-chips. Optional: **Renewals desk** — the other 80% of a real book:
  due-in-90-days with claims experience and a suggested rate stance.

## Beat 3 · The referral hero (6 min) — the centrepiece
Open **★ sub:900002 Calder Valley Fine Foods** (mid-market Commercial Combined, email + scanned
proposal + risk schedule):
1. **Documents & calls**: proposal PDF read by `ai_parse_document`; the **FD's phone call**
   (transcript through the SAME pipeline) volunteers a second production shift and "about
   twenty-four million now" — material facts on no form. Extracted hazards include
   *composite panels unconfirmed* and *frying line suppression unconfirmed* — THE UK food-manufacturing
   questions. The schedule arrived twice: v1 with drifted columns sits in **quarantine** (show later).
2. **Fair presentation**: broker states £8m turnover; filed accounts show **£24m** → Insurance
   Act 2015 flag; PL/Products basis + BI sums affected (EL wageroll to be confirmed).
3. **Accumulation**: the two Calder Valley sites take HX7 from 67% → **87% of the £25m capacity** —
   over the 80% referral line, live, at point of quote. "Most carriers reconcile this quarterly
   in a spreadsheet."
4. **Treaty check**: £23.5m of property against a **£5m net line** — the surplus treaty absorbs
   the rest within its 4 lines; the card shows net/ceded split and would flag **"facultative
   required"** above treaty capacity. Cross-links the Reinsurance Workbench. *"Your net line is
   £5m — why is the workbench silent about reinsurance?" — it isn't.*
5. **Client & account**: Calder Valley is not one submission — the account card shows the group
   also holds **fleet (£14.7k)** and **D&O (£3.8k)** with us. Account-level context at point of
   quote (client master built CustomerLake-profile-compatible).
6. **Price build-up**: named loadings — theft/malicious damage (real police.uk counts), flood
   (EA RoFRS High band), claims experience; IPT; broker target £140k vs technical ~£192k → adequacy ~73%.
7. **Recommendation: REFER** to a senior underwriter with the exact terms (£100k flood excess) and
   subjectivities (audited turnover 14 days · survey 60 days · composite panel confirmation).
8. HITL: terms and subjectivities are **editable** — revise and re-record to run a negotiation
   round (each version audited). Click **Refer** → audit row **with the full as-at dossier
   snapshot**. Draft the letter → the comms agent writes the subjectivities verbatim →
   **Approve & record**. The recorded subjectivities land in the **diary** with parsed due dates.
9. Optional: **Ask the supervisor** — watch the tool trace (real UC function calls).

## Beat 4 · Decline with dignity (2 min)
Open **★ sub:900003 Midland Metal & Waste**: excluded trade → coded decline `APP-EXCL-WASTE`
citing guide UG-9.2; screening shows an **internal watchlist hit** (Derek Ashworth). Draft the
decline letter — it cites appetite ONLY; the watchlist reason stays internal (show the
Governance → conduct tab). Sanctions panel states the true-OFSI-match path: freeze + escalate.

## Beat 4b · MTAs — where the leakage lives (2 min)
Open the **MTAs** page (29 in the queue). Open **★ mta:900010** — the Calder Valley HX7 site adds
**+£4m buildings** mid-term: the DELTA check runs the accumulation **before/after (67% → 83%)**,
computes the **pro-rata additional premium (~£12.8k)** for the days remaining, grades the
authority the delta needs, and recommends **refer** — *the policy is in force, but the change
still breaches the referral line.* "Endorsements are ~70% of operational leakage because nobody
re-runs the checks on the delta. Here the delta IS a first-class decision" — approve/refer is
recorded to the same audit trail.

## Beat 5 · Try a submission (2 min)
Live what-if: same trade, switch district **M1 → HX7** — watch the flood loading and the
accumulation dial move. These are the same UC functions, called live.

## Beat 6 · Ingestion (2 min)
Source-assets map (live counts, honest roadmap rows) → quality contract from the DLT event log →
**quarantine drill**: the drifted schedule rows with `_rescued_data`, and the illegible fax below
the extraction-confidence gate. "Nothing silently lost."
**Loss-run gauntlet** card: five carriers' loss runs in five formats — a clean Aviva CSV, a
pipe-delimited Zurich text export with a totals row, a broker re-export with junk headers and
£-formatted amounts, a scanned image, our own PDF — normalised into ONE claims-experience table
(20 claims), with a **reconciliation view** showing what each file yielded and, crucially, the
ambiguous scan **held out** below the confidence gate rather than guessed at. This is the "prove
your OCR" answer.

## Beat 7 · Governance (3 min)
Decisions & audit → click your decision → **"exactly what the underwriter saw"** (the as-at
dossier snapshot — the auditor's question answered). Models & AI activity: the **SS1/23-shaped
model risk register** (owner, purpose, validation, monitoring — live from Unity Catalog) and the
activity log now records **every live agent interaction**. Then **the UC mask**: this app's
service principal is outside `underwriting_conduct_readers`, so watchlist reasons are redacted by
Unity Catalog itself.
**Subjectivity diary**: every recorded subjectivity with its parsed due date; for anything at
risk, the comms agent drafts the **day-12 chaser** to the broker — HITL approve, then it's logged.
Underwriters decide; the system remembers.

## Beat 8 · Underwriting AI + Insight (2 min)
Agent bench (tiles open real endpoints) → ask the supervisor about the book → Insight: the
**embedded** AI/BI dashboard + Genie answering with its SQL shown.

## Beat 8b · Appetite & Rate Committee (3 min) — the one nobody else has
The underwriting guide is DATA here. Pick food_manufacturing (adequacy ~73%) → propose 6.0‰ →
the open pipeline **reprices live** (Δ premium, new adequacy) → record to the committee register
(attributable, with the impact archived) → **Apply**: new quotes price on the new guide
immediately. "Your guide today is a PDF; here it's a governed table with an audit trail."

## Close · One Book (1 min)
The consolidation argument: live commercial lane + labelled placeholders (personal ×claims,
specialty ×reinsurance, MGA binders) — renewals, MTAs and post-bind obligations are now LIVE
lanes, not placeholders — each new line is a lane on the same governed chassis, not another silo.

## Q&A armour (practitioner-proofing)
- "EPC isn't a rating factor" — correct, and we never price on it: MEES/ESG lens only.
- "Crime data — my postcode rating has that" — here it's *evidence* for the named theft loading,
  district-grain labelled; production rates at full postcode.
- "Flood banding?" — evidence is the real EA register (areas naming the town); the band is
  curated from published RoFRS statistics, England only, surface water on the roadmap.
- "Would you really auto-decline?" — no; agents advise, the decline is recorded by a named human
  with a coded reason. Escalate-not-bind throughout.
- "GLM pricing?" — deliberately not duplicated: that's the sibling Pricing Workbench; this desk
  consumes a technical price and decides.
- "Prove your OCR on real loss runs" — the gauntlet: five carriers, five formats, one normalised
  table, a reconciliation view, and the ambiguous scan held out rather than hallucinated.
- "You auto-bind with no human? Conduct risk!" — only inside the e-trade authority row the
  committee itself governs, only when EVERY rule clears (appetite, screening, accumulation,
  adequacy floor); each one carries a full audit row and pack. Anything else lands with a human.
- "What about mid-term changes?" — the MTA lane: delta accumulation, delta authority, pro-rata AP,
  same audit trail.
