# DEMO_RUN.md — step-by-step demo (≈20 min + optional beats)

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

## Beat 1 · Portfolio Control Tower (3 min) — the Head of Underwriting
- Four vital signs: GWP vs plan, **retention % + rate change** (a real book is 70–80% renewals),
  rate adequacy, accumulation hot districts.
- **Prove it's real**: click the accumulation tile → reconciling district rows → "show the SQL".
- Funnel by channel: e-trade converts in minutes, email in days — "the 40% never quoted problem".

## Beat 2 · Inbox → the fast-track hero (3 min)
- Inbox is ranked by **P(bind)** (batch-scored model) with SLA clocks and why-chips.
- Open **★ sub:900001 Fenwick & Moss** (Altrincham shop package, e-trade):
  - all checks green; **straight-through eligible** under the e-trade authority row;
  - screening panel: a REAL near-miss — director "Emran Ali" vs OFSI-listed "Emraan ALI" —
    resolved on DOB/nationality and logged. *False-positive handling is where screening lives.*
  - price shows **IPT 12%** and commission — a UK quote without IPT is fake.
  - Record the decision → gold_decision_audit.

## Beat 3 · The referral hero (6 min) — the centrepiece
Open **★ sub:900002 Calder Valley Fine Foods** (mid-market Commercial Combined, email + scanned
proposal + risk schedule):
1. **Documents**: proposal PDF read by `ai_parse_document`; extracted hazards include
   *composite panels unconfirmed* and *frying line suppression unconfirmed* — THE UK food-manufacturing
   questions. The schedule arrived twice: v1 with drifted columns sits in **quarantine** (show later).
2. **Fair presentation**: broker states £8m turnover; filed accounts show **£24m** → Insurance
   Act 2015 flag; EL/PL basis + BI sums affected.
3. **Accumulation**: the two Calder Valley sites take HX7 from 67% → **87% of the £25m capacity** —
   over the 80% referral line, live, at point of quote. "Most carriers reconcile this quarterly
   in a spreadsheet."
4. **Price build-up**: named loadings — theft/malicious damage (real police.uk counts), flood
   (EA RoFRS High band), claims experience; IPT; broker target £140k vs technical ~£192k → adequacy ~73%.
5. **Recommendation: REFER** to a senior underwriter with the exact terms (£100k flood excess) and
   subjectivities (audited turnover 14 days · survey 60 days · composite panel confirmation).
6. HITL: click **Refer** → audit row. Draft the letter → the comms agent writes the
   subjectivities verbatim → **Approve & record**.
7. Optional: **Ask the supervisor** — watch the tool trace (real UC function calls).

## Beat 4 · Decline with dignity (2 min)
Open **★ sub:900003 Midland Metal & Waste**: excluded trade → coded decline `APP-EXCL-WASTE`
citing guide UG-9.2; screening shows an **internal watchlist hit** (Derek Ashworth). Draft the
decline letter — it cites appetite ONLY; the watchlist reason stays internal (show the
Governance → conduct tab). Sanctions panel states the true-OFSI-match path: freeze + escalate.

## Beat 5 · Try a submission (2 min)
Live what-if: same trade, switch district **M1 → HX7** — watch the flood loading and the
accumulation dial move. These are the same UC functions, called live.

## Beat 6 · Ingestion (2 min)
Source-assets map (live counts, honest roadmap rows) → quality contract from the DLT event log →
**quarantine drill**: the drifted schedule rows with `_rescued_data`, and the illegible fax below
the extraction-confidence gate. "Nothing silently lost."

## Beat 7 · Governance (2 min)
Decisions & audit (the rows you just wrote) → Models & AI activity → **the UC mask**: this app's
service principal is outside `underwriting_conduct_readers`, so watchlist reasons are redacted by
Unity Catalog itself.

## Beat 8 · Underwriting AI + Insight (2 min)
Agent bench (tiles open real endpoints) → ask the supervisor about the book → Insight: the
**embedded** AI/BI dashboard + Genie answering with its SQL shown.

## Close · One Book (1 min)
The consolidation argument: live commercial lane + labelled placeholders (personal ×claims,
specialty ×reinsurance, renewals, MTAs, post-bind obligations, MGA binders) — each new line is a
lane on the same governed chassis, not another silo.

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
