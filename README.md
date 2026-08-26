# Underwriting Workbench — Bricksurance SE

End-to-end **commercial underwriting workbench** on Databricks: broker submissions in every format
land in one governed lakehouse, are extracted (Document AI), enriched with real UK open data,
triaged, checked against appetite / authority / accumulation / sanctions, technically priced, and
decided by a human underwriter with a complete dossier — minutes not days, every step recorded in
Unity Catalog.

> **About this demo** — Bricksurance SE is a synthetic insurer; all portfolio data is generated.
> This is not a Databricks product: it is a working demonstration built entirely on Databricks
> services (Lakeflow Declarative Pipelines, Unity Catalog, Feature Store, MLflow + Model Serving,
> Foundation Model API, Mosaic AI Agent Framework, AI/BI Genie + Dashboards, Databricks Apps).
> Bundled open datasets (EA flood, police.uk crime, EPC bands, OFSI sanctions list, ONS
> geography) are real and OGL-licensed. Rating logic is illustrative.

## What it shows

1. **Process management end-to-end** — full submission lifecycle (received → … → bound /
   declined / NTU / lost) with SLA clocks, funnel by channel, auditor timeline.
2. **Ingestion of many sources** — broker emails, scanned PDF proposal forms
   (`ai_parse_document`), risk schedules with schema drift (quarantined), portal feed, PAS book,
   bundled real open data, simulated API enrichment (+ a real Companies House API example).
3. **Governed process** — decision audit, data inventory, AI activity log, UC dynamic masking,
   real lineage, authority & conduct view.
4. **Agents where they help** — role agents (risk profile, appetite, pricing adequacy, broker
   comms, challenge) + one real tool-calling supervisor over UC functions + Genie, all behind
   the workbench UI with human-in-the-loop decisions. Agents advise; humans bind.
5. **Underwriting discipline, measured** — every referral rule and every pricing-pen decision
   lands in a governed metric trunk; the questions a Head of Underwriting actually asks (how
   often does a rule bite, what does the pen do with it, where does discretion leak) are
   answered in Genie, not in a spreadsheet request.
6. **Referral Control** — the referral rulebook as governed SCD2 data over a full fire-vector
   telemetry substrate: a continuous loop that discovers this week's problem rules, recommends a
   governed change from a closed action set (with a mandatory tail exhibit), emulates it, takes a
   human approval, then tracks realised-vs-predicted and recommends reversals when the world
   shifts — with time-travel across the book. Compliance-locked rules are computed but never
   changed. See `docs/REFERRAL_CONTROL_SPEC.md`.
7. **A governed agent tool surface (MCP)** — the same UC functions, Genie spaces and workflows are
   reachable over Model Context Protocol by internal copilots, an external broker's agent, and Claude —
   under the same Unity Catalog governance, into the same audit trail. **The app is one client among
   several.** Managed MCP over UC functions + Genie (identity-scoped by per-principal grants) plus a
   custom FastMCP app for the external write-path. Escalate-not-bind at the protocol level: no tool
   binds, approves or sends — mutating tools return `pending_human_approval`. See `docs/MCP_ARCHITECTURE.md`.

Build spec: `UNDERWRITING_WORKBENCH_BUILD_BRIEF.md` · Conventions: `CONVENTIONS.md` ·
Deployment: `docs/DEPLOY.md` (asset inventory + fresh-workspace runbook) ·
Demo run: `docs/DEMO_RUN.md` + [Google Doc](https://docs.google.com/document/d/1-J6OfcRAekJUEwmA3kWD3GpBZx7OoNT0LbDLA7j-jRY/edit).

## Quick start

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev          # two-pass: re-run after models exist
# then run jobs in order: 00 setup → ingest → ml → ai → agent; see docs/DEPLOY.md
```
