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

Build spec: `UNDERWRITING_WORKBENCH_BUILD_BRIEF.md` · Conventions: `CONVENTIONS.md` ·
Deployment: `docs/DEPLOY.md` (asset inventory + fresh-workspace runbook).

## Quick start

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev          # two-pass: re-run after models exist
# then run jobs in order: 00 setup → ingest → ml → ai → agent; see docs/DEPLOY.md
```
