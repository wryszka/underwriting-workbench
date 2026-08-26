# Databricks notebook source
# MAGIC %md
# MAGIC # 05f · MCP tool functions — read/compute UC functions the managed MCP servers expose (Lane F)
# MAGIC
# MAGIC The Databricks **managed** MCP server (`/api/2.0/mcp/functions/{catalog}/{schema}`) exposes every
# MAGIC UC function in the schema as an agent tool — but UC functions are **read/compute only (no DML)**,
# MAGIC so every MUTATING tool lives in the custom FastMCP app instead (and returns a proposal_id). This
# MAGIC notebook adds the read/compute convenience + governance functions the F2 (decision-support) and
# MAGIC F4 (governance/audit) audiences need, composing the existing crux + Lane E functions/tables.
# MAGIC
# MAGIC Trust boundaries are enforced by **per-principal EXECUTE grants** on these functions (see the
# MAGIC grants notebook / DEPLOY.md), not by separate endpoints. Rich COMMENTs = the agent tool
# MAGIC descriptions (mirrored in `docs/MCP_TOOL_CONTRACTS.md`). SQL-UDF params are threaded via a
# MAGIC one-row param relation JOINed to the tables (never a param inside an aggregate — Lane E gotcha).
# MAGIC
# MAGIC NOT in the reset path (CREATE OR REPLACE FUNCTION revokes grants — re-grant/redeploy after).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"


def create_fn(sql):
    spark.sql(sql.format(F=fqn))
    print("  created:", sql.split("FUNCTION")[1].split("(")[0].strip())

# COMMAND ----------

# MAGIC %md ## `gold_mcp_activity` — the MCP-call audit trail (one row per tool invocation)
# MAGIC MCP clients (the F1 custom app + the demo harness) write here; managed-server calls are also
# MAGIC captured by UC system audit (documented backstop). The Governance "Agent traffic" card reads it.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gold_mcp_activity (
  event_ts TIMESTAMP, server STRING, tool STRING, tool_version STRING, principal STRING,
  agent_identity STRING, client_type STRING, arg_hash STRING, result_hash STRING,
  latency_ms DOUBLE, refused BOOLEAN, refusal_code STRING, refusal_reason STRING,
  submission_id STRING
) USING DELTA
""")
spark.sql(f"ALTER TABLE {fqn}.gold_mcp_activity SET TBLPROPERTIES ('layer'='gold','demo'='underwriting_workbench','lane'='mcp')")
spark.sql(f"COMMENT ON TABLE {fqn}.gold_mcp_activity IS 'MCP tool-call audit: one row per tool invocation across all client types (app / broker agent / underwriter copilot / audit agent) — timestamp, server, tool+version, calling principal, agent identity string, arg/result hashes, latency, and structured refusal reason. One audit trail, several clients.'")
print("  gold_mcp_activity ready")

# COMMAND ----------

# MAGIC %md ## F2 · `fn_assemble_dossier` — one call that composes the crux (decision-support convenience)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_assemble_dossier(sid STRING)
RETURNS STRUCT<submission_public_id STRING, summary_json STRING, appetite_json STRING,
  authority_json STRING, accumulation_json STRING, price_json STRING, sanctions_json STRING,
  recommendation_json STRING>
COMMENT 'Assemble the full underwriting dossier for one submission in a single call: composes the crux functions (extract summary, appetite, authority, accumulation, technical price, sanctions screen, recommendation) and returns each section as a JSON string an agent can read. READ-ONLY, no state change. Input: submission_public_id like sub:900002.'
RETURN SELECT named_struct(
  'submission_public_id', sid,
  'summary_json', to_json({F}.fn_extract_summary(sid)),
  'appetite_json', to_json({F}.fn_appetite_check(sid)),
  'authority_json', to_json({F}.fn_authority_check(sid)),
  'accumulation_json', to_json({F}.fn_accumulation_impact(sid)),
  'price_json', to_json({F}.fn_technical_price(sid)),
  'sanctions_json', to_json({F}.fn_sanctions_screen(sid)),
  'recommendation_json', to_json({F}.fn_recommendation(sid)))
""")

# COMMAND ----------

# MAGIC %md ## F2 · `fn_fire_pattern_precedent` — historical outcomes for a submission's referral fire-pattern

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_fire_pattern_precedent(sid STRING)
RETURNS STRUCT<submission_public_id STRING, fired_pattern ARRAY<STRING>, n_precedents BIGINT,
  approval_pct DOUBLE, decline_walk_pct DOUBLE, top_clause STRING, note STRING>
COMMENT 'Reviewer precedent lookup: the set of referral rules this submission fires (its fire-pattern), and the historical outcome distribution over gold_referral_telemetry for transactions that fired ANY of those rules — approval rate, decline/price-walk rate, and the most common bound-with-terms clause. What usually happens to risks like this. READ-ONLY. Input: submission_public_id.'
RETURN SELECT named_struct(
  'submission_public_id', sid, 'fired_pattern', pat.rules,
  'n_precedents', coalesce(h.n, 0),
  'approval_pct', round(h.appr / nullif(h.n, 0), 3),
  'decline_walk_pct', round(h.dw / nullif(h.n, 0), 3),
  'top_clause', h.top_clause,
  'note', concat('Fires ', array_size(pat.rules), ' rule(s); ', coalesce(h.n, 0), ' historical precedents on the same rules.'))
FROM (SELECT array_sort(transform({F}.fn_referral_events_from_checks(sid), e -> e.rule_id)) AS rules) pat
CROSS JOIN (
  SELECT count(*) AS n,
         count_if(outcome IN ('bound_clean','bound_with_terms')) AS appr,
         count_if(outcome IN ('declined','price_walked')) AS dw,
         max_by(terms_applied, CASE WHEN terms_applied IS NOT NULL THEN 1 ELSE 0 END) AS top_clause
  FROM {F}.gold_referral_telemetry
  WHERE fired AND array_contains(transform({F}.fn_referral_events_from_checks(sid), e -> e.rule_id), rule_id)
) h
""")

# COMMAND ----------

# MAGIC %md ## F4 · `fn_rulebook_as_of` — the governed rulebook as it stood at a date (SCD2 as-of)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_rulebook_as_of(p_as_of DATE)
RETURNS ARRAY<STRUCT<rule_id STRING, rule_name STRING, category STRING, disposition STRING,
  compliance_lock BOOLEAN, threshold_config STRING, rule_version STRING, valid_from DATE>>
COMMENT 'The governed referral rulebook exactly as it stood on a given date (SCD2 as-of query over ref_referral_rules: valid_from <= date < valid_to). Returns every rule in force then with its version, disposition, parameters and compliance lock. READ-ONLY. Input: as_of date (YYYY-MM-DD).'
RETURN SELECT collect_list(named_struct(
  'rule_id', r.rule_id, 'rule_name', r.rule_name, 'category', r.category, 'disposition', r.disposition,
  'compliance_lock', r.compliance_lock, 'threshold_config', r.threshold_config,
  'rule_version', r.rule_version, 'valid_from', r.valid_from))
FROM (SELECT p_as_of AS d) p
JOIN {F}.ref_referral_rules r ON r.valid_from <= p.d AND (r.valid_to IS NULL OR r.valid_to > p.d)
""")

# COMMAND ----------

# MAGIC %md ## F4 · `fn_decision_replay` — replay a decision under its contemporaneous rulebook (grandma-Jane)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_decision_replay(sid STRING)
RETURNS STRUCT<submission_public_id STRING, fired_rules_json STRING, recommendation_json STRING,
  price_buildup_json STRING, rulebook_versions_json STRING, note STRING>
COMMENT 'Replay one submission decision under the rulebook in force: the referral fire-vector (rules fired + the rule VERSION each fired under), the composed recommendation, the technical price build-up with its NAMED loadings, and the versions of the fired rules. The auditor grandma-Jane view. READ-ONLY. Input: submission_public_id like sub:900002.'
RETURN SELECT named_struct(
  'submission_public_id', sid,
  'fired_rules_json', to_json({F}.fn_referral_events_from_checks(sid)),
  'recommendation_json', to_json({F}.fn_recommendation(sid)),
  'price_buildup_json', to_json({F}.fn_technical_price(sid)),
  'rulebook_versions_json', to_json(transform({F}.fn_referral_events_from_checks(sid),
     e -> named_struct('rule_id', e.rule_id, 'rule_version', e.rule_version))),
  'note', 'Replayed under the current rulebook version of each fired rule (ref_referral_rules SCD2).')
""")

# COMMAND ----------

# MAGIC %md ## F4 · `fn_change_ledger` — rule-change history with predicted-vs-realised + drift

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_change_ledger(p_status STRING)
RETURNS ARRAY<STRUCT<change_id STRING, rule_id STRING, action STRING, status STRING, proposed_by STRING,
  from_version STRING, to_version STRING, predicted_hours_released DOUBLE, realised_hours_saved DOUBLE,
  predicted_gwp_delta DOUBLE, realised_gwp_effect DOUBLE, drift_flag BOOLEAN, drift_note STRING,
  rationale STRING>>
COMMENT 'The referral-rule change ledger with predicted-vs-realised tracking and drift flags. Filter by lifecycle status (proposed / approved / live / monitored / reversed / retired) or pass "all". READ-ONLY. Input: status filter or "all".'
RETURN SELECT collect_list(named_struct(
  'change_id', c.change_id, 'rule_id', c.rule_id, 'action', c.action, 'status', c.status,
  'proposed_by', c.proposed_by, 'from_version', c.from_version, 'to_version', c.to_version,
  'predicted_hours_released', c.predicted_hours_released, 'realised_hours_saved', c.realised_hours_saved,
  'predicted_gwp_delta', c.predicted_gwp_delta, 'realised_gwp_effect', c.realised_gwp_effect,
  'drift_flag', c.drift_flag, 'drift_note', c.drift_note, 'rationale', c.rationale))
FROM (SELECT p_status AS s) p
JOIN {F}.gold_rule_changes c ON (p.s = 'all' OR p.s IS NULL OR c.status = p.s)
""")

# COMMAND ----------

# MAGIC %md ## F4 · `fn_ai_activity_log` — every AI/agent touch incl. MCP calls (unified audit view)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_ai_activity_log(p_scope STRING)
RETURNS ARRAY<STRUCT<event_ts TIMESTAMP, source STRING, principal STRING, client_type STRING,
  activity STRING, detail STRING, refused BOOLEAN>>
COMMENT 'Unified AI/agent activity log: in-app agent narrations (gold_ai_activity) AND MCP tool calls (gold_mcp_activity), one normalised list newest first. Scope "mcp" returns MCP calls only, "agent" the in-app agents, "all" both; or pass a submission_public_id to scope to one submission. READ-ONLY. Input: scope ("all" / "mcp" / "agent" / sub:NNNNNN).'
RETURN SELECT slice(array_sort(collect_list(r), (a, b) -> CASE WHEN a.event_ts > b.event_ts THEN -1 WHEN a.event_ts < b.event_ts THEN 1 ELSE 0 END), 1, 200)
FROM (
  SELECT named_struct('event_ts', m.event_ts, 'source', concat('mcp:', m.server), 'principal', m.principal,
                      'client_type', m.client_type, 'activity', m.tool,
                      'detail', coalesce(m.refusal_reason, m.result_hash), 'refused', m.refused) AS r
  FROM {F}.gold_mcp_activity m
  WHERE (p_scope IN ('all','mcp') OR m.submission_id = p_scope)
  UNION ALL
  SELECT named_struct('event_ts', a.recorded_at, 'source', concat('agent:', a.agent), 'principal', a.agent,
                      'client_type', 'app', 'activity', a.activity,
                      'detail', a.reasoning, 'refused', false) AS r
  FROM {F}.gold_ai_activity a
  WHERE (p_scope IN ('all','agent') OR a.submission_public_id = p_scope)
) x
""")

# COMMAND ----------

# MAGIC %md ## Smoke — the read functions resolve and compose

# COMMAND ----------

import datetime
today = datetime.date.today().isoformat()

d = spark.sql(f"SELECT {fqn}.fn_assemble_dossier('sub:900002') AS d").first().d
assert d["recommendation_json"] and '"action"' in d["recommendation_json"], "dossier missing recommendation"
print("fn_assemble_dossier(900002): recommendation present ✓")

p = spark.sql(f"SELECT {fqn}.fn_fire_pattern_precedent('sub:900002') AS p").first().p
print("fn_fire_pattern_precedent(900002): pattern", p["fired_pattern"], "n_precedents", p["n_precedents"])
assert p["fired_pattern"] and len(p["fired_pattern"]) >= 1, "900002 should fire >=1 rule"

rb = spark.sql(f"SELECT {fqn}.fn_rulebook_as_of(DATE'{today}') AS r").first().r
print("fn_rulebook_as_of(today):", len(rb), "rules in force")
assert 20 <= len(rb) <= 30, f"expected ~24 rules, got {len(rb)}"

rp = spark.sql(f"SELECT {fqn}.fn_decision_replay('sub:900002') AS r").first().r
assert rp["fired_rules_json"] and rp["price_buildup_json"], "replay missing sections"
print("fn_decision_replay(900002): fire-vector + price build-up present ✓")

cl = spark.sql(f"SELECT {fqn}.fn_change_ledger('all') AS c").first().c
print("fn_change_ledger(all):", len(cl), "changes")
assert len(cl) >= 3, "expected the seeded change ledger"

al = spark.sql(f"SELECT {fqn}.fn_ai_activity_log('all') AS a").first().a
print("fn_ai_activity_log(all):", len(al), "entries (agent + mcp)")

print("✅ 05f MCP read functions — dossier / precedent / rulebook / replay / ledger / activity verified")
