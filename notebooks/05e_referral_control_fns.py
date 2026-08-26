# Databricks notebook source
# MAGIC %md
# MAGIC # 05e · Referral Control functions — metrics · isolation · recommend · emulate (deterministic)
# MAGIC
# MAGIC The deterministic engine behind Referral Control. All read `gold_referral_telemetry` through an
# MAGIC `as_of` DATE parameter (time-travel), and all compute — the agents only narrate what these
# MAGIC produce (invariant 9). Closed action set: `remove` · `re_threshold` · `auto_apply_clause` ·
# MAGIC `convert_to_auto_decline` · `reopen_to_referral` · `reprice_instead_of_refer` · `split_question`
# MAGIC · `keep`. **Compliance-locked rules always return `keep`** — enforced HERE, at the function level.
# MAGIC
# MAGIC SQL-UDF pattern: a one-row **param relation** `p (rid, asof, recent_cut, prior_cut)` is JOINed to
# MAGIC the tables, so every `as_of` comparison is a local join/filter predicate — never a correlated
# MAGIC aggregate (which Spark rejects). `ref_referral_rules` drives (LEFT JOIN telemetry) so rules with
# MAGIC no history still return a clean row. Runs after `05d` in the ml job; NOT in the reset path.

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

# MAGIC %md ## fn_rule_metrics — per-rule metric tuple as of a date (loaded UW hour = GBP 95)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_rule_metrics(p_rule_id STRING, p_as_of DATE)
RETURNS STRUCT<rule_id STRING, as_of DATE, disposition STRING, compliance_lock BOOLEAN, category STRING,
  fires BIGINT, approvals BIGINT, approval_pct DOUBLE, decline_walk_pct DOUBLE, price_walk_pct DOUBLE,
  noadj_pct DOUBLE, isolated_fires BIGINT, isolated_share DOUBLE, avg_lr DOUBLE, isolated_lr DOUBLE,
  cofire_lr DOUBLE, gwp_bound DOUBLE, gwp_at_stake DOUBLE, shadow_fires BIGINT,
  top_clause STRING, top_clause_share DOUBLE, review_effort_hours DOUBLE, touch_cost_gbp DOUBLE,
  recent_fires BIGINT, prior_fires BIGINT, recent_stake DOUBLE, prior_stake DOUBLE>
COMMENT 'Per-rule referral metric tuple as of a date, over gold_referral_telemetry (as_of_date <= p_as_of): fire count, approval / decline+price-walk / price-walk / no-adjustment rates, isolation (fires alone), loss ratio overall vs isolated vs co-fire, GWP bound, shadow GWP-at-stake (auto-decline), dominant bound-with-terms clause + share, touch cost (fires x review effort x GBP95/hr), and recent/prior 90-day windows for drift. Inputs: rule_id, as_of date.'
RETURN SELECT named_struct(
  'rule_id', p_rule_id, 'as_of', p_as_of,
  'disposition', m.disposition, 'compliance_lock', m.compliance_lock, 'category', m.category,
  'fires', m.fires, 'approvals', m.approvals,
  'approval_pct', round(m.approvals / nullif(m.fires, 0), 3),
  'decline_walk_pct', round(m.dw / nullif(m.fires, 0), 3),
  'price_walk_pct', round(m.pw / nullif(m.fires, 0), 3),
  'noadj_pct', round(m.noadj / nullif(m.approvals, 0), 3),
  'isolated_fires', m.isolated, 'isolated_share', round(m.isolated / nullif(m.fires, 0), 3),
  'avg_lr', round(m.avg_lr, 1), 'isolated_lr', round(m.iso_lr, 1), 'cofire_lr', round(m.co_lr, 1),
  'gwp_bound', round(m.gwp_bound, 0), 'gwp_at_stake', round(m.stake, 0), 'shadow_fires', m.shadow,
  'top_clause', m.top_clause, 'top_clause_share', round(m.top_clause_n / nullif(m.bwt, 0), 3),
  'review_effort_hours', m.effort, 'touch_cost_gbp', round(m.fires * m.effort * 95.0, 0),
  'recent_fires', m.recent_fires, 'prior_fires', m.prior_fires,
  'recent_stake', round(m.recent_stake, 0), 'prior_stake', round(m.prior_stake, 0))
FROM (
  SELECT
    any_value(rr.disposition) AS disposition, any_value(rr.compliance_lock) AS compliance_lock,
    any_value(rr.category) AS category, any_value(rr.review_effort_hours) AS effort,
    count_if(t.fired) AS fires,
    count_if(t.fired AND t.outcome IN ('bound_clean','bound_with_terms')) AS approvals,
    count_if(t.fired AND t.outcome IN ('declined','price_walked')) AS dw,
    count_if(t.fired AND t.outcome = 'price_walked') AS pw,
    count_if(t.fired AND t.outcome IN ('bound_clean','bound_with_terms') AND t.loading_pts = 0) AS noadj,
    count_if(t.fired AND t.co_fire_count = 0) AS isolated,
    avg(CASE WHEN t.fired AND t.loss_ratio_pct IS NOT NULL THEN t.loss_ratio_pct END) AS avg_lr,
    avg(CASE WHEN t.fired AND t.co_fire_count = 0 AND t.loss_ratio_pct IS NOT NULL THEN t.loss_ratio_pct END) AS iso_lr,
    avg(CASE WHEN t.fired AND t.co_fire_count > 0 AND t.loss_ratio_pct IS NOT NULL THEN t.loss_ratio_pct END) AS co_lr,
    sum(CASE WHEN t.fired THEN t.gwp ELSE 0 END) AS gwp_bound,
    sum(CASE WHEN t.would_fire THEN t.technical_premium ELSE 0 END) AS stake,
    count_if(t.would_fire) AS shadow,
    count_if(t.fired AND t.outcome = 'bound_with_terms' AND t.terms_applied IS NOT NULL) AS bwt,
    max_by(t.terms_applied, CASE WHEN t.terms_applied IS NOT NULL THEN 1 ELSE 0 END) AS top_clause,
    count_if(t.fired AND t.terms_applied IS NOT NULL) AS top_clause_n,
    count_if(t.fired AND t.as_of_date > p.recent_cut) AS recent_fires,
    count_if(t.fired AND t.as_of_date <= p.recent_cut AND t.as_of_date > p.prior_cut) AS prior_fires,
    sum(CASE WHEN t.would_fire AND t.as_of_date > p.recent_cut THEN t.technical_premium ELSE 0 END) AS recent_stake,
    sum(CASE WHEN t.would_fire AND t.as_of_date <= p.recent_cut AND t.as_of_date > p.prior_cut THEN t.technical_premium ELSE 0 END) AS prior_stake
  FROM (SELECT p_rule_id AS rid, p_as_of AS asof, date_sub(p_as_of, 90) AS recent_cut, date_sub(p_as_of, 180) AS prior_cut) p
  JOIN {F}.ref_referral_rules rr ON rr.rule_id = p.rid AND rr.valid_to IS NULL
  LEFT JOIN {F}.gold_referral_telemetry t ON t.rule_id = p.rid AND t.as_of_date <= p.asof
) m
""")

# COMMAND ----------

# MAGIC %md ## fn_isolation_analysis — what uniquely fires on this rule, and what else catches the rest

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_isolation_analysis(p_rule_id STRING, p_as_of DATE)
RETURNS STRUCT<rule_id STRING, total_fires BIGINT, isolated_fires BIGINT, isolated_share DOUBLE,
  isolated_gwp DOUBLE, isolated_lr DOUBLE, cofire_fires BIGINT>
COMMENT 'Isolation analysis for a referral rule as of a date: how many fires are ISOLATED (fired alone -> only this rule catches the risk) vs co-fired, and the isolated GWP + loss ratio - the defence for remove/convert. Co-fire partner rules are materialised in gold_rule_cofire_partners. Inputs: rule_id, as_of date.'
RETURN SELECT named_struct(
  'rule_id', p_rule_id, 'total_fires', a.total, 'isolated_fires', a.iso,
  'isolated_share', round(a.iso / nullif(a.total, 0), 3),
  'isolated_gwp', a.iso_gwp, 'isolated_lr', a.iso_lr, 'cofire_fires', a.cofire)
FROM (
  SELECT count_if(t.fired) AS total, count_if(t.fired AND t.co_fire_count = 0) AS iso,
         round(sum(CASE WHEN t.fired AND t.co_fire_count = 0 THEN t.gwp ELSE 0 END), 0) AS iso_gwp,
         round(avg(CASE WHEN t.fired AND t.co_fire_count = 0 AND t.loss_ratio_pct IS NOT NULL THEN t.loss_ratio_pct END), 1) AS iso_lr,
         count_if(t.fired AND t.co_fire_count > 0) AS cofire
  FROM (SELECT p_rule_id AS rid, p_as_of AS asof) p
  LEFT JOIN {F}.gold_referral_telemetry t ON t.rule_id = p.rid AND t.as_of_date <= p.asof
) a
""")

# COMMAND ----------

# MAGIC %md ## fn_recommend_action — deterministic EV → one action from the closed set (locked ⇒ keep)

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_recommend_action(p_rule_id STRING, p_as_of DATE)
RETURNS STRUCT<rule_id STRING, as_of DATE, action STRING, severity STRING, headline STRING,
  reason STRING, portfolio_gbp_note STRING, ops_note STRING,
  referrals_released BIGINT, hours_released DOUBLE, gwp_impact DOUBLE,
  evidence STRUCT<fires BIGINT, approval_pct DOUBLE, decline_walk_pct DOUBLE, noadj_pct DOUBLE,
    isolated_share DOUBLE, isolated_lr DOUBLE, cofire_lr DOUBLE, top_clause STRING,
    top_clause_share DOUBLE, gwp_at_stake DOUBLE, recent_stake DOUBLE, prior_stake DOUBLE,
    touch_cost_gbp DOUBLE, compliance_lock BOOLEAN>>
COMMENT 'Deterministic recommendation for a referral rule as of a date. Returns ONE action from the closed set (keep / remove / re_threshold / auto_apply_clause / convert_to_auto_decline / reopen_to_referral / reprice_instead_of_refer / split_question) with the EV arithmetic as structured evidence and both currencies (portfolio GBP + operational hours). COMPLIANCE-LOCKED rules always return keep. Inputs: rule_id, as_of date.'
RETURN SELECT named_struct(
  'rule_id', p_rule_id, 'as_of', p_as_of, 'action', d.act, 'severity', d.sev,
  'headline', CASE d.act
      WHEN 'reopen_to_referral' THEN concat('Reopen ', p_rule_id, ' to referral (probation)')
      WHEN 'convert_to_auto_decline' THEN concat('Convert ', p_rule_id, ' to auto-decline')
      WHEN 'auto_apply_clause' THEN concat('Auto-apply the standard clause for ', p_rule_id)
      WHEN 'remove' THEN concat('Remove ', p_rule_id)
      WHEN 're_threshold' THEN concat('Re-threshold ', p_rule_id, ' on materiality')
      ELSE concat('Keep ', p_rule_id) END,
  'reason', CASE d.act
      WHEN 'reopen_to_referral' THEN concat('Auto-declined GWP-at-stake rose from GBP ', format_number(d.prior_stake,0), ' to GBP ', format_number(d.recent_stake,0), ' recently - a mix shift means good risk is being turned away; reopen to referral on probation, never straight to accept.')
      WHEN 'convert_to_auto_decline' THEN concat(format_number(d.decline_walk_pct*100,0), '% of ', d.fires, ' referrals were declined or walked on price; the few that bound run a ', format_number(d.avg_lr,0), '% loss ratio - nothing of value is lost by auto-declining.')
      WHEN 'auto_apply_clause' THEN concat(format_number(d.top_clause_share*100,0), '% of bound-with-terms cases apply the same clause (', d.top_clause, ') - auto-apply it and re-threshold to stop referring what is decided the same way every time.')
      WHEN 'remove' THEN concat(format_number(d.approval_pct*100,0), '% approved, ', format_number(d.noadj_pct*100,0), '% with no adjustment, and only ', format_number(d.isolated_share*100,0), '% fire in isolation - other rules already catch these risks.')
      WHEN 're_threshold' THEN concat(d.fires, ' fires with ', format_number(d.approval_pct*100,0), '% approved and ', format_number(d.noadj_pct*100,0), '% unadjusted - raise the materiality threshold to cut referral noise.')
      ELSE CASE WHEN d.compliance_lock THEN 'Compliance-locked (sanctions / regulatory / treaty) - computed, never changed.'
                WHEN coalesce(d.isolated_lr,0) - coalesce(d.cofire_lr,0) >= 15 THEN concat('Looks removable, but its ISOLATED fires run a ', format_number(d.isolated_lr,0), '% loss ratio vs ', format_number(d.cofire_lr,0), '% for co-fires - it is catching real risk. Keep.')
                ELSE 'Within tolerance - keep under watch.' END END,
  'portfolio_gbp_note', CASE WHEN d.act = 'convert_to_auto_decline' THEN concat('Portfolio: GBP ', format_number(d.gwp_bound,0), ' of low-value bound premium forgone')
       WHEN d.act = 'reopen_to_referral' THEN concat('Portfolio: ~GBP ', format_number(d.recent_stake,0), ' of good-risk GWP recoverable')
       ELSE 'Portfolio: neutral' END,
  'ops_note', CASE WHEN d.act IN ('remove','convert_to_auto_decline','auto_apply_clause')
            THEN concat('Ops: ~', format_number(d.fires * d.effort,0), ' referral-hours released (GBP ', format_number(d.fires * d.effort * 95,0), ')')
       WHEN d.act = 're_threshold' THEN concat('Ops: ~', format_number(d.fires * d.effort * 0.5,0), ' referral-hours released')
       ELSE 'Ops: neutral' END,
  'referrals_released', CASE WHEN d.act IN ('remove','convert_to_auto_decline','auto_apply_clause','re_threshold') THEN d.fires ELSE 0 END,
  'hours_released', CASE WHEN d.act IN ('remove','convert_to_auto_decline','auto_apply_clause') THEN round(d.fires * d.effort, 0)
                        WHEN d.act = 're_threshold' THEN round(d.fires * d.effort * 0.5, 0) ELSE 0 END,
  'gwp_impact', CASE WHEN d.act = 'convert_to_auto_decline' THEN -d.gwp_bound
                    WHEN d.act = 'reopen_to_referral' THEN d.recent_stake ELSE 0.0 END,
  'evidence', named_struct('fires', d.fires, 'approval_pct', d.approval_pct, 'decline_walk_pct', d.decline_walk_pct,
    'noadj_pct', d.noadj_pct, 'isolated_share', d.isolated_share, 'isolated_lr', d.isolated_lr,
    'cofire_lr', d.cofire_lr, 'top_clause', d.top_clause, 'top_clause_share', d.top_clause_share,
    'gwp_at_stake', d.gwp_at_stake, 'recent_stake', d.recent_stake, 'prior_stake', d.prior_stake,
    'touch_cost_gbp', d.touch_cost_gbp, 'compliance_lock', d.compliance_lock))
FROM (
  SELECT m.fires, m.approval_pct, m.decline_walk_pct, m.noadj_pct, m.isolated_share, m.isolated_lr,
         m.cofire_lr, m.top_clause, m.top_clause_share, m.gwp_at_stake, m.recent_stake, m.prior_stake,
         m.touch_cost_gbp, m.compliance_lock, m.avg_lr, m.gwp_bound, m.review_effort_hours AS effort,
    CASE
      WHEN m.compliance_lock THEN 'keep'
      WHEN m.disposition = 'auto_decline' AND m.prior_stake > 0 AND m.recent_stake >= 3 * m.prior_stake THEN 'reopen_to_referral'
      WHEN m.disposition = 'refer' AND m.decline_walk_pct >= 0.85 AND coalesce(m.approval_pct,0) <= 0.12 THEN 'convert_to_auto_decline'
      WHEN coalesce(m.top_clause_share,0) >= 0.5 AND coalesce(m.approval_pct,0) >= 0.6 THEN 'auto_apply_clause'
      WHEN coalesce(m.approval_pct,0) >= 0.85 AND coalesce(m.noadj_pct,0) >= 0.70 AND coalesce(m.isolated_share,1) <= 0.08 THEN 'remove'
      WHEN m.fires >= 400 AND coalesce(m.approval_pct,0) >= 0.85 AND coalesce(m.noadj_pct,0) >= 0.70 THEN 're_threshold'
      ELSE 'keep' END AS act,
    CASE
      WHEN m.compliance_lock THEN 'locked'
      WHEN m.disposition = 'auto_decline' AND m.prior_stake > 0 AND m.recent_stake >= 3 * m.prior_stake THEN 'high'
      WHEN m.disposition = 'refer' AND m.decline_walk_pct >= 0.85 AND coalesce(m.approval_pct,0) <= 0.12 THEN 'high'
      WHEN coalesce(m.top_clause_share,0) >= 0.5 AND coalesce(m.approval_pct,0) >= 0.6 THEN 'medium'
      WHEN coalesce(m.approval_pct,0) >= 0.85 AND coalesce(m.noadj_pct,0) >= 0.70 AND coalesce(m.isolated_share,1) <= 0.08 THEN 'medium'
      WHEN m.fires >= 400 AND coalesce(m.approval_pct,0) >= 0.85 AND coalesce(m.noadj_pct,0) >= 0.70 THEN 'medium'
      ELSE 'low' END AS sev
  FROM (SELECT {F}.fn_rule_metrics(p_rule_id, p_as_of) AS m) x
) d
""")

# COMMAND ----------

# MAGIC %md ## fn_emulate_rule_change — replay the book with a change + the MANDATORY tail exhibit

# COMMAND ----------

create_fn("""
CREATE OR REPLACE FUNCTION {F}.fn_emulate_rule_change(p_rule_id STRING, p_action STRING, p_as_of DATE)
RETURNS STRUCT<rule_id STRING, action STRING, as_of DATE,
  referrals_released BIGINT, hours_released DOUBLE, gwp_delta DOUBLE, isolated_freed BIGINT,
  predicted_lr_delta DOUBLE, affected_cases BIGINT,
  tail_exhibit ARRAY<STRUCT<company_name STRING, policy_number STRING, transaction_type STRING,
    gwp DOUBLE, outcome STRING, loss_ratio_pct DOUBLE>>, note STRING>
COMMENT 'Emulate a proposed rule change on the book as of a date and return the impact pack + the MANDATORY surviving tail exhibit (named policies decided differently, with GWP + outcome). referrals_released / hours_released / gwp_delta depend on the action. Preview only - a human approves (escalate-not-bind). Inputs: rule_id, action, as_of date.'
RETURN SELECT named_struct(
  'rule_id', p_rule_id, 'action', p_action, 'as_of', p_as_of,
  'referrals_released', CASE WHEN p_action IN ('remove','convert_to_auto_decline','auto_apply_clause') THEN a.fires
                            WHEN p_action = 're_threshold' THEN cast(round(a.fires * 0.5) AS BIGINT) ELSE 0 END,
  'hours_released', CASE WHEN p_action IN ('remove','convert_to_auto_decline','auto_apply_clause') THEN round(a.fires * a.effort, 0)
                        WHEN p_action = 're_threshold' THEN round(a.fires * a.effort * 0.5, 0) ELSE 0 END,
  'gwp_delta', CASE WHEN p_action = 'convert_to_auto_decline' THEN -a.gwp_bound
                   WHEN p_action = 'reopen_to_referral' THEN a.stake
                   WHEN p_action = 'reprice_instead_of_refer' THEN round(a.gwp_bound * 0.03, 0) ELSE 0.0 END,
  'isolated_freed', a.isolated,
  'predicted_lr_delta', CASE WHEN p_action = 'convert_to_auto_decline' THEN round((55.0 - a.avg_lr) * 0.02, 2)
                            WHEN p_action = 'reopen_to_referral' THEN -2.0 ELSE 0.0 END,
  'affected_cases', size(a.tail_all), 'tail_exhibit', slice(a.tail_all, 1, 20),
  'note', concat('Emulated ', p_action, ' on ', p_rule_id, ' as of ', p_as_of,
                 '. Tail exhibit = the cases decided differently. Preview only - a human approves.'))
FROM (
  SELECT
    count_if(t.fired) AS fires, count_if(t.fired AND t.co_fire_count = 0) AS isolated,
    sum(CASE WHEN t.fired THEN t.gwp ELSE 0 END) AS gwp_bound,
    sum(CASE WHEN t.would_fire THEN t.technical_premium ELSE 0 END) AS stake,
    avg(CASE WHEN t.fired AND t.loss_ratio_pct IS NOT NULL THEN t.loss_ratio_pct END) AS avg_lr,
    any_value(rr.review_effort_hours) AS effort,
    filter(collect_list(CASE WHEN (p.action = 'reopen_to_referral' AND t.would_fire)
                               OR (p.action <> 'reopen_to_referral' AND t.fired AND t.outcome IN ('bound_clean','bound_with_terms'))
                             THEN named_struct('company_name', t.company_name, 'policy_number', t.policy_number,
                                  'transaction_type', t.transaction_type, 'gwp', t.gwp, 'outcome', t.outcome,
                                  'loss_ratio_pct', t.loss_ratio_pct) END), x -> x IS NOT NULL) AS tail_all
  FROM (SELECT p_rule_id AS rid, p_as_of AS asof, p_action AS action) p
  JOIN {F}.ref_referral_rules rr ON rr.rule_id = p.rid AND rr.valid_to IS NULL
  LEFT JOIN {F}.gold_referral_telemetry t ON t.rule_id = p.rid AND t.as_of_date <= p.asof
) a
""")

# COMMAND ----------

# MAGIC %md ## Smoke — the storylines produce the expected recommendations as of today

# COMMAND ----------

import datetime
today = datetime.date.today().isoformat()

def rec(rid):
    r = spark.sql(f"SELECT {fqn}.fn_recommend_action('{rid}', DATE'{today}') AS r").first().r
    print(f"  {rid:32s} → {r['action']:24s} [{r['severity']}]  {r['reason'][:80]}")
    return r["action"]

print("Recommendations as of", today)
got = {rid: rec(rid) for rid in
       ["HAZARDOUS_ACTIVITY_HEIGHT", "DUAL_TRADE_DECLARED", "RENEWAL_UNCHANGED_RISK",
        "EVENT_ATTENDANCE_LIMIT", "NEW_VENTURE_TRADING_HISTORY", "SANCTIONS_SCREEN_HIT"]}

assert got["SANCTIONS_SCREEN_HIT"] == "keep", "compliance-locked must be keep"
assert got["DUAL_TRADE_DECLARED"] == "remove", f"S4 expected remove, got {got['DUAL_TRADE_DECLARED']}"
assert got["NEW_VENTURE_TRADING_HISTORY"] == "keep", f"S5 expected keep, got {got['NEW_VENTURE_TRADING_HISTORY']}"
assert got["EVENT_ATTENDANCE_LIMIT"] == "auto_apply_clause", f"S3 expected auto_apply_clause, got {got['EVENT_ATTENDANCE_LIMIT']}"
assert got["RENEWAL_UNCHANGED_RISK"] == "re_threshold", f"S7 expected re_threshold, got {got['RENEWAL_UNCHANGED_RISK']}"
assert got["HAZARDOUS_ACTIVITY_HEIGHT"] in ("convert_to_auto_decline", "reopen_to_referral"), \
    f"S1/S2 expected convert or reopen, got {got['HAZARDOUS_ACTIVITY_HEIGHT']}"

locked = spark.sql(f"SELECT {fqn}.fn_recommend_action('TREATY_CAPACITY_LIMIT', DATE'{today}') AS r").first().r
assert locked["action"] == "keep" and locked["severity"] == "locked", "locked rule must be keep/locked"

em = spark.sql(f"SELECT {fqn}.fn_emulate_rule_change('HAZARDOUS_ACTIVITY_HEIGHT','convert_to_auto_decline', DATE'{today}') AS e").first().e
print(f"  emulate HAZARDOUS convert: affected={em['affected_cases']} tail_rows={len(em['tail_exhibit'])} gwp_delta={em['gwp_delta']}")
assert len(em["tail_exhibit"]) > 0, "tail exhibit is mandatory and must be non-empty"

iso_d = spark.sql(f"SELECT {fqn}.fn_isolation_analysis('DUAL_TRADE_DECLARED', DATE'{today}') AS i").first().i
iso_h = spark.sql(f"SELECT {fqn}.fn_isolation_analysis('HAZARDOUS_ACTIVITY_HEIGHT', DATE'{today}') AS i").first().i
print(f"  isolation DUAL_TRADE={iso_d['isolated_share']} HAZARDOUS={iso_h['isolated_share']}")
assert iso_d["isolated_share"] <= 0.05 and iso_h["isolated_share"] >= 0.7

print("✅ 05e Referral Control functions — recommendations + emulation + isolation verified on storylines")
