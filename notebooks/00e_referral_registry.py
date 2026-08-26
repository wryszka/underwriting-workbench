# Databricks notebook source
# MAGIC %md
# MAGIC # 00e · Referral Control — rules as data (SCD2 registry) — SUPERSEDES flat rule seeding
# MAGIC
# MAGIC **Referral Control** turns the referral rulebook into governed, effective-dated data. This
# MAGIC notebook OWNS `ref_referral_rules` and supersedes the flat rule seeding that used to live in
# MAGIC `00c` (E1: 5 workflow rules) and `00d` (E6: 7 analytics rules). Those notebooks now only
# MAGIC generate *events/facts* — the rule rows are all seeded here, in one place, as an SCD2 table.
# MAGIC
# MAGIC What changes vs the old flat table:
# MAGIC * **SCD2**: `valid_from` / `valid_to` (NULL = current) + `is_current`. A rule change appends a
# MAGIC   new version and closes the prior one (the governance write-path in `07e` does this live);
# MAGIC   the crux reads the CURRENT (`valid_to IS NULL`) version, so decisions replay under the
# MAGIC   rulebook that was in force at the time.
# MAGIC * **Governed attributes**: `category` (taxonomy), `disposition` (refer / auto_decline /
# MAGIC   auto_apply_clause / accept), `compliance_lock` (sanctions / regulatory / treaty — the engine
# MAGIC   computes their metrics but NEVER recommends removing or weakening them), `review_effort_hours`
# MAGIC   (touch-cost input).
# MAGIC
# MAGIC **Byte-identical guarantee (invariant 10):** every CURRENT rule is seeded with the EXACT
# MAGIC threshold the crux used to hardcode, so once the crux reads the registry (05b/05d refactor)
# MAGIC heroes 900001-900004 reproduce byte-for-byte. The hero-gate in `98_smoke_test` proves it.
# MAGIC
# MAGIC Backward-compatible: keeps the original column names (`threshold_config`, `rule_version`,
# MAGIC `source_check`, `rule_scope`, `effective_from`) so existing readers (e.g. `fn_wageroll_check`)
# MAGIC are unaffected; the new columns are additive. Runs AFTER `00c` (before any rule reader).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
import json

TODAY = datetime.date.today()
# Rulebook epoch — the rulebook predates the 2-3yr backfill window (00f). All seed rules are in
# force from here; the backfill and future book (00f) sit inside [epoch, today+3mo].
RULEBOOK_EPOCH = datetime.date(TODAY.year - 3, 1, 1)
APPROVER = "underwriting_governance_committee"

print(f"Referral Control registry → {fqn}  epoch={RULEBOOK_EPOCH}")

# COMMAND ----------

# MAGIC %md ## The rule taxonomy
# MAGIC ~24 rules across the taxonomy the spec calls for: risk-selection, exposure size, cross-cover
# MAGIC interactions, question-set design, distribution, lifecycle, pricing-adjacent, and compliance
# MAGIC (locked). The 12 rules that already carry live behaviour or generated history are seeded with
# MAGIC their EXACT existing `rule_id` + `threshold_config` (so the crux and `00d`'s feed projection
# MAGIC keep resolving); the rest give the analytics breadth + carry the seeded storylines that the
# MAGIC detection engine (07d) discovers.
# MAGIC
# MAGIC `scope`: `workflow` = fires live in the crux + UI · `analytics_only` = retrospective history.
# MAGIC `disposition`: `refer` (raise to a human) · `auto_decline` · `auto_apply_clause` · `accept`.

# COMMAND ----------

# rule_id, name, category, description, params, unit, disposition, scope, source_check,
#   compliance_lock, review_effort_hours
RULES = [
    # ---- 5 WORKFLOW rules (fire live in the crux; EXACT thresholds preserved) ----------------
    ("SI_AUTHORITY_BAND", "Sum insured above authority band", "exposure_size",
     "Total sum insured exceeds the underwriter authority band and must refer up.",
     {"default": 5_000_000}, "GBP", "refer", "workflow", "fn_authority_check", False, 1.5),
    ("ROFRS_HIGH", "Flood band High", "risk_selection",
     "A location sits in EA RoFRS flood-band High; flood-High requires senior authority.",
     {"default": 1}, "band", "refer", "workflow", "fn_extract_summary", False, 1.0),
    ("FAIR_PRESENTATION_MISMATCH", "Fair-presentation turnover mismatch", "risk_selection",
     "Filed-accounts turnover materially exceeds declared turnover (Insurance Act 2015 duty).",
     {"default": 1.5}, "ratio", "refer", "workflow", "fn_extract_summary", False, 2.0),
    ("ACCUMULATION_CAPACITY", "District accumulation over referral line", "exposure_size",
     "Post-bind property accumulation in a district reaches the referral line (80% of capacity).",
     {"default": 0.80}, "ratio", "refer", "workflow", "fn_accumulation_impact", False, 1.5),
    ("MAX_WAGEROLL", "Max wageroll above authority band", "exposure_size",
     "Declared employers-liability wageroll (the EL rating basis) exceeds the authority band.",
     {"etrade": 2_000_000, "standard": 5_000_000, "senior": 12_000_000}, "GBP",
     "refer", "workflow", "fn_wageroll_check", False, 1.0),

    # ---- COMPLIANCE-LOCKED rules (metrics computed; change NEVER recommended) -----------------
    ("SANCTIONS_SCREEN_HIT", "Sanctions / watchlist screen hit", "compliance",
     "OFSI or internal-watchlist screen hit on the entity or a director. Freeze and escalate.",
     {"default": 1}, "flag", "auto_decline", "workflow", "fn_sanctions_screen", True, 2.5),
    ("REGULATORY_EXCLUDED_TRADE", "Regulatory / appetite excluded trade", "compliance",
     "Trade sits on the regulatory/appetite exclusion list (e.g. waste transfer) — auto-decline.",
     {"default": 1}, "flag", "auto_decline", "workflow", "fn_appetite_check", True, 2.0),
    ("TREATY_CAPACITY_LIMIT", "Outward treaty capacity limit", "compliance",
     "Risk would breach the outward reinsurance treaty capacity for the class — treaty-bound.",
     {"default": 1}, "flag", "refer", "analytics_only", None, True, 1.5),

    # ---- 7 ANALYTICS rules already carrying generated history (EXACT ids + params) ------------
    ("MAX_TURNOVER", "Turnover above authority band", "exposure_size",
     "Declared turnover exceeds the authority band for the class.",
     {"default": 20_000_000}, "GBP", "refer", "analytics_only", None, False, 0.8),
    ("MAX_SUMS_INSURED", "Sums insured above authority band", "exposure_size",
     "Total declared sums insured exceed the authority band.",
     {"default": 25_000_000}, "GBP", "refer", "analytics_only", None, False, 1.0),
    ("HAZARDOUS_ACTIVITY_HEIGHT", "Hazardous activity — work at height", "risk_selection",
     "Declared working-at-height activity above the delegated threshold (scaffolding/roofing). "
     "A niche hazard rule: most referrals are declined or walk on price.",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 1.2),
    ("CLAIMS_FREQUENCY", "Adverse claims frequency", "risk_selection",
     "Claims frequency over the trailing period exceeds the referral threshold.",
     {"default": 3}, "count", "refer", "analytics_only", None, False, 1.5),
    ("FLOOD_POSTCODE", "Flood-exposed postcode", "risk_selection",
     "Risk postcode sits in a flagged flood-exposed district (peril concentration).",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 1.0),
    ("PRICE_BELOW_TECHNICAL_FLOOR", "Price below technical floor", "pricing_adjacent",
     "Proposed price falls below the technical floor for the class (discretion referral).",
     {"default": 0.85}, "ratio", "refer", "analytics_only", None, False, 0.7),
    ("RENEWAL_RATE_CHANGE_TOLERANCE", "Renewal rate-change outside tolerance", "pricing_adjacent",
     "Renewal rate change falls outside the portfolio tolerance band.",
     {"default": 0.05}, "ratio", "refer", "analytics_only", None, False, 0.6),

    # ---- NEW taxonomy + storyline rules (discovered by the detection engine) ------------------
    # S3 high-volume release → auto_apply_clause + re_threshold
    ("EVENT_ATTENDANCE_LIMIT", "Event / attendance capacity limit", "exposure_size",
     "Declared event attendance or venue capacity exceeds the delegated threshold; a large minority "
     "bind with the same crowd-management clause every time.",
     {"default": 5_000}, "count", "refer", "analytics_only", None, False, 0.9),
    # S4 remove candidate → high approval, high NOADJ, near-zero isolation
    ("DUAL_TRADE_DECLARED", "Dual-trade declared", "question_design",
     "Submission declares a second trade in the free-text 'other activities' field; nearly always "
     "already caught by the specific trade/appetite rules.",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 0.6),
    # S5 looks removable but isn't → healthy approval, but isolated fires carry a worse loss ratio
    ("NEW_VENTURE_TRADING_HISTORY", "New venture — short trading history", "risk_selection",
     "Fewer than the minimum years of trading history for the class.",
     {"default": 3}, "years", "refer", "analytics_only", None, False, 1.0),
    # S7 renewal noise → biggest raw volume, re_threshold on materiality
    ("RENEWAL_UNCHANGED_RISK", "Renewal referral on unchanged risk", "lifecycle",
     "Renewal auto-referred despite no material change to the risk since last term.",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 0.5),
    # taxonomy breadth: distribution, cross-cover, lifecycle, exposure
    ("DELEGATED_AUTHORITY_BINDER", "Delegated-authority binder boundary", "distribution",
     "Risk sits at the edge of a delegated-authority binder's premium/limit boundary.",
     {"default": 250_000}, "GBP", "refer", "analytics_only", None, False, 1.0),
    ("CROSS_COVER_PL_EL_DUAL", "Cross-cover PL/EL dual-fire", "cross_cover",
     "PL and EL exposure interaction both trip a limit — a dual-firing cross-cover rule.",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 0.8),
    ("MTA_IMMATERIAL_CHANGE", "MTA referral on immaterial change", "lifecycle",
     "Mid-term adjustment auto-referred for a change below the materiality threshold.",
     {"default": 1}, "flag", "refer", "analytics_only", None, False, 0.4),
    ("CONTRACT_SIZE_SINGLE", "Single contract size limit", "exposure_size",
     "A single contract value exceeds the delegated single-contract threshold.",
     {"default": 10_000_000}, "GBP", "refer", "analytics_only", None, False, 1.0),
]

print(f"  taxonomy: {len(RULES)} rules")

# COMMAND ----------

# MAGIC %md ## Materialise the SCD2 registry (all seed rules current: valid_to NULL, is_current true)
# MAGIC Superseded historical versions are written LATER by the governance write-path (`07e`) and the
# MAGIC backfill (`00f`), which close a row (`valid_to`) and append the next `rule_version`.

# COMMAND ----------

rows = []
for (rid, name, cat, desc, params, unit, disp, scope, src, lock, effort) in RULES:
    rows.append((
        rid, name, cat, desc, json.dumps(params), disp, lock, scope, src, unit, float(effort),
        "v1", RULEBOOK_EPOCH, None, APPROVER, None, True, RULEBOOK_EPOCH.isoformat()))

SCHEMA = ("rule_id string, rule_name string, category string, description string, "
          "threshold_config string, disposition string, compliance_lock boolean, rule_scope string, "
          "source_check string, unit string, review_effort_hours double, rule_version string, "
          "valid_from date, valid_to date, approved_by string, change_id string, is_current boolean, "
          "effective_from string")

df = spark.createDataFrame(rows, SCHEMA)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ref_referral_rules")
spark.sql(f"ALTER TABLE {fqn}.ref_referral_rules SET TBLPROPERTIES "
          f"('layer'='reference', 'demo'='underwriting_workbench', 'lane'='referral_control', "
          f"'scd'='type2')")
spark.sql(f"COMMENT ON TABLE {fqn}.ref_referral_rules IS "
          f"'Referral rulebook as governed SCD2 config. One row per rule VERSION; the current "
          f"version has valid_to IS NULL / is_current = true. compliance_lock rules (sanctions, "
          f"regulatory, treaty) are computed but never recommended for removal or weakening.'")
print(f"  ref_referral_rules written: {spark.table(f'{fqn}.ref_referral_rules').count()} rows")

# COMMAND ----------

# MAGIC %md ## Verification — byte-identical guardrails + registry integrity

# COMMAND ----------

reg = {r.rule_id: r for r in spark.sql(
    f"SELECT * FROM {fqn}.ref_referral_rules WHERE valid_to IS NULL").collect()}

# 1) The 5 workflow rules exist with the EXACT thresholds the crux hardcoded (hero byte-identical).
def _param(rid, key):
    return json.loads(reg[rid].threshold_config)[key]

assert _param("SI_AUTHORITY_BAND", "default") == 5_000_000, "SI band threshold drifted"
assert _param("ACCUMULATION_CAPACITY", "default") == 0.80, "accumulation threshold drifted"
assert _param("FAIR_PRESENTATION_MISMATCH", "default") == 1.5, "fair-presentation threshold drifted"
assert _param("ROFRS_HIGH", "default") == 1, "rofrs band drifted"
wr = json.loads(reg["MAX_WAGEROLL"].threshold_config)
assert wr == {"etrade": 2_000_000, "standard": 5_000_000, "senior": 12_000_000}, f"wageroll params drifted: {wr}"
for rid in ("SI_AUTHORITY_BAND", "ROFRS_HIGH", "FAIR_PRESENTATION_MISMATCH",
            "ACCUMULATION_CAPACITY", "MAX_WAGEROLL"):
    assert reg[rid].rule_scope == "workflow", f"{rid} must stay workflow-scope"
    assert reg[rid].rule_version == "v1", f"{rid} current version must be v1 (hero events tag v1)"

# 2) The 7 analytics rules 00d's feed projection depends on all exist with their ids + params.
for rid, key, val in [("MAX_TURNOVER", "default", 20_000_000), ("MAX_SUMS_INSURED", "default", 25_000_000),
                      ("PRICE_BELOW_TECHNICAL_FLOOR", "default", 0.85),
                      ("RENEWAL_RATE_CHANGE_TOLERANCE", "default", 0.05)]:
    assert _param(rid, key) == val, f"{rid} param drifted"
for rid in ("HAZARDOUS_ACTIVITY_HEIGHT", "CLAIMS_FREQUENCY", "FLOOD_POSTCODE"):
    assert rid in reg, f"analytics rule {rid} missing — 00d feed projection would break"

# 3) Compliance-locked rules: locked, and disposition is refer or auto_decline (never accept/apply).
locked = {rid: r for rid, r in reg.items() if r.compliance_lock}
assert {"SANCTIONS_SCREEN_HIT", "REGULATORY_EXCLUDED_TRADE", "TREATY_CAPACITY_LIMIT"} <= set(locked), \
    f"expected the three compliance locks, got {sorted(locked)}"
for rid, r in locked.items():
    assert r.disposition in ("refer", "auto_decline"), f"{rid} locked rule has odd disposition {r.disposition}"

# 4) Every rule has a valid disposition + category; SCD2 columns coherent.
VALID_DISP = {"refer", "auto_decline", "auto_apply_clause", "accept"}
VALID_CAT = {"risk_selection", "exposure_size", "cross_cover", "question_design",
             "distribution", "lifecycle", "pricing_adjacent", "compliance"}
for rid, r in reg.items():
    assert r.disposition in VALID_DISP, f"{rid} bad disposition {r.disposition}"
    assert r.category in VALID_CAT, f"{rid} bad category {r.category}"
    assert r.is_current and r.valid_to is None, f"{rid} seed row must be current"
    assert r.review_effort_hours and r.review_effort_hours > 0, f"{rid} missing review effort"

n_by_cat = {r.category: 0 for r in reg.values()}
for r in reg.values():
    n_by_cat[r.category] += 1
print("  current rules by category:", dict(sorted(n_by_cat.items())))
print("  workflow:", sum(1 for r in reg.values() if r.rule_scope == "workflow"),
      "· analytics_only:", sum(1 for r in reg.values() if r.rule_scope == "analytics_only"),
      "· compliance-locked:", len(locked))
assert 20 <= len(reg) <= 30, f"expected ~24 current rules, got {len(reg)}"

print("✅ 00e Referral Control registry — SCD2, byte-identical guardrails verified")
