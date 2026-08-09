# Databricks notebook source
# MAGIC %md
# MAGIC # 00c · Lane E setup — referral & pricing-discretion analytics (ADDITIVE)
# MAGIC
# MAGIC Client-driven lane (a practitioner question about referral effectiveness and pricing
# MAGIC discretion). **Strictly additive** to the seed=42 universe built in `00`:
# MAGIC
# MAGIC * runs AFTER `00` and reads its output (`landing_pas_policies`, `landing_submissions_feed`);
# MAGIC * every new random draw uses an **independent `random.Random(4242)` / `default_rng(4242)`**
# MAGIC   instance — it never consumes from the seed=42 stream, so no existing row moves
# MAGIC   (invariant 10 / hard-constraint 3);
# MAGIC * new REFERENCE + generated FACT tables only; the one write to an existing table is a
# MAGIC   single APPENDED hero row (`sub:900004`) to `landing_submissions_feed`, guarded by the
# MAGIC   scoped-checksum assert (existing submissions excluded) below.
# MAGIC
# MAGIC Phases seeded here: E1 `ref_referral_rules` · E2 wageroll + hero 900004 · E3a personas +
# MAGIC `gold_transactions` + generated referral-event history · E4 `ref_adjustment_reasons` +
# MAGIC `gold_premium_components`. Materialisation of `gold_referral_events` is in `03c`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
import random

# Lane E RNG — isolated instance, never the module-global `random` the seed=42 book used.
LANE_E_SEED = 4242
rnd = random.Random(LANE_E_SEED)
TODAY = datetime.date.today()
# 2025-anchored coverage window that re-anchors relative to today on every reset.
YEAR_START = datetime.date(TODAY.year - 1, 1, 1)


def write(df, name, layer):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.{name}")
    spark.sql(f"ALTER TABLE {fqn}.{name} SET TBLPROPERTIES "
              f"('layer'='{layer}', 'demo'='underwriting_workbench', 'lane'='referral_discretion')")
    print(f"  {name}: {spark.table(f'{fqn}.{name}').count()} rows")


# The landing_submissions_feed schema is a fixed contract owned by notebook 00; restated here so
# the hero append matches it exactly. If 00's schema changes, this must track it (smoke asserts shape).
def sub_schema_str():
    return ("submission_public_id string, received_ts string, channel string, broker_id string, "
            "company_number string, company_name string, trade_group string, sic_code_declared string, "
            "segment string, postcode_district string, n_locations int, "
            "turnover_stated long, employees int, floor_area_m2 int, construction_type string, year_built int, "
            "buildings_si long, plant_si long, contents_si long, stock_si long, bi_si long, bi_indemnity_months int, "
            "el_limit long, pl_limit long, git_si long, deterioration_of_stock_si long, money_si long, "
            "target_premium long, incumbent_insurer string, notes string, "
            "lifecycle_state string, outcome string, quoted_premium long, quote_ts string, decided_ts string, decline_code string")


print(f"Lane E setup → {fqn}  rng={LANE_E_SEED}  as_at={TODAY}")

# COMMAND ----------

# MAGIC %md ## E1 · `ref_referral_rules` — the referral rulebook as governed config
# MAGIC Every referral rule the crux can raise, versioned, with its threshold configuration. The
# MAGIC crux functions are UNCHANGED — this table names and versions the rules so `gold_referral_events`
# MAGIC and the metric view can analyse *every* rule, not just wageroll. Thresholds are illustrative.

# COMMAND ----------

import json

# threshold_config is JSON keyed by authority band where a rule is band-dependent, else {"default": …}.
REFERRAL_RULES = [
    ("SI_AUTHORITY_BAND", "Sum insured above authority band",
     "Total sum insured exceeds the underwriter authority band and must refer up.",
     {"default": 5_000_000}, "GBP", "v1", "fn_authority_check"),
    ("ROFRS_HIGH", "Flood band High",
     "A location sits in EA RoFRS flood-band High; flood-High requires senior authority.",
     {"default": 1}, "band", "v1", "fn_extract_summary"),
    ("FAIR_PRESENTATION_MISMATCH", "Fair-presentation turnover mismatch",
     "Filed-accounts turnover materially exceeds declared turnover (Insurance Act 2015).",
     {"default": 1.5}, "ratio", "v1", "fn_extract_summary"),
    ("ACCUMULATION_CAPACITY", "District accumulation over referral line",
     "Post-bind property accumulation in a district reaches the referral line (80% of capacity).",
     {"default": 0.80}, "ratio", "v1", "fn_accumulation_impact"),
    # MAX_WAGEROLL is appended by E2 (its rule + fn + hero land together).
]

rules_rows = [(rid, name, desc, json.dumps(cfg), unit, ver, src, YEAR_START.isoformat())
              for (rid, name, desc, cfg, unit, ver, src) in REFERRAL_RULES]
write(spark.createDataFrame(
        rules_rows,
        "rule_id string, rule_name string, description string, threshold_config string, "
        "unit string, rule_version string, source_check string, effective_from string"),
      "ref_referral_rules", "reference")

# COMMAND ----------

# MAGIC %md ## Scoped-checksum baseline (hard-constraint 3)
# MAGIC The existing seed=42 book must be byte-identical. `00c` appends only the Lane E hero id
# MAGIC (`sub:900004`). Capture the checksum over submissions EXCLUDING Lane E ids NOW (before the
# MAGIC append), and re-verify it immediately after the append below. The smoke test recomputes the
# MAGIC same scoped checksum independently.

# COMMAND ----------

LANE_E_SUB_IDS = ("sub:900004",)
_excl = ", ".join(f"'{i}'" for i in LANE_E_SUB_IDS)


def scoped_checksum():
    r = spark.sql(f"""SELECT count(*) n, sum(crc32(to_json(struct(*)))) ck
                      FROM {fqn}.landing_submissions_feed
                      WHERE submission_public_id NOT IN ({_excl})""").first()
    return r.n, r.ck


_ck_before = scoped_checksum()
print(f"existing-book (excl. Lane E) baseline: rows={_ck_before[0]} checksum={_ck_before[1]}")

# COMMAND ----------

# MAGIC %md ## E2 · MAX_WAGEROLL rule + wageroll sidecar + hero `sub:900004`
# MAGIC Wageroll = the declared Employers' Liability rating basis. New rule: declared wageroll above
# MAGIC the authority band refers up. Config-driven thresholds (illustrative): e-trade £2.0m,
# MAGIC standard £5.0m, senior £12.0m. Appended to `ref_referral_rules` (additive row).

# COMMAND ----------

WAGEROLL_THRESHOLDS = {"etrade": 2_000_000, "standard": 5_000_000, "senior": 12_000_000}
spark.sql(f"""
  INSERT INTO {fqn}.ref_referral_rules VALUES
  ('MAX_WAGEROLL', 'Max wageroll above authority band',
   'Declared employers-liability wageroll (the EL rating basis) exceeds the authority band and must refer up.',
   '{json.dumps(WAGEROLL_THRESHOLDS)}', 'GBP', 'v1', 'fn_wageroll_check', '{YEAR_START.isoformat()}')
""")
print("  ref_referral_rules += MAX_WAGEROLL")

# COMMAND ----------

# MAGIC %md ### `silver_submission_wageroll` — declared wageroll per submission (sidecar)
# MAGIC Generated for the whole existing book (rng 4242), correlated with declared turnover and
# MAGIC labour-intensity by trade. NOT added as a column to any existing table — joins on
# MAGIC `submission_public_id`. (A live `ai_query` extraction of the hero's statement is
# MAGIC demonstrated in `01d_wageroll_extraction`; this seed keeps the table reset-safe.)

# COMMAND ----------

# Labour share of turnover by trade (wageroll ÷ turnover) — labour-heavy trades higher.
LABOUR_SHARE = {
    "construction_contractors": 0.46, "metal_engineering": 0.40, "light_manufacturing": 0.34,
    "food_manufacturing": 0.30, "hospitality_restaurant": 0.38, "hotels_leisure": 0.36,
    "healthcare_clinics": 0.42, "hair_beauty": 0.44, "education_training": 0.48,
    "warehousing_logistics": 0.26, "motor_trade": 0.24, "wholesale": 0.18,
    "retail_shop": 0.20, "office_professional": 0.30, "property_owners": 0.06,
    "waste_recycling": 0.28, "nightclubs": 0.34,
}
DEFAULT_SHARE = 0.28

feed = spark.sql(f"""SELECT submission_public_id, trade_group, turnover_stated
                     FROM {fqn}.landing_submissions_feed""").collect()
wr_rows = []
for r in feed:
    if r.submission_public_id == "sub:900004":
        continue  # hero seeded explicitly below
    turnover = r.turnover_stated or 0
    share = LABOUR_SHARE.get(r.trade_group, DEFAULT_SHARE) * rnd.uniform(0.80, 1.20)
    wageroll = int(max(0, turnover * share))
    wr_rows.append((r.submission_public_id, wageroll, "generated", round(rnd.uniform(0.86, 0.98), 2)))
# Hero 900004: declared wageroll £6.8m — above the £5.0m standard band → MAX_WAGEROLL fires.
wr_rows.append(("sub:900004", 6_800_000, "ai_parse_document + ai_query", 0.93))

write(spark.createDataFrame(
        wr_rows,
        "submission_public_id string, declared_wageroll long, source string, extraction_confidence double"),
      "silver_submission_wageroll", "silver")

# COMMAND ----------

# MAGIC %md ### Hero `sub:900004` — Harwood & Vane Scaffolding Ltd (single-trigger wageroll referral)
# MAGIC The ONE write to an existing table: append the 900004 row to `landing_submissions_feed`
# MAGIC (rng 4242, rolling date "this morning"). Scaffolding = `construction_contractors`. Sized so
# MAGIC total SI < £5m, non-flood district (LS10), no fair-presentation/accumulation issue — so the
# MAGIC only NAMED referral rule that fires is MAX_WAGEROLL. Byte-identical after reset.

# COMMAND ----------

_now = datetime.datetime.now()
H4_RECEIVED = _now - datetime.timedelta(hours=1)   # e-trade/portal mid-market, ~an hour old
# Field order mirrors landing_submissions_feed sub_schema (36 fields) exactly.
HERO_900004 = (
    "sub:900004", H4_RECEIVED.isoformat(), "portal", "BRK-005", "08814430",
    "Harwood & Vane Scaffolding Ltd", "construction_contractors", "43999", "mid_market", "LS10", 1,
    14_800_000, 178, 1_400, "steel_frame_clad", 2004,
    800_000, 1_500_000, 150_000, 350_000, 1_200_000, 12, 10_000_000, 5_000_000, 0, 0, 5_000,
    72_000, "Aviva", None,
    "received", None, None, None, None, None)
# Append-only; the scoped-checksum assert below proves no existing row moved.
spark.createDataFrame([HERO_900004], sub_schema_str()) \
     .write.mode("append").saveAsTable(f"{fqn}.landing_submissions_feed")
print("  landing_submissions_feed += sub:900004 (append)")

# Re-verify: the existing book (excl. Lane E ids) is byte-identical after the append.
_ck_after = scoped_checksum()
assert _ck_after == _ck_before, \
    f"HARD-CONSTRAINT 3 VIOLATED: existing-book checksum changed {_ck_before} → {_ck_after}"
assert spark.sql(f"SELECT count(*) c FROM {fqn}.landing_submissions_feed "
                 f"WHERE submission_public_id='sub:900004'").first().c == 1, "900004 append failed"
print(f"  scoped checksum unchanged ✓ (existing rows {_ck_after[0]})")

# COMMAND ----------

# MAGIC %md ## E3a · underwriter personas — `ref_underwriter_persona`
# MAGIC A pricing-behaviour label per underwriter (disciplined / median / generous), joined to the
# MAGIC existing `ref_underwriter` on `underwriter_id`. NOT `ref_underwriters` (that would collide
# MAGIC with the existing singular table). The generous cohort gives more away on referred renewals —
# MAGIC the signal the metric view surfaces. Deterministic assignment.

# COMMAND ----------

PERSONA_MAP = {
    "UW-01": "disciplined", "UW-03": "disciplined", "UW-06": "disciplined", "UW-09": "disciplined",
    "UW-02": "median", "UW-05": "median", "UW-07": "median", "UW-12": "median",
    "UW-04": "generous", "UW-08": "generous", "UW-10": "generous", "UW-11": "generous",
}
PERSONA_ADJ = {"disciplined": -2.0, "median": 0.0, "generous": 4.0}  # extra give-away points
persona_rows = [(uw, p, PERSONA_ADJ[p],
                 {"disciplined": "Prices to technical; rarely discounts beyond retention need",
                  "median": "Typical discretion within delegated authority",
                  "generous": "Leans on the pricing pen, especially at renewal"}[p])
                for uw, p in PERSONA_MAP.items()]
write(spark.createDataFrame(
        persona_rows,
        "underwriter_id string, persona string, giveaway_adj_pts double, persona_note string"),
      "ref_underwriter_persona", "reference")

_by_persona = {}
for uw, p in PERSONA_MAP.items():
    _by_persona.setdefault(p, []).append(uw)
for p in _by_persona:
    _by_persona[p].sort()

# COMMAND ----------

# MAGIC %md ## E4 · `ref_adjustment_reasons` — named discount/load reason codes

# COMMAND ----------

ADJUSTMENT_REASONS = [
    ("NB_COMPETITIVE_DISCOUNT", "New-business competitive discount", "discount", "acquisition"),
    ("RENEWAL_RETENTION_DISCOUNT", "Renewal retention discount", "discount", "retention"),
    ("BROKER_COMMITMENT_DISCOUNT", "Broker commitment / volume discount", "discount", "distribution"),
    ("CLAIMS_EXPERIENCE_LOAD", "Adverse claims-experience load", "load", "technical"),
    ("RISK_FEATURE_LOAD", "Risk-feature load (survey / exposure)", "load", "technical"),
    ("MANUAL_OTHER", "Other manual adjustment (note required)", "either", "manual"),
]
write(spark.createDataFrame(
        ADJUSTMENT_REASONS,
        "reason_code string, label string, direction string, category string"),
      "ref_adjustment_reasons", "reference")

DISCOUNT_REASON = {"NEW_BUSINESS": "NB_COMPETITIVE_DISCOUNT",
                   "RENEWAL": "RENEWAL_RETENTION_DISCOUNT",
                   "MTA": "BROKER_COMMITMENT_DISCOUNT"}

# COMMAND ----------

# MAGIC %md ## E3a + E4 · `gold_transactions` + generated referral history + `gold_premium_components`
# MAGIC Roll the in-force PAS book forward into NB/RN/MTA transaction facts across the 2025 window.
# MAGIC A planted, discoverable discretion signal on MAX_WAGEROLL-referred transactions
# MAGIC (renewal give-away > MTA > NB; generous persona gives more) — survives a GROUP BY, doesn't
# MAGIC scream from a scatter. **Facts only** — no inbox rows, no lifecycle; the renewal workbench
# MAGIC stays a One Book placeholder. Every referred transaction emits exactly one MAX_WAGEROLL event.

# COMMAND ----------

import hashlib

# Pull the in-force book (deterministic order for reproducible sampling).
pol = spark.sql(f"""
    SELECT policy_number, trade_group, segment, gross_premium, commission_pct,
           coalesce(product_line, CASE WHEN segment='mid_market' THEN 'commercial_combined'
                                       ELSE 'commercial_package' END) AS product
    FROM {fqn}.landing_pas_policies
    WHERE policy_status = 'in_force'
    ORDER BY policy_number
""").collect()

# Labour-heavy trades are the natural home of high wageroll → referred cohort drawn from them.
HIGH_WAGEROLL_TRADES = {"construction_contractors", "metal_engineering", "light_manufacturing",
                        "food_manufacturing", "warehousing_logistics", "hospitality_restaurant"}
labour_pol = [p for p in pol if p.trade_group in HIGH_WAGEROLL_TRADES]
rnd.shuffle(labour_pol)
rnd.shuffle(pol)


def eff_date():
    return (YEAR_START + datetime.timedelta(days=rnd.randint(0, 364)))


def new_id(prefix, n):
    return f"{prefix}-{n:06d}"


transactions, components, gen_events = [], [], []


def emit(txn_id, policy_id, submission_id, ttype, product, uw_id, technical, giveaway_pts,
         referred, wageroll=None):
    """Append one transaction + its premium-component decomposition (+ a MAX_WAGEROLL event if referred)."""
    ed = eff_date()
    comm_pct = 0.225 if product == "commercial_package" else 0.20
    adj = round(technical * giveaway_pts / 100.0)          # +ve = discount given away
    charged = int(round(technical - adj))
    ipt = int(round(charged * 0.12))
    commission = int(round(charged * comm_pct))
    transactions.append((txn_id, policy_id, submission_id, ttype, ed.isoformat(), product, uw_id,
                         float(technical), float(charged), float(ipt), float(comm_pct)))
    entered_at = datetime.datetime.combine(ed, datetime.time(rnd.randint(9, 17), 0)).isoformat()
    components.append((txn_id, "TECHNICAL", None, float(technical), uw_id, entered_at))
    if adj > 0:
        components.append((txn_id, "DISCOUNT", DISCOUNT_REASON[ttype], float(-adj), uw_id, entered_at))
    elif adj < 0:
        components.append((txn_id, "LOAD", "CLAIMS_EXPERIENCE_LOAD", float(-adj), uw_id, entered_at))
    components.append((txn_id, "IPT", None, float(ipt), uw_id, entered_at))
    components.append((txn_id, "COMMISSION", None, float(commission), uw_id, entered_at))
    if referred:
        latency = round(rnd.uniform(2, 96), 1)
        resolved = datetime.datetime.combine(ed, datetime.time(rnd.randint(9, 17), 0)) + \
            datetime.timedelta(hours=latency)
        outcome = ("quoted_with_adjustment" if adj > 0 else "quoted_as_recommended")
        roll = rnd.random()
        if roll > 0.92:
            outcome = "ntu"
        elif roll > 0.88:
            outcome = "declined"
        eid = hashlib.sha256(f"{txn_id}|MAX_WAGEROLL|{ed.isoformat()}".encode()).hexdigest()[:32]
        gen_events.append((eid, submission_id, txn_id, "MAX_WAGEROLL", "v1",
                           float(wageroll), 5_000_000.0, "GBP",
                           datetime.datetime.combine(ed, datetime.time(9, 0)).isoformat(),
                           resolved.isoformat(), uw_id, outcome, latency, "generated_2025"))


def pick_uw(target_persona):
    return rnd.choice(_by_persona[target_persona])


def giveaway_for(ttype, persona):
    base = {"RENEWAL": (8.0, 4.0), "MTA": (5.0, 4.0), "NEW_BUSINESS": (2.0, 4.0)}[ttype]
    g = rnd.gauss(*base) + PERSONA_ADJ[persona]
    return max(-5.0, min(30.0, g))


# ---- Referred cohort: ~400 MAX_WAGEROLL fires, split 70% RN / 20% NB / 10% MTA ----------
REFERRED_SPLIT = [("RENEWAL", 280), ("NEW_BUSINESS", 80), ("MTA", 40)]
_seq = 0
_lp = 0
for ttype, count in REFERRED_SPLIT:
    for _ in range(count):
        src = labour_pol[_lp % len(labour_pol)]
        _lp += 1
        # generous handles ~30% of referred renewals; NB/MTA lean less generous
        if ttype == "RENEWAL":
            persona = rnd.choices(["generous", "median", "disciplined"], weights=[30, 45, 25])[0]
        else:
            persona = rnd.choices(["generous", "median", "disciplined"], weights=[15, 55, 30])[0]
        uw = pick_uw(persona)
        wageroll = rnd.randint(51, 150) * 100_000       # £5.1m–£15.0m → above the £5m standard band
        technical = int(src.gross_premium * rnd.uniform(0.95, 1.25))
        prefix = {"RENEWAL": "RN", "NEW_BUSINESS": "NB", "MTA": "MT"}[ttype]
        txn_id = new_id(prefix, _seq)
        sub_id = txn_id if ttype == "NEW_BUSINESS" else None  # RN/MTA carry synthetic lineage (null sub)
        _seq += 1
        emit(txn_id, src.policy_number, sub_id, ttype, src.product, uw,
             technical, giveaway_for(ttype, persona), referred=True, wageroll=wageroll)

# ---- Non-referred background: credible denominator, ~1pt give-away, no events ----------
BG_SPLIT = [("RENEWAL", 1900), ("MTA", 420), ("NEW_BUSINESS", 480)]
for ttype, count in BG_SPLIT:
    for _ in range(count):
        src = pol[_seq % len(pol)]
        uw = rnd.choice(list(PERSONA_MAP))       # non-referred give-away is persona-independent (~1pt)
        technical = int(src.gross_premium * rnd.uniform(0.95, 1.25))
        g = max(-4.0, min(8.0, rnd.gauss(1.0, 1.5)))
        prefix = {"RENEWAL": "RN", "NEW_BUSINESS": "NB", "MTA": "MT"}[ttype]
        txn_id = new_id(prefix, _seq)
        sub_id = txn_id if ttype == "NEW_BUSINESS" else None
        _seq += 1
        emit(txn_id, src.policy_number, sub_id, ttype, src.product, uw, technical, g, referred=False)

# ---- Write ----------------------------------------------------------------------------
write(spark.createDataFrame(
        transactions,
        "transaction_id string, policy_id string, submission_id string, transaction_type string, "
        "effective_date string, product string, underwriter_id string, technical_premium double, "
        "charged_premium double, ipt_amount double, commission_pct double"),
      "gold_transactions", "gold")

write(spark.createDataFrame(
        components,
        "transaction_id string, component_type string, reason_code string, amount double, "
        "entered_by string, entered_at string"),
      "gold_premium_components", "gold")

write(spark.createDataFrame(
        gen_events,
        "referral_event_id string, submission_id string, transaction_id string, rule_id string, "
        "rule_version string, triggering_value double, threshold_value double, unit string, "
        "fired_at string, resolved_at string, decided_by string, outcome string, "
        "time_to_decision_hours double, event_source string"),
      "landing_referral_events_generated", "landing")

# COMMAND ----------

# MAGIC %md ## Lane E generation sanity (asserted here + independently in the smoke test)

# COMMAND ----------

fires = spark.sql(f"SELECT count(*) c FROM {fqn}.landing_referral_events_generated WHERE rule_id='MAX_WAGEROLL'").first().c
split = {r.transaction_type: r.c for r in spark.sql(f"""
    SELECT t.transaction_type, count(*) c
    FROM {fqn}.landing_referral_events_generated e JOIN {fqn}.gold_transactions t USING (transaction_id)
    GROUP BY t.transaction_type""").collect()}
print(f"MAX_WAGEROLL generated fires: {fires} | split: {split}")
assert 380 <= fires <= 420, f"expected ~400 fires, got {fires}"

# Reconcile: TECHNICAL + DISCOUNT + LOAD = charged, for every transaction.
bad = spark.sql(f"""
    SELECT count(*) c FROM (
      SELECT c.transaction_id, sum(CASE WHEN c.component_type IN ('TECHNICAL','DISCOUNT','LOAD')
                                        THEN c.amount ELSE 0 END) AS comp_charged,
             any_value(t.charged_premium) AS charged
      FROM {fqn}.gold_premium_components c JOIN {fqn}.gold_transactions t USING (transaction_id)
      GROUP BY c.transaction_id
      HAVING abs(comp_charged - charged) > 1.0)""").first().c
assert bad == 0, f"{bad} transactions where components do not reconcile to charged"

# Planted signal: referred renewal give-away mean > referred NB give-away mean.
sig = spark.sql(f"""
    SELECT t.transaction_type,
           avg((t.technical_premium - t.charged_premium) / t.technical_premium * 100) AS giveaway_pts
    FROM {fqn}.gold_transactions t
    JOIN {fqn}.landing_referral_events_generated e USING (transaction_id)
    GROUP BY t.transaction_type""").collect()
gv = {r.transaction_type: r.giveaway_pts for r in sig}
print("referred give-away pts by type:", {k: round(v, 2) for k, v in gv.items()})
assert gv.get("RENEWAL", 0) > gv.get("NEW_BUSINESS", 99), "renewal give-away must exceed NB give-away"

print("✅ 00c Lane E setup — E1 + E2 + E3a + E4 generation complete")
