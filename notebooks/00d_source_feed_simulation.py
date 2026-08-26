# Databricks notebook source
# MAGIC %md
# MAGIC # 00d · Lane E.1 — rule landscape (E6) + simulated source feeds (E7) — ADDITIVE
# MAGIC
# MAGIC Delta on Lane E. Two things:
# MAGIC
# MAGIC * **E6 — rule landscape.** The shipped history had effectively one interesting rule
# MAGIC   (MAX_WAGEROLL). Real portfolios have a rule *landscape*. This adds 7 **analytics-only**
# MAGIC   rules (ref rows + generated events only — NO crux functions, NO UI chips, NO extraction
# MAGIC   fields) so Genie follow-ups have somewhere to go. ~2,000 new events + their transactions.
# MAGIC * **E7 — simulated source feeds.** The demo plays BACKWARDS (dashboard → Genie → "where did
# MAGIC   this come from"). So the canonical events are projected into three messy RAW feeds + an
# MAGIC   authority-matrix feed, which a DLT pipeline (`02d_referral_conformance`) conforms back into
# MAGIC   the SAME `gold_referral_events`. This notebook lands the raw feeds and the code-map.
# MAGIC
# MAGIC **Isolation:** all E6/E7 generation uses an independent `Random(4243)` — a NEW stream, run
# MAGIC after all existing generation (seed=42 book AND Lane E's 4242 stream). It never draws from
# MAGIC either, so the seed=42 book and the Lane E MAX_WAGEROLL events stay byte-identical. Runs after
# MAGIC `00c_lane_e_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import datetime
import hashlib
import json
import random

E61_SEED = 4243
rnd = random.Random(E61_SEED)
TODAY = datetime.date.today()
YEAR_START = datetime.date(TODAY.year - 1, 1, 1)


def write(df, name, layer):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.{name}")
    spark.sql(f"ALTER TABLE {fqn}.{name} SET TBLPROPERTIES "
              f"('layer'='{layer}', 'demo'='underwriting_workbench', 'lane'='referral_discretion')")
    print(f"  {name}: {spark.table(f'{fqn}.{name}').count()} rows")


print(f"Lane E.1 → {fqn}  rng={E61_SEED}  as_at={TODAY}")

# COMMAND ----------

# MAGIC %md ## Idempotency — clear any prior E6/E7 output so re-running 00d never accumulates
# MAGIC On a normal reset, `00c` has just rebuilt the Lane E facts fresh (MAX_WAGEROLL only); this
# MAGIC notebook then appends the landscape. If 00d is re-run standalone, remove its own prior output
# MAGIC first: the analytics-only rules, their events, the LX- transactions and their components. The
# MAGIC seed=42 book and the 4242 MAX_WAGEROLL rows are never touched.

# COMMAND ----------


def _table_exists(name):
    return spark.catalog.tableExists(f"{fqn}.{name}")


ANALYTICS_RULE_IDS = ["MAX_TURNOVER", "MAX_SUMS_INSURED", "HAZARDOUS_ACTIVITY_HEIGHT", "CLAIMS_FREQUENCY",
                      "FLOOD_POSTCODE", "PRICE_BELOW_TECHNICAL_FLOOR", "RENEWAL_RATE_CHANGE_TOLERANCE"]
_rids = ", ".join(f"'{r}'" for r in ANALYTICS_RULE_IDS)
# (ref_referral_rules is owned by 00e_referral_registry; 00d no longer deletes/inserts rule rows —
#  it only clears + regenerates the analytics EVENTS and their LX- transactions below.)
if _table_exists("landing_referral_events_generated"):
    # E6 events = analytics rules, plus MAX_TURNOVER co-fires (rule_id in the set). MAX_WAGEROLL untouched.
    spark.sql(f"DELETE FROM {fqn}.landing_referral_events_generated WHERE rule_id IN ({_rids})")
if _table_exists("gold_transactions"):
    spark.sql(f"DELETE FROM {fqn}.gold_transactions WHERE transaction_id LIKE 'LX-%'")
if _table_exists("gold_premium_components"):
    spark.sql(f"DELETE FROM {fqn}.gold_premium_components WHERE transaction_id LIKE 'LX-%'")
print("  cleared any prior E6/E7 output (analytics rules, their events, LX- txns/components)")

# COMMAND ----------

# MAGIC %md ## E6 · rule landscape — analytics rules now seeded in `00e_referral_registry`
# MAGIC The 7 analytics-only rules (MAX_TURNOVER, MAX_SUMS_INSURED, HAZARDOUS_ACTIVITY_HEIGHT,
# MAGIC CLAIMS_FREQUENCY, FLOOD_POSTCODE, PRICE_BELOW_TECHNICAL_FLOOR, RENEWAL_RATE_CHANGE_TOLERANCE)
# MAGIC are seeded by `00e` (Referral Control registry) with their governed category/disposition/lock.
# MAGIC This notebook now only generates their retrospective EVENTS + transactions (below); `00e` runs
# MAGIC before it. The `rule_id`s below must match `00e`.

# COMMAND ----------

# (analytics rule rows are seeded in 00e_referral_registry — this notebook generates their events.)
print(f"  analytics rule events for: {ANALYTICS_RULE_IDS}")

# COMMAND ----------

# MAGIC %md ## E6 · generate the rule-landscape events (+ their transactions & components)
# MAGIC ~2,000 events across the 7 rules, appended to the Lane E facts with rng 4243. The existing
# MAGIC MAX_WAGEROLL events (400) are READ, never regenerated. ~15% of referred transactions fire
# MAGIC ≥2 rules (co-fire realism); MAX_TURNOVER co-fires on ~40% of the existing MAX_WAGEROLL txns.

# COMMAND ----------

# Existing Lane E facts (from 00c) — read, do not regenerate.
wageroll_txns = spark.sql(f"""
    SELECT t.transaction_id, t.policy_id, t.transaction_type, t.product, t.underwriter_id,
           t.technical_premium, t.effective_date
    FROM {fqn}.gold_transactions t
    JOIN {fqn}.landing_referral_events_generated e USING (transaction_id)
    WHERE e.rule_id = 'MAX_WAGEROLL'
    ORDER BY t.transaction_id""").collect()

pol = spark.sql(f"""
    SELECT policy_number, trade_group, segment, gross_premium,
           coalesce(product_line, CASE WHEN segment='mid_market' THEN 'commercial_combined'
                                       ELSE 'commercial_package' END) AS product
    FROM {fqn}.landing_pas_policies WHERE policy_status='in_force' ORDER BY policy_number""").collect()
personas = {r.underwriter_id: r.persona for r in
            spark.sql(f"SELECT underwriter_id, persona FROM {fqn}.ref_underwriter_persona").collect()}
uw_ids = sorted(personas)
PERSONA_ADJ = {"disciplined": -2.0, "median": 0.0, "generous": 4.0, "unclassified": 0.0}

FLOOD_DISTRICTS = ["HX7", "YO8", "TN9", "GL2", "CA1", "DG1"]   # 6 clustered districts for FLOOD_POSTCODE
HEIGHT_TRADES = {"construction_contractors", "metal_engineering"}  # scaffolding/roofing home

# Per-rule behaviour: (type_weights NB/RN/MTA, giveaway_mean, giveaway_sd, load(+)/discount(-),
#                      decline_rate, latency_median, family)
RULE_BEHAVIOUR = {
    "MAX_TURNOVER":                 dict(tw=(30, 55, 15), gm=3.0, sd=3.0, decl=0.06, lat=8),
    "MAX_SUMS_INSURED":             dict(tw=(60, 30, 10), gm=2.5, sd=3.0, decl=0.05, lat=8),
    "HAZARDOUS_ACTIVITY_HEIGHT":    dict(tw=(45, 45, 10), gm=-3.0, sd=2.0, decl=0.10, lat=10, load=True),
    "CLAIMS_FREQUENCY":             dict(tw=(10, 85, 5), gm=-1.5, sd=3.0, decl=0.18, lat=30),
    "FLOOD_POSTCODE":               dict(tw=(40, 50, 10), gm=-2.5, sd=2.5, decl=0.08, lat=12, load="RISK_FEATURE_LOAD"),
    "PRICE_BELOW_TECHNICAL_FLOOR":  dict(tw=(75, 20, 5), gm=9.0, sd=4.0, decl=0.05, lat=6),
    "RENEWAL_RATE_CHANGE_TOLERANCE": dict(tw=(0, 100, 0), gm=6.0, sd=4.0, decl=0.07, lat=6, q3spike=True),
}
# Target primary fires per rule (sum ~1800). Extra co-fires (~10% of LX txns) land on top, so
# E6 total ≈ 1964 and the full generated history ≈ 2364 incl. MAX_WAGEROLL's untouched 400 → in [2300,2500].
RULE_TARGET = {"MAX_TURNOVER": 310, "MAX_SUMS_INSURED": 260, "HAZARDOUS_ACTIVITY_HEIGHT": 190,
               "CLAIMS_FREQUENCY": 260, "FLOOD_POSTCODE": 230, "PRICE_BELOW_TECHNICAL_FLOOR": 280,
               "RENEWAL_RATE_CHANGE_TOLERANCE": 270}

TYPE_PREFIX = {"NEW_BUSINESS": "NB", "RENEWAL": "RN", "MTA": "MT"}
TTYPES = ["NEW_BUSINESS", "RENEWAL", "MTA"]

e6_txns, e6_comps, e6_events = [], [], []
_seq = 500_000  # E6 transaction id space, disjoint from 00c's 0-based sequence


def eff_date_for(beh):
    if beh.get("q3spike") and rnd.random() < 0.55:
        # Q3 (Jul–Sep) rate-strengthening push — a visible quarterly-trend story
        start = datetime.date(YEAR_START.year, 7, 1)
        return start + datetime.timedelta(days=rnd.randint(0, 91))
    return YEAR_START + datetime.timedelta(days=rnd.randint(0, 364))


def make_event(rule_id, txn_id, sub_id, ed, uw, technical, ttype, extra_load_reason=None, new_txn=True):
    beh = RULE_BEHAVIOUR[rule_id]
    persona = personas.get(uw, "median")
    g = rnd.gauss(beh["gm"], beh["sd"]) + (PERSONA_ADJ[persona] if beh["gm"] > 0 else 0)
    g = max(-8.0, min(30.0, g))
    adj = round(technical * g / 100.0)                    # +ve = discount, -ve = load
    charged = int(round(technical - adj))
    comm_pct = 0.225 if "package" in txn_product[txn_id] else 0.20
    ipt = int(round(charged * 0.12))
    entered_at = datetime.datetime.combine(ed, datetime.time(rnd.randint(9, 17), 0)).isoformat()
    # Write the transaction + components ONLY for a rule's primary fire on a fresh LX- txn.
    # Co-fires (a 2nd rule on an existing txn) add an EVENT only — never a duplicate txn/components.
    if new_txn and txn_id.startswith("LX-"):
        e6_txns.append((txn_id, txn_policy[txn_id], sub_id, ttype, ed.isoformat(),
                        txn_product[txn_id], uw, float(technical), float(charged), float(ipt), float(comm_pct)))
        e6_comps.append((txn_id, "TECHNICAL", None, float(technical), uw, entered_at))
        if adj > 0:
            e6_comps.append((txn_id, "DISCOUNT", {"NEW_BUSINESS": "NB_COMPETITIVE_DISCOUNT",
                             "RENEWAL": "RENEWAL_RETENTION_DISCOUNT", "MTA": "BROKER_COMMITMENT_DISCOUNT"}[ttype],
                             float(-adj), uw, entered_at))
        elif adj < 0:
            lr = extra_load_reason or (beh["load"] if isinstance(beh.get("load"), str) else "RISK_FEATURE_LOAD")
            e6_comps.append((txn_id, "LOAD", lr, float(-adj), uw, entered_at))
        e6_comps.append((txn_id, "IPT", None, float(ipt), uw, entered_at))
        e6_comps.append((txn_id, "COMMISSION", None, float(round(charged * comm_pct)), uw, entered_at))
    # outcome + latency
    lat = round(max(1.0, rnd.gauss(beh["lat"], beh["lat"] * 0.4)), 1)
    if adj > 0:
        outcome = "quoted_with_adjustment"
    elif adj < 0:
        outcome = "quoted_with_adjustment"
    else:
        outcome = "quoted_as_recommended"
    roll = rnd.random()
    if roll < beh["decl"]:
        outcome = "declined"
    elif roll < beh["decl"] + 0.06:
        outcome = "ntu"
    resolved = datetime.datetime.combine(ed, datetime.time(9, 0)) + datetime.timedelta(hours=lat)
    eid = hashlib.sha256(f"{txn_id}|{rule_id}|{ed.isoformat()}".encode()).hexdigest()[:32]
    e6_events.append((eid, sub_id, txn_id, rule_id, "v1",
                      float(rnd.randint(1, 100)), 0.0, "unit",
                      datetime.datetime.combine(ed, datetime.time(9, 0)).isoformat(),
                      resolved.isoformat(), uw, outcome, lat, "generated_2025"))


# Build transaction lookups. E6 owns fresh transactions (LX-*) for its primary fires; co-fires
# attach a second rule to an EXISTING MAX_WAGEROLL transaction (no new txn/components).
txn_product, txn_policy = {}, {}

# 1) MAX_TURNOVER co-fires on ~40% of the existing MAX_WAGEROLL transactions.
cofire_pool = list(wageroll_txns)
rnd.shuffle(cofire_pool)
n_turnover_cofire = int(len(cofire_pool) * 0.40)
turnover_remaining = RULE_TARGET["MAX_TURNOVER"] - n_turnover_cofire
for wt in cofire_pool[:n_turnover_cofire]:
    txn_product[wt.transaction_id] = wt.product
    txn_policy[wt.transaction_id] = wt.policy_id
    make_event("MAX_TURNOVER", wt.transaction_id, None,
               datetime.date.fromisoformat(wt.effective_date), wt.underwriter_id,
               int(wt.technical_premium), wt.transaction_type, new_txn=False)

# 2) Primary fires for each rule on fresh LX- transactions.
plan = dict(RULE_TARGET)
plan["MAX_TURNOVER"] = max(0, turnover_remaining)


def pick_policy(rule_id):
    if rule_id == "HAZARDOUS_ACTIVITY_HEIGHT":
        cand = [p for p in pol if p.trade_group in HEIGHT_TRADES]
        return rnd.choice(cand) if cand else rnd.choice(pol)
    return rnd.choice(pol)


for rule_id, target in plan.items():
    beh = RULE_BEHAVIOUR[rule_id]
    tw = beh["tw"]
    for _ in range(target):
        ttype = rnd.choices(TTYPES, weights=tw)[0] if sum(tw) else "RENEWAL"
        p = pick_policy(rule_id)
        uw = rnd.choice(uw_ids)
        technical = int(p.gross_premium * rnd.uniform(0.95, 1.25))
        txn_id = f"LX-{_seq:06d}"; _seq += 1
        txn_product[txn_id] = p.product
        txn_policy[txn_id] = p.policy_number
        sub_id = None
        ed = eff_date_for(beh)
        make_event(rule_id, txn_id, sub_id, ed, uw, technical, ttype)

# 3) Extra co-fires: for ~15% overall co-fire, add a second analytics rule to some LX- txns.
lx_ids = [t[0] for t in e6_txns]
rnd.shuffle(lx_ids)
n_extra_cofire = int(len(lx_ids) * 0.10)
for txn_id in lx_ids[:n_extra_cofire]:
    other = rnd.choice([r for r in RULE_TARGET if r not in ("RENEWAL_RATE_CHANGE_TOLERANCE",)])
    # attach a second event to this existing txn (event only — no new txn/components)
    row = [t for t in e6_txns if t[0] == txn_id][0]
    make_event(other, txn_id, None, datetime.date.fromisoformat(row[4]), row[6], int(row[7]), row[3], new_txn=False)

print(f"  generated: {len(e6_events)} events · {len(e6_txns)} new txns · {len(e6_comps)} components")

# COMMAND ----------

# MAGIC %md ## Append E6 facts to the Lane E tables (MAX_WAGEROLL rows untouched)

# COMMAND ----------

spark.createDataFrame(
    e6_txns,
    "transaction_id string, policy_id string, submission_id string, transaction_type string, "
    "effective_date string, product string, underwriter_id string, technical_premium double, "
    "charged_premium double, ipt_amount double, commission_pct double"
).write.mode("append").saveAsTable(f"{fqn}.gold_transactions")

spark.createDataFrame(
    e6_comps,
    "transaction_id string, component_type string, reason_code string, amount double, "
    "entered_by string, entered_at string"
).write.mode("append").saveAsTable(f"{fqn}.gold_premium_components")

spark.createDataFrame(
    e6_events,
    "referral_event_id string, submission_id string, transaction_id string, rule_id string, "
    "rule_version string, triggering_value double, threshold_value double, unit string, "
    "fired_at string, resolved_at string, decided_by string, outcome string, "
    "time_to_decision_hours double, event_source string"
).write.mode("append").saveAsTable(f"{fqn}.landing_referral_events_generated")

# Canonical checkpoint: the full generated history (MAX_WAGEROLL + landscape) — E7 conforms to this.
total = spark.table(f"{fqn}.landing_referral_events_generated").count()
by_rule = {r.rule_id: r.c for r in spark.sql(
    f"SELECT rule_id, count(*) c FROM {fqn}.landing_referral_events_generated GROUP BY rule_id").collect()}
print(f"  landing_referral_events_generated total = {total}")
for k in sorted(by_rule): print(f"    {k}: {by_rule[k]}")
assert 2300 <= total <= 2500, f"expected ~2400 total generated fires, got {total}"
assert by_rule.get("MAX_WAGEROLL") == 400, f"MAX_WAGEROLL must stay 400, got {by_rule.get('MAX_WAGEROLL')}"

print("✅ 00d E6 rule landscape complete")

# COMMAND ----------

# MAGIC %md ## E7 · project the canonical events into three messy RAW source feeds
# MAGIC The demo plays backwards: dashboard → Genie → "where did this come from?". So the canonical
# MAGIC `landing_referral_events_generated` is split across three raw systems, each with its own
# MAGIC dialect and quirks. A DLT pipeline (`02d_referral_conformance`) conforms them back into the
# MAGIC SAME `gold_referral_events` — the conformance, not this generation, is the lineage beat.
# MAGIC
# MAGIC Split: PAS/rating engine owns exposure + property rules; e-trade owns pricing-discretion +
# MAGIC wageroll; case tool provides outcomes for all. Every event lands in ≥1 of the three feeds.

# COMMAND ----------

canon = spark.sql(f"""
    SELECT e.*, t.charged_premium, t.technical_premium
    FROM {fqn}.landing_referral_events_generated e
    LEFT JOIN {fqn}.gold_transactions t USING (transaction_id)""").collect()

# The demo artefact: source_code → rule_id, per system. PAS uses opaque UW_REF_NNN codes; e-trade
# uses verbose SNAKE codes. This table is where "your five systems say this five ways" gets settled.
PAS_CODE = {  # opaque rating-engine codes
    "MAX_WAGEROLL": "UW_REF_017", "SI_AUTHORITY_BAND": "UW_REF_004", "ROFRS_HIGH": "UW_REF_022",
    "FAIR_PRESENTATION_MISMATCH": "UW_REF_031", "ACCUMULATION_CAPACITY": "UW_REF_009",
    "MAX_TURNOVER": "UW_REF_002", "MAX_SUMS_INSURED": "UW_REF_005",
    "HAZARDOUS_ACTIVITY_HEIGHT": "UW_REF_044", "CLAIMS_FREQUENCY": "UW_REF_051",
    "FLOOD_POSTCODE": "UW_REF_023", "PRICE_BELOW_TECHNICAL_FLOOR": "UW_REF_060",
    "RENEWAL_RATE_CHANGE_TOLERANCE": "UW_REF_062",
}
ETRADE_CODE = {  # broker-portal verbose codes (same logical rules, different names)
    "MAX_WAGEROLL": "WAGEROLL_LIMIT_EXCEEDED", "SI_AUTHORITY_BAND": "SI_OVER_BAND",
    "ROFRS_HIGH": "FLOOD_HIGH", "MAX_TURNOVER": "TURNOVER_OVER_BAND",
    "MAX_SUMS_INSURED": "TSI_OVER_BAND", "HAZARDOUS_ACTIVITY_HEIGHT": "HEIGHT_WORK",
    "PRICE_BELOW_TECHNICAL_FLOOR": "PRICE_UNDER_FLOOR",
    "RENEWAL_RATE_CHANGE_TOLERANCE": "RATE_CHANGE_OOT", "FLOOD_POSTCODE": "FLOOD_PC",
}
# Which system originates each rule's referral (the other systems just don't emit that rule).
PAS_RULES = {"SI_AUTHORITY_BAND", "ROFRS_HIGH", "FAIR_PRESENTATION_MISMATCH", "ACCUMULATION_CAPACITY",
             "MAX_TURNOVER", "MAX_SUMS_INSURED", "HAZARDOUS_ACTIVITY_HEIGHT", "CLAIMS_FREQUENCY",
             "FLOOD_POSTCODE"}
ETRADE_RULES = {"MAX_WAGEROLL", "PRICE_BELOW_TECHNICAL_FLOOR", "RENEWAL_RATE_CHANGE_TOLERANCE"}

code_map_rows = []
for rid in set(list(PAS_CODE) + list(ETRADE_CODE)):
    if rid in PAS_RULES and rid in PAS_CODE:
        code_map_rows.append(("pas_rating", PAS_CODE[rid], rid, "data_platform_team",
                              "opaque rating-engine code; meaning from the licence matrix"))
    if rid in ETRADE_RULES and rid in ETRADE_CODE:
        code_map_rows.append(("etrade_portal", ETRADE_CODE[rid], rid, "data_platform_team",
                              "broker-portal verbose code"))
write(spark.createDataFrame(
        code_map_rows,
        "source_system string, source_code string, rule_id string, mapped_by string, note string"),
      "ref_rule_code_map", "reference")

# COMMAND ----------

# MAGIC %md ### Feed 1 · `raw_pas_referrals` — CDC-style nested JSON (policy-core / rating-engine shaped)

# COMMAND ----------

pas_records = []
for e in canon:
    if e.rule_id not in PAS_RULES:
        continue
    # local timestamp, no offset; nested payload; opaque code
    rec = {
        "op": "INSERT", "seq": e.referral_event_id[:12],
        "payload": {
            "referral": {
                "policy_txn_ref": e.transaction_id,
                "rule": {"code": PAS_CODE[e.rule_id], "version": e.rule_version},
                "observed_value": e.triggering_value,
                "raised_ts_local": e.fired_at,      # local time, no zone
                "underwriter": e.decided_by,
            }
        },
    }
    pas_records.append((json.dumps(rec),))
write(spark.createDataFrame(pas_records, "raw_record string"), "raw_pas_referrals", "raw")

# COMMAND ----------

# MAGIC %md ### Feed 2 · `raw_etrade_referrals` — flat JSON, different field names, string amounts

# COMMAND ----------

etrade_records = []
for e in canon:
    if e.rule_id not in ETRADE_RULES:
        continue
    rec = {
        "quoteRef": e.transaction_id,
        "reasonCode": ETRADE_CODE[e.rule_id],
        "triggerVal": str(e.triggering_value),
        "raisedAt": e.fired_at,                     # local time, no offset
        "uw": e.decided_by,
        "indicativePremium": ("£" + f"{e.charged_premium:,.0f}") if e.charged_premium else None,
    }
    etrade_records.append((json.dumps(rec),))
write(spark.createDataFrame(etrade_records, "raw_record string"), "raw_etrade_referrals", "raw")

# COMMAND ----------

# MAGIC %md ### Feed 3 · `raw_case_outcomes` — daily CSV-shaped drops (underwriter case tool)
# MAGIC Outcomes + charged premium + reason text, keyed on a `case_ref` that needs a bridge to the
# MAGIC transaction id. ~2% orphan rows (no matching transaction) — realism + a DQ quarantine beat.

# COMMAND ----------

case_rows, bridge_rows = [], []
for i, e in enumerate(canon):
    case_ref = f"CASE-{100000 + i}"
    # Bridge is event-grain (transaction_id + rule_id) so co-fires — one txn, several rules — each
    # resolve to their own case row without fanning out the conformance join.
    bridge_rows.append((case_ref, e.transaction_id, e.rule_id))
    reason = {
        "declined": "Outside appetite / authority — declined",
        "ntu": "Quoted; not taken up by client",
        "quoted_with_adjustment": "Quoted with pricing adjustment",
        "quoted_as_recommended": "Quoted as recommended",
    }.get(e.outcome, "Quoted")
    charged = f"{e.charged_premium:.2f}" if e.charged_premium else ""
    case_rows.append((case_ref, e.resolved_at, e.outcome, charged, reason))
# ~2% orphans: case rows whose case_ref is NOT in the bridge (system drift / late policy creation).
n_orphan = max(1, int(len(canon) * 0.02))
for j in range(n_orphan):
    case_rows.append((f"CASE-ORPH-{j:04d}",
                      (YEAR_START + datetime.timedelta(days=rnd.randint(0, 364))).isoformat(),
                      rnd.choice(["quoted_as_recommended", "declined"]), "0.00", "Orphan — no linked transaction"))
rnd.shuffle(case_rows)
write(spark.createDataFrame(
        case_rows, "case_ref string, resolved_ts string, outcome string, charged_premium_str string, reason_text string"),
      "raw_case_outcomes", "raw")
write(spark.createDataFrame(bridge_rows, "case_ref string, transaction_id string, rule_id string"),
      "raw_case_bridge", "raw")
print(f"  raw_case_outcomes: {len(case_rows)} rows incl. {n_orphan} orphans")

# COMMAND ----------

# MAGIC %md ### Feed 4 · `raw_authority_matrix` — licence/threshold spreadsheet (with a drift)
# MAGIC Thresholds per band. One band value DISAGREES with the PAS rating config — the drift beat.
# MAGIC The MAX_WAGEROLL standard band here reads £5.5m vs the £5.0m the rating engine enforces.

# COMMAND ----------

auth_rows = [
    ("MAX_WAGEROLL", "etrade", 2_000_000, "matches PAS"),
    ("MAX_WAGEROLL", "standard", 5_500_000, "DRIFT — PAS config enforces 5,000,000"),  # planted disagreement
    ("MAX_WAGEROLL", "senior", 12_000_000, "matches PAS"),
    ("MAX_TURNOVER", "standard", 20_000_000, "matches PAS"),
    ("MAX_SUMS_INSURED", "standard", 25_000_000, "matches PAS"),
    ("SI_AUTHORITY_BAND", "underwriter", 5_000_000, "matches PAS"),
]
write(spark.createDataFrame(
        auth_rows, "rule_id string, authority_band string, threshold_value long, note string"),
      "raw_authority_matrix", "raw")

print("✅ 00d E7 raw source feeds + code map complete")

