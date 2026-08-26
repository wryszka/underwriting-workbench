# Databricks notebook source
# MAGIC %md
# MAGIC # 00f · Referral Control — fire-vector telemetry + backfill + future book + storylines
# MAGIC
# MAGIC The substrate for the whole Referral Control feature: `gold_referral_telemetry`. Unlike
# MAGIC `gold_referral_events` (fires only), this logs the **full fire-vector with no short-circuit** —
# MAGIC every rule that fired (and, for auto_decline/accept rules, `would_fire` = the shadow signal) on
# MAGIC every case, with the eventual outcome joined. This is what the detection engine (07d), isolation
# MAGIC analysis, co-fire patterns, emulation, and the reviewer agent all read.
# MAGIC
# MAGIC **Time-travel book.** Cases span `[epoch (≈3y ago), today + 90d]`. Every Referral Control query
# MAGIC reads through a single `as_of_date` filter (`as_of_date <= :as_of`): the demo's "advance one
# MAGIC month" just moves that parameter over PRE-GENERATED data, and scrubbing to ANY date works.
# MAGIC
# MAGIC **Seeded storylines** (deterministic, DISCOVERED by 07d — never hardcoded in the UI):
# MAGIC 1. `HAZARDOUS_ACTIVITY_HEIGHT` — niche hazard, ~95% declined/price-walked → convert_to_auto_decline.
# MAGIC 2. drift+reversal — a future new-channel mix-shift with better risk → reopen_to_referral (cause: mix shift).
# MAGIC 3. `EVENT_ATTENDANCE_LIMIT` — high volume, same clause every time → auto_apply_clause + re_threshold.
# MAGIC 4. `DUAL_TRADE_DECLARED` — high approval, high NOADJ, ~zero isolation → remove.
# MAGIC 5. `NEW_VENTURE_TRADING_HISTORY` — healthy overall BUT isolated fires carry worse LR → keep.
# MAGIC 6. `SANCTIONS_SCREEN_HIT` — high fire count, compliance-locked → keep (never touched).
# MAGIC 7. `RENEWAL_UNCHANGED_RISK` — biggest raw volume, unchanged risks → re_threshold on materiality.
# MAGIC
# MAGIC **Isolation (invariant 10):** independent `Random(4245)` — a NEW stream, run after the seed=42
# MAGIC book and the Lane E streams (4242/4243/4244). It never draws from them, and writes only its own
# MAGIC NEW table (`gold_referral_telemetry`) — no existing row moves; heroes stay byte-identical.

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

RC_SEED = 4245
rnd = random.Random(RC_SEED)
TODAY = datetime.date.today()
EPOCH = datetime.date(TODAY.year - 3, 1, 1)     # backfill start (matches rulebook epoch in 00e)
FUTURE_END = TODAY + datetime.timedelta(days=90)  # ~3 months of pre-generated future book
DECISION_EPOCH = datetime.date(TODAY.year - 3, 1, 1)

print(f"Referral Control telemetry → {fqn}  rng={RC_SEED}  window=[{EPOCH} .. {FUTURE_END}]  anchor={TODAY}")

# COMMAND ----------

# MAGIC %md ## Inputs — the governed rulebook + a real book of policies to hang cases on
# MAGIC Cases reference real in-force policies (names/trades/premium) so the tail exhibit shows named
# MAGIC policies. Rules + their governed disposition/effort come from `ref_referral_rules` (00e).

# COMMAND ----------

rules = {r.rule_id: r for r in spark.sql(f"""
    SELECT rule_id, rule_version, disposition, compliance_lock, review_effort_hours, category, rule_scope
    FROM {fqn}.ref_referral_rules WHERE valid_to IS NULL""").collect()}
assert "HAZARDOUS_ACTIVITY_HEIGHT" in rules, "00f must run AFTER 00e (registry)"

pol = spark.sql(f"""
    SELECT policy_number, trade_group, segment, gross_premium,
           coalesce(product_line, CASE WHEN segment='mid_market' THEN 'commercial_combined'
                                       ELSE 'commercial_package' END) AS product
    FROM {fqn}.landing_pas_policies WHERE policy_status='in_force' ORDER BY policy_number""").collect()
personas = {r.underwriter_id: r.persona for r in
            spark.sql(f"SELECT underwriter_id, persona FROM {fqn}.ref_underwriter_persona").collect()}
uw_ids = sorted(personas)
LOADED_HOURLY_COST = 95.0    # £/hr fully-loaded underwriter time — the touch-cost currency

# Synthetic policyholder names for the tail exhibit (deterministic; not real entities).
FORENAMES = ["Ashworth", "Beckworth", "Calder", "Dunmore", "Ellery", "Fairhurst", "Garrow", "Halstead",
             "Ingleby", "Jarrold", "Kentmere", "Lowther", "Marsden", "Netherwood", "Oakden", "Pennington",
             "Quarmby", "Rushworth", "Selby", "Thorne", "Ulverston", "Vardy", "Wharton", "Yeadon"]
TRADE_WORD = {"construction_contractors": "Contracts", "metal_engineering": "Engineering",
              "food_manufacturing": "Foods", "light_manufacturing": "Manufacturing",
              "warehousing_logistics": "Logistics", "hospitality_restaurant": "Hospitality",
              "hotels_leisure": "Leisure", "retail_shop": "Retail", "wholesale": "Trading",
              "motor_trade": "Motors", "property_owners": "Estates", "office_professional": "Associates",
              "waste_recycling": "Recycling", "healthcare_clinics": "Health", "hair_beauty": "Studios",
              "education_training": "Academy", "nightclubs": "Entertainment"}
SUFFIX = ["Ltd", "Ltd", "Ltd", "(UK) Ltd", "Group Ltd", "Holdings Ltd"]


def make_name(trade):
    return f"{rnd.choice(FORENAMES)} {TRADE_WORD.get(trade, 'Commercial')} {rnd.choice(SUFFIX)}"


def value_band(gwp):
    if gwp < 10_000: return "<10k"
    if gwp < 50_000: return "10k-50k"
    if gwp < 250_000: return "50k-250k"
    return ">250k"

# COMMAND ----------

# MAGIC %md ## Case + telemetry generation
# MAGIC A **case** = one NB/RN/MTA transaction that hit the referral desk. Each case fires ≥1 rule; we
# MAGIC emit one telemetry row per fired rule (+ a `would_fire` shadow row for auto_decline/accept
# MAGIC rules). `co_fire_count` = number of OTHER rules that fired on the case (0 = isolated). The
# MAGIC eventual `outcome` uses the Referral Control vocabulary: declined / price_walked / bound_clean /
# MAGIC bound_with_terms.

# COMMAND ----------

telemetry = []      # one row per (case, rule)
_seq = 700_000      # RC- transaction id space, disjoint from 00c (0-) and 00d (500k-)
TTYPES = ["NEW_BUSINESS", "RENEWAL", "MTA"]
TPREFIX = {"NEW_BUSINESS": "NB", "RENEWAL": "RN", "MTA": "MT"}
OUTCOMES = ["declined", "price_walked", "bound_clean", "bound_with_terms"]


def pick_date(window):
    if window == "future":
        return TODAY + datetime.timedelta(days=rnd.randint(1, 90))
    if window == "recent":                                   # last ~120 days before anchor
        return TODAY - datetime.timedelta(days=rnd.randint(1, 120))
    return EPOCH + datetime.timedelta(days=rnd.randint(0, (TODAY - EPOCH).days))  # full backfill


def pick_policy(trades=None):
    if trades:
        cand = [p for p in pol if p.trade_group in trades]
        if cand:
            return rnd.choice(cand)
    return rnd.choice(pol)


def emit_case(primary_rule, outcome, *, window="past", co_rules=(), channel="broker",
              gwp=None, lr=None, noadj=False, terms_clause=None, trades=None, ttype=None,
              would_fire_only=False):
    """Emit the telemetry rows for one case. `co_rules` also fire on the same case (co-fire).
    would_fire_only: the case is auto-declined by a converted rule — log a shadow (would_fire) row."""
    global _seq
    p = pick_policy(trades)
    ttype = ttype or rnd.choices(TTYPES, weights=(35, 50, 15))[0]
    ed = pick_date(window)
    uw = rnd.choice(uw_ids)
    technical = int((gwp or p.gross_premium) * rnd.uniform(0.95, 1.25))
    # loading: +ve = discount given away, encoded as a % of technical
    if outcome in ("bound_clean", "bound_with_terms"):
        adj_pts = 0.0 if noadj else round(rnd.uniform(1.5, 9.0), 1)
    else:
        adj_pts = 0.0
    charged = int(round(technical * (1 - adj_pts / 100.0)))
    bound = outcome in ("bound_clean", "bound_with_terms")
    case_gwp = float(charged) if bound else 0.0
    loss_ratio = float(round(lr, 1)) if (bound and lr is not None) else None
    name = make_name(p.trade_group)
    txn_id = f"RC-{_seq:06d}"; _seq += 1
    sub_id = txn_id if ttype == "NEW_BUSINESS" else None
    fired_rules = [primary_rule] + list(co_rules)
    co_count = len(fired_rules) - 1
    decided_by = "system" if would_fire_only else uw

    def row(rule_id, fired, would_fire):
        r = rules.get(rule_id)
        eid = hashlib.sha256(f"{txn_id}|{rule_id}|{ed.isoformat()}|{would_fire}".encode()).hexdigest()[:32]
        telemetry.append((
            eid, ed, txn_id, sub_id, p.policy_number, name, rule_id,
            r.rule_version if r else "v1", r.disposition if r else "refer",
            bool(r.compliance_lock) if r else False,
            fired, would_fire, outcome, decided_by, terms_clause,
            case_gwp, float(technical), float(adj_pts), loss_ratio,
            ttype, p.trade_group, p.product, channel, value_band(case_gwp or technical), co_count,
            ed > TODAY))

    if would_fire_only:
        row(primary_rule, False, True)       # shadow: rule is auto_decline; it WOULD have referred
    else:
        for rid in fired_rules:
            row(rid, True, False)

# COMMAND ----------

# MAGIC %md ### Storyline 4 · `DUAL_TRADE_DECLARED` — remove candidate (high approval, high NOADJ, ~0 isolation)
# MAGIC Everything it catches is already caught by another rule → it NEVER fires alone; approvals are
# MAGIC high and mostly unadjusted; no LR contribution. The detection engine should recommend `remove`.

# COMMAND ----------

CO_POOL = ["MAX_TURNOVER", "MAX_SUMS_INSURED", "CLAIMS_FREQUENCY", "SI_AUTHORITY_BAND", "FLOOD_POSTCODE"]
for _ in range(190):
    oc = rnd.choices(["bound_clean", "bound_with_terms", "price_walked", "declined"],
                     weights=(72, 16, 7, 5))[0]
    emit_case("DUAL_TRADE_DECLARED", oc, window="past",
              co_rules=[rnd.choice(CO_POOL)],                 # ALWAYS co-fires → ~0 isolation
              noadj=(rnd.random() < 0.80), lr=rnd.gauss(48, 8))

# COMMAND ----------

# MAGIC %md ### Storyline 7 · `RENEWAL_UNCHANGED_RISK` — renewal noise (biggest raw volume) → re_threshold

# COMMAND ----------

for _ in range(620):
    oc = rnd.choices(["bound_clean", "bound_with_terms", "price_walked", "declined"],
                     weights=(88, 5, 4, 3))[0]
    iso = rnd.random() < 0.75                                 # mostly isolated renewal noise
    co = [] if iso else [rnd.choice(["RENEWAL_RATE_CHANGE_TOLERANCE", "MTA_IMMATERIAL_CHANGE"])]
    emit_case("RENEWAL_UNCHANGED_RISK", oc, window="past", co_rules=co, ttype="RENEWAL",
              noadj=(rnd.random() < 0.85), lr=rnd.gauss(46, 7))

# COMMAND ----------

# MAGIC %md ### Storyline 3 · `EVENT_ATTENDANCE_LIMIT` — high volume, SAME clause every time → auto_apply_clause + re_threshold

# COMMAND ----------

for _ in range(260):
    oc = rnd.choices(["bound_with_terms", "bound_clean", "price_walked", "declined"],
                     weights=(55, 30, 8, 7))[0]
    clause = "CROWD_MGMT_CLAUSE_07" if oc == "bound_with_terms" else None
    emit_case("EVENT_ATTENDANCE_LIMIT", oc, window="past",
              co_rules=([] if rnd.random() < 0.7 else ["MAX_SUMS_INSURED"]),
              terms_clause=clause, noadj=(rnd.random() < 0.5), lr=rnd.gauss(52, 9),
              trades={"hotels_leisure", "hospitality_restaurant", "nightclubs"})

# COMMAND ----------

# MAGIC %md ### Storyline 5 · `NEW_VENTURE_TRADING_HISTORY` — looks removable but ISN'T (isolated fires worse LR) → keep

# COMMAND ----------

for _ in range(210):
    isolated = rnd.random() < 0.28
    if isolated:
        oc = rnd.choices(["bound_clean", "bound_with_terms", "price_walked", "declined"],
                         weights=(55, 20, 12, 13))[0]
        lr = rnd.gauss(74, 9)                                 # ISOLATED fires → clearly worse LR
        co = []
    else:
        oc = rnd.choices(["bound_clean", "bound_with_terms", "price_walked", "declined"],
                         weights=(80, 10, 6, 4))[0]
        lr = rnd.gauss(47, 8)                                 # co-fires (caught with others) → fine
        co = [rnd.choice(["CLAIMS_FREQUENCY", "SI_AUTHORITY_BAND"])]
    emit_case("NEW_VENTURE_TRADING_HISTORY", oc, window="past", co_rules=co,
              noadj=(rnd.random() < 0.4), lr=lr)

# COMMAND ----------

# MAGIC %md ### Storyline 6 · `SANCTIONS_SCREEN_HIT` — high fire count, COMPLIANCE-LOCKED → keep (never touched)

# COMMAND ----------

for _ in range(130):
    oc = rnd.choices(["declined", "bound_clean", "price_walked"], weights=(80, 12, 8))[0]
    emit_case("SANCTIONS_SCREEN_HIT", oc, window="past",
              co_rules=([] if rnd.random() < 0.85 else ["REGULATORY_EXCLUDED_TRADE"]),
              lr=rnd.gauss(50, 10))

# COMMAND ----------

# MAGIC %md ### Storyline 1 · `HAZARDOUS_ACTIVITY_HEIGHT` — niche hazard, ~95% declined/price-walked → convert_to_auto_decline
# MAGIC ~200 backfill referrals; the handful that bind are small GWP and poor LR (nothing of value lost).
# MAGIC Mostly fires ALONE (unique catch). Concentrated in the last ~18 months so the trend reads clearly.

# COMMAND ----------

HAZ_TRADES = {"construction_contractors", "metal_engineering"}
for _ in range(205):
    oc = rnd.choices(["declined", "price_walked", "bound_with_terms", "bound_clean"],
                     weights=(58, 37, 3, 2))[0]
    # small GWP for the few that bind; poor LR
    small_gwp = rnd.randint(1_800, 6_500)
    emit_case("HAZARDOUS_ACTIVITY_HEIGHT", oc, window=("recent" if rnd.random() < 0.55 else "past"),
              co_rules=([] if rnd.random() < 0.85 else ["CLAIMS_FREQUENCY"]),   # mostly isolated
              gwp=small_gwp, lr=rnd.gauss(82, 10), trades=HAZ_TRADES)

# COMMAND ----------

# MAGIC %md ### Storyline 2 · drift + reversal — FUTURE mix-shift on the hazard segment → reopen_to_referral
# MAGIC A new broker channel (`BRK-NEWCO`) pushes GROWING volume in the hazard segment in the future
# MAGIC months, with visibly BETTER risk features (good LR if referred) — so the shadow "declined
# MAGIC GWP-at-stake" grows ~4x. If the hazard rule has been converted to auto_decline, these fire
# MAGIC `would_fire` (shadow) and are auto-declined; the engine should recommend `reopen_to_referral`
# MAGIC (never straight to accept), citing the mix shift. Volume ramps across the 3 future months.

# COMMAND ----------

haz_is_auto_decline = rules["HAZARDOUS_ACTIVITY_HEIGHT"].disposition == "auto_decline"
# Ramp: month +1 ~14 cases, +2 ~28, +3 ~56 → ~4x growth in GWP-at-stake across the window.
for month, n in ((0, 14), (1, 28), (2, 56)):
    for _ in range(n):
        d = TODAY + datetime.timedelta(days=month * 30 + rnd.randint(1, 30))
        p = pick_policy(HAZ_TRADES)
        technical = int(p.gross_premium * rnd.uniform(1.0, 1.4))    # bigger, better risks
        good_gwp = int(technical * rnd.uniform(0.97, 1.03))
        name = make_name(p.trade_group)
        txn_id = f"RC-{_seq:06d}"; _seq += 1
        r = rules["HAZARDOUS_ACTIVITY_HEIGHT"]
        # If auto_decline now: outcome declined (auto), shadow would_fire=true, GWP-at-stake = technical.
        # If still refer: it binds clean (good risk) — the segment is visibly improving either way.
        oc = "declined" if haz_is_auto_decline else "bound_clean"
        eid = hashlib.sha256(f"{txn_id}|HAZARDOUS_ACTIVITY_HEIGHT|{d.isoformat()}|shadow".encode()).hexdigest()[:32]
        telemetry.append((
            eid, d, txn_id, None, p.policy_number, name, "HAZARDOUS_ACTIVITY_HEIGHT",
            r.rule_version, r.disposition, bool(r.compliance_lock),
            (not haz_is_auto_decline), haz_is_auto_decline, oc,
            "system" if haz_is_auto_decline else rnd.choice(uw_ids), None,
            0.0 if haz_is_auto_decline else float(good_gwp), float(technical), 0.0,
            None if haz_is_auto_decline else float(round(rnd.gauss(44, 6), 1)),
            "NEW_BUSINESS", p.trade_group, p.product, "BRK-NEWCO", value_band(technical), 0, True))

# COMMAND ----------

# MAGIC %md ### Background book — denominator, co-fire realism, and the workflow rules' own history
# MAGIC A broad spread of the exposure/pricing/lifecycle rules across the full window so per-rule rates
# MAGIC sit against a credible book (tens-to-hundreds per rule), with ~12% co-fires.

# COMMAND ----------

BG_RULES = ["MAX_TURNOVER", "MAX_SUMS_INSURED", "CLAIMS_FREQUENCY", "FLOOD_POSTCODE",
            "PRICE_BELOW_TECHNICAL_FLOOR", "RENEWAL_RATE_CHANGE_TOLERANCE", "SI_AUTHORITY_BAND",
            "MAX_WAGEROLL", "CONTRACT_SIZE_SINGLE", "DELEGATED_AUTHORITY_BINDER",
            "CROSS_COVER_PL_EL_DUAL", "MTA_IMMATERIAL_CHANGE"]
BG_COUNT = {"MAX_TURNOVER": 240, "MAX_SUMS_INSURED": 180, "CLAIMS_FREQUENCY": 160, "FLOOD_POSTCODE": 150,
            "PRICE_BELOW_TECHNICAL_FLOOR": 190, "RENEWAL_RATE_CHANGE_TOLERANCE": 170,
            "SI_AUTHORITY_BAND": 130, "MAX_WAGEROLL": 150, "CONTRACT_SIZE_SINGLE": 90,
            "DELEGATED_AUTHORITY_BINDER": 110, "CROSS_COVER_PL_EL_DUAL": 120, "MTA_IMMATERIAL_CHANGE": 140}
for rid, n in BG_COUNT.items():
    for _ in range(n):
        oc = rnd.choices(["bound_clean", "bound_with_terms", "price_walked", "declined"],
                         weights=(64, 18, 10, 8))[0]
        co = [] if rnd.random() < 0.88 else [rnd.choice([r for r in BG_RULES if r != rid])]
        emit_case(rid, oc, window=("future" if rnd.random() < 0.06 else "past"),
                  co_rules=co, noadj=(rnd.random() < 0.45), lr=rnd.gauss(50, 11))

print(f"  generated telemetry rows: {len(telemetry)}  (haz currently auto_decline={haz_is_auto_decline})")

# COMMAND ----------

# MAGIC %md ## Write `gold_referral_telemetry`

# COMMAND ----------

SCHEMA = ("telemetry_id string, as_of_date date, transaction_id string, submission_id string, "
          "policy_number string, company_name string, rule_id string, rule_version string, "
          "disposition string, compliance_lock boolean, fired boolean, would_fire boolean, "
          "outcome string, decided_by string, terms_applied string, gwp double, technical_premium double, "
          "loading_pts double, loss_ratio_pct double, transaction_type string, trade_group string, "
          "product string, channel string, value_band string, co_fire_count int, is_future boolean")
df = spark.createDataFrame(telemetry, SCHEMA)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_referral_telemetry")
spark.sql(f"ALTER TABLE {fqn}.gold_referral_telemetry SET TBLPROPERTIES "
          f"('layer'='gold', 'demo'='underwriting_workbench', 'lane'='referral_control')")
spark.sql(f"COMMENT ON TABLE {fqn}.gold_referral_telemetry IS "
          f"'Full referral fire-vector (no short-circuit): one row per (case, rule) that fired, plus "
          f"would_fire shadow rows for auto_decline/accept rules, with the eventual outcome joined. "
          f"Read through an as_of_date filter (as_of_date <= chosen date) for time-travel. The substrate "
          f"for detection, isolation, co-fire, emulation and the reviewer agent.'")
print(f"  gold_referral_telemetry: {spark.table(f'{fqn}.gold_referral_telemetry').count()} rows")

# COMMAND ----------

# MAGIC %md ## Verification — storyline signals are present and discoverable

# COMMAND ----------

def rule_stats(rid, as_of=None):
    where = f"rule_id='{rid}' AND fired"
    if as_of:
        where += f" AND as_of_date <= DATE'{as_of}'"
    r = spark.sql(f"""
        SELECT count(*) fires,
               avg(CASE WHEN outcome IN ('bound_clean','bound_with_terms') THEN 1.0 ELSE 0 END) approval,
               avg(CASE WHEN outcome IN ('declined','price_walked') THEN 1.0 ELSE 0 END) decline_walk,
               avg(CASE WHEN co_fire_count=0 THEN 1.0 ELSE 0 END) isolation,
               avg(loss_ratio_pct) lr
        FROM {fqn}.gold_referral_telemetry WHERE {where}""").first()
    return r


# No short-circuit: co-fires exist (multiple rules per case).
multi = spark.sql(f"""SELECT count(*) c FROM (
    SELECT transaction_id FROM {fqn}.gold_referral_telemetry WHERE fired
    GROUP BY transaction_id HAVING count(*) > 1)""").first().c
print("cases with >1 fired rule (co-fires):", multi)
assert multi > 100, "expected co-fire cases (fire-vector, not short-circuit)"

# S1 hazard: high decline/price-walk, mostly isolated.
h = rule_stats("HAZARDOUS_ACTIVITY_HEIGHT", as_of=TODAY.isoformat())
print(f"S1 HAZARDOUS: fires={h.fires} decline/walk={h.decline_walk:.2f} isolation={h.isolation:.2f}")
assert h.decline_walk >= 0.85 and h.isolation >= 0.7, "S1 signal missing"

# S4 dual-trade: high approval, near-zero isolation.
d = rule_stats("DUAL_TRADE_DECLARED", as_of=TODAY.isoformat())
print(f"S4 DUAL_TRADE: fires={d.fires} approval={d.approval:.2f} isolation={d.isolation:.2f}")
assert d.approval >= 0.80 and d.isolation <= 0.05, "S4 signal missing"

# S5 new-venture: isolated fires carry worse LR than co-fires.
lr_iso = spark.sql(f"""SELECT avg(loss_ratio_pct) FROM {fqn}.gold_referral_telemetry
    WHERE rule_id='NEW_VENTURE_TRADING_HISTORY' AND fired AND co_fire_count=0""").first()[0]
lr_co = spark.sql(f"""SELECT avg(loss_ratio_pct) FROM {fqn}.gold_referral_telemetry
    WHERE rule_id='NEW_VENTURE_TRADING_HISTORY' AND fired AND co_fire_count>0""").first()[0]
print(f"S5 NEW_VENTURE: isolated LR={lr_iso:.1f} vs co-fire LR={lr_co:.1f}")
assert lr_iso - lr_co >= 15, "S5 signal missing (isolated fires should be materially worse)"

# S3 event: a dominant single clause among bound_with_terms.
clause = spark.sql(f"""SELECT count(*) c FROM {fqn}.gold_referral_telemetry
    WHERE rule_id='EVENT_ATTENDANCE_LIMIT' AND fired AND terms_applied='CROWD_MGMT_CLAUSE_07'""").first().c
print(f"S3 EVENT: CROWD_MGMT_CLAUSE_07 applications={clause}")
assert clause >= 80, "S3 signal missing (repeated clause)"

# S6 sanctions: locked + present.
assert rules["SANCTIONS_SCREEN_HIT"].compliance_lock, "S6 rule must be locked"
s6 = rule_stats("SANCTIONS_SCREEN_HIT", as_of=TODAY.isoformat())
print(f"S6 SANCTIONS: fires={s6.fires} (compliance-locked)")
assert s6.fires >= 80, "S6 volume missing"

# S7 renewal noise: biggest raw volume + high approval.
r7 = rule_stats("RENEWAL_UNCHANGED_RISK", as_of=TODAY.isoformat())
print(f"S7 RENEWAL_UNCHANGED: fires={r7.fires} approval={r7.approval:.2f}")
assert r7.fires >= 500 and r7.approval >= 0.85, "S7 signal missing"

# S2 future mix-shift: shadow GWP-at-stake grows across the future months (new channel).
ramp = {r.mo: r.stake for r in spark.sql(f"""
    SELECT floor(datediff(as_of_date, DATE'{TODAY}')/30) mo,
           sum(CASE WHEN channel='BRK-NEWCO' THEN gwp + technical_premium ELSE 0 END) stake
    FROM {fqn}.gold_referral_telemetry
    WHERE rule_id='HAZARDOUS_ACTIVITY_HEIGHT' AND is_future GROUP BY 1 ORDER BY 1""").collect()}
print("S2 future NEWCO stake by month:", {k: round(v or 0) for k, v in ramp.items()})
assert len([m for m in ramp if m is not None and m >= 0]) >= 2, "S2 future ramp missing"

# Time-travel: past-only view is smaller than the full (incl. future) view.
n_now = spark.sql(f"SELECT count(*) c FROM {fqn}.gold_referral_telemetry WHERE as_of_date <= DATE'{TODAY}'").first().c
n_all = spark.sql(f"SELECT count(*) c FROM {fqn}.gold_referral_telemetry").first().c
print(f"as_of today rows={n_now} · all rows={n_all} · future={n_all - n_now}")
assert n_all > n_now, "future book missing (as_of scrubbing would show nothing new)"

print("✅ 00f Referral Control telemetry — substrate + 7 storyline signals verified")
