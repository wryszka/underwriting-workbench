# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup + synthetic data generation — Bricksurance SE Underwriting
# MAGIC
# MAGIC Deterministic (seed=42), rolling-date synthetic universe for the Underwriting Workbench:
# MAGIC the **PAS book** (in-force commercial policies + claims history), the **submission feed**
# MAGIC (12 months of broker submissions across e-trade / portal / email channels), **company
# MAGIC profiles** (Companies-House-shaped, synthetic), reference data (brokers, underwriters,
# MAGIC appetite, authority matrix, rate guide, rebuild benchmark, internal watchlist), and the
# MAGIC **bundled real open data** (OFSI sanctions list, police.uk crime, EA flood areas, EPC band
# MAGIC mix, ONS centroids — see `data/open/PROVENANCE.md`).
# MAGIC
# MAGIC The three hero submissions (`sub:900001/2/3`) are hand-seeded and SACRED — byte-identical
# MAGIC on every reset (dates re-anchor to `current_date()`).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
dbutils.widgets.text("seed", "42")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
SEED = int(dbutils.widgets.get("seed"))
fqn = f"{catalog}.{schema}"

import csv, datetime, json, os, random, shutil

from pyspark.sql import functions as F

random.seed(SEED)
TODAY = datetime.date.today()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
for vol in ("submission_inbox", "open_data", "ingest_checkpoints", "comms_out"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}.{vol}")
print(f"target = {fqn}  seed={SEED}  as_at={TODAY}")


def write(df, name, layer):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.{name}")
    spark.sql(f"ALTER TABLE {fqn}.{name} SET TBLPROPERTIES ('layer'='{layer}', 'demo'='underwriting_workbench')")
    print(f"  {name}: {spark.table(f'{fqn}.{name}').count()} rows")

# COMMAND ----------

# MAGIC %md ## Bundled real open data → `open_data` Volume + ref tables
# MAGIC Real, OGL-licensed extracts fetched at BUILD time (never at demo time) — provenance in
# MAGIC `data/open/PROVENANCE.md`. Copied to the Volume so the bronze pipeline ingests them like
# MAGIC any external feed.

# COMMAND ----------

OPEN_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "data", "open"))
VOL_OPEN = f"/Volumes/{catalog}/{schema}/open_data"
for fname in sorted(os.listdir(OPEN_DIR)):
    shutil.copyfile(os.path.join(OPEN_DIR, fname), f"{VOL_OPEN}/{fname}")
print("open_data volume:", os.listdir(VOL_OPEN))


def _read_open(fname):
    with open(os.path.join(OPEN_DIR, fname), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# ONS district centroids (the commercial book's geographic grain)
cent = _read_open("ons_district_centroids.csv")
DISTRICTS = [r["postcode_district"] for r in cent]
write(spark.createDataFrame([(r["postcode_district"], float(r["lat"]), float(r["lon"])) for r in cent],
                            "postcode_district string, lat double, lon double"),
      "ref_postcode_centroid", "reference")

# police.uk crime counts. GOTCHA surfaced honestly: Greater Manchester Police has not
# submitted street-level data to police.uk since 2019 — GMP-area districts come back 0.
# We flag those rows imputed=true and use the non-GMP median, and the app labels the gap.
crime_raw = _read_open("police_uk_crime_by_district.csv")
GMP_PREFIX = ("M", "BL", "OL", "WN", "WA")
by_d = {}
for r in crime_raw:
    d = by_d.setdefault(r["postcode_district"], {"burglary": 0, "criminal-damage-arson": 0, "month": r["month"]})
    d[r["category"]] = int(r["count"]) if r["count"] not in ("", "None") else 0
non_gmp = [v["burglary"] + v["criminal-damage-arson"] for k, v in by_d.items()
           if not any(k.startswith(p) and k[len(p):len(p) + 1].isdigit() for p in GMP_PREFIX)]
non_gmp_median = sorted(non_gmp)[len(non_gmp) // 2]
crime_rows = []
for k, v in sorted(by_d.items()):
    total = v["burglary"] + v["criminal-damage-arson"]
    is_gmp = any(k.startswith(p) and k[len(p):len(p) + 1].isdigit() for p in GMP_PREFIX)
    imputed = bool(is_gmp and total == 0)
    eff = non_gmp_median if imputed else total
    crime_rows.append((k, v["month"], v["burglary"], v["criminal-damage-arson"], total, imputed, eff))
write(spark.createDataFrame(crime_rows,
      "postcode_district string, month string, burglary int, criminal_damage_arson int, "
      "reported_total int, imputed boolean, effective_count int"),
      "ref_crime_open", "reference")

# EA flood evidence (REAL): the alert/warning areas in the live EA register that NAME each
# district's town, plus the rivers involved. The High/Medium/Low BAND is curated from the
# EA's published RoFRS (Risk of Flooding from Rivers and Sea) statistics — England only —
# and labelled as curated; production would use the full property-level RoFRS dataset
# (+ the separate surface-water dataset, noted as roadmap).
FLOOD_BAND = {  # curated per RoFRS: Calder Valley + Severn at Gloucester = High; defended tidal city centres = Low
    "HX7": "High", "HX6": "High", "GL1": "High",
    "OX1": "Medium", "CB1": "Medium", "M20": "Medium", "PR1": "Medium", "HD1": "Medium",
    "NG1": "Medium", "RG1": "Medium", "LE1": "Medium", "BL1": "Medium", "BL3": "Medium", "WS2": "Medium",
}
flood_raw = _read_open("ea_flood_areas_by_district.csv")
flood_rows = []
for r in sorted(flood_raw, key=lambda x: x["postcode_district"]):
    d = r["postcode_district"]
    flood_rows.append((d, r["town_terms"], int(r["named_area_count"]), int(r["named_warning_count"]),
                       r["rivers"], r["sample_areas"], FLOOD_BAND.get(d, "Low")))
write(spark.createDataFrame(flood_rows,
      "postcode_district string, town_terms string, named_area_count int, named_warning_count int, "
      "rivers string, sample_areas string, flood_band string"),
      "ref_flood_open", "reference")
assert dict((r[0], r[6]) for r in flood_rows)["HX7"] == "High" and dict((r[0], r[6]) for r in flood_rows)["WA14"] == "Low"

# EPC non-domestic band mix (curated MHCLG summary) — MEES/ESG lens ONLY, never a rating factor.
epc = _read_open("epc_nondom_band_mix_by_district.csv")
write(spark.createDataFrame(
      [(r["postcode_district"],) + tuple(int(r[f"pct_{b}"]) for b in "abcdefg") for r in epc],
      "postcode_district string, pct_a int, pct_b int, pct_c int, pct_d int, pct_e int, pct_f int, pct_g int"),
      "ref_epc_mix_open", "reference")

# OFSI consolidated list (REAL, 12k primary names). Screening runs against this for real;
# the demo's watchlist hit is on the separate SYNTHETIC internal watchlist below.
ofsi = _read_open("ofsi_consolidated_list.csv")
write(spark.createDataFrame(
      [(r["name"], r["group_type"], r["dob"], r["nationality"], r["country"], r["regime"],
        r["listed_on"], r["group_id"]) for r in ofsi],
      "name string, group_type string, dob string, nationality string, country string, "
      "regime string, listed_on string, group_id string"),
      "ref_sanctions_ofsi", "reference")

# COMMAND ----------

# MAGIC %md ## Reference data — brokers, underwriters, appetite, authority, rate guide

# COMMAND ----------

# Brokers: the three canonical Bricksurance brokers (shared universe with claims_workbench)
# + two commercial specialists.
BROKERS = [
    ("BRK-001", "Aldgate Risk Partners", "ARP-114", "Commercial motor & fleet", "Priya Nair", "newbiz@aldgaterisk.example"),
    ("BRK-002", "Caldwell & Vane", "CDV-227", "Household & high-net-worth property", "James Caldwell", "newbiz@caldwellvane.example"),
    ("BRK-003", "Northgate Insurance Brokers", "NGB-305", "SME & regional retail", "Ellen Okafor", "newbiz@northgatebrokers.example"),
    ("BRK-004", "Pennine Commercial Risks", "PCR-402", "Mid-market manufacturing & wholesale", "David Whitworth", "submissions@penninecommercial.example"),
    ("BRK-005", "Harborough & Slate", "HSL-518", "Regional commercial combined", "Fiona Slate", "submissions@harboroughslate.example"),
]
write(spark.createDataFrame(BROKERS,
      "broker_id string, broker_name string, producer_code string, segment string, contact_name string, contact_email string"),
      "ref_broker", "reference")

# Underwriters + desks. Authority comes from the matrix by grade (below).
UNDERWRITERS = [
    ("UW-01", "Priya Sharma", "sme_package", "assistant_underwriter"),
    ("UW-02", "Tom Okonkwo", "sme_package", "underwriter"),
    ("UW-03", "Rachel Byrne", "sme_package", "underwriter"),
    ("UW-04", "Marcus Webb", "mid_market", "underwriter"),
    ("UW-05", "Aisha Rahman", "mid_market", "underwriter"),
    ("UW-06", "Helen Craddock", "mid_market", "senior_underwriter"),   # hero 900002's referral route
    ("UW-07", "Steven Doyle", "mid_market", "senior_underwriter"),
    ("UW-08", "Josh Whittaker", "casualty", "underwriter"),
    ("UW-09", "Marta Kowalczyk", "casualty", "senior_underwriter"),
    ("UW-10", "Gareth Lloyd", "trading_desk", "assistant_underwriter"),
    ("UW-11", "Nadia Hussein", "trading_desk", "underwriter"),
    ("UW-12", "Alan Prentice", "leadership", "head_of_underwriting"),
]
write(spark.createDataFrame(UNDERWRITERS, "underwriter_id string, underwriter_name string, desk string, grade string"),
      "ref_underwriter", "reference")

# Authority matrix: who can sign what. E-trade "system" authority enables straight-through.
AUTHORITY = [
    # grade, max_total_si, max_gross_premium, max_hazard_grade, flood_high_allowed, escalate_to
    ("system_etrade",        500_000,     2_500, 2, False, "underwriter"),
    ("assistant_underwriter", 1_000_000,  5_000, 2, False, "underwriter"),
    ("underwriter",           5_000_000, 50_000, 3, False, "senior_underwriter"),
    ("senior_underwriter",   50_000_000, 400_000, 4, True,  "head_of_underwriting"),
    ("head_of_underwriting", 150_000_000, 1_500_000, 5, True, "board"),
]
write(spark.createDataFrame(AUTHORITY,
      "grade string, max_total_si long, max_gross_premium long, max_hazard_grade int, flood_high_allowed boolean, escalate_to string"),
      "ref_authority_matrix", "reference")

# Appetite by trade group. status: core / selective / excluded. UK SIC 2007 codes.
# guide_section = the underwriting-guide citation used in coded decline reasons.
APPETITE = [
    # trade_group, sic, hazard(1-5), status, decline_code, guide_section, capacity_note
    ("retail_shop",            "47190", 1, "core",      None, "UG-3.1", "Core book"),
    ("office_professional",    "70229", 1, "core",      None, "UG-3.2", "Core book"),
    ("hair_beauty",            "96020", 1, "core",      None, "UG-3.3", "Core book"),
    ("education_training",     "85590", 1, "core",      None, "UG-3.4", "Core book"),
    ("healthcare_clinics",     "86900", 2, "core",      None, "UG-3.5", "Core book"),
    ("wholesale",              "46900", 2, "core",      None, "UG-3.6", "Core book"),
    ("property_owners",        "68209", 2, "core",      None, "UG-3.7", "MEES/EPC portfolio lens applies"),
    ("warehousing_logistics",  "52103", 2, "core",      None, "UG-3.8", "Sprinkler discount available"),
    ("light_manufacturing",    "25990", 3, "core",      None, "UG-4.1", "Survey ≥ £2m SI"),
    ("motor_trade",            "45200", 3, "selective", None, "UG-4.2", "No open-lot flood exposure"),
    ("hospitality_restaurant", "56101", 3, "selective", None, "UG-4.3", "Frying/extraction protections required"),
    ("hotels_leisure",         "55100", 3, "selective", None, "UG-4.4", "Sleeping-risk fire standards"),
    ("construction_contractors","43999", 4, "selective", None, "UG-4.5", "Depth/height limits; hot-work permit"),
    ("metal_engineering",      "25620", 4, "selective", None, "UG-4.6", "Hot-work controls; survey required"),
    ("food_manufacturing",     "10890", 4, "selective", None, "UG-4.7", "Composite-panel % must be confirmed; survey required"),
    ("waste_recycling",        "38320", 5, "excluded",  "APP-EXCL-WASTE", "UG-9.2", "Excluded trade — fire load/stock combustibility; specialist markets"),
    ("nightclubs",             "56302", 5, "excluded",  "APP-EXCL-NIGHT", "UG-9.3", "Excluded trade — late-licence assault/PL experience"),
]
write(spark.createDataFrame(APPETITE,
      "trade_group string, sic_code string, hazard_grade int, appetite_status string, decline_code string, "
      "guide_section string, appetite_note string"),
      "ref_appetite", "reference")

# Rate guide (illustrative but realistically-shaped; full GLM engine = pricing-workbench, cross-linked).
RATE_GUIDE = [
    # trade_group, property ‰, BI ‰, EL £/employee, PL £/£1k turnover, min_premium £
    ("retail_shop",             4.0, 2.0,  60, 0.9,  400),
    ("office_professional",     1.6, 1.0,  45, 0.5,  350),
    ("hair_beauty",             3.0, 1.6,  55, 1.2,  300),
    ("education_training",      2.0, 1.2,  50, 1.0,  400),
    ("healthcare_clinics",      2.2, 1.4,  70, 1.5,  500),
    ("wholesale",               3.0, 1.6,  75, 0.7,  600),
    ("property_owners",         2.4, 1.4,  50, 0.4,  500),
    ("warehousing_logistics",   3.4, 1.8,  95, 0.6,  750),
    ("light_manufacturing",     4.2, 2.2, 120, 1.1,  900),
    ("motor_trade",             4.6, 2.4, 110, 1.3,  800),
    ("hospitality_restaurant",  5.2, 2.8, 105, 1.4,  650),
    ("hotels_leisure",          4.8, 2.6, 100, 1.2, 1200),
    ("construction_contractors",4.4, 2.0, 210, 1.8,  900),
    ("metal_engineering",       5.0, 2.6, 170, 1.4, 1000),
    ("food_manufacturing",      5.5, 2.8, 180, 1.6, 1500),
    ("waste_recycling",        12.0, 6.0, 260, 2.5, 5000),
    ("nightclubs",             10.0, 5.0, 240, 3.5, 4000),
]
write(spark.createDataFrame(RATE_GUIDE,
      "trade_group string, property_rate_permille double, bi_rate_permille double, "
      "el_rate_per_employee double, pl_rate_per_1k_turnover double, min_premium int"),
      "ref_rate_guide", "reference")

# Rebuild-cost benchmark by construction type (BCIS-shaped, illustrative) — underinsurance check.
REBUILD = [
    ("brick_traditional", 2200), ("steel_frame_clad", 1250), ("concrete_frame", 1900),
    ("timber_frame", 2000), ("composite_panel_clad", 1350), ("listed_heritage", 3400),
]
write(spark.createDataFrame(REBUILD, "construction_type string, rebuild_cost_per_m2 int"),
      "ref_rebuild_benchmark", "reference")

# Outward reinsurance structure (property surplus treaty) — the net-line check on referrals.
# Bricksurance Re (sibling workbench) writes the treaty; the desk must know what the treaty
# absorbs per risk and when facultative cover is required.
TREATY = [
    # treaty_id, treaty_name, applies_to, net_retention_per_risk, surplus_lines, per_risk_capacity, reinsurer, notes
    ("TR-SURP-2026", "Property Surplus Treaty 2026", "property", 5_000_000, 4, 25_000_000,
     "Bricksurance Re", "Per-risk: retain GBP 5m net, cede up to 4 lines (GBP 20m). Above GBP 25m per risk = facultative required."),
    ("TR-CXL-2026", "Casualty XoL 2026", "liability", 2_000_000, 0, 10_000_000,
     "Bricksurance Re", "EL/PL excess-of-loss GBP 8m xs GBP 2m (context only in this demo)."),
]
write(spark.createDataFrame(TREATY,
      "treaty_id string, treaty_name string, applies_to string, net_retention_per_risk long, "
      "surplus_lines int, per_risk_capacity long, reinsurer string, notes string"),
      "ref_treaty_structure", "reference")

# Internal watchlist — SYNTHETIC (the only place a screening "hit" can come from in this demo).
WATCHLIST = [
    ("WL-001", "Derek Ashworth",  "individual", "Director of entity whose prior policy was avoided for non-disclosure (2023)", "underwriting_conduct"),
    ("WL-002", "Braxfield Storage Solutions Ltd", "entity", "Arson repudiation upheld 2022 — director overlap monitoring", "claims_intel"),
    ("WL-003", "Colin Marsh", "individual", "Broker-reported impersonation attempt on renewal 2024", "fraud_intel"),
    ("WL-004", "Vantry Imports Ltd", "entity", "Trade description inconsistent with premises use at survey", "survey_intel"),
    ("WL-005", "Janet Okri", "individual", "Material non-disclosure of prosecution history on 2022 proposal", "underwriting_conduct"),
    ("WL-006", "Stanhope Metal Traders Ltd", "entity", "Unpaid premium / broker account dispute — refer to credit control", "credit_control"),
]
write(spark.createDataFrame(WATCHLIST, "watchlist_id string, name string, subject_type string, reason string, source string"),
      "ref_internal_watchlist", "reference")

CITY = {"EC1", "E1", "SE1", "SW1", "N1", "M1", "B1", "LS1", "BS1", "NE1", "CF10", "S1", "NG1", "LE1", "CV1"}
# ref_district_capacity is written AFTER the PAS book below — capacity is calibrated to the
# generated in-force exposure so utilisation is meaningful (HX7 pinned to £25m = 67%).

# COMMAND ----------

# MAGIC %md ## Company profiles — Companies-House-shaped, synthetic
# MAGIC Incorporation, SIC, accounts status, filed turnover, directors. Real Companies House
# MAGIC gives no CCJs or credit scores, so neither do we; micro-entities file without turnover.

# COMMAND ----------

random.seed(SEED + 2)

FIRST = ["Amelia", "Oliver", "Sophia", "Harry", "Isla", "George", "Ava", "Noah", "Emily", "Jack",
         "Grace", "Leo", "Freya", "Oscar", "Poppy", "Arthur", "Priya", "Imran", "Ellen", "David",
         "Fiona", "Marcus", "Aisha", "Steven", "Helen", "Josh", "Marta", "Gareth", "Nadia", "Alan"]
LAST = ["Hughes", "Patel", "Walsh", "Thompson", "Okafor", "Bennett", "Kaur", "Murray", "Ellis", "Nowak",
        "Doyle", "Ferguson", "Ademola", "Price", "Whitfield", "Sharma", "Ashcroft", "Byrne", "Webb", "Slate",
        "Craddock", "Whitworth", "Okri", "Marsh", "Prentice", "Lloyd", "Hussein", "Kowalczyk", "Rahman", "Okonkwo"]
TOWN_BY_DISTRICT = {"WA14": "Altrincham", "HX7": "Hebden Bridge", "HX6": "Sowerby Bridge", "WS2": "Walsall",
                    "HD1": "Huddersfield", "PR1": "Preston", "M1": "Manchester", "LS1": "Leeds", "B1": "Birmingham"}
NAME_A = ["Northern", "Pennine", "Albion", "Stanhope", "Riverside", "Crown", "Victoria", "Meridian", "Harbour",
          "Regent", "Fairfield", "Granite", "Beacon", "Orchard", "Summit", "Anchor", "Copper", "Fenland", "Weaver", "Calder"]
NAME_B = {"retail_shop": ["Trading", "Retail", "Homewares", "Supplies", "Stores"],
          "office_professional": ["Consulting", "Design Studio", "Associates", "Partners", "Solutions"],
          "hair_beauty": ["Hair Studio", "Beauty Rooms", "Grooming Co"],
          "education_training": ["Training", "Learning", "Tuition"],
          "healthcare_clinics": ["Clinic", "Health", "Dental Care"],
          "wholesale": ["Wholesale", "Distribution", "Trading Co"],
          "property_owners": ["Estates", "Property", "Holdings", "Investments"],
          "warehousing_logistics": ["Logistics", "Storage", "Freight", "Fulfilment"],
          "light_manufacturing": ["Engineering", "Components", "Products", "Fabrications"],
          "motor_trade": ["Motors", "Autocentre", "Vehicle Services"],
          "hospitality_restaurant": ["Kitchen", "Dining", "Restaurant Group"],
          "hotels_leisure": ["Hotel", "Inns", "Leisure"],
          "construction_contractors": ["Construction", "Contractors", "Building Services"],
          "metal_engineering": ["Metalworks", "Precision Engineering", "Steel Fabrications"],
          "food_manufacturing": ["Fine Foods", "Food Group", "Bakeries", "Provisions"],
          "waste_recycling": ["Recycling", "Waste Services", "Reclamation"],
          "nightclubs": ["Nightclub Group", "Late Bar Co"]}
SIC_BY_TRADE = {t[0]: t[1] for t in APPETITE}
HAZARD_BY_TRADE = {t[0]: t[2] for t in APPETITE}
STATUS_BY_TRADE = {t[0]: t[3] for t in APPETITE}
TRADES_CORE = [t[0] for t in APPETITE if t[3] == "core"]
TRADES_SEL = [t[0] for t in APPETITE if t[3] == "selective"]

used_names = set()


def company_name(trade):
    for _ in range(40):
        nm = f"{random.choice(NAME_A)} {random.choice(NAME_B[trade])} Ltd"
        if nm not in used_names:
            used_names.add(nm)
            return nm
    nm = f"{random.choice(NAME_A)} {random.choice(NAME_B[trade])} ({random.randint(2, 99)}) Ltd"
    used_names.add(nm)
    return nm


def mk_directors(n):
    return [f"{random.choice(FIRST)} {random.choice(LAST)}" for _ in range(n)]


N_PROFILES = 9000   # most companies submit ONCE a year; a deliberate ~20% tail remarkets (see submission loop)
profiles, PROFILE_BY_CO = [], {}
for i in range(N_PROFILES):
    trade = random.choice(TRADES_CORE * 3 + TRADES_SEL)
    co_no = f"{random.randint(2_000_000, 13_999_999):08d}"
    name = company_name(trade)
    inc_days = random.randint(500, 12_000)
    turnover = None
    base_t = random.choice([120, 250, 400, 800, 1_500, 3_000, 6_000, 12_000, 25_000]) * 1000
    if base_t >= 800_000:                      # micro-entities file without turnover
        turnover = int(base_t * random.uniform(0.85, 1.15))
    overdue = random.random() < 0.06
    sic = SIC_BY_TRADE[trade]
    if random.random() < 0.03:                  # declared-trade vs SIC mismatch beat
        sic = random.choice(list(SIC_BY_TRADE.values()))
    row = (co_no, name, (TODAY - datetime.timedelta(days=inc_days)).isoformat(), sic,
           "active" if random.random() > 0.015 else "liquidation",
           overdue, turnover, json.dumps(mk_directors(random.randint(1, 4))), trade, base_t)
    profiles.append(row)
    PROFILE_BY_CO[co_no] = row

# HERO profiles (sacred). 900001's director "Emran Ali" is a deliberate NEAR-MISS against the
# real OFSI primary name "Emraan ALI" — resolved as a false positive on DOB/nationality.
HERO_PROFILES = [
    ("09384712", "Fenwick & Moss Homewares Ltd", (TODAY - datetime.timedelta(days=4180)).isoformat(),
     "47190", "active", False, None,                       # micro-entity: no filed turnover
     json.dumps(["Claire Fenwick", "Emran Ali"]), "retail_shop", 750_000),
    ("06120843", "Calder Valley Fine Foods Ltd", (TODAY - datetime.timedelta(days=7300)).isoformat(),
     "10890", "active", True, 24_000_000,                  # FULL accounts: £24m vs £8m stated
     json.dumps(["Margaret Calder", "Piotr Nowak", "Simon Aldous"]), "food_manufacturing", 24_000_000),
    ("07551209", "Midland Metal & Waste Recycling Ltd", (TODAY - datetime.timedelta(days=5100)).isoformat(),
     "38320", "active", False, 3_100_000,
     json.dumps(["Derek Ashworth", "Karen Tolley"]), "waste_recycling", 3_100_000),
]
for h in HERO_PROFILES:
    PROFILE_BY_CO[h[0]] = h
write(spark.createDataFrame(profiles + HERO_PROFILES,
      "company_number string, company_name string, incorporation_date string, sic_code string, "
      "company_status string, accounts_overdue boolean, filed_turnover long, directors_json string, "
      "trade_group string, true_turnover long"),
      "landing_company_profiles", "landing")

# COMMAND ----------

# MAGIC %md ## Client master — the multi-policy keystone (CustomerLake-compatible shape)
# MAGIC One governed party id (`client_id`) across submissions, policies and claims. Fields are
# MAGIC aligned with a CustomerLake Profile-Agent "golden profile" output so a real estate can
# MAGIC swap this synthetic build for identity resolution over messy PAS/CRM/claims parties.

# COMMAND ----------

random.seed(SEED + 7)
clients, CLIENT_BY_CO = [], {}
for i, prof in enumerate(profiles + HERO_PROFILES):
    cid = f"CL-{100001 + i}" if prof[0] not in ("09384712", "06120843", "07551209") else         {"09384712": "CL-900001", "06120843": "CL-900002", "07551209": "CL-900003"}[prof[0]]
    since = (TODAY - datetime.timedelta(days=random.randint(200, 4000))).isoformat()
    clients.append((cid, prof[0], prof[1], prof[8], "mid_market" if prof[9] >= 3_000_000 else "sme",
                    since, "synthetic_profile (CustomerLake Profile Agent in production)", 1.0))
    CLIENT_BY_CO[prof[0]] = cid
write(spark.createDataFrame(clients,
      "client_id string, company_number string, legal_name string, trade_group string, segment string, "
      "client_since string, resolution_source string, profile_confidence double"),
      "ref_client", "reference")

# COMMAND ----------

# MAGIC %md ## PAS book — in-force commercial policies + claims history
# MAGIC ~30k in-force + recent lapsed (retention story). HX7 in-force property SI is hand-seeded
# MAGIC to exactly £16.75m so hero 900002's marginal £5m lands at 87% of the £25m capacity.

# COMMAND ----------

random.seed(SEED + 3)
CONSTRUCTIONS = ["brick_traditional", "steel_frame_clad", "concrete_frame", "timber_frame", "composite_panel_clad"]
REBUILD_RATE = dict(REBUILD)
DISTRICT_POOL = [d for d in DISTRICTS if d != "HX7"]           # HX7 hand-seeded below
DISTRICT_W = [3 if d in CITY else 1 for d in DISTRICT_POOL]
RATE_BY_TRADE = {r[0]: r for r in RATE_GUIDE}

N_POLICIES = 30_000
policies = []


def mk_policy(seq, district=None, trade=None, forced_property_si=None, status="in_force"):
    trade = trade or random.choice(TRADES_CORE * 3 + TRADES_SEL)
    district = district or random.choices(DISTRICT_POOL, weights=DISTRICT_W, k=1)[0]
    segment = "mid_market" if random.random() < 0.18 else "sme"
    mult = random.choice([8, 12, 20, 35]) if segment == "mid_market" else random.choice([1, 1, 2, 3])
    tenant = trade in ("retail_shop", "office_professional", "hair_beauty") and random.random() < 0.6
    buildings = 0 if tenant else random.randint(150, 900) * 1000 * mult
    contents = random.randint(20, 120) * 1000 * mult
    stock = random.randint(10, 150) * 1000 * mult if trade not in ("office_professional",) else 0
    if forced_property_si is not None:
        scale = forced_property_si / max(buildings + contents + stock, 1)
        buildings, contents, stock = int(buildings * scale), int(contents * scale), int(stock * scale)
        buildings += forced_property_si - (buildings + contents + stock)   # exact
    bi = int((contents + stock + buildings * 0.3) * random.uniform(0.8, 1.6))
    employees = max(2, int((buildings + contents + stock) / 60_000) + random.randint(0, 8))
    turnover = employees * random.randint(60, 140) * 1000
    rg = RATE_BY_TRADE[trade]
    prem = max(rg[5], int(((buildings + contents + stock) * rg[1] + bi * rg[2]) / 1000
                          + employees * rg[3] + (turnover / 1000) * rg[4]) )
    prem = int(prem * random.uniform(0.85, 1.2))
    incep_days_ago = random.randint(0, 364) if status == "in_force" else random.randint(365, 720)
    inception = TODAY - datetime.timedelta(days=incep_days_ago)
    rate_change = round(random.uniform(-0.02, 0.11), 3)
    floor_area = max(80, int((buildings if buildings else contents * 4) / random.choice([1250, 1900, 2200])))
    return (f"BSE-C-{seq:07d}", trade, SIC_BY_TRADE[trade], segment, district,
            random.choice(CONSTRUCTIONS if trade != "food_manufacturing" else CONSTRUCTIONS + ["composite_panel_clad"] * 3),
            random.randint(1955, 2018), floor_area, tenant, employees, turnover,
            buildings, contents, stock, bi, 10_000_000, random.choice([2, 5]) * 1_000_000,
            prem, round(prem / (1 + rate_change)), rate_change, 0.225 if segment == "sme" else 0.20,
            inception.isoformat(), (inception + datetime.timedelta(days=365)).isoformat(),
            None, status)


for i in range(1, N_POLICIES + 1):
    policies.append(mk_policy(i))

# Lapsed recent policies → retention KPI (~88%)
for i in range(N_POLICIES + 1, N_POLICIES + 4_000):
    policies.append(mk_policy(i, status="lapsed"))

# HX7 hand-seeded in-force book: property SI sums to EXACTLY £16,750,000 (67% of £25m capacity)
HX7_SIS = [3_400_000, 2_650_000, 2_100_000, 1_900_000, 1_650_000, 1_400_000,
           1_150_000, 900_000, 700_000, 500_000, 250_000, 150_000]
assert sum(HX7_SIS) == 16_750_000
for j, si in enumerate(HX7_SIS):
    policies.append(mk_policy(90_000 + j, district="HX7",
                              trade=random.choice(["retail_shop", "light_manufacturing", "hospitality_restaurant", "wholesale"]),
                              forced_property_si=si))

# Multi-policy accounts: ~1,800 clients also hold fleet / cyber / D&O policies with us —
# the account-underwriting dimension. Calder Valley (hero) holds a PROFITABLE fleet (28
# vehicles) + D&O, so the 900002 referral becomes an ACCOUNT conversation.
random.seed(SEED + 8)
extra_policies = []
_mp_clients = random.sample([c for c in clients if c[0] not in ("CL-900001", "CL-900003")], 1800)
_seq = 200_000
for c in _mp_clients:
    for pl in random.sample(["fleet", "cyber", "directors_officers"], random.choice([1, 1, 2])):
        _seq += 1
        prem = {"fleet": random.randint(8, 40) * 450, "cyber": random.randint(2, 18) * 320,
                "directors_officers": random.randint(2, 12) * 380}[pl]
        incep = TODAY - datetime.timedelta(days=random.randint(0, 364))
        extra_policies.append((f"BSE-C-{_seq:07d}", c[3], SIC_BY_TRADE.get(c[3], "70229"), c[4],
                               random.choices(DISTRICT_POOL, weights=DISTRICT_W, k=1)[0],
                               "n/a", 2000, 0, False, 10, 0, 0, 0, 0, 0, 10_000_000, 5_000_000,
                               prem, round(prem / 1.04), 0.04, 0.20,
                               incep.isoformat(), (incep + datetime.timedelta(days=365)).isoformat(),
                               None, "in_force", pl, c[0]))
# Calder Valley's account (deterministic): fleet 28 vehicles + D&O, both profitable, in force
for pn, pl, prem in (("BSE-C-0900002", "fleet", 14_700), ("BSE-C-0900003", "directors_officers", 3_800)):
    incep = TODAY - datetime.timedelta(days=210)
    extra_policies.append((pn, "food_manufacturing", "10890", "mid_market", "HX7", "n/a", 2000, 0, False,
                           160, 24_000_000, 0, 0, 0, 0, 10_000_000, 5_000_000, prem, round(prem / 1.05),
                           0.05, 0.20, incep.isoformat(), (incep + datetime.timedelta(days=365)).isoformat(),
                           None, "in_force", pl, "CL-900002"))

pol_schema = ("policy_number string, trade_group string, sic_code string, segment string, postcode_district string, "
              "construction_type string, year_built int, floor_area_m2 int, tenant boolean, employees int, turnover long, "
              "buildings_si long, contents_si long, stock_si long, bi_si long, el_limit long, pl_limit long, "
              "gross_premium long, prior_premium long, rate_change_pct double, commission_pct double, "
              "inception_date string, expiry_date string, broker_note string, policy_status string")
pol_schema_mp = pol_schema + ", product_line string, client_id string"
_core = [tuple(p) + ("commercial_combined" if p[3] == "mid_market" else "commercial_package", None)
         for p in policies]
pol_df = spark.createDataFrame(_core + extra_policies, pol_schema_mp).withColumn(
    "broker_id", F.expr("CASE WHEN pmod(abs(hash(policy_number)),10)<=2 THEN 'BRK-003' "
                        "WHEN pmod(abs(hash(policy_number)),10)<=4 THEN 'BRK-004' "
                        "WHEN pmod(abs(hash(policy_number)),10)<=6 THEN 'BRK-005' "
                        "WHEN pmod(abs(hash(policy_number)),10)<=7 THEN 'BRK-001' "
                        "WHEN pmod(abs(hash(policy_number)),10)<=8 THEN 'BRK-002' ELSE 'DIRECT' END"))
write(pol_df, "landing_pas_policies", "landing")

# District property capacity (accumulation appetite): calibrated to the generated in-force book
# so the Control Tower reads sensibly — most districts 45-70% (green), GL1/S1 deliberately amber
# (~83-86%), HX7 (Calder Valley) pinned at £25m so the hand-seeded £16.75m = 67% and hero
# 900002's marginal £5m lands at exactly 87% (over the 80% referral line, under breach).
from collections import defaultdict

_inforce = defaultdict(int)
for p in policies:
    if p[-1] == "in_force":
        _inforce[p[4]] += p[11] + p[12] + p[13]   # district → buildings+contents+stock
AMBER_TARGET = {"GL1": 0.83, "S1": 0.86}          # flood-High GL1 amber = a nice Control Tower story
cap_rows = []
for d in DISTRICTS:
    if d == "HX7":
        cap = 25_000_000
    else:
        util = AMBER_TARGET.get(d, 0.45 + (sum(ord(c) for c in d) % 26) / 100.0)
        cap = int(round(_inforce[d] / util, -5)) if _inforce[d] else 20_000_000
    cap_rows.append((d, cap))
write(spark.createDataFrame(cap_rows, "postcode_district string, property_capacity_gbp long"),
      "ref_district_capacity", "reference")

# COMMAND ----------

# Claims history against the PAS book (burning-cost basis). Perils weighted by trade;
# Calder Valley (HX7/HX6) carries a REAL flood-loss cluster — the district has flooded
# repeatedly (Boxing Day 2015, Feb 2020 storms), which the burning cost should remember.
random.seed(SEED + 4)
PERILS = ["fire", "escape_of_water", "storm", "flood", "theft", "impact", "el_injury", "pl_injury", "bi"]


def peril_for(trade, district):
    w = {"fire": 10, "escape_of_water": 14, "storm": 12, "flood": 3, "theft": 16,
         "impact": 8, "el_injury": 10, "pl_injury": 9, "bi": 4}
    if trade in ("food_manufacturing", "metal_engineering", "light_manufacturing"):
        w["fire"] += 14; w["el_injury"] += 8; w["bi"] += 6
    if trade in ("retail_shop", "wholesale", "warehousing_logistics"):
        w["theft"] += 12
    if district in ("HX7", "HX6"):
        w["flood"] += 30
    ks, ws = list(w.keys()), list(w.values())
    return random.choices(ks, weights=ws, k=1)[0]


SEV = {"fire": (45_000, 8.0), "escape_of_water": (9_500, 3.0), "storm": (7_000, 2.5), "flood": (38_000, 5.0),
       "theft": (6_500, 2.2), "impact": (5_200, 2.0), "el_injury": (22_000, 6.0), "pl_injury": (14_000, 5.0),
       "bi": (30_000, 6.0)}
claims = []
pol_sample = random.sample(policies, 15_500)
for k, p in enumerate(pol_sample):
    for _ in range(random.choices([1, 2, 3], weights=[78, 17, 5], k=1)[0]):
        peril = peril_for(p[1], p[4])
        base, tail = SEV[peril]
        paid = int(base * random.expovariate(1.0) * random.uniform(0.4, tail) / 2) + random.randint(250, 1500)
        loss_days_ago = random.randint(5, 1095)
        open_flag = loss_days_ago < 120 and random.random() < 0.5
        claims.append((f"CLM-{100000 + len(claims):06d}", p[0], peril,
                       (TODAY - datetime.timedelta(days=loss_days_ago)).isoformat(),
                       paid if not open_flag else int(paid * 0.4),
                       int(paid * random.uniform(1.0, 1.3)) if open_flag else paid,
                       "open" if open_flag else "settled"))
write(spark.createDataFrame(claims,
      "claim_id string, policy_number string, peril string, loss_date string, paid long, incurred long, status string"),
      "landing_pas_claims", "landing")

# COMMAND ----------

# MAGIC %md ## Submission feed — 12 months of broker submissions + the three SACRED heroes
# MAGIC Historical rows carry funnel outcomes (quoted / bound / declined / NTU / lost /
# MAGIC quote_expired) with channel-realistic conversion; ~350 recent rows are the open pipeline.

# COMMAND ----------

random.seed(SEED + 5)
CHANNELS = ["etrade", "portal", "email"]
CH_W = [55, 25, 20]
QUOTE_RATE = {"etrade": 0.92, "portal": 0.68, "email": 0.52}
BROKER_BY_SEGMENT = {"sme": ["BRK-003", "BRK-005", "BRK-002"], "mid_market": ["BRK-004", "BRK-001", "BRK-005"]}
INCUMBENTS = ["Aviva", "AXA", "RSA", "Zurich", "Allianz", "NIG", "Covea", "Self-insured", None]

profile_pool = [p for p in profiles if p[4] == "active"]
random.shuffle(profile_pool)
subs, sub_states = [], {"received": 0}
N_SUBS = 8_000
# Realistic remarket shape: ~90% of submissions are a company's ONLY approach this year;
# the rest re-approach (renewal shopping / broker remarketing) drawn from a concentrated tail —
# so "returning client" is a meaningful signal (~20% of the open queue), not noise.
UNIQUE_N = int(N_SUBS * 0.90)
REPEAT_TAIL = profile_pool[:1500]
for i in range(N_SUBS):
    prof = profile_pool[i % len(profile_pool)] if i < UNIQUE_N else random.choice(REPEAT_TAIL)
    trade, base_t = prof[8], prof[9]
    segment = "mid_market" if base_t >= 3_000_000 else "sme"
    channel = random.choices(CHANNELS, weights=CH_W, k=1)[0] if segment == "sme" else random.choices(["email", "portal"], weights=[75, 25], k=1)[0]
    broker = random.choice(BROKER_BY_SEGMENT[segment])
    district = random.choices(DISTRICT_POOL, weights=DISTRICT_W, k=1)[0]
    days_ago = random.randint(0, 364)
    received = datetime.datetime.combine(TODAY - datetime.timedelta(days=days_ago),
                                         datetime.time(random.randint(8, 17), random.randint(0, 59)))
    mult = base_t / 500_000
    tenant = trade in ("retail_shop", "office_professional", "hair_beauty") and random.random() < 0.6
    buildings = 0 if tenant else int(random.randint(200, 900) * 1000 * max(mult, 0.5))
    contents = int(random.randint(20, 90) * 1000 * max(mult, 0.4))
    stock = int(random.randint(10, 120) * 1000 * max(mult, 0.4)) if trade != "office_professional" else 0
    bi = int((contents + stock + buildings * 0.3) * random.uniform(0.8, 1.6))
    employees = max(2, int(base_t / 90_000))
    turnover_stated = int(base_t * random.uniform(0.8, 1.05))
    floor_area = max(80, int((buildings if buildings else contents * 4) / 1600))
    rg = RATE_BY_TRADE[trade]
    tech = max(rg[5], int(((buildings + contents + stock) * rg[1] + bi * rg[2]) / 1000
                          + employees * rg[3] + (turnover_stated / 1000) * rg[4]))
    target = int(tech * random.uniform(0.7, 1.05)) if channel == "email" and random.random() < 0.7 else None

    # lifecycle outcome
    in_appetite = STATUS_BY_TRADE[trade] != "excluded"
    is_open = days_ago <= 10
    if is_open:
        state = random.choices(["received", "extracted", "triaged", "enriched", "assessed", "referred", "quoted", "awaiting_broker"],
                               weights=[10, 10, 14, 14, 18, 8, 16, 10], k=1)[0]
        outcome, quoted_prem, quote_ts, decided_ts, decline_code = None, None, None, None, None
        if state in ("quoted", "awaiting_broker"):
            quoted_prem = int(tech * random.uniform(0.92, 1.12))
            quote_ts = received + datetime.timedelta(hours=random.randint(2, 96))
    else:
        state = "closed"
        if not in_appetite or random.random() > QUOTE_RATE[channel]:
            outcome = "declined" if (not in_appetite or random.random() < 0.6) else "withdrawn"
            decline_code = STATUS_BY_TRADE[trade] == "excluded" and {t[0]: t[4] for t in APPETITE}[trade] or ("APP-REF-RISK" if outcome == "declined" else None)
            quoted_prem, quote_ts = None, None
            decided_ts = received + datetime.timedelta(hours=random.randint(1, 120))
        else:
            quoted_prem = int(tech * random.uniform(0.92, 1.12))
            lag_h = {"etrade": (0, 1), "portal": (4, 72), "email": (24, 240)}[channel]
            quote_ts = received + datetime.timedelta(hours=random.randint(*lag_h))
            outcome = random.choices(["bound", "lost", "ntu", "quote_expired"], weights=[38, 32, 18, 12], k=1)[0]
            decided_ts = quote_ts + datetime.timedelta(hours=random.randint(6, 400))
            decline_code = None
    subs.append((f"sub:{100001 + i}", received.isoformat(), channel, broker, prof[0], prof[1], trade,
                 prof[3], segment, district, 1 if segment == "sme" else random.choice([1, 1, 2, 3]),
                 turnover_stated, employees, floor_area, random.choice(CONSTRUCTIONS), random.randint(1955, 2018),
                 buildings, 0, contents, stock, bi, 12, 10_000_000,
                 random.choice([2, 5]) * 1_000_000, 0, 0, random.choice([2_500, 5_000, 10_000]),
                 target, random.choice(INCUMBENTS), None,
                 state, outcome, quoted_prem,
                 quote_ts.isoformat() if quote_ts else None,
                 decided_ts.isoformat() if decided_ts else None, decline_code))

# ---- SACRED HEROES ----------------------------------------------------------------------
# Heroes arrive "this morning" RELATIVE TO NOW (SLA clocks read fresh at any demo hour)
_NOW = datetime.datetime.now()
H1_RECEIVED = _NOW - datetime.timedelta(minutes=25)   # e-trade — minutes old
H2_RECEIVED = _NOW - datetime.timedelta(hours=2)      # email mid-market
H3_RECEIVED = _NOW - datetime.timedelta(hours=3)
HEROES = [
    # sub:900001 — Fenwick & Moss Homewares (clean fast-track SME shop package, e-trade)
    ("sub:900001", H1_RECEIVED.isoformat(), "etrade", "BRK-003", "09384712", "Fenwick & Moss Homewares Ltd",
     "retail_shop", "47190", "sme", "WA14", 1,
     720_000, 5, 210, "brick_traditional", 1987,
     0, 0, 45_000, 75_000, 180_000, 12, 10_000_000, 2_000_000, 0, 0, 2_500,
     None, "NIG", None, "received", None, None, None, None, None),
    # sub:900002 — Calder Valley Fine Foods (mid-market Commercial Combined referral)
    ("sub:900002", H2_RECEIVED.isoformat(), "email", "BRK-004", "06120843", "Calder Valley Fine Foods Ltd",
     "food_manufacturing", "10890", "mid_market", "HX7", 6,
     8_000_000, 120, 14_500, "composite_panel_clad", 1979,
     14_000_000, 6_000_000, 0, 3_500_000, 10_000_000, 24, 10_000_000, 5_000_000, 250_000, 500_000, 0,
     140_000, "RSA", None, "received", None, None, None, None, None),
    # sub:900003 — Midland Metal & Waste Recycling (excluded trade + watchlist decline)
    ("sub:900003", H3_RECEIVED.isoformat(), "email", "BRK-005", "07551209", "Midland Metal & Waste Recycling Ltd",
     "waste_recycling", "38320", "mid_market", "WS2", 1,
     3_100_000, 28, 6_200, "steel_frame_clad", 1996,
     3_200_000, 800_000, 150_000, 250_000, 900_000, 12, 10_000_000, 5_000_000, 0, 0, 0,
     32_000, "Self-insured", None, "received", None, None, None, None, None),
]
subs.extend(HEROES)

# ---- Malformed rows (the DQ/quarantine story; all historical/closed so the open queue stays clean)
random.seed(SEED + 6)
for j in range(200):
    prof = random.choice(profile_pool)
    trade = prof[8]
    days_ago = random.randint(11, 364)
    received = datetime.datetime.combine(TODAY - datetime.timedelta(days=days_ago), datetime.time(11, 0))
    kind = "el_low" if j < 80 else ("bad_channel" if j < 150 else "no_turnover")
    subs.append((f"sub:{108001 + j}", received.isoformat(),
                 "fax" if kind == "bad_channel" else "email",
                 random.choice(["BRK-003", "BRK-005"]), prof[0], prof[1], trade, prof[3], "sme",
                 random.choices(DISTRICT_POOL, weights=DISTRICT_W, k=1)[0], 1,
                 None if kind == "no_turnover" else 400_000, 6, 300, "brick_traditional", 1990,
                 0, 0, 60_000, 40_000, 90_000, 12,
                 1_000_000 if kind == "el_low" else 10_000_000,   # below the £5m statutory EL minimum
                 2_000_000, 0, 0, 2_500, None, None, None,
                 "closed", "withdrawn", None, None,
                 (received + datetime.timedelta(hours=48)).isoformat(), None))

sub_schema = ("submission_public_id string, received_ts string, channel string, broker_id string, "
              "company_number string, company_name string, trade_group string, sic_code_declared string, "
              "segment string, postcode_district string, n_locations int, "
              "turnover_stated long, employees int, floor_area_m2 int, construction_type string, year_built int, "
              "buildings_si long, plant_si long, contents_si long, stock_si long, bi_si long, bi_indemnity_months int, "
              "el_limit long, pl_limit long, git_si long, deterioration_of_stock_si long, money_si long, "
              "target_premium long, incumbent_insurer string, notes string, "
              "lifecycle_state string, outcome string, quoted_premium long, quote_ts string, decided_ts string, decline_code string")
write(spark.createDataFrame(subs, sub_schema), "landing_submissions_feed", "landing")

# COMMAND ----------

# MAGIC %md ## Summary asserts
# MAGIC Hero 900002's six locations arrive through the REAL file path (00b schedule v2 →
# MAGIC bronze pipeline); the two Calder Valley sites carry exactly £5m property SI in HX7 →
# MAGIC in-force 16.75m + 5m = 21.75m = 87% of the £25m capacity.

# COMMAND ----------

# Sanity: heroes present, HX7 seeded exactly
hx7 = spark.sql(f"SELECT sum(buildings_si+contents_si+stock_si) s FROM {fqn}.landing_pas_policies "
                f"WHERE postcode_district='HX7' AND policy_status='in_force'").first().s
assert hx7 == 16_750_000, f"HX7 in-force property SI must be £16.75m, got {hx7}"
for sid in ("sub:900001", "sub:900002", "sub:900003"):
    assert spark.sql(f"SELECT count(*) c FROM {fqn}.landing_submissions_feed WHERE submission_public_id='{sid}'").first().c == 1
print(f"✅ 00 complete — HX7 in-force £{hx7:,}; heroes seeded; as_at {TODAY}")
