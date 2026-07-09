# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Landing files — the messy inbound reality
# MAGIC
# MAGIC Writes the genuinely-hard inbound formats into the `submission_inbox` Volume:
# MAGIC broker **emails** (.txt), scanned-look **PDF proposal forms + loss runs** (fpdf2),
# MAGIC and **risk schedules** (SOV) — including hero 900002's drifted-columns v1 (quarantined
# MAGIC by the bronze pipeline) and the corrected v2 the broker re-sent.
# MAGIC
# MAGIC File naming contract: `sub-<id>_<doctype>.<ext>` — extraction joins documents back to
# MAGIC submissions by the `sub-(\d+)` reference in the filename (claims_workbench pattern).

# COMMAND ----------

# MAGIC %pip install fpdf2 --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
INBOX = f"/Volumes/{catalog}/{schema}/submission_inbox"

import datetime
TODAY = datetime.date.today()


def _dt(days_ago, fmt="%d %B %Y"):
    return (TODAY - datetime.timedelta(days=days_ago)).strftime(fmt)


def write_text(fname, body):
    with open(f"{INBOX}/{fname}", "w", encoding="utf-8") as f:
        f.write(body)
    print(f"  {fname} ({len(body)} chars)")

# COMMAND ----------

# MAGIC %md ## Broker emails

# COMMAND ----------

EMAIL_900002 = f"""From: David Whitworth <submissions@penninecommercial.example>
To: newbusiness@bricksurance.example
Date: {TODAY.strftime('%a, %d %b %Y')} 09:12
Subject: New business - Calder Valley Fine Foods Ltd - Commercial Combined - renewal {_dt(-53)}

Hi team,

Please find attached proposal form, loss runs and schedule of premises for Calder Valley
Fine Foods Ltd (Companies House 06120843), a family-owned chilled and ambient food
manufacturer trading since 2006. Currently with RSA, holding rate has moved against them
and the client is open to remarketing. Renewal date {_dt(-53)} so we would appreciate
terms within the fortnight.

Headlines:
 - Food manufacturing (ready meals, chilled sauces, ambient preserves), turnover approx GBP 8m
 - Six premises across the North: two production sites (Hebden Bridge and Mytholmroyd),
   three distribution units (Huddersfield area, Preston, Walsall) and the Halifax head office
 - Buildings GBP 14,000,000 / Plant & machinery GBP 6,000,000 / Stock GBP 3,500,000
 - Business interruption GBP 10,000,000 gross profit on a 24 month indemnity period
 - EL GBP 10m / Products & Public Liability GBP 5m / Goods in transit GBP 250k
 - Deterioration of stock GBP 500,000 (chilled/frozen)
 - 120 employees across the sites

The client is budgeting around GBP 140,000 plus IPT. Please note the schedule attached is
exported from our system - let me know if any issues reading it.

Three-year claims experience attached - the 2020 storm season flood claim at Mytholmroyd
was fully reinstated with resilience improvements (pumps, stock racking raised 400mm).

Kind regards,
David Whitworth
Pennine Commercial Risks (PCR-402)
"""

EMAIL_900003 = f"""From: Fiona Slate <submissions@harboroughslate.example>
To: newbusiness@bricksurance.example
Date: {TODAY.strftime('%a, %d %b %Y')} 10:03
Subject: Midland Metal & Waste Recycling Ltd - Commercial Combined enquiry

Good morning,

We have been approached by Midland Metal & Waste Recycling Ltd (Companies House 07551209),
a waste transfer station and metal recycling operation on Bloxwich Lane, Walsall WS2.
They are currently self-insuring the property account after their previous insurer
non-renewed, and are looking for a full programme:

 - Buildings and fixed plant GBP 3,200,000, mobile plant GBP 800,000
 - Stock (sorted metals) GBP 150,000, business interruption GBP 900,000
 - EL GBP 10m, PL GBP 5m
 - 28 employees, two shifts, EA permitted site (waste transfer + ELV)

Directors are Derek Ashworth and Karen Tolley. Budget indication around GBP 32,000.

I appreciate waste risks are not everyone's cup of tea - grateful for a quick steer either
way so we can manage the client's expectations.

Best regards,
Fiona Slate
Harborough & Slate (HSL-518)
"""

EMAIL_900001 = f"""From: no-reply@northgate-etrade.example
To: etrade-gateway@bricksurance.example
Date: {TODAY.strftime('%a, %d %b %Y')} 08:47
Subject: [E-TRADE] Shop package submission NGB-305/{TODAY.strftime('%y')}-4471 - Fenwick & Moss Homewares Ltd

Structured e-trade submission received via Northgate Insurance Brokers portal.
Reference: sub-900001. Payload delivered to the broker-portal feed (JSON).
Trade: Homeware retail (tenant) | Altrincham WA14
Contents GBP 45,000 | Stock GBP 75,000 | BI (GP, 12 months) GBP 180,000 | Money GBP 2,500
EL GBP 10m | PL GBP 2m | 5 employees | 3 years claim-free (NIG)
This message is a delivery receipt; the structured payload is the system of record.
"""

CHASER_900002 = f"""From: David Whitworth <submissions@penninecommercial.example>
To: newbusiness@bricksurance.example
Date: {TODAY.strftime('%a, %d %b %Y')} 15:40
Subject: RE: Calder Valley Fine Foods - corrected schedule attached

Team - our apologies, our BMS exported the premises schedule with our internal column
headers which I gather your system rejected. Corrected version attached using your
standard template. Same six locations, no changes to values.

David
"""

# A barely-legible fax scan (OCR garble) → extraction confidence < 0.6 → quarantined for
# human review. sub:108150 is one of the fax-channel malformed feed rows, so the story lines up.
FAX_108150 = """[OCR OUTPUT - QUALITY: POOR - SOURCE: FAX 0121-XXX-XXXX]
C0MMERC1AL 1NSURANCE PR0P0SAL ... [illegible] ... Ltd
tr@de: gener@l st0r@ge &. d1str1buti0n ?? s um 1nsured GBP 2#0,000 [smudged]
bu1ld1ngs: [illegible] c0ntents: 6O,OOO st0ck: 4O#OOO
EL: [torn] PL: 2,OOO,OOO empl0yees: s1x
[remainder of page illegible]
"""
write_text("sub-108150_fax_scan.txt", FAX_108150)

write_text("sub-900001_etrade_receipt.txt", EMAIL_900001)

# COMMAND ----------

# MAGIC %md ## Call transcripts — material facts said on calls that never reach a form
# MAGIC The extensibility proof: a NEW SOURCE is a folder drop + one extraction prompt. The
# MAGIC transcripts land in the SAME Volume, flow through the SAME Auto Loader + Document AI
# MAGIC pipeline, and the governance/quarantine/audit come for free.
# MAGIC Hero beat: Calder Valley's FD mentions a SECOND PRODUCTION SHIFT and "about twenty-four
# MAGIC million now" — material facts on no form, cross-checkable against the proposal.

# COMMAND ----------

CALL_900002_FD = f"""CALL TRANSCRIPT — {TODAY.strftime('%d %b %Y')} 11:20 (14 min)
Parties: Marcus Webb (Bricksurance underwriting) · Margaret Calder (FD, Calder Valley Fine Foods)
Channel: recorded line, consent given. Auto-transcribed.

MW: Thanks for the time. I'm working through the proposal your broker sent across.
MC: Of course. We're keen to move — RSA's renewal terms were a shock.
MW: Understood. Can I confirm the operation — two production sites in the valley?
MC: Yes, Hebden Bridge is the main mill, Mytholmroyd does the chilled lines. Since we took on
    the SECOND PRODUCTION SHIFT last spring both sites run sixteen hours a day now.
MW: That's useful — the proposal shows 120 employees, is that current?
MC: That was last year's headcount. With the second shift we're nearer a hundred and sixty.
MW: And turnover — the form says eight million?
MC: Oh, that must be old. We're doing about TWENTY-FOUR MILLION now — the supermarket
    own-label contract changed everything.
MW: That's a material difference — we'll need audited figures.
MC: Whatever you need. Also worth saying: after the 2020 flood we put in the pumps and raised
    all the racking. The Environment Agency alert system pages our site manager directly.
MW: Good to know. Last thing — the fryer line suppression, is that installed?
MC: The new Ansul system went into fryer line B in January. Line A is gravity-fed, no frying.
MW: Perfect, that answers one of our subjectivities. I'll come back with terms this week.
"""

CALL_900002_BROKER_CHASE = f"""CALL TRANSCRIPT — {TODAY.strftime('%d %b %Y')} 15:05 (3 min)
Parties: David Whitworth (Pennine Commercial Risks) · Bricksurance new business desk
Channel: recorded line. Auto-transcribed.

DW: Just chasing the Calder Valley submission — the client's renewal is close and I'll be
    honest, whoever quotes first with sensible terms probably takes it.
BX: It's with a senior underwriter today — flood terms and the turnover point to resolve.
DW: Understood. If the flood excess is the sticking point they'd wear a hundred thousand,
    between us. Speed matters more than shaving the premium here.
BX: Noted on the file. You'll have terms before Friday.
"""

CALL_100774_STOCK = f"""CALL TRANSCRIPT — {TODAY.strftime('%d %b %Y')} 09:48 (6 min)
Parties: Bricksurance trading desk · caller states they are the owner, Fenland Wholesale
Channel: recorded line. Auto-transcribed.

CALLER: You have our renewal in — I wanted to flag the stock figure is wrong on the schedule.
DESK: Go on.
CALLER: It says one-eighty. Pre-Christmas we peak well over three hundred thousand in the
    warehouse, October through December. Rest of the year one-eighty is fair.
DESK: So a seasonal stock increase clause around 65 percent uplift for the last quarter.
CALLER: That's exactly it. Our old insurer had it worded that way.
DESK: We'll reflect that in the quote — thanks for calling it in before we bound it.
"""

CALL_ETRADE_SUPPORT = f"""CALL TRANSCRIPT — {TODAY.strftime('%d %b %Y')} 10:31 (2 min)
Parties: Northgate Insurance Brokers service line · Bricksurance e-trade support
Channel: recorded line. Auto-transcribed.

NB: Quick one — the portal rejected a shop package because the money limit field wouldn't
    take more than five thousand. Client wants seven-five.
BX: Above the e-trade envelope — it'll route to an underwriter automatically if you submit
    with a note. Takes it out of straight-through but it's a same-day turnaround.
NB: That works. Sending it now.
"""

write_text("sub-900002_call_fd.txt", CALL_900002_FD)
write_text("sub-900002_call_broker_chase.txt", CALL_900002_BROKER_CHASE)
write_text("sub-100774_call_stock.txt", CALL_100774_STOCK)
write_text("call_etrade_support_20260708.txt", CALL_ETRADE_SUPPORT)
write_text("sub-900002_email.txt", EMAIL_900002)
write_text("sub-900002_email_chaser.txt", CHASER_900002)
write_text("sub-900003_email.txt", EMAIL_900003)

# COMMAND ----------

# MAGIC %md ## Risk schedules — drifted v1 (quarantine beat) + corrected v2

# COMMAND ----------

# The bronze pipeline expects the standard broker template columns. v1 arrives with the
# broker's internal BMS headers → schema drift → rescued → quarantined. v2 is clean.
SCHEDULE_HEADER_STD = "submission_ref,loc_no,site_name,postcode_district,site_type,construction_type,year_built,floor_area_m2,buildings_si,plant_si,stock_si"
SCHEDULE_HEADER_DRIFT = "SubRef,Loc#,Premises Description,PostDist,Usage,Constr,YrBlt,Area SqM,Bldg Sum Insured GBP,Plant SI GBP,Stock SI GBP"

# SACRED: HX7 property SI (buildings+plant+stock) sums to EXACTLY £5,000,000 →
# in-force £16.75m + £5m = £21.75m = 87% of the £25m HX7 capacity. Totals stay
# £14m buildings / £6m plant / £3.5m stock (the declared sums).
H2_LOCS = [
    ("sub:900002", 1, "Hebden Bridge Mill - production", "HX7", "manufacturing", "composite_panel_clad", 1979, 5200, 2300000, 850000, 250000),
    ("sub:900002", 2, "Mytholmroyd Works - production & chill", "HX7", "manufacturing", "composite_panel_clad", 1984, 3100, 1000000, 500000, 100000),
    ("sub:900002", 3, "Elland Road Depot - distribution", "HD1", "warehouse", "steel_frame_clad", 2001, 2800, 3900000, 1600000, 1300000),
    ("sub:900002", 4, "Preston Cold Store - distribution", "PR1", "warehouse", "steel_frame_clad", 2008, 1900, 3200000, 1500000, 1200000),
    ("sub:900002", 5, "Walsall Crossdock - distribution", "WS2", "warehouse", "steel_frame_clad", 1999, 1100, 2900000, 1350000, 650000),
    ("sub:900002", 6, "Halifax HQ - office", "HX6", "office", "brick_traditional", 1965, 400, 700000, 200000, 0),
]
assert sum(r[8] + r[9] + r[10] for r in H2_LOCS if r[3] == "HX7") == 5_000_000, "HX7 marginal must be exactly £5m"
assert sum(r[8] for r in H2_LOCS) == 14_000_000 and sum(r[9] for r in H2_LOCS) == 6_000_000 and sum(r[10] for r in H2_LOCS) == 3_500_000

v1 = SCHEDULE_HEADER_DRIFT + "\n" + "\n".join(",".join(str(x) for x in r) for r in H2_LOCS)
v2 = SCHEDULE_HEADER_STD + "\n" + "\n".join(",".join(str(x) for x in r) for r in H2_LOCS)
write_text("sub-900002_schedule_v1.csv", v1)
write_text("sub-900002_schedule_v2.csv", v2)

# COMMAND ----------

# MAGIC %md ## Scanned-look PDFs — proposal form + loss run (fpdf2)

# COMMAND ----------

from fpdf import FPDF


class Doc(FPDF):
    def header(self):
        self.set_font("Courier", "B", 9)
        self.set_text_color(90, 90, 90)
        self.cell(0, 5, "SCANNED DOCUMENT - PENNINE COMMERCIAL RISKS BMS EXPORT", align="C")
        self.ln(8)


def pdf_block(pdf, title, lines):
    pdf.set_font("Courier", "B", 10)
    pdf.set_text_color(20, 20, 20)
    pdf.cell(0, 6, title)
    pdf.ln(7)
    pdf.set_font("Courier", "", 9)
    for ln in lines:
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(pdf.w - pdf.l_margin - pdf.r_margin, 4.6, ln)
    pdf.ln(3)


def save_pdf(pdf, fname):
    data = bytes(pdf.output())
    with open(f"{INBOX}/{fname}", "wb") as f:
        f.write(data)
    print(f"  {fname} ({len(data)} bytes)")


# --- Hero 900002 proposal form -----------------------------------------------------------
pdf = Doc()
pdf.add_page()
pdf_block(pdf, "COMMERCIAL COMBINED PROPOSAL FORM (page 1 of 2)", [
    "Proposer:            Calder Valley Fine Foods Ltd",
    "Company number:      06120843     Est: 2006     Legal form: Private limited",
    "Business:            Manufacture of chilled ready meals, cooking sauces and ambient",
    "                     preserves. Production, chilled storage and distribution.",
    "Annual turnover:     GBP 8,000,000 (estimate provided by proposer)",
    "Employees:           120 (production 84 / warehouse 24 / office 12)",
    "Premises:            6 locations per attached schedule of premises",
    "Principal site:      Hebden Bridge Mill, Valley Road, Hebden Bridge HX7",
])
pdf_block(pdf, "MATERIAL FACTS - CONSTRUCTION & PROCESSES", [
    "Composite panels:    Production areas are clad in insulated composite panels.",
    "                     Percentage of panel by core type NOT CONFIRMED by proposer.",
    "                     Panel survey requested by broker - awaiting date.",
    "Cooking processes:   Batch cooking with two thermal oil fryers (frying line B),",
    "                     steam kettles and retort. Extraction ductwork cleaned annually.",
    "                     Fixed suppression on fryer line: NOT CONFIRMED.",
    "Refrigeration:       Two ammonia plant rooms (Hebden Bridge, Mytholmroyd).",
    "Waste:               Compactor within 10m of panel wall at Mytholmroyd.",
])
pdf_block(pdf, "FLOOD & LOCATION", [
    "The proposer confirms the Hebden Bridge and Mytholmroyd sites lie within the",
    "River Calder valley. Flood resilience works completed 2021: pump chamber,",
    "stock racking raised 400mm, penstock valve on yard drainage.",
])
pdf.add_page()
pdf_block(pdf, "COMMERCIAL COMBINED PROPOSAL FORM (page 2 of 2) - SUMS PROPOSED", [
    "Buildings:                        GBP 14,000,000",
    "Plant & machinery:                GBP  6,000,000",
    "Stock (incl chilled/frozen):      GBP  3,500,000",
    "Business interruption (GP):       GBP 10,000,000   Indemnity period: 24 months",
    "Deterioration of stock:           GBP    500,000",
    "Goods in transit:                 GBP    250,000",
    "Employers liability:              GBP 10,000,000",
    "Products / public liability:      GBP  5,000,000",
])
pdf_block(pdf, "DECLARATION", [
    "I/we declare that the statements and particulars are true and complete and that",
    "no material facts have been withheld. Signed: M Calder (Director), " + _dt(2),
])
save_pdf(pdf, "sub-900002_proposal_form.pdf")

# --- Hero 900002 loss run ----------------------------------------------------------------
pdf = Doc()
pdf.add_page()
pdf_block(pdf, "CONFIRMED CLAIMS EXPERIENCE - CALDER VALLEY FINE FOODS LTD (3 YEARS + PRIOR)", [
    "Insurer: RSA   Broker: Pennine Commercial Risks   Produced: " + _dt(3),
    "-" * 78,
    f"{_dt(160,'%d/%m/%Y')}  ESCAPE OF WATER   Mytholmroyd Works       PAID GBP  18,400  SETTLED",
    f"{_dt(300,'%d/%m/%Y')}  GOODS IN TRANSIT  M62 J24 incident        PAID GBP   7,900  SETTLED",
    f"{_dt(540,'%d/%m/%Y')}  THEFT             Walsall Crossdock       PAID GBP  12,650  SETTLED",
    f"{_dt(760,'%d/%m/%Y')}  MACHINERY (BI)    Fryer line B outage     PAID GBP  41,200  SETTLED",
    f"{_dt(980,'%d/%m/%Y')}  MINOR INJURY (EL) Production op - laceration PAID GBP 9,300 SETTLED",
    "-" * 78,
    "PRIOR MATERIAL LOSS (outside 3-year window, disclosed):",
    "Feb 2020    FLOOD    Mytholmroyd Works    PAID GBP 486,000    SETTLED",
    "Storm Ciara/Dennis river flooding; full reinstatement with resilience improvements.",
    "-" * 78,
    "Total paid (3 years): GBP 89,450 across 5 claims. No open claims.",
])
save_pdf(pdf, "sub-900002_loss_run.pdf")

# --- Hero 900003 broker presentation -----------------------------------------------------
pdf = Doc()
pdf.add_page()
pdf_block(pdf, "RISK PRESENTATION - MIDLAND METAL & WASTE RECYCLING LTD", [
    "Broker: Harborough & Slate   Produced: " + _dt(1),
    "Site: Bloxwich Lane, Walsall WS2 - waste transfer station + ELV metal recycling",
    "EA permit: WML-XXXX (waste transfer, end-of-life vehicles). 28 employees, 2 shifts.",
    "Buildings & fixed plant GBP 3,200,000 / mobile plant GBP 800,000 / stock GBP 150,000",
    "BI GBP 900,000 (12 months). EL GBP 10m. PL GBP 5m.",
    "Fire protection: hose reels, 2x water cannon over stock bays, quarterly hot-spot",
    "thermal imaging of pile. No sprinklers. Previous insurer non-renewed 2024.",
    "Directors: Derek Ashworth, Karen Tolley.",
])
save_pdf(pdf, "sub-900003_risk_presentation.pdf")

# COMMAND ----------

# MAGIC %md ## The loss-run gauntlet — five carriers, five formats, one canonical table
# MAGIC "Prove your OCR can parse 5-year bordereaux with varying deductibles from five different
# MAGIC carriers." Challenge accepted: RSA narrative PDF (above), Aviva CSV, Zurich pipe-delimited
# MAGIC with a total row, a broker Excel-export with junk header rows and £-formatted amounts,
# MAGIC and one ambiguous scan that MUST quarantine rather than guess.

# COMMAND ----------

LR_AVIVA = f"""claim_ref,date_of_loss,cause,paid_gbp,outstanding_gbp,policy_deductible_gbp,period
AV-2021-88231,{_dt(1700,'%Y-%m-%d')},Escape of water,14250,0,500,2021-2022
AV-2022-90112,{_dt(1350,'%Y-%m-%d')},Storm damage,6800,0,500,2022-2023
AV-2023-91455,{_dt(980,'%Y-%m-%d')},Theft of stock,11900,0,1000,2023-2024
AV-2024-93872,{_dt(600,'%Y-%m-%d')},Impact - vehicle,4300,0,1000,2024-2025
AV-2025-95210,{_dt(240,'%Y-%m-%d')},Employers liability,0,18500,0,2025-2026
"""
write_text("sub-100200_lossrun_aviva.csv", LR_AVIVA)

LR_ZURICH = f"""CLAIMS EXPERIENCE STATEMENT | ZURICH COMMERCIAL | 5 YEAR SUMMARY
ClaimNo|LossDate|Peril|GrossIncurred|ExcessCarried|Status
ZC/19/44821|{_dt(1800,'%d.%m.%Y')}|Fire - electrical|32400|2500|CLOSED
ZC/21/50233|{_dt(1200,'%d.%m.%Y')}|Water damage|8750|1000|CLOSED
ZC/23/55190|{_dt(700,'%d.%m.%Y')}|Public liability - slip|12600|0|CLOSED
ZC/25/58834|{_dt(150,'%d.%m.%Y')}|Storm|3100|1000|OPEN
TOTAL|||56850||
"""
write_text("sub-100300_lossrun_zurich.txt", LR_ZURICH)

LR_BROKER = f"""Prepared by Harborough & Slate for remarketing,,,,
Client claims history - all carriers - 5 years,,,,
,,,,
Year,Insurer,Type of Loss,"Amount Paid","Excess"
{TODAY.year-4},NIG,"Burst pipe - stock damage","£9,450","£500"
{TODAY.year-3},NIG,"Break-in / theft","£15,200","£1,000"
{TODAY.year-2},Aviva,"Fork lift impact","£7,850","£1,000"
{TODAY.year-1},Aviva,"Slip injury (PL)","£22,300","£0"
{TODAY.year},Aviva,"Minor water leak","£1,900","£500"
"""
write_text("sub-100400_lossrun_broker_export.csv", LR_BROKER)

# The ambiguous one — overlapping periods, unclear currency, illegible rows → must quarantine
LR_AMBIG = """[SCAN QUALITY: POOR - RE-PHOTOGRAPHED FAX]
CLAIMS SUMMARY 2019-2024 (?) / policy years overlap 2021-2023 shown twice
loss 1: warehouse f1re .... 45,ooo (curr?) .... paid?
loss 2: theft [illegible] 12k or 21k
loss 3: liab / see attached (no attachment)
NB figures may be EUR - cedant operates cross-border
"""
write_text("sub-100500_lossrun_scan.txt", LR_AMBIG)

# COMMAND ----------

import os
files = sorted(os.listdir(INBOX))
print(f"submission_inbox: {len(files)} files")
for f in files:
    print(" -", f)
assert {"sub-900002_email.txt", "sub-900002_schedule_v1.csv", "sub-900002_schedule_v2.csv",
        "sub-900002_proposal_form.pdf", "sub-900002_loss_run.pdf", "sub-900003_email.txt",
        "sub-900003_risk_presentation.pdf", "sub-900001_etrade_receipt.txt"} <= set(files)
print("✅ 00b complete")
