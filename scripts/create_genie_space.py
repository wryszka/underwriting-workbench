#!/usr/bin/env python3
"""Create (or additively UPDATE) the 'Underwriting — Ask the Book' Genie space over the gold marts.
Reproducible. Uses the genie-rooms skill's GenieSpaceBuilder.

Usage:
  python3 scripts/create_genie_space.py [profile] [warehouse_id] [catalog] [schema] [existing_space_id]

If existing_space_id is given, the space is PATCHed IN PLACE (same id the app embeds) with the full
desired asset list — this is how Lane E adds the discipline metric view + its facts without minting a
new id. Prints the space_id on success."""
import json
import pathlib
import subprocess
import sys

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
wh = sys.argv[2] if len(sys.argv) > 2 else "a3b61648ea4809e3"
cat = sys.argv[3] if len(sys.argv) > 3 else "lr_dev_aws_us_catalog"
sch = sys.argv[4] if len(sys.argv) > 4 else "underwriting_workbench"
existing_space_id = sys.argv[5] if len(sys.argv) > 5 else None

BUILDER = pathlib.Path.home() / ".vibe/marketplace/plugins/fe-internal-tools/skills/genie-rooms/resources"
sys.path.insert(0, str(BUILDER))
from genie_space_builder import GenieSpaceBuilder  # noqa: E402

fqn = f"{cat}.{sch}"
TITLE = "Underwriting — Ask the Book (Bricksurance SE)"
space = GenieSpaceBuilder(
    title=TITLE,
    description=("Natural-language analytics over the commercial underwriting book: submission funnel by "
                 "channel, accumulation vs district capacity, broker performance, rate adequacy, renewals "
                 "and the live pipeline."),
    warehouse_id=wh,
)
space.set_instructions(
    "You answer questions about a UK commercial insurer's underwriting book. gold_pipeline_funnel has the "
    "submission funnel by month and channel (received/quoted/bound/declined/ntu/lost — NTU means not taken up "
    "and is distinct from lost-to-competitor; avg_hours_to_quote measures speed). gold_accumulation has "
    "property accumulation per postcode district vs capacity (utilisation_pct, rag; >=80 is referral "
    "territory). gold_broker_scorecard has per-broker quote rate, hit ratio, speed and data quality. "
    "gold_rate_adequacy has quoted vs technical premium by trade. gold_renewals has retention and rate "
    "change. gold_submission_lifecycle is the live open pipeline with SLA status. Money is GBP; premiums "
    "exclude IPT (12%) unless stated; report sums insured in millions.\n\n"
    # --- Lane E: referral & pricing-discretion vocabulary (additive block) ---
    "REFERRAL & PRICING DISCRETION: mv_underwriting_discipline is a METRIC VIEW — always aggregate its "
    "measures with MEASURE(), e.g. SELECT transaction_type, MEASURE(discretion_ratio) FROM "
    "mv_underwriting_discipline WHERE rule_id='MAX_WAGEROLL' GROUP BY transaction_type. A 'referral' is a "
    "rule firing on a transaction; rule_id names the rule (MAX_WAGEROLL is the maximum-wageroll referral — "
    "wageroll is the Employers' Liability payroll rating basis). transaction_type is NEW_BUSINESS / RENEWAL "
    "/ MTA. 'Discretion ratio' = charged ÷ technical premium (1.0 = priced to technical, below 1.0 = "
    "discount given away). 'Give-away' = technical minus charged, in points of technical. 'Leakage' is "
    "give-away concentrated on renewals. persona labels underwriters disciplined/median/generous. "
    "referral_rate is fires ÷ transactions. Use gold_referral_events for rule-fire detail and "
    "gold_transactions for the transaction facts; the metric view is the preferred single source."
)
# Existing marts + Lane E facts + the metric view. Kept sorted by identifier (builder invariant).
TABLES = ["gold_accumulation", "gold_broker_scorecard", "gold_pipeline_funnel",
          "gold_rate_adequacy", "gold_referral_events", "gold_renewals",
          "gold_submission_lifecycle", "gold_transactions", "mv_underwriting_discipline"]
for t in sorted(TABLES):
    space.add_table(f"{fqn}.{t}")
space.validate()

DESC = ("Commercial underwriting analytics: funnel, accumulation, brokers, adequacy, renewals, and "
        "referral effectiveness & pricing discretion (Max Wageroll referrals, discretion ratio, leakage).")

if existing_space_id:
    # Additive UPDATE in place — keeps the space_id the app embeds. Fetch etag first.
    cur = subprocess.run(["databricks", "api", "get", f"/api/2.0/genie/spaces/{existing_space_id}",
                          "--profile", prof], capture_output=True, text=True)
    etag = json.loads(cur.stdout).get("etag") if cur.stdout else None
    payload = {"title": TITLE, "description": DESC, "warehouse_id": wh,
               "serialized_space": space.to_json()}
    if etag:
        payload["etag"] = etag
    open("/tmp/update_genie_space_uw.json", "w").write(json.dumps(payload))
    out = subprocess.run(["databricks", "api", "patch", f"/api/2.0/genie/spaces/{existing_space_id}",
                          "--profile", prof, "--json", "@/tmp/update_genie_space_uw.json"],
                         capture_output=True, text=True)
    print(out.stdout[:800] or out.stderr[:800])
    print("UPDATED SPACE_ID:", existing_space_id)
else:
    payload = {
        "title": TITLE, "description": DESC,
        "parent_path": "/Workspace/Users/laurence.ryszka@databricks.com",
        "warehouse_id": wh, "serialized_space": space.to_json(),
    }
    open("/tmp/create_genie_space_uw.json", "w").write(json.dumps(payload))
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/genie/spaces", "--profile", prof,
                          "--json", "@/tmp/create_genie_space_uw.json"], capture_output=True, text=True)
    print(out.stdout[:800] or out.stderr[:800])
    try:
        print("SPACE_ID:", json.loads(out.stdout)["space_id"])
    except Exception:  # noqa: BLE001
        pass
