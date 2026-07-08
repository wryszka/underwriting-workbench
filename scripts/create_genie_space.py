#!/usr/bin/env python3
"""Create the 'Underwriting — Ask the Book' Genie space over the gold marts. Reproducible.
Usage: python3 scripts/create_genie_space.py [profile] [warehouse_id] [catalog] [schema]
Prints the space_id on success. Uses the genie-rooms skill's GenieSpaceBuilder."""
import json
import pathlib
import subprocess
import sys

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
wh = sys.argv[2] if len(sys.argv) > 2 else "a3b61648ea4809e3"
cat = sys.argv[3] if len(sys.argv) > 3 else "lr_dev_aws_us_catalog"
sch = sys.argv[4] if len(sys.argv) > 4 else "underwriting_workbench"

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
    "exclude IPT (12%) unless stated; report sums insured in millions."
)
for t in ["gold_pipeline_funnel", "gold_accumulation", "gold_broker_scorecard",
          "gold_rate_adequacy", "gold_renewals", "gold_submission_lifecycle"]:
    space.add_table(f"{fqn}.{t}")
space.validate()

payload = {
    "title": TITLE,
    "description": "Commercial underwriting analytics: funnel, accumulation, brokers, adequacy, renewals.",
    "parent_path": "/Workspace/Users/laurence.ryszka@databricks.com",
    "warehouse_id": wh,
    "serialized_space": space.to_json(),
}
open("/tmp/create_genie_space_uw.json", "w").write(json.dumps(payload))
out = subprocess.run(["databricks", "api", "post", "/api/2.0/genie/spaces", "--profile", prof,
                      "--json", "@/tmp/create_genie_space_uw.json"], capture_output=True, text=True)
print(out.stdout[:800] or out.stderr[:800])
try:
    print("SPACE_ID:", json.loads(out.stdout)["space_id"])
except Exception:  # noqa: BLE001
    pass
