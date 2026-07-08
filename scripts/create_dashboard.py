#!/usr/bin/env python3
"""Create/update + publish the 'Underwriting Portfolio' Lakeview dashboard (embedded in the app).

Usage: python3 scripts/create_dashboard.py [profile] [warehouse_id] [catalog] [schema] [dashboard_id]
- builds the JSON from the template below with the catalog/schema substituted
- creates (or updates when dashboard_id given) via the SDK — create validates the spec
- publishes with embed_credentials=True so the app iframe renders it
Prints DASHBOARD_ID at the end. Also writes dashboards/underwriting_board.lvdash.json for the repo.
GOTCHA honoured: symbol-map widgets are spec version 2; charts are version 3.
"""
import json
import pathlib
import sys

from databricks.sdk import WorkspaceClient

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
wh = sys.argv[2] if len(sys.argv) > 2 else "a3b61648ea4809e3"
cat = sys.argv[3] if len(sys.argv) > 3 else "lr_dev_aws_us_catalog"
sch = sys.argv[4] if len(sys.argv) > 4 else "underwriting_workbench"
existing_id = sys.argv[5] if len(sys.argv) > 5 else None
F = f"{cat}.{sch}"

DATASETS = [
    {"name": "ds_portfolio", "displayName": "Portfolio by trade",
     "queryLines": [f"SELECT trade_group, gwp, plan_gwp, loss_ratio_3y_pct, avg_rate_change_pct FROM {F}.gold_portfolio_position ORDER BY gwp DESC"]},
    {"name": "ds_funnel", "displayName": "Funnel by channel",
     "queryLines": [f"SELECT channel, sum(received) received, sum(quoted) quoted, sum(bound) bound, sum(ntu) ntu, sum(lost) lost FROM {F}.gold_pipeline_funnel GROUP BY channel"]},
    {"name": "ds_renewals", "displayName": "Retention by month",
     "queryLines": [f"SELECT month, round(sum(in_force)/(sum(in_force)+sum(lapsed))*100,1) retention_pct, round(avg(avg_rate_change_pct),1) rate_change_pct FROM {F}.gold_renewals GROUP BY month ORDER BY month"]},
    {"name": "ds_adequacy", "displayName": "Rate adequacy by trade",
     "queryLines": [f"SELECT trade_group, adequacy_pct, quotes_12m FROM {F}.gold_rate_adequacy ORDER BY adequacy_pct"]},
    {"name": "ds_accum", "displayName": "Accumulation map",
     "queryLines": [f"SELECT postcode_district, lat, lon, in_force_property_si, utilisation_pct, flood_band FROM {F}.gold_accumulation WHERE lat IS NOT NULL"]},
]


def bar(name, ds, x, y, title, w=3, h=6, x0=0, y0=0):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
              "datasetName": ds, "fields": [{"name": x, "expression": f"`{x}`"},
                                            {"name": y, "expression": f"`{y}`"}], "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "bar",
                     "encodings": {"x": {"fieldName": x, "scale": {"type": "categorical"}},
                                   "y": {"fieldName": y, "scale": {"type": "quantitative"}}},
                     "frame": {"title": title, "showTitle": True}}},
            "position": {"x": x0, "y": y0, "width": w, "height": h}}


def line(name, ds, x, y, title, w=3, h=6, x0=0, y0=0):
    out = bar(name, ds, x, y, title, w, h, x0, y0)
    out["widget"]["spec"]["widgetType"] = "line"
    out["widget"]["spec"]["encodings"]["x"]["scale"] = {"type": "categorical"}
    return out


MAP = {"widget": {"name": "w_accum_map", "queries": [{"name": "main_query", "query": {
        "datasetName": "ds_accum",
        "fields": [{"name": "lat", "expression": "`lat`"}, {"name": "lon", "expression": "`lon`"},
                   {"name": "in_force_property_si", "expression": "`in_force_property_si`"},
                   {"name": "utilisation_pct", "expression": "`utilisation_pct`"},
                   {"name": "postcode_district", "expression": "`postcode_district`"}],
        "disaggregated": True}}],
       "spec": {"version": 2, "widgetType": "symbol-map",
                "encodings": {"coordinates": {"latitude": {"fieldName": "lat"}, "longitude": {"fieldName": "lon"}},
                              "size": {"fieldName": "in_force_property_si", "scale": {"type": "quantitative"}},
                              "color": {"fieldName": "utilisation_pct", "scale": {"type": "quantitative"}}},
                "mark": {"opacity": 0.75},
                "frame": {"title": "Property accumulation vs capacity by district (size = SI, colour = utilisation %)", "showTitle": True}}},
       "position": {"x": 0, "y": 12, "width": 6, "height": 9}}

layout = [
    bar("w_gwp", "ds_portfolio", "trade_group", "gwp", "In-force GWP by trade", 3, 6, 0, 0),
    bar("w_funnel", "ds_funnel", "channel", "bound", "Bound by channel", 3, 6, 3, 0),
    line("w_ret", "ds_renewals", "month", "retention_pct", "Retention % by month", 3, 6, 0, 6),
    bar("w_adq", "ds_adequacy", "trade_group", "adequacy_pct", "Rate adequacy % by trade (quoted vs technical)", 3, 6, 3, 6),
    MAP,
]
dash = {"datasets": DATASETS,
        "pages": [{"name": "main", "displayName": "Underwriting Portfolio",
                   "layout": layout, "pageType": "PAGE_TYPE_CANVAS"}]}

out_path = pathlib.Path(__file__).resolve().parents[1] / "dashboards" / "underwriting_board.lvdash.json"
out_path.parent.mkdir(exist_ok=True)
out_path.write_text(json.dumps(dash, indent=1))
print("wrote", out_path)

w = WorkspaceClient(profile=prof)
from databricks.sdk.service.dashboards import Dashboard

ser = json.dumps(dash)
if existing_id:
    d = w.lakeview.update(existing_id, Dashboard(display_name="Underwriting Portfolio — Bricksurance SE",
                                                 serialized_dashboard=ser, warehouse_id=wh))
else:
    d = w.lakeview.create(Dashboard(display_name="Underwriting Portfolio — Bricksurance SE",
                                    serialized_dashboard=ser, warehouse_id=wh,
                                    parent_path=f"/Workspace/Users/{w.current_user.me().user_name}"))
w.lakeview.publish(d.dashboard_id, embed_credentials=True, warehouse_id=wh)
print("DASHBOARD_ID:", d.dashboard_id)
