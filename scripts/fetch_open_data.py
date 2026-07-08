"""Fetch the bundled real open-data extracts (run at BUILD time, never at demo time).

Outputs to data/open/:
  police_uk_crime_by_district.csv   — burglary + criminal-damage counts near each district centroid
  ea_flood_areas_by_district.csv    — EA flood alert/warning areas naming each district town (full register match)

The demo bundles these files so it never depends on external APIs at run time.
Provenance + licences: data/open/PROVENANCE.md
"""
import csv, json, time, urllib.request, urllib.parse, pathlib, sys

OUT = pathlib.Path(__file__).resolve().parents[1] / "data" / "open"
CRIME_MONTH = "2026-02"  # most recent month with published street-level data at build time

# Commercial-book postcode districts (claims_workbench's 30 + commercial/hero additions)
DISTRICTS = [
    ("B1", 52.479, -1.908), ("B15", 52.464, -1.928), ("BL1", 53.585, -2.435), ("BL3", 53.567, -2.443),
    ("BS1", 51.453, -2.597), ("CB1", 52.198, 0.137), ("CF10", 51.479, -3.176), ("CO1", 51.889, 0.903),
    ("CV1", 52.408, -1.510), ("E1", 51.516, -0.060), ("EC1", 51.524, -0.099), ("GL1", 51.864, -2.238),
    ("LE1", 52.635, -1.132), ("LS1", 53.797, -1.546), ("LS6", 53.819, -1.575), ("M1", 53.479, -2.236),
    ("M14", 53.443, -2.222), ("M20", 53.413, -2.230), ("N1", 51.538, -0.103), ("NE1", 54.972, -1.613),
    ("NG1", 52.954, -1.149), ("OL1", 53.546, -2.116), ("OL9", 53.537, -2.139), ("OX1", 51.750, -1.260),
    ("RG1", 51.456, -0.969), ("S1", 53.380, -1.466), ("SE1", 51.501, -0.091), ("SW1", 51.497, -0.137),
    ("WN3", 53.535, -2.640), ("WN5", 53.530, -2.668),
    # commercial + hero districts
    ("WA14", 53.387, -2.349),  # Altrincham — hero 900001
    ("HX7", 53.742, -2.012),   # Hebden Bridge / Mytholmroyd — hero 900002 (Calder Valley)
    ("HX6", 53.708, -1.907),   # Sowerby Bridge — Calder Valley
    ("WS2", 52.590, -1.995),   # Walsall — hero 900003
    ("HD1", 53.646, -1.785),   # Huddersfield
    ("PR1", 53.759, -2.703),   # Preston
]


def get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "bricksurance-demo-build/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def fetch_crime():
    rows = []
    for cat in ("burglary", "criminal-damage-arson"):
        for d, lat, lon in DISTRICTS:
            url = (f"https://data.police.uk/api/crimes-street/{cat}"
                   f"?lat={lat}&lng={lon}&date={CRIME_MONTH}")
            try:
                n = len(get(url))
            except Exception as e:  # noqa: BLE001 - build-time script, log and continue
                print(f"  ! {cat} {d}: {e}", file=sys.stderr)
                n = None
            rows.append({"postcode_district": d, "month": CRIME_MONTH, "category": cat, "count": n})
            print(f"  crime {cat:22s} {d:5s} -> {n}")
            time.sleep(0.6)
    with open(OUT / "police_uk_crime_by_district.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["postcode_district", "month", "category", "count"])
        w.writeheader(); w.writerows(rows)


# Town name(s) per district — matched against EA flood-area labels/descriptions. An EA
# flood alert/warning area that NAMES the town is a far better risk proxy than a
# count-of-areas-within-radius (which just measures urban river density).
TOWNS = {
    "B1": ["Birmingham city"], "B15": ["Edgbaston"], "BL1": ["Bolton"], "BL3": ["Bolton"],
    "BS1": ["Bristol city"], "CB1": ["Cambridge"], "CF10": ["Cardiff"], "CO1": ["Colchester"],
    "CV1": ["Coventry"], "E1": ["Whitechapel", "Tower Hamlets"], "EC1": ["Clerkenwell", "Islington"],
    "GL1": ["Gloucester"], "LE1": ["Leicester"], "LS1": ["Leeds city"], "LS6": ["Headingley"],
    "M1": ["Manchester city"], "M14": ["Fallowfield"], "M20": ["Didsbury"], "N1": ["Islington"],
    "NE1": ["Newcastle"], "NG1": ["Nottingham"], "OL1": ["Oldham"], "OL9": ["Chadderton"],
    "OX1": ["Oxford"], "RG1": ["Reading"], "S1": ["Sheffield city"], "SE1": ["Southwark", "Lambeth"],
    "SW1": ["Westminster"], "WN3": ["Wigan"], "WN5": ["Orrell", "Wigan"],
    "WA14": ["Altrincham"], "HX7": ["Hebden Bridge", "Mytholmroyd"], "HX6": ["Sowerby Bridge"],
    "WS2": ["Walsall"], "HD1": ["Huddersfield"], "PR1": ["Preston"],
}


def fetch_flood():
    # One fetch of the FULL national register (~4,600 areas), then per-district matching.
    print("  fetching full EA flood-area register…")
    items = get("https://environment.data.gov.uk/flood-monitoring/id/floodAreas?_limit=10000").get("items", [])
    print(f"  register: {len(items)} areas")
    rows = []
    for d, lat, lon in DISTRICTS:
        towns = TOWNS[d]
        named = [i for i in items
                 if any(t.lower() in (i.get("label", "") or "").lower() for t in towns)]
        warning = [i for i in named if "W" in (i.get("notation", "") or "")[3:6]]  # fwdCode contains FW for warning areas
        rivers = sorted({i.get("riverOrSea", "") for i in named if i.get("riverOrSea")})[:6]
        sample = "; ".join(i.get("label", "") for i in named[:3])
        rows.append({"postcode_district": d, "lat": lat, "lon": lon, "town_terms": " | ".join(towns),
                     "named_area_count": len(named), "named_warning_count": len(warning),
                     "rivers": " | ".join(rivers), "sample_areas": sample})
        print(f"  flood {d:5s} {towns[0]:14s} -> {len(named)} named areas ({', '.join(rivers[:2])})")
    with open(OUT / "ea_flood_areas_by_district.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["postcode_district", "lat", "lon", "town_terms",
                                          "named_area_count", "named_warning_count", "rivers", "sample_areas"])
        w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    print("== police.uk street-level crime ==")
    fetch_crime()
    print("== EA flood-monitoring flood areas ==")
    fetch_flood()
    print("done")
