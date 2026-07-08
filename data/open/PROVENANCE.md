# Bundled open data — provenance & licences

These extracts are **real, freely available UK open data**, fetched once at build time and
bundled so the demo never depends on an external API at run time (reliability by design).
All sources are published under the **Open Government Licence v3.0** unless noted.
Re-fetch: `python3 scripts/fetch_open_data.py` (crime + flood).

| File | Source | What it is | Notes |
|---|---|---|---|
| `ofsi_consolidated_list.csv` | OFSI (HM Treasury) Consolidated List of Financial Sanctions Targets — `ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv`, list dated **2026-06-03** | 12,034 primary names (Individual / Entity / Ship) with DOB, nationality, regime, group id | Slimmed to primary names + screening columns. Screening in the demo runs against this REAL list; the demo's watchlist "hit" lives on a separate **synthetic internal watchlist** — never a real listed entity. |
| `police_uk_crime_by_district.csv` | police.uk street-level crime API (`data.police.uk`), month **2026-02** | Burglary + criminal-damage-and-arson incident counts within ~1 mile of each postcode-district centroid | Used ONLY as the evidence base for the theft & malicious-damage loading in the technical price build-up. District granularity is a demo simplification — production would rate at full postcode/geocode. |
| `ea_flood_areas_by_district.csv` | Environment Agency real-time flood-monitoring API (`environment.data.gov.uk/flood-monitoring`) — full national register (4,208 areas) | The EA flood alert/warning areas whose **label names each district's town** (e.g. HX7 → "Hebden Water at Hebden Bridge", River Calder areas), with rivers and counts | **England only** (Wales = NRW, Scotland = SEPA). This file is the *evidence*; the High/Medium/Low **band** in `ref_flood_open` is curated from the EA's published **RoFRS** (Risk of Flooding from Rivers and Sea) statistics and labelled as curated — production would use the property-level RoFRS dataset + the separate surface-water dataset. |
| `ons_district_centroids.csv` | ONS Open Geography (approximate postcode-district centroids) | Lat/lon per postcode district | Same 30-district base as the sibling claims demo + commercial/hero districts (WA14, HX7, HX6, WS2, HD1, PR1). |
| `epc_nondom_band_mix_by_district.csv` | Curated summary of MHCLG **Energy Performance of Buildings** open statistics (non-domestic EPCs, England & Wales) | Approximate non-domestic EPC band mix per district | **Curated, not a raw extract** (the register bulk download requires registration): England-wide band distribution from the published quarterly statistics, apportioned per district by building-age profile. Used ONLY for the MEES letting-ban / portfolio ESG lens — **never** a per-risk rating factor. |

Companies-House-style company profiles are **synthetic** (generated in notebook 00 — synthetic
insurer, synthetic insureds) — the live Companies House API integration is demonstrated
separately in `notebooks/91_companies_house_live_api_example` (off the demo's critical path).
