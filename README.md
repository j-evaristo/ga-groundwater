# Georgia USGS Groundwater Data & Explorer

**Evaristo Critical Zone Hydrology Lab · University of Georgia**

Complete water-level record for USGS groundwater wells in
Georgia, covering both continuous recorder wells and periodic (field-visit-only) wells, plus an interactive offline viewer.

**Source:** USGS National Water Information System — daily values
(`waterservices.usgs.gov/nwis/dv`) and field measurements
(`api.waterdata.usgs.gov` OGC field-measurements collection).

## Contents

| Path | What it is |
|---|---|
| `ga_groundwater_explorer.html` | **Interactive viewer** — open directly in any browser (no server needed). Keep the `data/` folder next to it. |
| `data/csv/USGS_<site>.csv` | Daily values per well: `site_no, date, parm_cd, stat_cd, value, qualifiers`. |
| `data/discrete/USGS_<site>.csv` | Discrete field measurements per well (tape-down / transducer visits). |
| `data/sites_metadata.csv` | One row per well: name, coordinates, county, aquifer, well depth, altitude, period of record, counts. |
| `data/raw/` | Original USGS site file, series catalog, and raw daily-values responses (provenance). |
| `data/sites_index.js`, `data/sites/` | Compact data files used by the viewer. |
| `download_ga_groundwater.py` | Downloads everything from USGS (re-run to refresh). |
| `build_viewer_data.py` | Rebuilds the viewer data files from the CSVs. |

## Well types

The explorer contains two clearly-marked classes of wells:

- **Recorder wells** — continuous daily water-level records (plus any field
  measurements, overlaid on the daily hydrograph)
- **Periodic wells** — field measurements only (tape-down / transducer visits,
  no recorder). Marked with a "periodic" badge in the list, a Well type chip,
  smaller fainter map dots, and a Type filter in the sidebar. Wells with fewer
  than 3 usable level measurements are excluded from the viewer (they remain
  in the downloaded CSVs).

## Dataset summary

- Georgia groundwater wells with continuous (daily-values) water-level records,
  dominated by parameter **72019** (depth to water below land surface, ft)
- Discrete field measurements for all wells statewide (recorder and
  periodic), from the USGS field-measurements API
- Depth-to-water values increase downward: a rising value is a falling water
  table. Negative depths are artesian (water above land surface). The viewer's
  vertical axis is inverted accordingly so "up" always reads as a rising table.

## Viewer features

- Search box + sortable well list (number, name, record length, well depth,
  recency) and a clickable Georgia map; search matches county and aquifer too
- Full-period hydrograph (depth axis inverted), discrete field measurements
  overlaid, drag-to-zoom overview strip, range presets, crosshair readout
- Summary tiles including a water-table trend (ft/yr) for the selected range
- Seasonal pattern, level duration curve, and annual mean water levels —
  all recomputed for the selected date range
- Data tables (annual / monthly / percentiles), light/dark themes,
  deep links: `ga_groundwater_explorer.html#site=<well number>`

## Refreshing the data

```
python download_ga_groundwater.py   # refreshes the well catalog + re-downloads everything through today
python build_viewer_data.py         # rebuilds the viewer data files
```

The included GitHub Actions workflow (`.github/workflows/update-data.yml`) runs
these automatically every day and deploys to GitHub Pages.

**Note:** recent values are provisional and subject to revision by USGS.
Cite as: U.S. Geological Survey, National Water Information System (NWISWeb).
