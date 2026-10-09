# Georgia USGS Groundwater Data & Explorer

**Evaristo Critical Zone Hydrology Lab**

Complete water-level record for USGS groundwater wells in
Georgia, covering both continuous recorder wells and periodic (field-visit-only) wells, plus an interactive offline viewer.

**Source:** USGS Water Data APIs (`api.waterdata.usgs.gov`) — daily values
and field measurements, updated nightly.

## Contents

| Path | What it is |
|---|---|
| `ga_groundwater_explorer.html` | **Interactive viewer** — open directly in any browser (no server needed). Keep the `data/` folder next to it. |
| `data/csv/USGS_<site>.csv` | Daily values per well: `site_no, date, parm_cd, stat_cd, value, qualifiers, time_series_id`. |
| `data/discrete/USGS_<site>.csv` | Discrete field measurements per well (tape-down / transducer visits). |
| `data/sites_metadata.csv` | One row per well: name, coordinates, county, aquifer, well depth, altitude, period of record, counts. |
| `data/raw/` | Well metadata from the USGS Water Data APIs (legacy RDB column layout) and the daily time series included (`series.json`). |
| `data/state/sync_state.json` | Nightly update watermark and reconcile position. |
| `data/sites_index.js`, `data/sites/` | Compact data files used by the viewer. |
| `download_ga_groundwater.py`, `waterdata.py` | Update everything from the USGS Water Data APIs. |
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
python download_ga_groundwater.py   # applies everything USGS changed since the last run
python build_viewer_data.py         # rebuilds the viewer data files
```

The included GitHub Actions workflow (`.github/workflows/update-data.yml`) runs
these automatically every day and deploys to GitHub Pages. Field measurements
are kept in a rolling release asset (`field-measurements`) that each run
restores and periodically refreshes, so a rate-limited measurements API never
blocks the nightly daily-values update. An optional `USGS_API_KEY` repository
secret raises the API limits.

## Data source and update method

Data come from the USGS Water Data APIs (`api.waterdata.usgs.gov/ogcapi/v1`),
which replaced the legacy WaterServices (decommissioned 2027-02-22). The full
record of every well is kept in the nightly `dataset` release; each night's
run applies only what USGS added or changed since the previous run (new days,
approvals and revisions of any age, withdrawn values) and re-reads a rolling
slice of wells in full, so every record is re-verified against USGS about
every two months. See `waterdata.py`.

The legacy WaterServices served about 22,000 values that the new APIs do
not (mostly approved history before a series' current period of record).
They are kept, marked by a legacy `time_series_id` rather than a USGS time-series id,
and the final WaterServices-era archive is preserved unchanged as the
`legacy-final` release.

**Note:** recent values are provisional and subject to revision by USGS.
Cite as: U.S. Geological Survey, National Water Information System, accessed
via the USGS Water Data APIs.
