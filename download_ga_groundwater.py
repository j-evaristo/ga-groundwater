"""
Update all USGS groundwater-level data for Georgia wells from the USGS Water
Data APIs (api.waterdata.usgs.gov/ogcapi/v1):

  - daily values of every recorder well, kept in the repository's nightly
    dataset archive and updated incrementally (see waterdata.py)
  - all field measurements (field-measurements collection: one statewide
    query, or a rolling county-by-county refresh with DISCRETE_PARTITION=county)
  - site metadata, written in the legacy RDB column layout

Outputs:
  data/raw/gw_sites_expanded.rdb      recorder-well metadata
  data/raw/gw_sites_all_expanded.rdb  every groundwater site in the state
  data/raw/series.json                in-scope daily series
  data/state/sync_state.json          sync watermark and reconcile position
  data/csv/USGS_<site>.csv            daily values: site_no, date, parm_cd, stat_cd,
                                      value, qualifiers, time_series_id
  data/discrete/USGS_<site>.csv       field measurements
"""

import csv
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date

import waterdata

OGC_FM = "https://api.waterdata.usgs.gov/ogcapi/v1/collections/field-measurements/items"
FM_PROPS = ("monitoring_location_id,time,time_of_day,parameter_code,value,unit_of_measure,"
            "vertical_datum,approval_status,qualifier")
STATE_FIPS = "13"

# Water-level parameter codes. 72019/61055 are depths below a datum
# (increase downward); the others are elevations (increase upward).
LEVEL_PARAMS = ["72019", "62610", "62611", "72020", "72150", "61055"]

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(ROOT, "data", "raw")
DISC_DIR = os.path.join(ROOT, "data", "discrete")
END_DT = date.today().isoformat()
USER_AGENT = "GA-groundwater-research/2.0 (+https://github.com/j-evaristo/ga-groundwater; contact: evaristo@uga.edu)"
HEADERS = {"User-Agent": USER_AGENT}
OGC_MIN_INTERVAL = 1.1  # seconds between OGC requests (anonymous quota)
API_KEY = waterdata.API_KEY
OBSOLETE = ["gw_series_catalog.rdb"]   # legacy WaterServices file no longer produced
# Wall-clock deadline for the measurements sweep (set by refresh_discrete).
# The GitHub runner would otherwise be killed from outside by the workflow
# timeout, which never lets the cached-measurements fallback engage.
SWEEP_DEADLINE = None


class SweepBudgetExhausted(RuntimeError):
    pass


DISC_COLS = ["site_no", "time", "parameter_code", "value", "unit_of_measure",
             "vertical_datum", "approval_status", "qualifier"]

os.makedirs(DISC_DIR, exist_ok=True)
note = waterdata.note
parse_rdb = waterdata.parse_rdb


def fetch(url, tries=10, timeout=180):
    """GET with retries. 429s are quota exhaustion, not transient failures -
    honor Retry-After (or back off in minutes) and keep the same URL so cursor
    pagination resumes exactly where it stopped."""
    last = None
    for attempt in range(tries):
        try:
            headers = dict(HEADERS)
            if API_KEY:
                headers["X-Api-Key"] = API_KEY
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 404:
                return None
            if e.code == 429:
                ra = e.headers.get("Retry-After") if e.headers else None
                wait = int(ra) if ra and str(ra).isdigit() else min(900, 60 * (2 ** attempt))
                if SWEEP_DEADLINE is not None and time.time() + wait > SWEEP_DEADLINE:
                    raise SweepBudgetExhausted("sweep budget exhausted during a rate-limit wait")
                print(f"rate-limited (429); waiting {wait}s before resuming", flush=True)
                time.sleep(wait)
                continue
            if e.code < 500:
                raise RuntimeError(f"HTTP {e.code} for {url[:200]}")
            time.sleep(3 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, OSError,
                http.client.HTTPException, ValueError) as e:
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"failed after {tries} tries: {url} ({last})")


def fetch_measurements(query):
    """Page through one field-measurements query, filtered server-side to the
    water-level parameters; returns ({site_no: [rows]}, pages)."""
    by_site = {}
    url = (f"{OGC_FM}?{query}&parameter_code={','.join(LEVEL_PARAMS)}"
           f"&limit=50000&skipGeometry=true&properties={FM_PROPS}&f=json")
    pages = 0
    last_req = 0.0
    while url:
        if SWEEP_DEADLINE is not None and time.time() > SWEEP_DEADLINE:
            raise SweepBudgetExhausted(f"sweep budget exhausted after {pages} pages")
        wait = OGC_MIN_INTERVAL - (time.time() - last_req)
        if wait > 0:
            time.sleep(wait)
        last_req = time.time()
        text = fetch(url)
        if text is None:
            raise RuntimeError("discrete batch: empty response")
        d = json.loads(text)
        for ft in d.get("features", []):
            p = ft.get("properties", {})
            mlid = p.get("monitoring_location_id", "")
            if not mlid.startswith("USGS-") or p.get("parameter_code") not in LEVEL_PARAMS:
                continue
            site_no = mlid[5:]
            q = p.get("qualifier")
            when = p.get("time") or ""
            if when and "T" not in when:
                # v1 splits the visit time into a date and a time of day
                when = f"{when}T{p.get('time_of_day') or '12:00:00+00:00'}"
            by_site.setdefault(site_no, []).append({
                "site_no": site_no,
                "time": when,
                "parameter_code": p.get("parameter_code", ""),
                "value": p.get("value", ""),
                "unit_of_measure": p.get("unit_of_measure", ""),
                "vertical_datum": p.get("vertical_datum", ""),
                "approval_status": p.get("approval_status", ""),
                "qualifier": ";".join(q) if isinstance(q, list) else (q or ""),
            })
        pages += 1
        if pages % 10 == 0:
            print(f"discrete batch: page {pages}", flush=True)
        nxt = [l["href"] for l in d.get("links", []) if l.get("rel") == "next"]
        url = nxt[0] if nxt else None
    return by_site, pages


def write_discrete(by_site):
    n_rows = 0
    for site_no, rows in by_site.items():
        rows.sort(key=lambda r: (r["time"], r["parameter_code"]))
        with open(os.path.join(DISC_DIR, f"USGS_{site_no}.csv"), "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=DISC_COLS)
            w.writeheader()
            w.writerows(rows)
        n_rows += len(rows)
    return n_rows


def download_discrete():
    """One paginated statewide field-measurements query, split into per-well
    CSVs for every well in the state (recorder and periodic). Far fewer API
    requests than per-site fetching, which matters for the anonymous quota."""
    by_site, pages = fetch_measurements(f"state_code={STATE_FIPS}")
    n_rows = write_discrete(by_site)
    print(f"discrete batch: {pages} pages, {len(by_site)} wells, {n_rows} measurements",
          flush=True)
    return n_rows


def county_wells():
    """{county code: {site_no}} for every groundwater site, from the site file."""
    out = {}
    for r in parse_rdb(os.path.join(RAW, "gw_sites_all_expanded.rdb")):
        c = r.get("county_cd", "").strip()
        if c:
            out.setdefault(c, set()).add(r["site_no"])
    return out


def read_county_state(marker):
    """{"done": {county: epoch}, "tried": {county: epoch}} saved in the marker;
    empty for a missing marker or the older plain-date one."""
    try:
        with open(marker, encoding="utf-8") as f:
            state = json.load(f)
        return {"done": dict(state.get("done", {})), "tried": dict(state.get("tried", {}))}
    except (OSError, ValueError, AttributeError, TypeError):
        return {"done": {}, "tried": {}}


def refresh_discrete_by_county(marker, reuse_days):
    """Rolling refresh for a state whose statewide sweep cannot finish within
    the shared anonymous API quota (DISCRETE_PARTITION=county). Whole counties
    are re-fetched, stalest first, until the sweep budget runs out; each
    finished county replaces its wells' files, and the refresh times persist
    in the marker (part of the release store), so every county is refreshed
    within a few nights. A county that could not finish moves to the back of
    the next night's queue, so one oversized county cannot starve the rest."""
    state = read_county_state(marker)
    done, tried = state["done"], state["tried"]
    wells = county_wells()
    counties = sorted(wells)
    stored = {fn[5:-4] for fn in os.listdir(DISC_DIR) if fn.startswith("USGS_")}
    cutoff = time.time() - reuse_days * 86400
    due = sorted((c for c in counties if done.get(c, 0) < cutoff),
                 key=lambda c: max(done.get(c, 0), tried.get(c, 0)))
    if not due:
        print(f"discrete: all {len(counties)} counties refreshed within "
              f"{reuse_days:g} d", flush=True)
        return 0
    n_rows = finished = 0
    for c in due:
        try:
            by_site, pages = fetch_measurements(f"state_code={STATE_FIPS}&county_code={c}")
        except SweepBudgetExhausted as e:
            tried[c] = int(time.time())
            print(f"discrete: {e} in county {c}", flush=True)
            break
        except Exception as e:
            tried[c] = int(time.time())
            print(f"WARN: county {c} measurements failed ({e})", flush=True)
            continue
        known = len(wells[c] & stored)
        if not by_site and known >= 5:
            # wells with stored measurements cannot all lose them at once: the
            # county filter is not matching, so keep their files and retry later
            tried[c] = int(time.time())
            print(f"WARN: county {c} returned no measurements although {known} of "
                  f"its wells have them; keeping the stored files", flush=True)
            continue
        n_rows += write_discrete(by_site)
        done[c] = int(time.time())
        tried.pop(c, None)
        finished += 1
        print(f"discrete: county {c} refreshed ({pages} pages, {len(by_site)} wells)",
              flush=True)
    with open(marker, "w", encoding="utf-8") as f:
        json.dump({"swept": END_DT, "done": done, "tried": tried}, f, sort_keys=True)
    left = sum(1 for c in counties if done.get(c, 0) < cutoff)
    print(f"discrete: {finished} counties refreshed this run, {left} of "
          f"{len(counties)} still due", flush=True)
    return n_rows


def refresh_discrete():
    """Refresh field measurements, reusing the previous sweep when a recent
    one exists (DISCRETE_REUSE_DAYS) and falling back to it if the
    rate-limited measurements API cannot complete a fresh sweep. Field visits
    are infrequent (typically quarterly), so a several-day-old sweep loses
    nothing while keeping the nightly daily-values refresh reliable."""
    global SWEEP_DEADLINE
    marker = os.path.join(ROOT, "data", "discrete_marker.txt")
    reuse_days = float(os.environ.get("DISCRETE_REUSE_DAYS", "0") or 0)
    have = len(os.listdir(DISC_DIR)) if os.path.isdir(DISC_DIR) else 0
    county_mode = os.environ.get("DISCRETE_PARTITION", "") == "county"
    if county_mode:
        budget_min = float(os.environ.get("DISCRETE_SWEEP_BUDGET_MIN", "0") or 0)
        if budget_min > 0:
            SWEEP_DEADLINE = time.time() + budget_min * 60
        try:
            return refresh_discrete_by_county(marker, reuse_days)
        finally:
            SWEEP_DEADLINE = None
    if reuse_days > 0 and have and os.path.exists(marker):
        age_days = (time.time() - os.path.getmtime(marker)) / 86400.0
        if age_days < reuse_days:
            print(f"discrete: reusing {have} well files from the previous sweep "
                  f"({age_days:.1f} d old; refresh due after {reuse_days:g} d)", flush=True)
            return 0
    budget_min = float(os.environ.get("DISCRETE_SWEEP_BUDGET_MIN", "0") or 0)
    try:
        if budget_min > 0:
            SWEEP_DEADLINE = time.time() + budget_min * 60
        n = download_discrete()
    except Exception as e:
        if have:
            print(f"WARN: measurements sweep failed ({e}); keeping the previous "
                  f"{have} well files", flush=True)
            return 0
        raise
    finally:
        SWEEP_DEADLINE = None
    with open(marker, "w", encoding="utf-8") as f:
        f.write(END_DT + "\n")
    return n


def main():
    if "--discrete-only" not in sys.argv:
        pipeline = waterdata.Pipeline(
            ROOT, "groundwater", STATE_FIPS, LEVEL_PARAMS, USER_AGENT,
            rdb_files={"gw_sites_expanded.rdb": "state"}, site_type_prefix="GW",
            all_sites_rdb="gw_sites_all_expanded.rdb", reconcile_bins=1,
            # the field-measurements store owns discrete/; fall back to the
            # dataset archive's copy only when the store did not restore
            restore_exclude=("discrete/",) if os.listdir(DISC_DIR) else ())
        pipeline.run()
        for name in OBSOLETE:
            path = os.path.join(RAW, name)
            if os.path.exists(path):
                os.remove(path)
    n_disc = refresh_discrete()
    print(f"DONE: {n_disc} field-measurement rows refreshed", flush=True)


if __name__ == "__main__":
    main()
