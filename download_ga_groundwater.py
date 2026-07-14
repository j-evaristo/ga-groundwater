"""
Download all USGS groundwater-level data for Georgia recorder wells.

Fetches, for every Georgia groundwater site with continuous (daily-values)
water-level records:
  - full period-of-record daily values (waterservices.usgs.gov/nwis/dv)
  - all discrete field measurements (api.waterdata.usgs.gov OGC
    field-measurements collection, one paginated statewide query)
  - complete site metadata (expanded site file + series catalog)

Outputs:
  data/raw/ga_gw_sites_expanded.rdb   expanded site metadata
  data/raw/ga_gw_series_catalog.rdb   series catalog (period of record)
  data/raw/json/<site>.json           raw daily-values responses (provenance)
  data/csv/USGS_<site>.csv            daily values: site_no, date, parm_cd, stat_cd, value, qualifiers
  data/discrete/USGS_<site>.csv       discrete field measurements
  data/download_log.csv               per-site download status
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

DV_BASE = "https://waterservices.usgs.gov/nwis/dv/"
SITE_SERVICE = "https://waterservices.usgs.gov/nwis/site/"
OGC_FM = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items"
STATE_CD = "ga"
STATE_FIPS = "13"

# Water-level parameter codes. 72019/61055 are depths below a datum
# (increase downward); the others are elevations (increase upward).
LEVEL_PARAMS = ["72019", "62610", "62611", "72020", "72150", "61055"]

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(ROOT, "data", "raw")
CSV_DIR = os.path.join(ROOT, "data", "csv")
DISC_DIR = os.path.join(ROOT, "data", "discrete")
JSON_DIR = os.path.join(RAW, "json")
END_DT = date.today().isoformat()
HEADERS = {"User-Agent": "GA-groundwater-research/1.0 (contact: evaristo@uga.edu)"}
OGC_MIN_INTERVAL = 1.1  # seconds between OGC requests (anonymous quota)
API_KEY = os.environ.get("USGS_API_KEY", "").strip()

DISC_COLS = ["site_no", "time", "parameter_code", "value", "unit_of_measure",
             "vertical_datum", "approval_status", "qualifier"]

for d in (JSON_DIR, CSV_DIR, DISC_DIR):
    os.makedirs(d, exist_ok=True)


def fetch(url, tries=4, timeout=180):
    """GET with retries. 429s from the OGC host are quota exhaustion, not
    transient failures - honor Retry-After (or back off in minutes) and keep
    the same URL so cursor pagination resumes exactly where it stopped."""
    if url.startswith(OGC_FM):
        tries = max(tries, 10)
    last = None
    for attempt in range(tries):
        try:
            headers = dict(HEADERS)
            if API_KEY and url.startswith(OGC_FM):
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
                print(f"rate-limited (429); waiting {wait}s before resuming", flush=True)
                time.sleep(wait)
                continue
            time.sleep(3 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"failed after {tries} tries: {url} ({last})")


def fetch_catalogs():
    """Refresh the GA groundwater site list and series catalog so newly
    instrumented wells are picked up automatically."""
    targets = [
        ("gw_sites_expanded.rdb",
         SITE_SERVICE + "?format=rdb&stateCd=" + STATE_CD + "&siteType=GW"
         "&hasDataTypeCd=dv&siteStatus=all&siteOutput=expanded"),
        ("gw_series_catalog.rdb",
         SITE_SERVICE + "?format=rdb&stateCd=" + STATE_CD + "&siteType=GW"
         "&outputDataTypeCd=dv&siteStatus=all&seriesCatalogOutput=true"),
        ("gw_sites_all_expanded.rdb",
         SITE_SERVICE + "?format=rdb&stateCd=" + STATE_CD + "&siteType=GW"
         "&siteStatus=all&siteOutput=expanded"),
    ]
    for name, url in targets:
        path = os.path.join(RAW, name)
        try:
            text = fetch(url)
            if text is None or not text.lstrip().startswith("#"):
                raise RuntimeError("unexpected response from site service")
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"refreshed {name}", flush=True)
        except Exception as e:
            if os.path.exists(path):
                print(f"WARN: could not refresh {name} ({e}); using existing copy", flush=True)
            else:
                raise


def parse_rdb(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        header = None
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if header is None:
                header = parts
                continue
            if parts[0] and parts[0][-1] in "sdn" and parts[0][:-1].isdigit():
                continue
            rows.append(dict(zip(header, parts)))
    return rows


def build_site_plan():
    cat = parse_rdb(os.path.join(RAW, "gw_series_catalog.rdb"))
    plan = {}
    for r in cat:
        if r["data_type_cd"] != "dv" or r["parm_cd"] not in LEVEL_PARAMS:
            continue
        s = plan.setdefault(r["site_no"], {"begin": r["begin_date"]})
        s["begin"] = min(s["begin"], r["begin_date"])
    return plan


def download_site_dv(site_no, info):
    parm_list = ",".join(LEVEL_PARAMS)
    url = (f"{DV_BASE}?format=json&sites={site_no}&parameterCd={parm_list}"
           f"&startDT={info['begin']}&endDT={END_DT}")
    text = fetch(url)
    if text is None:
        return site_no, 0, "404"
    with open(os.path.join(JSON_DIR, f"{site_no}.json"), "w", encoding="utf-8") as f:
        f.write(text)
    data = json.loads(text)
    ts_list = data.get("value", {}).get("timeSeries", [])
    n = 0
    with open(os.path.join(CSV_DIR, f"USGS_{site_no}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["site_no", "date", "parm_cd", "stat_cd", "value", "qualifiers"])
        for ts in ts_list:
            var = ts.get("variable", {})
            parm = var.get("variableCode", [{}])[0].get("value", "")
            if parm not in LEVEL_PARAMS:
                continue
            stat = (var.get("options", {}).get("option", [{}])[0]
                    .get("optionCode", ""))
            nodata = var.get("noDataValue", -999999)
            for block in ts.get("values", []):
                for v in block.get("value", []):
                    raw = v.get("value")
                    quals = " ".join(v.get("qualifiers", []))
                    try:
                        num = float(raw)
                    except (TypeError, ValueError):
                        num = None
                    if num is not None and num == nodata:
                        num = None
                    w.writerow([site_no, v["dateTime"][:10], parm, stat,
                                "" if num is None else raw, quals])
                    n += 1
    return site_no, n, "ok"


def download_discrete(plan):
    """One paginated statewide field-measurements query, split into per-well
    CSVs for every well in the state (recorder and periodic). Far fewer API requests than
    per-site fetching, which matters for the anonymous quota."""
    by_site = {}
    url = f"{OGC_FM}?state_code={STATE_FIPS}&limit=10000&f=json"
    pages = 0
    last_req = 0.0
    while url:
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
            by_site.setdefault(site_no, []).append({
                "site_no": site_no,
                "time": p.get("time", ""),
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
    n_rows = 0
    for site_no, rows in by_site.items():
        rows.sort(key=lambda r: (r["time"], r["parameter_code"]))
        with open(os.path.join(DISC_DIR, f"USGS_{site_no}.csv"), "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=DISC_COLS)
            w.writeheader()
            w.writerows(rows)
        n_rows += len(rows)
    print(f"discrete batch: {pages} pages, {len(by_site)} wells, {n_rows} measurements",
          flush=True)
    return n_rows


def refresh_discrete(plan):
    """Refresh field measurements, reusing the previous sweep when a recent
    one exists (DISCRETE_REUSE_DAYS) and falling back to it if the
    rate-limited measurements API cannot complete a fresh sweep. Field visits
    are infrequent (typically quarterly), so a several-day-old sweep loses
    nothing while keeping the nightly daily-values refresh reliable."""
    marker = os.path.join(ROOT, "data", "discrete_marker.txt")
    reuse_days = float(os.environ.get("DISCRETE_REUSE_DAYS", "0") or 0)
    have = len(os.listdir(DISC_DIR)) if os.path.isdir(DISC_DIR) else 0
    if reuse_days > 0 and have and os.path.exists(marker):
        age_days = (time.time() - os.path.getmtime(marker)) / 86400.0
        if age_days < reuse_days:
            print(f"discrete: reusing {have} well files from the previous sweep "
                  f"({age_days:.1f} d old; refresh due after {reuse_days:g} d)", flush=True)
            return 0
    try:
        n = download_discrete(plan)
    except Exception as e:
        if have:
            print(f"WARN: measurements sweep failed ({e}); keeping the previous "
                  f"{have} well files", flush=True)
            return 0
        raise
    with open(marker, "w", encoding="utf-8") as f:
        f.write(END_DT + "\n")
    return n


def main():
    fetch_catalogs()
    plan = build_site_plan()
    print(f"{len(plan)} wells to download through {END_DT}", flush=True)
    if "--dry-run" in sys.argv:
        return
    if "--discrete-only" in sys.argv:
        n_disc = refresh_discrete(plan)
        print(f"DISCRETE-ONLY DONE: {n_disc} rows", flush=True)
        return
    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(download_site_dv, s, info): s for s, info in plan.items()}
        for fut in as_completed(futures):
            site = futures[fut]
            try:
                site_no, n, status = fut.result()
            except Exception as e:
                site_no, n, status = site, 0, f"error: {e}"
            results.append((site_no, n, status))
            done += 1
            if done % 50 == 0 or status not in ("ok", "404"):
                print(f"[{done}/{len(plan)}] {site_no}: {status} ({n} rows)", flush=True)
    n_disc = refresh_discrete(plan)
    with open(os.path.join(ROOT, "data", "download_log.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["site_no", "dv_rows", "status"])
        for row in sorted(results):
            w.writerow(row)
    ok = sum(1 for _, _, s in results if s == "ok")
    total = sum(n for _, n, _ in results)
    print(f"DONE: {ok}/{len(plan)} wells ok, {total} daily rows, {n_disc} discrete rows", flush=True)
    bad = [(s, st) for s, _, st in results if st not in ("ok", "404")]
    if bad:
        print("FAILURES:", bad, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
