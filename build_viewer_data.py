"""
Build viewer data files from downloaded USGS groundwater CSVs.

Outputs:
  data/sites_index.js       - metadata for all wells + GA boundary (loaded by viewer at startup)
  data/sites/USGS_<no>.js   - compact per-well daily series + discrete points (loaded on demand)
  data/sites_metadata.csv   - consolidated well metadata table
"""

import csv
import json
import math
import os
from datetime import date

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(ROOT, "data", "raw")
ASSETS = os.path.join(ROOT, "assets")
CSV_DIR = os.path.join(ROOT, "data", "csv")
DISC_DIR = os.path.join(ROOT, "data", "discrete")
SITES_DIR = os.path.join(ROOT, "data", "sites")
os.makedirs(SITES_DIR, exist_ok=True)

EPOCH = date(1970, 1, 1)
GAP_DAYS = 45  # gaps longer than this start a new segment

# preference order: depth-to-water first (dominant in GA), then elevations
PARAM_PREF = ["72019", "62611", "62610", "72020", "72150", "61055"]
STAT_PREF = ["00003", "00008", "00001", "00002"]
STATE = "GA"
LEVEL_PARAMS = set(PARAM_PREF)

NAT_AQFR = {
    "S400FLORDN": "Floridan aquifer system",
    "S100SECSLP": "Southeastern Coastal Plain aquifer system",
    "N400PDMBRX": "Piedmont and Blue Ridge crystalline-rock aquifers",
    "S100SURFCL": "Surficial aquifer system",
    "N500VLYRDG": "Valley and Ridge aquifers",
    "N100CACSTL": "California Coastal Basin aquifers",
    "N100BSNRGB": "Basin and Range basin-fill aquifers",
    "S100CNRLVL": "Central Valley aquifer system",
    "N100PCFNWV": "Pacific Northwest volcanic-rock aquifers",
    "N100PCFNWB": "Pacific Northwest basin-fill aquifers",
    "N9999OTHER": "Other aquifers",
}
LOC_AQFR = {
    "120FLRDU": "Upper Floridan aquifer",
    "120FLRDL": "Lower Floridan aquifer",
    "120FLRD": "Floridan aquifer system",
    "110SFCL": "Surficial aquifer",
    "124CLBR": "Claiborne aquifer",
    "125CLTN": "Clayton aquifer",
    "122BRCKU": "Upper Brunswick aquifer",
    "320CRSL": "Crystalline rocks",
}


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


def epoch_day(iso):
    y, m, d = int(iso[0:4]), int(iso[5:7]), int(iso[8:10])
    return (date(y, m, d) - EPOCH).days


def load_counties():
    lookup = {}
    with open(os.path.join(ASSETS, "national_county.txt"), encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 4:
                lookup[(parts[1], parts[2])] = f"{parts[3]}, {parts[0]}"
    return lookup


def clean_num(v):
    if v is None:
        return None
    if v == int(v) and abs(v) < 1e15:
        return int(v)
    if v != 0:
        mag = int(math.floor(math.log10(abs(v))))
        v = round(v, max(0, 5 - mag))
        if v == int(v):
            return int(v)
    return v


def build_segments(day_values):
    segs = []
    cur_start, cur_vals, prev_day = None, [], None
    for d, v in day_values:
        if prev_day is None:
            cur_start, cur_vals = d, [v]
        else:
            gap = d - prev_day
            if gap > GAP_DAYS:
                segs.append([cur_start, cur_vals])
                cur_start, cur_vals = d, [v]
            else:
                cur_vals.extend([None] * (gap - 1))
                cur_vals.append(v)
        prev_day = d
    if cur_vals:
        segs.append([cur_start, cur_vals])
    return segs


def process_site_csv(site_no):
    path = os.path.join(CSV_DIR, f"USGS_{site_no}.csv")
    if not os.path.exists(path):
        return None
    per_key = {}   # (parm, stat) -> {epoch_day: value}
    quals = {"P": 0, "e": 0}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw = row["value"]
            if raw == "":
                v = None
            else:
                try:
                    v = float(raw)
                except ValueError:
                    v = None
            d = epoch_day(row["date"])
            bucket = per_key.setdefault((row["parm_cd"], row["stat_cd"]), {})
            if d not in bucket or (bucket[d] is None and v is not None):
                bucket[d] = v
            qparts = row["qualifiers"].split()
            if "P" in qparts:
                quals["P"] += 1
            if "e" in qparts:
                quals["e"] += 1
    series = {}
    for (parm, stat), days in per_key.items():
        pts = sorted((d, clean_num(v)) for d, v in days.items() if v is not None)
        if not pts:
            continue
        series[parm + ":" + stat] = {
            "segs": build_segments(pts), "n": len(pts),
            "b": pts[0][0], "e": pts[-1][0],
        }
    return series, quals


def process_discrete(site_no, pref_parm):
    """Discrete points for the preferred parameter -> [[epochDay, value], ...];
    also usable-measurement counts per parameter.

    The API reports one field visit as several rows (one per vertical datum,
    plus valueless "NoMeasurement" rows), so measurements are deduplicated by
    (visit time, parameter) and only rows with a parseable value are counted.
    For the plotted parameter, only its most common vertical datum is used so
    a series never mixes datums."""
    path = os.path.join(DISC_DIR, f"USGS_{site_no}.csv")
    counts = {}
    pts = []
    if not os.path.exists(path):
        return pts, counts
    seen = set()
    pref_rows = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            parm = row["parameter_code"]
            try:
                v = float(row["value"])
            except (TypeError, ValueError):
                continue
            t = row["time"]
            if len(t) < 10:
                continue
            key = (t, parm)
            if key in seen:
                continue
            seen.add(key)
            counts[parm] = counts.get(parm, 0) + 1
            if parm == pref_parm:
                pref_rows.append((t, row.get("vertical_datum", ""), v))
    if pref_rows:
        datum_n = {}
        for _, d, _v in pref_rows:
            datum_n[d] = datum_n.get(d, 0) + 1
        modal = max(datum_n, key=lambda d: datum_n[d])
        pts = [[epoch_day(t[:10]), clean_num(v)] for t, d, v in pref_rows if d == modal]
    pts.sort()
    return pts, counts


def series_sort_key(key):
    parm, stat = key.split(":")
    pi = PARAM_PREF.index(parm) if parm in PARAM_PREF else 99
    si = STAT_PREF.index(stat) if stat in STAT_PREF else 99
    return (pi, si)


MIN_DISC_PERIODIC = 3   # periodic wells need at least this many usable level measurements


def pick_disc_param(counts):
    """Most-measured level parameter; PARAM_PREF order breaks ties."""
    best = None
    for p, c in counts.items():
        if p not in LEVEL_PARAMS:
            continue
        rank = (-c, PARAM_PREF.index(p) if p in PARAM_PREF else 99)
        if best is None or rank < best[0]:
            best = (rank, p)
    return best[1] if best else None


def main():
    counties = load_counties()
    all_path = os.path.join(RAW, "gw_sites_all_expanded.rdb")
    rec_path = os.path.join(RAW, "gw_sites_expanded.rdb")
    sites = parse_rdb(all_path if os.path.exists(all_path) else rec_path)
    meta = {s["site_no"]: s for s in sites}

    ga = json.load(open(os.path.join(ASSETS, "state_boundary.json"), encoding="utf-8"))
    boundary = [[round(x, 3), round(y, 3)] for x, y in ga["geometry"]["coordinates"][0]]

    index, meta_rows = [], []
    total_vals = total_disc = 0
    n_recorder = n_periodic = n_skipped_sparse = 0
    disc_sites = {fn[5:-4] for fn in os.listdir(DISC_DIR) if fn.startswith("USGS_")}
    for site_no in sorted(set(meta) | disc_sites):
        if site_no not in meta:
            continue   # measurement at a site absent from the state site file
        result = process_site_csv(site_no)
        series, quals = (result if result is not None else ({}, {"P": 0, "e": 0}))
        if series:
            pref = min(series, key=series_sort_key)
            pref_parm = pref.split(":")[0]
            recorder = True
        else:
            pref = ""
            counts_probe = process_discrete(site_no, "__none__")[1]
            pref_parm = pick_disc_param(counts_probe)
            if pref_parm is None:
                continue
            recorder = False
        disc, disc_counts = process_discrete(site_no, pref_parm)
        if not recorder and len(disc) < MIN_DISC_PERIODIC:
            n_skipped_sparse += 1
            continue
        if recorder:
            n_recorder += 1
        else:
            n_periodic += 1
        m = meta[site_no]

        out = {"series": {}, "disc": disc}
        stats_idx = {}
        for key, s in series.items():
            out["series"][key] = {"segs": s["segs"]}
            stats_idx[key] = [s["b"], s["e"], s["n"]]
            total_vals += s["n"]
        total_disc += len(disc)
        with open(os.path.join(SITES_DIR, f"USGS_{site_no}.js"), "w", encoding="utf-8") as f:
            f.write("window.__WELL_DATA=window.__WELL_DATA||{};")
            f.write(f"window.__WELL_DATA[{json.dumps(site_no)}]=")
            f.write(json.dumps(out, separators=(",", ":")))
            f.write(";if(window.__onWellData)window.__onWellData(" + json.dumps(site_no) + ");")

        def fnum(key):
            v = m.get(key, "").strip()
            try:
                return float(v)
            except ValueError:
                return None

        county = counties.get((m.get("state_cd", ""), m.get("county_cd", "")), "")
        nat = m.get("nat_aqfr_cd", "").strip()
        loc = m.get("aqfr_cd", "").strip()
        # nD = points plotted in the viewer; nDo = every other downloaded
        # measurement row (other parameters/datums or unparseable), so
        # nD + nDo always equals the raw measurement-row count for the well
        n_disc_other = sum(disc_counts.values()) - len(disc)
        entry = {
            "no": site_no,
            "rec": 1 if recorder else 0,
            "nm": m.get("station_nm", "").strip(),
            "lat": fnum("dec_lat_va"),
            "lon": fnum("dec_long_va"),
            "cty": county,
            "huc": m.get("huc_cd", "").strip(),
            "alt": fnum("alt_va"),
            "altd": m.get("alt_datum_cd", "").strip(),
            "wd": fnum("well_depth_va"),
            "naq": NAT_AQFR.get(nat, nat),
            "laq": LOC_AQFR.get(loc, loc),
            "stats": stats_idx,
            "pref": pref,
            "nD": len(disc),
            "nDo": n_disc_other,
            "nP": quals["P"],
            "nE": quals["e"],
        }
        if not recorder:
            entry["dparm"] = pref_parm
            entry["db"] = disc[0][0]
            entry["de"] = disc[-1][0]
            entry["dn"] = len(disc)
        # drop empty fields to keep the index lean (13k+ wells); keep
        # structural keys even when empty/zero
        keep = {"no", "nm", "rec", "stats", "pref"}
        entry = {k: v for k, v in entry.items() if k in keep or (v is not None and v != "")}
        index.append(entry)

        pref_s = series[pref] if recorder else {"b": disc[0][0], "e": disc[-1][0], "n": 0}
        meta_rows.append({
            "site_no": site_no,
            "well_type": "recorder" if recorder else "periodic",
            "station_nm": entry["nm"],
            "dec_lat_va": m.get("dec_lat_va", "").strip(),
            "dec_long_va": m.get("dec_long_va", "").strip(),
            "coord_datum": m.get("dec_coord_datum_cd", "").strip(),
            "county": county,
            "huc_cd": entry.get("huc", ""),
            "alt_va": m.get("alt_va", "").strip(),
            "alt_datum_cd": entry.get("altd", ""),
            "well_depth_ft": m.get("well_depth_va", "").strip(),
            "hole_depth_ft": m.get("hole_depth_va", "").strip(),
            "nat_aqfr_cd": nat,
            "nat_aqfr_name": NAT_AQFR.get(nat, ""),
            "local_aqfr_cd": loc,
            "local_aqfr_name": LOC_AQFR.get(loc, ""),
            "begin_date": date.fromordinal(EPOCH.toordinal() + pref_s["b"]).isoformat(),
            "end_date": date.fromordinal(EPOCH.toordinal() + pref_s["e"]).isoformat(),
            "n_daily_values": pref_s["n"],
            "preferred_series": pref,
            "all_series": " ".join(sorted(series)),
            "n_discrete_pref_param": len(disc),
            "n_discrete_other_params": n_disc_other,
            "n_provisional_days": quals["P"],
            "n_estimated_days": quals["e"],
        })

    index_obj = {
        "generated": date.today().isoformat(),
        "state": STATE,
        "boundary": boundary,
        "sites": index,
    }
    with open(os.path.join(ROOT, "data", "sites_index.js"), "w", encoding="utf-8") as f:
        f.write("window.__WELL_INDEX=")
        f.write(json.dumps(index_obj, separators=(",", ":")))
        f.write(";")

    with open(os.path.join(ROOT, "data", "sites_metadata.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(meta_rows[0].keys()))
        w.writeheader()
        w.writerows(meta_rows)

    print(f"index: {len(index)} wells ({n_recorder} recorder, {n_periodic} periodic; "
          f"{n_skipped_sparse} periodic wells with <{MIN_DISC_PERIODIC} measurements excluded), "
          f"{total_vals} daily values, {total_disc} discrete points")
    sizes = sorted((os.path.getsize(os.path.join(SITES_DIR, fn)), fn) for fn in os.listdir(SITES_DIR))
    print(f"largest well file: {sizes[-1][1]} {sizes[-1][0]/1e6:.1f} MB")
    print(f"total sites dir: {sum(s for s, _ in sizes)/1e6:.1f} MB")
    print(f"index size: {os.path.getsize(os.path.join(ROOT,'data','sites_index.js'))/1e3:.0f} KB")


if __name__ == "__main__":
    main()
