"""
USGS Water Data APIs (OGC API v1) client and incremental daily-values sync.

Shared by the four explorer pipelines (GA/CA streamflow, GA/CA groundwater)
and kept identical across their repositories. WaterServices, which the
pipelines used to re-download every station's full record from each night,
is decommissioned on 2027-02-22; the Water Data APIs instead keep the full
history in the repository's nightly dataset archive and apply only what USGS
changed since the previous run:

  1. restore     the previous dataset archive (station CSVs, site metadata,
                 sync state); the first run seeds from the legacy archive
  2. metadata    combined-metadata -> in-scope daily series + site files in
                 the legacy RDB column layout the builders read
  3. incremental /daily rows whose last_modified falls after the watermark
                 (minus a 48 h overlap): new days, approvals, revisions and
                 withdrawn values of any age
  4. new series  a station's full record when a series appears
  5. reconcile   full records for a rolling slice of stations each night, so
                 deletions (which last_modified cannot show) and anything
                 missed converge; every station is re-read about every two
                 months

Rows are keyed (time_series_id, date). Values the new APIs do not serve but
WaterServices did (history before a series' metadata begin date, or series
with no daily data in the new APIs) are kept as legacy rows and only replaced
when a v1 series comes to cover their dates.
"""

import csv
import html
import http.client
import json
import os
import shutil
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://api.waterdata.usgs.gov/ogcapi/v1/collections"
API_KEY = os.environ.get("USGS_API_KEY", "").strip()
PAGE = 50000                       # maximum records per page
URL_BUDGET = 7500                  # GET URLs above ~8.2 KB are rejected (414)
MIN_INTERVAL = 1.1                 # seconds between requests
MAX_PAGES = int(os.environ.get("WATERDATA_MAX_PAGES") or (200 if API_KEY else 20))
BIN_ROWS = 45000                   # full-record bin size: one page per request
BIN_IDS = 400
OVERLAP = timedelta(hours=48)      # re-read window: last_modified can lag
SHRINK_NIGHTS = 3                  # a smaller catalog is accepted after this many nights

DAILY_PROPS = ("time_series_id,monitoring_location_id,parameter_code,statistic_id,"
               "time,value,approval_status,qualifier,last_modified")
SERIES_PROPS = ("id,monitoring_location_id,agency_code,monitoring_location_number,"
                "monitoring_location_name,parameter_code,statistic_id,data_type,begin,end,"
                "primary,sublocation_identifier,web_description,site_type_code,state_code,"
                "county_code,hydrologic_unit_code,basin_code,drainage_area,"
                "contributing_drainage_area,altitude,vertical_datum,well_constructed_depth,"
                "hole_constructed_depth,national_aquifer_code,aquifer_code,aquifer_type_code,"
                "construction_date")
SITE_PROPS = ("id,agency_code,monitoring_location_number,monitoring_location_name,"
              "site_type_code,state_code,county_code,hydrologic_unit_code,basin_code,"
              "altitude,vertical_datum,well_constructed_depth,hole_constructed_depth,"
              "national_aquifer_code,aquifer_code,aquifer_type_code,construction_date")

RDB_COLS = ["agency_cd", "site_no", "station_nm", "site_tp_cd", "dec_lat_va", "dec_long_va",
            "dec_coord_datum_cd", "state_cd", "county_cd", "huc_cd", "basin_cd", "alt_va",
            "alt_datum_cd", "drain_area_va", "contrib_drain_area_va", "well_depth_va",
            "hole_depth_va", "nat_aqfr_cd", "aqfr_cd", "aqfr_type_cd", "construction_dt"]

SF_COLS = ["site_no", "date", "stat_cd", "discharge_cfs", "qualifiers", "method_id", "method_desc"]
GW_COLS = ["site_no", "date", "parm_cd", "stat_cd", "value", "qualifiers", "time_series_id"]


class ApiError(RuntimeError):
    pass


class RequestTooLarge(ApiError):
    pass


class BudgetExhausted(ApiError):
    pass


def note(msg):
    """Warning that also shows up as an annotation on the GitHub Actions run."""
    prefix = "::warning::" if os.environ.get("GITHUB_ACTIONS") else "WARN: "
    print(prefix + msg, flush=True)


def utcnow():
    return datetime.now(timezone.utc).replace(microsecond=0)


# ----------------------------------------------------------------------------
# HTTP client
# ----------------------------------------------------------------------------

class Client:
    def __init__(self, user_agent, budget_min=None):
        self.user_agent = user_agent
        self.requests = 0
        self._last = 0.0
        self.deadline = time.time() + budget_min * 60 if budget_min else None

    def _get(self, url):
        last = None
        for attempt in range(8):
            if self.deadline and time.time() > self.deadline:
                raise BudgetExhausted("API time budget spent")
            wait = MIN_INTERVAL - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()
            headers = {"User-Agent": self.user_agent, "Accept": "application/geo+json"}
            if API_KEY:
                headers["X-Api-Key"] = API_KEY
            try:
                self.requests += 1
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=180) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                cache = (e.headers.get("X-Cache") or "").lower() if e.headers else ""
                if e.code == 414 or (e.code == 403 and "cloudfront" in cache):
                    raise RequestTooLarge(f"HTTP {e.code} for a {len(url)}-byte URL")
                if e.code == 429:
                    ra = e.headers.get("Retry-After") if e.headers else None
                    pause = int(ra) if ra and str(ra).isdigit() else 60 * (attempt + 1)
                    if self.deadline and time.time() + pause > self.deadline:
                        raise BudgetExhausted("rate-limited beyond the API time budget")
                    print(f"rate-limited (429); waiting {pause}s", flush=True)
                    time.sleep(pause)
                    last = "HTTP 429"
                    continue
                if e.code < 500:
                    try:
                        body = e.read()[:300].decode("utf-8", "replace")
                    except (OSError, http.client.HTTPException, ValueError):
                        body = ""
                    raise ApiError(f"HTTP {e.code}: {body}")
                last = f"HTTP {e.code}"
            except (urllib.error.URLError, TimeoutError, OSError,
                    http.client.HTTPException, ValueError) as e:
                last = repr(e)
            time.sleep(min(90, 5 * 2 ** attempt))
        if last == "HTTP 429":
            raise BudgetExhausted("still rate-limited after repeated waits")
        raise ApiError(f"failed after retries: {url[:200]} ({last})")

    def items(self, collection, params, max_pages=None):
        """All features of a query, following rel=next verbatim. Returns
        (features, complete, pages); complete is False when the page cap cut
        it short."""
        q = {"f": "json", "limit": PAGE}
        q.update(params)
        url = f"{API}/{collection}/items?" + urllib.parse.urlencode(
            q, safe=",:/'", quote_via=urllib.parse.quote)
        if len(url) > URL_BUDGET:
            raise RequestTooLarge(f"{len(url)}-byte URL")
        feats, pages = [], 0
        cap = max_pages or MAX_PAGES
        while url:
            if pages >= cap:
                return feats, False, pages
            d = self._get(url)
            pages += 1
            page = d.get("features") or []
            feats.extend(page)
            nxt = [l["href"] for l in d.get("links", []) if l.get("rel") == "next"]
            url = nxt[0] if nxt and page else None
        return feats, True, pages

    def items_for_ids(self, collection, ids, params, id_field="monitoring_location_id"):
        """Same query over a list of IDs, split into URL-sized chunks.
        Returns (features, complete, single_page) where single_page means
        every chunk came back in one page (no keyset paging involved)."""
        feats, complete, single = [], True, True
        ids = list(ids)
        size = max(1, len(ids))
        while ids:
            chunk = ids[:size]
            try:
                got, ok, pages = self.items(collection, dict(params, **{id_field: ",".join(chunk)}))
            except RequestTooLarge:
                if size == 1:
                    raise
                size = max(1, size // 2)
                continue
            feats.extend(got)
            complete = complete and ok
            single = single and pages <= 1
            ids = ids[size:]
        return feats, complete, single


# ----------------------------------------------------------------------------
# Translation to the legacy formats the builders read
# ----------------------------------------------------------------------------

VALUE_CODES = [("LESSTHAN", "<"), ("GREATERTHAN", ">"), ("ESTIMATED", "e"), ("REVISED", "R")]
REASON_CODES = [("ICE", "Ice"), ("EQUIP", "Eqp"), ("UNAVAIL", "***"), ("RATINGDEV", "Rat"),
                ("ZEROFLOW", "ZFL"), ("SEASONAL", "Ssn"), ("MAINT", "Mnt"),
                ("DISCONTINUED", "Dis")]
IGNORED = {"BLWMIN", "FORCEINTERPOLATION", "DIFFDATUM", "REGULATED", "TEST",
           "ESTIMATED", "REVISED", "LESSTHAN", "GREATERTHAN"}


def legacy_qualifiers(approval_status, qualifier, value):
    """The single remark WaterServices reported: approval (A/P) plus at most
    one code - a value code when there is a value, else the reason code for
    the missing value. Verified to reproduce the legacy strings."""
    q = qualifier or []
    if isinstance(q, str):
        q = [t.strip() for t in q.split(",") if t.strip()]
    s = "A" if approval_status == "Approved" else "P"
    if value is not None:
        return next((f"{s} {t}" for c, t in VALUE_CODES if c in q), s)
    hit = next((f"{s} {t}" for c, t in REASON_CODES if c in q), None)
    if hit:
        return hit
    unknown = [c for c in q if c not in IGNORED]
    return f"{s} {unknown[0]}" if unknown else s


def value_text(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return repr(v)[:-2] if repr(v).endswith(".0") else repr(v)
    return str(v)


def num_text(v):
    if v in (None, ""):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def site_number(monitoring_location_id):
    """'USGS-02336000' -> '02336000'; agencies other than USGS exist
    (e.g. 'CA574-09527500')."""
    return monitoring_location_id.split("-", 1)[1] if "-" in monitoring_location_id else monitoring_location_id


def legacy_site(props, geometry):
    coords = (geometry or {}).get("coordinates") or [None, None]
    lon, lat = coords[0], coords[1]
    return {
        "agency_cd": props.get("agency_code") or "",
        "site_no": props.get("monitoring_location_number") or "",
        "station_nm": props.get("monitoring_location_name") or "",
        "site_tp_cd": props.get("site_type_code") or "",
        "dec_lat_va": num_text(round(lat, 8) if lat is not None else None),
        "dec_long_va": num_text(round(lon, 8) if lon is not None else None),
        "dec_coord_datum_cd": "WGS84" if lat is not None else "",
        "state_cd": props.get("state_code") or "",
        "county_cd": props.get("county_code") or "",
        "huc_cd": props.get("hydrologic_unit_code") or "",
        "basin_cd": props.get("basin_code") or "",
        "alt_va": num_text(props.get("altitude")),
        "alt_datum_cd": props.get("vertical_datum") or "",
        "drain_area_va": num_text(props.get("drainage_area")),
        "contrib_drain_area_va": num_text(props.get("contributing_drainage_area")),
        "well_depth_va": num_text(props.get("well_constructed_depth")),
        "hole_depth_va": num_text(props.get("hole_constructed_depth")),
        "nat_aqfr_cd": props.get("national_aquifer_code") or "",
        "aqfr_cd": props.get("aquifer_code") or "",
        "aqfr_type_cd": props.get("aquifer_type_code") or "",
        "construction_dt": props.get("construction_date") or "",
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


def write_rdb(path, sites, source):
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(f"# Site metadata from the USGS Water Data APIs ({source}),\n")
        f.write("# written in the legacy NWIS RDB column layout. Coordinates are WGS84.\n")
        f.write("\t".join(RDB_COLS) + "\n")
        for s in sorted(sites, key=lambda r: r["site_no"]):
            f.write("\t".join(str(s.get(c, "")).replace("\t", " ") for c in RDB_COLS) + "\n")


def method_desc(series_props):
    sub = html.unescape(series_props.get("sublocation_identifier") or "")
    web = series_props.get("web_description")
    return ", ".join(p for p in (sub, f"[{web}]" if web else "") if p)


# ----------------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------------

class Pipeline:
    """kind: 'streamflow' or 'groundwater'. rdb_files maps the site-file names
    the builder reads to 'state' (in-state stations) or 'extra' (stations
    picked up by extra_bbox, e.g. the GA-FL border strip)."""

    def __init__(self, root, kind, state_code, parameters, user_agent, rdb_files,
                 extra_bbox=None, site_type_prefix=None, reconcile_bins=2,
                 all_sites_rdb=None, budget_min=25, restore_exclude=()):
        self.root = root
        self.kind = kind
        self.state_code = state_code
        self.parameters = list(parameters)
        self.rdb_files = rdb_files
        self.extra_bbox = extra_bbox
        self.site_type_prefix = site_type_prefix
        self.reconcile_bins = int(os.environ.get("RECONCILE_BINS") or reconcile_bins)
        self.all_sites_rdb = all_sites_rdb
        self.restore_exclude = tuple(restore_exclude)
        self.known_ids = set()
        self.snapshot = None
        self.api = Client(user_agent, budget_min)
        self.data = os.path.join(root, "data")
        self.csv_dir = os.path.join(self.data, "csv")
        self.raw = os.path.join(self.data, "raw")
        self.state_dir = os.path.join(self.data, "state")
        for d in (self.csv_dir, self.raw, self.state_dir):
            os.makedirs(d, exist_ok=True)
        self.cols = SF_COLS if kind == "streamflow" else GW_COLS

    # --- dataset archive ---------------------------------------------------

    def _download(self, url, path):
        """True when downloaded, False when the asset does not exist (404).
        Any other persistent failure raises: guessing would risk publishing a
        rolled-back dataset."""
        last = None
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": self.api.user_agent})
                with urllib.request.urlopen(req, timeout=900) as resp, open(path, "wb") as f:
                    shutil.copyfileobj(resp, f)
                return True
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return False
                last = f"HTTP {e.code}"
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as e:
                last = repr(e)
            time.sleep(20 * (attempt + 1))
        raise RuntimeError(f"could not download {url} ({last})")

    def _extract(self, path, label):
        with tarfile.open(path, "r:gz") as tar:
            members = [m for m in tar.getmembers()
                       if m.isfile() and not m.name.startswith(("/", ".."))
                       and ".." not in m.name.split("/")
                       and not (self.restore_exclude and m.name.startswith(self.restore_exclude))]
            tar.extractall(self.data, members=members)
        csv_times = [m.mtime for m in members if m.name.startswith("csv/")]
        if csv_times:
            self.snapshot = datetime.fromtimestamp(max(csv_times), timezone.utc)
        print(f"restored {len(members)} files from {label}", flush=True)

    def restore(self):
        """Unpack the previous dataset archive (this repo's nightly 'dataset'
        release) over data/. Outside GitHub Actions, WATERDATA_ARCHIVE may
        name a local archive; with neither, the local data/ directory is used
        as-is. The frozen 'legacy-final' archive is used only when the
        'dataset' release has no archive at all and WATERDATA_ALLOW_SEED=1."""
        local = os.environ.get("WATERDATA_ARCHIVE")
        if local:
            self._extract(local, local)
            return
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        if not repo:
            if os.listdir(self.csv_dir):
                print("using the local data directory as the previous dataset", flush=True)
                return
            raise RuntimeError("no previous dataset available to update")
        path = os.path.join(tempfile.gettempdir(), "previous_dataset.tar.gz")
        base = f"https://github.com/{repo}/releases/download"
        # dataset-next.tar.gz exists only if a run died between uploading the
        # new archive and renaming it into place
        for name in ("dataset.tar.gz", "dataset-next.tar.gz"):
            if self._download(f"{base}/dataset/{name}", path):
                self._extract(path, f"dataset/{name}")
                return
        if os.environ.get("WATERDATA_ALLOW_SEED") == "1" and \
                self._download(f"{base}/legacy-final/dataset.tar.gz", path):
            self._extract(path, "legacy-final/dataset.tar.gz")
            return
        raise RuntimeError("the 'dataset' release has no archive; set WATERDATA_ALLOW_SEED=1 "
                           "to rebuild from the frozen legacy-final archive")

    def load_state(self):
        path = os.path.join(self.state_dir, "sync_state.json")
        if not os.path.exists(path):
            if os.path.exists(os.path.join(self.raw, "series.json")) and \
                    os.environ.get("WATERDATA_ALLOW_SEED") != "1":
                raise RuntimeError("the restored archive was written by this sync but has no "
                                   "sync state; refusing to re-seed (set WATERDATA_ALLOW_SEED=1)")
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def save_state(self, state):
        path = os.path.join(self.state_dir, "sync_state.json")
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(state, f, indent=1, sort_keys=True)
        os.replace(path + ".tmp", path)

    # --- metadata ----------------------------------------------------------

    def fetch_series(self):
        """combined-metadata rows for the pipeline's parameters' daily series,
        in the state (and in extra_bbox). Returns the raw feature list."""
        base = {"parameter_code": ",".join(self.parameters), "data_type": "Daily values",
                "properties": SERIES_PROPS}
        feats, ok, _ = self.api.items("combined-metadata", dict(base, state_code=self.state_code))
        if not ok:
            raise ApiError("combined-metadata did not fit the page cap")
        for f in feats:
            f["_scope"] = "state"
        if self.extra_bbox:
            extra, ok, _ = self.api.items("combined-metadata", dict(base, bbox=self.extra_bbox))
            if not ok:
                raise ApiError("combined-metadata (extra area) did not fit the page cap")
            seen = {f["properties"]["id"] for f in feats}
            for f in extra:
                if f["properties"]["id"] not in seen:
                    f["_scope"] = "extra"
                    feats.append(f)
        return feats

    def in_type(self, props):
        return not self.site_type_prefix or \
            (props.get("site_type_code") or "").startswith(self.site_type_prefix)

    def build_catalog(self, feats):
        """In-scope series map {series_id: info}, from combined-metadata rows:
        every series with daily data (non-null begin). As with WaterServices,
        a station's secondary (non-primary) series are kept alongside its
        primary one; the builders plot the most complete series and fill its
        gaps from the others."""
        series = {}
        for f in feats:
            p = f["properties"]
            if not self.in_type(p) or not p.get("begin"):
                continue
            site = site_number(p["monitoring_location_id"])
            parm, stat = p.get("parameter_code") or "", p.get("statistic_id") or ""
            series[p["id"]] = {
                "site": site, "mlid": p["monitoring_location_id"], "parm": parm,
                "stat": stat, "begin": p["begin"][:10],
                "end": (p.get("end") or "")[:10] or None,
                "primary": p.get("primary") == "Primary",
                "desc": method_desc(p), "scope": f.get("_scope", "state"),
            }
        return series

    def refresh_metadata(self, state):
        """Fetch series + site metadata; write the site files the builder
        reads. Returns (series, fresh). A catalog much smaller than the
        previous one is merged with it until it has repeated for
        SHRINK_NIGHTS nights, so neither a bad response nor a real retirement
        of many series can freeze the metadata."""
        prev_path = os.path.join(self.raw, "series.json")
        prev = {}
        if os.path.exists(prev_path):
            with open(prev_path, encoding="utf-8") as f:
                prev = json.load(f)
        try:
            feats = self.fetch_series()
        except ApiError as e:
            if prev:
                note(f"series metadata unavailable ({e}); keeping the previous catalog")
                return prev, False
            raise
        current = self.build_catalog(feats)
        self.known_ids = {f["properties"]["id"] for f in feats}
        fresh = True
        series = current
        if prev and len(current) < 0.9 * len(prev):
            nights = state.get("catalog_shrink_nights", 0) + 1
            if nights < SHRINK_NIGHTS:
                state["catalog_shrink_nights"] = nights
                note(f"series catalog shrank from {len(prev)} to {len(current)} "
                     f"(night {nights} of {SHRINK_NIGHTS}); keeping the retired series meanwhile")
                series = dict(prev)
                series.update(current)
                fresh = False
            else:
                note(f"series catalog shrank from {len(prev)} to {len(current)} for "
                     f"{SHRINK_NIGHTS} nights; accepting it")
        if fresh:
            state.pop("catalog_shrink_nights", None)
        sites = {}
        for f in feats:
            if not self.in_type(f["properties"]):
                continue
            s = legacy_site(f["properties"], f.get("geometry"))
            sites.setdefault(s["site_no"], (s, f.get("_scope", "state")))
        # stations published from the legacy record but absent from the new
        # metadata keep their previous site rows
        have_csv = {fn[5:-4] for fn in os.listdir(self.csv_dir) if fn.startswith("USGS_")}
        for name, scope in self.rdb_files.items():
            path = os.path.join(self.raw, name)
            if os.path.exists(path):
                for r in parse_rdb(path):
                    sn = r.get("site_no", "")
                    if sn in have_csv and sn not in sites:
                        sites[sn] = ({c: r.get(c, "") for c in RDB_COLS}, scope)
        for name, scope in self.rdb_files.items():
            write_rdb(os.path.join(self.raw, name),
                      [s for s, sc in sites.values() if sc == scope], "combined-metadata")
        with open(prev_path, "w", encoding="utf-8") as f:
            json.dump(series, f, indent=0, sort_keys=True)
        print(f"metadata: {len(series)} in-scope daily series at "
              f"{len({v['site'] for v in series.values()})} stations; {len(sites)} site rows", flush=True)
        return series, fresh

    def refresh_all_sites(self, state, max_age_days=7):
        """Every groundwater site in the state (monitoring-locations), for the
        periodic-well metadata; refreshed weekly."""
        if not self.all_sites_rdb:
            return
        path = os.path.join(self.raw, self.all_sites_rdb)
        last = state.get("all_sites_refreshed", "")
        if os.path.exists(path) and last and \
                (utcnow().date() - datetime.fromisoformat(last).date()).days < max_age_days:
            return
        try:
            feats, ok, _ = self.api.items("monitoring-locations", {
                "state_code": self.state_code, "filter": "site_type_code LIKE 'GW%'",
                "properties": SITE_PROPS})
        except ApiError as e:
            note(f"all-sites metadata unavailable ({e}); keeping the previous file")
            return
        prev_n = len(parse_rdb(path)) if os.path.exists(path) else 0
        if not ok or len(feats) < 0.9 * prev_n:
            note(f"all-sites metadata looks incomplete ({len(feats)} vs {prev_n}); keeping the previous file")
            return
        sites = []
        for f in feats:
            p = dict(f["properties"])
            p.setdefault("monitoring_location_number", site_number(f.get("id", "")))
            sites.append(legacy_site(p, f.get("geometry")))
        write_rdb(path, sites, "monitoring-locations")
        state["all_sites_refreshed"] = utcnow().date().isoformat()
        print(f"all-sites metadata: {len(sites)} groundwater sites", flush=True)

    # --- station store -----------------------------------------------------

    def csv_path(self, site):
        return os.path.join(self.csv_dir, f"USGS_{site}.csv")

    def load_site(self, site):
        """{(series_id, date): row-dict} for one station's CSV."""
        rows = {}
        path = self.csv_path(site)
        if not os.path.exists(path):
            return rows
        idcol = "method_id" if self.kind == "streamflow" else "time_series_id"
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                sid = r.get(idcol) or "legacy"
                if self.kind == "groundwater" and sid == "legacy":
                    sid = f"legacy:{r.get('parm_cd', '')}:{r.get('stat_cd', '')}"
                rows[(sid, r["date"])] = r
        return rows

    def save_site(self, site, rows, series):
        if not rows:
            return
        def rank(sid):
            info = series.get(sid)
            if info is None:
                return (2, "")
            return (0 if info.get("primary") else 1, info.get("begin") or "")
        if self.kind == "streamflow":
            order = sorted(rows.items(), key=lambda kv: (kv[1]["stat_cd"], rank(kv[0][0]),
                                                         kv[0][0], kv[0][1]))
        else:
            order = sorted(rows.items(), key=lambda kv: (kv[1]["parm_cd"], kv[1]["stat_cd"],
                                                         rank(kv[0][0]), kv[0][0], kv[0][1]))
        tmp = self.csv_path(site) + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.cols, extrasaction="ignore")
            w.writeheader()
            for _, r in order:
                w.writerow(r)
        os.replace(tmp, self.csv_path(site))

    def v1_row(self, p, info):
        value = p.get("value")
        quals = legacy_qualifiers(p.get("approval_status"), p.get("qualifier"), value)
        if self.kind == "streamflow":
            return {"site_no": info["site"], "date": p["time"], "stat_cd": info["stat"],
                    "discharge_cfs": value_text(value), "qualifiers": quals,
                    "method_id": p["time_series_id"], "method_desc": info["desc"]}
        return {"site_no": info["site"], "date": p["time"], "parm_cd": info["parm"],
                "stat_cd": info["stat"], "value": value_text(value), "qualifiers": quals,
                "time_series_id": p["time_series_id"]}

    def row_key(self, r):
        return (r["stat_cd"],) if self.kind == "streamflow" else (r["parm_cd"], r["stat_cd"])

    def info_key(self, info):
        return (info["stat"],) if self.kind == "streamflow" else (info["parm"], info["stat"])

    def coverage(self, site, series):
        """{(stat,) or (parm, stat): [(begin, end), ...]} of in-scope series."""
        cov = {}
        for info in series.values():
            if info["site"] == site:
                cov.setdefault(self.info_key(info), []).append(
                    (info["begin"], info["end"] or "9999-12-31"))
        return cov

    @staticmethod
    def covered(cov, key, date):
        return any(b <= date <= e for b, e in cov.get(key, ()))

    def row_estimate(self, site):
        path = self.csv_path(site)
        return os.path.getsize(path) // 55 if os.path.exists(path) else 1000

    def bins(self, sites):
        """Consecutive groups of stations of at most BIN_ROWS stored rows and
        BIN_IDS ids (a station larger than a bin gets a bin of its own)."""
        group, rows = [], 0
        for s in sites:
            n = self.row_estimate(s)
            if group and (rows + n > BIN_ROWS or len(group) >= BIN_IDS):
                yield group
                group, rows = [], 0
            group.append(s)
            rows += n
        if group:
            yield group

    # --- seeding from the legacy archive -----------------------------------

    def seed(self, series):
        """Label legacy rows with the v1 series that covers them. Stations
        whose series cannot be matched one-to-one (several series, or several
        legacy methods, for one statistic) are returned for a full v1 fetch."""
        by_key = {}
        for sid, info in series.items():
            by_key.setdefault((info["site"],) + self.info_key(info), []).append(sid)
        refetch, seeded, legacy = set(), 0, 0
        for fn in sorted(os.listdir(self.csv_dir)):
            if not (fn.startswith("USGS_") and fn.endswith(".csv")):
                continue
            site = fn[5:-4]
            with open(self.csv_path(site), encoding="utf-8", newline="") as f:
                old = list(csv.DictReader(f))
            methods, dates = {}, set()
            for r in old:
                rk = self.row_key(r)
                methods.setdefault(rk, set()).add(r.get("method_id", "") if self.kind == "streamflow" else "")
                if self.kind == "groundwater":
                    if (rk, r["date"]) in dates:
                        refetch.add(site)
                    dates.add((rk, r["date"]))
            new = {}
            for r in old:
                rk = self.row_key(r)
                cands = by_key.get((site,) + rk, [])
                one_to_one = len(cands) == 1 and len(methods[rk]) == 1
                if not one_to_one and (len(cands) > 1 or len(methods[rk]) > 1):
                    refetch.add(site)
                r = dict(r)
                date = r["date"]
                if one_to_one:
                    info = series[cands[0]]
                    if info["begin"] <= date <= (info["end"] or "9999-12-31"):
                        if self.kind == "streamflow":
                            r["method_id"], r["method_desc"] = cands[0], info["desc"]
                        else:
                            r["time_series_id"] = cands[0]
                        new[(cands[0], date)] = r
                        seeded += 1
                        continue
                if self.kind == "streamflow":
                    r["method_id"] = r.get("method_id") or "legacy"
                    lk = (r["method_id"], date)
                else:
                    r["time_series_id"] = "legacy"
                    lk = (f"legacy:{r['parm_cd']}:{r['stat_cd']}", date)
                if lk in new:
                    continue
                new[lk] = r
                legacy += 1
            self.save_site(site, new, series)
        print(f"seed: {seeded} rows matched to v1 series, {legacy} legacy-only rows; "
              f"{len(refetch)} stations need a full v1 fetch", flush=True)
        return refetch

    # --- applying v1 data ----------------------------------------------------

    def apply_rows(self, feats, series, replace_sites=None, single_page=True):
        """Upsert v1 daily rows. A v1 row supersedes any non-v1 (legacy) row
        for the same statistic and date. For stations in replace_sites (a
        complete full record was fetched), v1 is authoritative within its
        series' coverage: stored rows there are dropped first, so deletions
        and withdrawn values disappear - unless the response is implausibly
        small for what is stored. Returns (changed, unknown_series_sites)."""
        by_site, unknown, seen = {}, set(), set()
        for f in feats:
            p = f["properties"]
            sid = p.get("time_series_id")
            info = series.get(sid)
            if info is None:
                if sid not in self.known_ids:
                    unknown.add(site_number(p.get("monitoring_location_id") or ""))
                continue
            if (sid, p.get("time")) in seen:
                continue
            seen.add((sid, p.get("time")))
            by_site.setdefault(info["site"], []).append((p, info))
        replace_sites = set(replace_sites or ())
        changed_total = 0
        for site in set(by_site) | replace_sites:
            rows = self.load_site(site)
            before = dict(rows)
            got = by_site.get(site, [])
            if site in replace_sites:
                cov = self.coverage(site, series)
                inside = [k for k, r in rows.items() if self.covered(cov, self.row_key(r), k[1])]
                floor = 0.5 if single_page else 0.95
                if inside and len(got) < floor * len(inside):
                    note(f"station {site}: v1 returned {len(got)} rows for {len(inside)} stored "
                         f"in its coverage; not replacing")
                else:
                    for k in inside:
                        del rows[k]
            stale = {}
            for k, r in rows.items():
                if k[0] not in series:
                    stale.setdefault((self.row_key(r), k[1]), []).append(k)
            for p, info in got:
                for k in stale.pop((self.info_key(info), p["time"]), ()):
                    rows.pop(k, None)
                rows[(p["time_series_id"], p["time"])] = self.v1_row(p, info)
            changed = sum(1 for k, r in rows.items() if before.get(k) != r)
            changed += sum(1 for k in before if k not in rows)
            if changed:
                changed_total += changed
                self.save_site(site, rows, series)
        return changed_total, unknown - set(by_site)

    def refresh_sites(self, sites, series, label):
        """Full v1 records for stations, in bins. Returns (changed, complete
        stations); stations whose bin was cut short are left for a later run."""
        changed, done = 0, set()
        for group in self.bins(sorted(sites)):
            ids = sorted({i["mlid"] for i in series.values() if i["site"] in group})
            if not ids:
                done |= set(group)
                continue
            feats, complete, single = self.api.items_for_ids("daily", ids, {
                "parameter_code": ",".join(self.parameters), "skipGeometry": "true",
                "properties": DAILY_PROPS})
            up, _ = self.apply_rows(feats, series, replace_sites=set(group) if complete else (),
                                    single_page=single)
            changed += up
            if complete:
                done |= set(group)
            else:
                note(f"{label}: full records for {len(group)} stations exceeded the page cap; "
                     f"applied as updates only")
        return changed, done

    # --- incremental -------------------------------------------------------

    def fetch_window(self, lo, hi, series):
        """/daily rows with last_modified in [lo, hi]."""
        interval = f"{lo:%Y-%m-%dT%H:%M:%SZ}/{hi:%Y-%m-%dT%H:%M:%SZ}"
        base = {"parameter_code": ",".join(self.parameters), "last_modified": interval,
                "skipGeometry": "true", "properties": DAILY_PROPS}
        feats, complete, _ = self.api.items("daily", dict(base, state_code=self.state_code))
        extra_ids = sorted({i["mlid"] for i in series.values() if i["scope"] == "extra"})
        if extra_ids:
            more, ok, _ = self.api.items_for_ids("daily", extra_ids, base)
            feats += more
            complete = complete and ok
        return feats, complete

    def incremental(self, state, series):
        """Apply every change since the watermark. The span is one window
        unless it overflows the page cap, when it is halved recursively
        (down to an hour; a burst that still overflows is applied partially
        and left to the reconcile). The watermark advances only over windows
        that finished, so a run cut short resumes where it stopped.
        Returns (changed, unknown_sites, finished)."""
        now = utcnow()
        wm = datetime.fromisoformat(state["daily_watermark"])
        queue = [(wm - OVERLAP, now)]
        reached, changed, unknown = None, 0, set()
        while queue:
            lo, hi = queue.pop(0)
            try:
                feats, complete = self.fetch_window(lo, hi, series)
            except ApiError as e:
                if reached is None:
                    raise
                note(f"incremental stopped at {lo:%Y-%m-%d %H:%M} UTC ({e}); "
                     f"the next run resumes from there")
                break
            if not complete and hi - lo > timedelta(hours=1):
                mid = lo + (hi - lo) / 2
                queue[:0] = [(lo, mid), (mid, hi)]
                continue
            if not complete:
                note(f"changes stamped {lo:%Y-%m-%d %H:%M}-{hi:%H:%M} UTC exceed the page cap; "
                     f"applied partially (the rolling reconcile repairs the rest)")
            if not feats and hi - lo >= timedelta(hours=24):
                note(f"no daily-value changes reported for {lo:%Y-%m-%d %H:%M} to "
                     f"{hi:%Y-%m-%d %H:%M} UTC; not advancing the watermark past it")
                break
            up, unk = self.apply_rows(feats, series)
            changed += up
            unknown |= unk
            reached = hi
            print(f"incremental: {lo:%Y-%m-%d %H:%M} to {hi:%Y-%m-%d %H:%M} UTC: "
                  f"{len(feats)} rows, {up} changed", flush=True)
        finished = not queue and reached == now
        if reached is not None:
            state["daily_watermark"] = max(wm, reached).isoformat()
        return changed, unknown, finished

    # --- reconcile -----------------------------------------------------------

    def reconcile(self, state, series):
        sites = sorted({i["site"] for i in series.values()})
        if not sites or self.reconcile_bins <= 0:
            return 0
        pos = state.get("reconcile_pos", 0) % len(sites)
        rotated = sites[pos:] + sites[:pos]
        done = changed = 0
        for n, group in enumerate(self.bins(rotated)):
            if n >= self.reconcile_bins:
                break
            try:
                up, _ = self.refresh_sites(set(group), series, "reconcile")
                changed += up
            except BudgetExhausted:
                note("reconcile stopped: API time budget spent")
                break
            except ApiError as e:
                note(f"reconcile bin at {group[0]} failed ({e}); skipping it")
            done += len(group)
        pos = (pos + done) % len(sites)
        state["reconcile_pos"] = pos
        print(f"reconcile: {done} stations re-read in full, {changed} rows changed "
              f"(next starts at station {pos + 1} of {len(sites)})", flush=True)
        return changed

    # --- entry point ---------------------------------------------------------

    def run(self):
        self.restore()
        state = self.load_state()
        series, fresh = self.refresh_metadata(state)
        if self.all_sites_rdb:
            self.refresh_all_sites(state)
        if not self.known_ids:
            self.known_ids = set(state.get("known_series", [])) | set(series)
        prev_ids = set(state.get("series_ids", []))
        total = 0
        if "daily_watermark" not in state:
            refetch = self.seed(series)
            if refetch:
                up, done = self.refresh_sites(refetch, series, "seed")
                total += up
                if done != refetch:
                    raise ApiError(f"seed: {len(refetch - done)} stations could not be fetched in full")
            start = min(self.snapshot or utcnow(), utcnow()) - timedelta(days=7)
            if self.snapshot and utcnow() - self.snapshot > timedelta(days=14):
                note(f"seeding from an archive built {self.snapshot:%Y-%m-%d}; the first "
                     f"update reaches back to then")
            state["daily_watermark"] = start.isoformat()
            state["seeded"] = utcnow().date().isoformat()
            prev_ids = set(series)
        up, unknown, finished = self.incremental(state, series)
        total += up
        in_scope = {i["site"] for i in series.values()}
        new_sites = {series[s]["site"] for s in set(series) - prev_ids} | (unknown & in_scope)
        done = set()
        if new_sites:
            try:
                up, done = self.refresh_sites(new_sites, series, "new series")
                total += up
                print(f"new series: full records for {len(done)} of {len(new_sites)} stations",
                      flush=True)
            except ApiError as e:
                note(f"full records for new series failed ({e}); retrying next run")
        if fresh:
            state["series_ids"] = sorted(s for s, i in series.items()
                                         if s in prev_ids or i["site"] in done)
            state["known_series"] = sorted(self.known_ids)
        if finished:
            try:
                total += self.reconcile(state, series)
            except ApiError as e:
                note(f"reconcile skipped ({e})")
        else:
            note("skipping the reconcile until the incremental update has caught up")
        state["last_run"] = utcnow().isoformat()
        state["last_requests"] = self.api.requests
        self.save_state(state)
        print(f"waterdata: {total} rows changed, {self.api.requests} API requests", flush=True)
        return series
