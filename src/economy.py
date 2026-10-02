"""
economy.py — macro indicators (inflation, jobs, rates, growth) and FOMC dates.

Everything except the two fetch functions is pure and offline-testable.
Series come from FRED's public CSV endpoint (fredgraph.csv), which needs no
API key; the FOMC calendar is scraped from federalreserve.gov. Per-indicator
"next release" dates need a FRED_API_KEY (BLS blocks scripts, FRED's release
calendar requires a key); without one they are simply omitted.
"""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
from typing import Optional

import requests

_FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_FOMC_URL = "https://www.federalreserve.gov/monetaryPolicy/fomccalendars.htm"
_HEADERS  = {"User-Agent": "Mozilla/5.0 (FinancialAnalyzerApp)"}
_LOOKBACK_DAYS = 800   # enough for a YoY figure plus the prior month's YoY

# ISM isn't on FRED (licence withdrawn) and ismworld.org sits behind a bot
# check, so its monthly reading comes from ISM's own press release on PR
# Newswire — see fetch_ism_pmi. Its id is not a FRED series id.
ISM_ID = "ISM_PMI"

# kind decides how the headline number is derived from the raw series:
#   level → latest value            yoy → % change vs 12 obs earlier
#   diff  → change vs prior obs     mom → % change vs prior obs
# `unit` is the display suffix; `delta_unit` the suffix of the change line.
INDICATORS = [
    # id,                 label,                    group,       timing,       kind,    unit, freq
    ("CPIAUCSL",          "CPI (YoY)",              "Inflation", "Lagging",    "yoy",   "%",  "monthly"),
    ("PCEPI",             "PCE Price Index (YoY)",  "Inflation", "Lagging",    "yoy",   "%",  "monthly"),
    ("PAYEMS",            "Nonfarm Payrolls (MoM)", "Jobs",      "Coincident", "diff",  "K",  "monthly"),
    ("UNRATE",            "Unemployment Rate",      "Jobs",      "Lagging",    "level", "%",  "monthly"),
    ("ICSA",              "Initial Jobless Claims", "Jobs",      "Leading",    "level", "",   "weekly"),
    ("DFF",               "Fed Funds Rate",         "Rates",     "Policy",     "level", "%",  "daily"),
    ("DGS2",              "2Y Treasury Yield",      "Rates",     "Market",     "level", "%",  "daily"),
    ("DGS10",             "10Y Treasury Yield",     "Rates",     "Market",     "level", "%",  "daily"),
    ("T10Y2Y",            "10Y–2Y Spread",          "Rates",     "Leading",    "level", "pp", "daily"),
    ("A191RL1Q225SBEA",   "Real GDP (QoQ ann.)",    "Growth",    "Coincident", "level", "%",  "quarterly"),
    ("RSAFS",             "Retail Sales (MoM)",     "Growth",    "Coincident", "mom",   "%",  "monthly"),
    ("HOUST",             "Housing Starts",         "Growth",    "Leading",    "level", "K",  "monthly"),
    ("PERMIT",            "Building Permits",       "Growth",    "Leading",    "level", "K",  "monthly"),
    (ISM_ID,              "ISM Manufacturing PMI",  "Growth",    "Leading",    "level", "",   "monthly"),
    ("GACDFSA066MSFRBPHI", "Philly Fed Mfg Index",  "Growth",    "Leading",    "level", "",   "monthly"),
]
# Rule-of-thumb "what's a good number" per indicator: a one-line reading guide
# plus thresholds for a traffic-light status. These are widely used heuristics,
# not forecasts — and several (payroll breakeven, neutral rate) are debated.
# kind 'low'  → lower is better: good ≤ g, watch ≤ w, else bad
# kind 'high' → higher is better: good ≥ g, watch ≥ w, else bad
# No thresholds → informational only (no status shown).
BENCHMARKS = {
    "CPIAUCSL": {"text": "Fed wants ~2%. Above 3% = running hot", "kind": "low", "good": 2.5, "watch": 3.5},
    "PCEPI": {"text": "The Fed's official 2% target gauge. Above 3% = hot", "kind": "low", "good": 2.3, "watch": 3.0},
    "PAYEMS": {"text": "+100K to +200K = healthy. Below 0 = jobs lost", "kind": "high", "good": 100, "watch": 0},
    "UNRATE": {"text": "4–4.5% ≈ full employment. Above 5.5% = weak", "kind": "low", "good": 4.5, "watch": 5.5},
    "ICSA": {"text": "Below 250K = healthy. Above 300K = layoffs rising", "kind": "low", "good": 250_000, "watch": 300_000},
    "DFF": {"text": "Neutral rate ≈ 3% (Fed est.). Above = restrictive, below = easy"},
    "DGS2": {"text": "Tracks expected Fed moves. Above Fed funds = hikes priced in"},
    "DGS10": {"text": "Benchmark for mortgages and valuations. Higher = pressure on stocks"},
    "T10Y2Y": {"text": "Positive = normal curve. Negative (inverted) has preceded recessions", "kind": "high", "good": 0.25, "watch": 0.0},
    "A191RL1Q225SBEA": {"text": "2–3% = trend growth. Below 0 = contraction", "kind": "high", "good": 2.0, "watch": 0.0},
    "RSAFS": {"text": "+0.3% or more per month = solid. Negative = consumers pulling back", "kind": "high", "good": 0.3, "watch": 0.0},
    "HOUST": {"text": "~1.3M/yr = healthy. Below 1.1M = weak housing", "kind": "high", "good": 1300, "watch": 1100},
    "PERMIT": {"text": "Leads starts. ~1.3M/yr = healthy. Below 1.1M = weak", "kind": "high", "good": 1300, "watch": 1100},
    ISM_ID: {"text": "Above 50 = manufacturing expanding, below 50 = contracting. ~47.5 is the break-even for the overall economy", "kind": "high", "good": 50.0, "watch": 47.5},
    "GACDFSA066MSFRBPHI": {"text": "Above 0 = manufacturing expanding, below 0 = shrinking (like ISM's 50 line)", "kind": "high", "good": 5, "watch": -5},
}


def assess(series_id: str, value: float) -> Optional[str]:
    """'good' / 'watch' / 'bad' against BENCHMARKS, or None when the
    indicator has no thresholds (informational) or the value is missing."""
    b = BENCHMARKS.get(series_id)
    if not b or value is None or "kind" not in b:
        return None
    if b["kind"] == "low":
        return "good" if value <= b["good"] else "watch" if value <= b["watch"] else "bad"
    return "good" if value >= b["good"] else "watch" if value >= b["watch"] else "bad"


GROUP_ORDER = ["Inflation", "Jobs", "Rates", "Growth"]

_SERIES_ID_RE = re.compile(r"^[A-Z0-9]{2,30}$")


# ── parsing / math (pure) ────────────────────────────────────────────────────

def parse_fred_csv(text: str) -> list:
    """FRED CSV ('DATE,SERIES' header) → [(date_str, float)] oldest-first.
    Missing observations ('.', blank) are dropped."""
    rows = []
    for line in text.strip().splitlines()[1:]:
        parts = line.split(",")
        if len(parts) < 2:
            continue
        try:
            rows.append((parts[0].strip(), float(parts[1])))
        except ValueError:
            continue
    rows.sort(key=lambda r: r[0])
    return rows


def _value_at(rows: list, idx_from_end: int, kind: str):
    """Headline value as of the observation `idx_from_end` back (0 = latest).
    Returns None when there isn't enough history for the transform."""
    n = len(rows) + idx_from_end    # idx_from_end is ≤ 0 → position of the obs
    i = n - 1
    if i < 0:
        return None
    cur = rows[i][1]
    if kind == "level":
        return cur
    if kind in ("diff", "mom"):
        if i < 1:
            return None
        prev = rows[i - 1][1]
        if kind == "diff":
            return cur - prev
        return (cur / prev - 1) * 100 if prev else None
    if kind == "yoy":
        if i < 12:
            return None
        base = rows[i - 12][1]
        return (cur / base - 1) * 100 if base else None
    return None


def summarize(rows: list, kind: str) -> Optional[dict]:
    """Latest headline value, the previous period's value, and their change."""
    if not rows:
        return None
    latest = _value_at(rows, 0, kind)
    if latest is None:
        return None
    prior = _value_at(rows, -1, kind)
    return {
        "as_of":  rows[-1][0],
        "value":  round(latest, 2),
        "prior":  round(prior, 2) if prior is not None else None,
        "change": round(latest - prior, 2) if prior is not None else None,
        "history": [round(v, 2) for v in
                    (_value_at(rows, -k, kind) for k in range(min(len(rows), 36) - 1, -1, -1))
                    if v is not None],
    }


def parse_fomc_dates(html_text: str) -> list:
    """Federal Reserve calendar page → sorted list of meeting END dates
    (ISO strings), e.g. 'October 27-28' → '2026-10-28'. A trailing '*' marks
    a Summary-of-Economic-Projections meeting and is ignored here."""
    months = {m: i for i, m in enumerate(
        ["January", "February", "March", "April", "May", "June", "July",
         "August", "September", "October", "November", "December"], 1)}
    out = []
    sections = re.split(r'<a id="\d+">(\d{4}) FOMC Meetings</a>', html_text)
    # split yields [pre, year1, body1, year2, body2, ...]
    for year, body in zip(sections[1::2], sections[2::2]):
        for mon, days in re.findall(
                r'fomc-meeting__month[^>]*><strong>([A-Za-z/]+)</strong>.*?'
                r'fomc-meeting__date[^>]*>\s*([^<]+?)\s*<', body, flags=re.S):
            first_month = mon.split("/")[0]            # e.g. 'Apr/May' spans two
            last_month  = mon.split("/")[-1]
            if first_month not in months and last_month not in months:
                continue
            end_day = re.findall(r"\d+", days)
            if not end_day:
                continue
            m = months.get(last_month) or months.get(first_month)
            try:
                out.append(date(int(year), m, int(end_day[-1])).isoformat())
            except ValueError:
                continue
    return sorted(set(out))


def next_fomc(dates: list, today: date = None) -> Optional[dict]:
    """First meeting ending on/after today, plus days until its decision."""
    today = today or date.today()
    for d in dates:
        dd = datetime.strptime(d, "%Y-%m-%d").date()
        if dd >= today:
            return {"decision_date": d, "days_away": (dd - today).days}
    return None


# FRED release ids for the non-daily indicators (daily rate series have no
# meaningful "next release"). Resolved via /fred/series/release.
RELEASE_IDS = {
    "CPIAUCSL": 10,    # Consumer Price Index
    "PCEPI": 54,       # Personal Income and Outlays
    "PAYEMS": 50,      # Employment Situation
    "UNRATE": 50,
    "ICSA": 180,       # Weekly jobless claims
    "A191RL1Q225SBEA": 53,   # GDP
    "RSAFS": 9,        # Advance retail sales
    "HOUST": 27,       # New residential construction
    "PERMIT": 27,
    "GACDFSA066MSFRBPHI": 351,   # Philly Fed manufacturing survey
}
_FRED_API = "https://api.stlouisfed.org/fred/release/dates"


def get_api_key() -> Optional[str]:
    """FRED key from the environment, loading .env first (via fetcher's
    loader) so scripts like prefetch_data.py see it too."""
    from src import fetcher   # noqa: F401 — import runs fetcher._load_env()
    return os.environ.get("FRED_API_KEY") or None


# ── ISM manufacturing PMI (via PR Newswire press releases) ───────────────────

_PRN_SEARCH = ("https://www.prnewswire.com/search/news/"
               "?keyword=ISM%20Manufacturing%20PMI%20Report&pagesize=25")
_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}
# e.g. /news-releases/manufacturing-pmi-at-54-5-september-2026-ism-...
#      /news-releases/manufacturing-pmi-at-54-may-2026-ism-...   (whole number)
_ISM_SLUG_RE = re.compile(
    r"/news-releases/manufacturing-pmi-at-(\d{2})(?:-(\d))?-([a-z]+)-(\d{4})-ism-manufacturing-pmi-report")


def parse_ism_pmi(html_text: str) -> list:
    """PR Newswire search page → [('YYYY-MM-01', pmi)] oldest-first. The
    reading and report month are encoded in each release's URL slug."""
    out = {}
    for whole, frac, month, year in _ISM_SLUG_RE.findall(html_text):
        m = _MONTHS.get(month)
        if not m:
            continue
        out[f"{year}-{m:02d}-01"] = float(f"{whole}.{frac or 0}")
    return sorted(out.items())


def fetch_ism_pmi() -> list:
    """Recent ISM Manufacturing PMI readings, [] on failure."""
    try:
        r = requests.get(_PRN_SEARCH, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        return parse_ism_pmi(r.text)
    except requests.RequestException as e:
        _log(f"ism: {type(e).__name__}")
        return []


def next_ism_release(today: date = None, latest: str = None) -> str:
    """ISM publishes on the first business day of each month (10:00 ET).
    Weekends, New Year's Day and Labor Day are skipped; other holidays are
    not modelled, so treat as 'expected'. `latest` is the newest report
    month we already hold ('YYYY-MM-DD'): a release in month M reports month
    M-1, so if we have M-1 already, the next release is the following month's."""
    today = today or date.today()

    def first_bday(y, m):
        d = date(y, m, 1)
        while d.weekday() >= 5 or (m == 1 and d.day == 1) or \
                (m == 9 and d.weekday() == 0 and d.day <= 7):
            d += timedelta(days=1)
        return d

    def following(y, m):
        return (y + 1, 1) if m == 12 else (y, m + 1)

    y, m = today.year, today.month
    d = first_bday(y, m)
    prev_month = f"{y - 1 if m == 1 else y}-{12 if m == 1 else m - 1:02d}"
    if d < today or (latest and latest[:7] >= prev_month):
        y, m = following(y, m)
        d = first_bday(y, m)
    return d.isoformat()


# ── network ──────────────────────────────────────────────────────────────────
# Everything below runs under a hard overall deadline (DEADLINE_S): a slow or
# blocked FRED must degrade to "use the snapshot", never to a hung page.

_TIMEOUT    = (3, 6)    # (connect, read) seconds per request
DEADLINE_S  = 12        # whole-dashboard budget
_FRED_OBS   = "https://api.stlouisfed.org/fred/series/observations"


def _log(msg: str):
    import sys
    print(f"[economy] {msg}", file=sys.stderr)


def _start_date(today: date = None) -> str:
    return ((today or date.today()) - timedelta(days=_LOOKBACK_DAYS)).isoformat()


def parse_fred_json(payload: dict) -> list:
    """FRED API observations JSON → [(date, float)] oldest-first ('.' dropped)."""
    rows = []
    for o in payload.get("observations", []):
        try:
            rows.append((o["date"], float(o["value"])))
        except (KeyError, ValueError, TypeError):
            continue
    rows.sort(key=lambda r: r[0])
    return rows


def fetch_series(series_id: str, api_key: str = None, today: date = None) -> list:
    """One FRED series, last ~800 days; [] on failure. Uses the official API
    when a key is available (built for scripts), else the public CSV. Two
    quick attempts, no sleeping after the last."""
    if not _SERIES_ID_RE.match(series_id):
        return []
    start = _start_date(today)
    for attempt in range(2):
        try:
            if api_key:
                r = requests.get(_FRED_OBS, params={
                    "series_id": series_id, "api_key": api_key,
                    "file_type": "json", "observation_start": start,
                }, headers=_HEADERS, timeout=_TIMEOUT)
                r.raise_for_status()
                rows = parse_fred_json(r.json())
            else:
                r = requests.get(_FRED_CSV, params={"id": series_id, "cosd": start},
                                 headers=_HEADERS, timeout=_TIMEOUT)
                r.raise_for_status()
                rows = parse_fred_csv(r.text)
            if rows:
                return rows
            _log(f"{series_id}: empty response")
        except (requests.RequestException, ValueError) as e:
            _log(f"{series_id}: {type(e).__name__}")
        if attempt == 0:
            time.sleep(0.5)
    return []


def fetch_fomc_dates() -> list:
    """Scraped FOMC meeting dates (all years on the page), [] on failure."""
    try:
        r = requests.get(_FOMC_URL, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        return parse_fomc_dates(r.text)
    except requests.RequestException as e:
        _log(f"fomc: {type(e).__name__}")
        return []


def _next_date_for_release(release_id: int, api_key: str, today: date) -> Optional[str]:
    try:
        r = requests.get(_FRED_API, params={
            "release_id": release_id, "api_key": api_key, "file_type": "json",
            "include_release_dates_with_no_data": "true",
            "realtime_start": today.isoformat(), "realtime_end": "9999-12-31",
            "sort_order": "asc", "limit": 1,
        }, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        dates = r.json().get("release_dates") or []
        return dates[0]["date"] if dates else None
    except (requests.RequestException, ValueError, KeyError) as e:
        _log(f"release {release_id}: {type(e).__name__}")
        return None


def next_release_dates(api_key: str = None, today: date = None) -> dict:
    """Series id → next scheduled release date (ISO). {} without a key or on
    failure — the UI then simply omits the line. Bounded by DEADLINE_S."""
    if not api_key:
        return {}
    today = today or date.today()
    ids = sorted(set(RELEASE_IDS.values()))
    pool = ThreadPoolExecutor(max_workers=5)
    futs = {rid: pool.submit(_next_date_for_release, rid, api_key, today) for rid in ids}
    wait(list(futs.values()), timeout=DEADLINE_S)
    pool.shutdown(wait=False, cancel_futures=True)
    by_release = {rid: f.result() for rid, f in futs.items()
                  if f.done() and not f.cancelled() and f.exception() is None}
    return {sid: by_release[rid] for sid, rid in RELEASE_IDS.items()
            if by_release.get(rid)}


def fetch_dashboard(api_key: str = None, deadline: float = DEADLINE_S) -> dict:
    """
    Pull every indicator, the FOMC calendar and release dates in parallel,
    within `deadline` seconds overall — whatever hasn't finished is dropped.
    Returns {'indicators': [row...], 'fomc_dates': [...], 'errors': [ids],
    'fetched_at': ISO timestamp}; each row carries label/group/timing/unit/
    freq/next_release + summarize() output.
    """
    today = date.today()
    pool = ThreadPoolExecutor(max_workers=8)
    series_f = {ind[0]: (pool.submit(fetch_ism_pmi) if ind[0] == ISM_ID
                         else pool.submit(fetch_series, ind[0], api_key, today))
                for ind in INDICATORS}
    fomc_f = pool.submit(fetch_fomc_dates)
    rel_f = {}
    if api_key:
        rel_f = {rid: pool.submit(_next_date_for_release, rid, api_key, today)
                 for rid in sorted(set(RELEASE_IDS.values()))}

    wait(list(series_f.values()) + [fomc_f] + list(rel_f.values()), timeout=deadline)
    pool.shutdown(wait=False, cancel_futures=True)

    def _done(f, default):
        return f.result() if f.done() and not f.cancelled() and f.exception() is None else default

    by_release = {rid: _done(f, None) for rid, f in rel_f.items()}
    releases = {sid: by_release[rid] for sid, rid in RELEASE_IDS.items()
                if by_release.get(rid)}

    ism_rows = _done(series_f[ISM_ID], [])   # release date is computed, needs no API key
    releases[ISM_ID] = next_ism_release(today, ism_rows[-1][0] if ism_rows else None)

    rows, errors = [], []
    for (sid, label, group, timing, kind, unit, freq) in INDICATORS:
        sm = summarize(_done(series_f[sid], []), kind)
        if sm is None:
            errors.append(sid)
            continue
        rows.append({"id": sid, "label": label, "group": group, "timing": timing,
                     "unit": unit, "freq": freq,
                     "next_release": releases.get(sid), **sm})
    return {"indicators": rows, "fomc_dates": _done(fomc_f, []), "errors": errors,
            "fetched_at": datetime.now().isoformat(timespec="seconds")}


# ── snapshot (committed for Cloud cold starts / FRED outages) ────────────────

def merge_with_snapshot(live: dict, snapshot: Optional[dict]) -> dict:
    """Fill anything the live fetch missed from the snapshot, so a partial
    outage still renders a full dashboard. `stale_ids` lists snapshot-filled
    indicators so the UI can say so."""
    if not snapshot:
        return {**live, "stale_ids": []}
    have = {r["id"] for r in live["indicators"]}
    fill = [r for r in snapshot.get("indicators", []) if r["id"] not in have]
    order = {ind[0]: i for i, ind in enumerate(INDICATORS)}
    rows = sorted(live["indicators"] + fill, key=lambda r: order.get(r["id"], 99))
    return {
        "indicators": rows,
        "fomc_dates": live["fomc_dates"] or snapshot.get("fomc_dates", []),
        "errors": [i[0] for i in INDICATORS if i[0] not in {r["id"] for r in rows}],
        "fetched_at": live["fetched_at"],
        "snapshot_at": snapshot.get("fetched_at"),
        "stale_ids": [r["id"] for r in fill],
    }


def read_snapshot(path) -> Optional[dict]:
    import json
    try:
        with open(path) as f:
            d = json.load(f)
        return d if isinstance(d.get("indicators"), list) else None
    except (OSError, ValueError, AttributeError):
        return None


def write_snapshot(path, dashboard: dict) -> bool:
    import json
    try:
        with open(path, "w") as f:
            json.dump({k: dashboard[k] for k in ("indicators", "fomc_dates", "fetched_at")},
                      f, indent=1)
        return True
    except OSError:
        return False   # Cloud filesystem is read-only


def snapshot_is_fresh(snapshot: Optional[dict], max_age_hours: float = 24) -> bool:
    try:
        age = datetime.now() - datetime.fromisoformat(snapshot["fetched_at"])
        return age.total_seconds() < max_age_hours * 3600
    except (TypeError, KeyError, ValueError):
        return False
