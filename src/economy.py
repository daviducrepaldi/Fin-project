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
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from typing import Optional

import requests

_FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_FOMC_URL = "https://www.federalreserve.gov/monetaryPolicy/fomccalendars.htm"
_HEADERS  = {"User-Agent": "Mozilla/5.0 (FinancialAnalyzerApp)"}
_LOOKBACK_DAYS = 800   # enough for a YoY figure plus the prior month's YoY

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
    ("GACDFSA066MSFRBPHI", "Philly Fed Mfg Index",  "Growth",    "Leading",    "level", "",   "monthly"),
]
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
    """FRED key from the environment (fetcher._load_env fills it from .env)."""
    return os.environ.get("FRED_API_KEY") or None


def _next_date_for_release(release_id: int, api_key: str, today: date) -> Optional[str]:
    try:
        r = requests.get(_FRED_API, params={
            "release_id": release_id, "api_key": api_key, "file_type": "json",
            "include_release_dates_with_no_data": "true",
            "realtime_start": today.isoformat(), "realtime_end": "9999-12-31",
            "sort_order": "asc", "limit": 1,
        }, headers=_HEADERS, timeout=15)
        r.raise_for_status()
        dates = r.json().get("release_dates") or []
        return dates[0]["date"] if dates else None
    except (requests.RequestException, ValueError, KeyError):
        return None


def next_release_dates(api_key: str = None, today: date = None) -> dict:
    """Series id → next scheduled release date (ISO). {} without a key or on
    failure — the UI then simply omits the line."""
    if not api_key:
        return {}
    today = today or date.today()
    ids = sorted(set(RELEASE_IDS.values()))
    with ThreadPoolExecutor(max_workers=5) as pool:
        by_release = dict(zip(ids, pool.map(
            lambda rid: _next_date_for_release(rid, api_key, today), ids)))
    return {sid: by_release[rid] for sid, rid in RELEASE_IDS.items()
            if by_release.get(rid)}


# ── network ──────────────────────────────────────────────────────────────────

def fetch_series(series_id: str, today: date = None, retries: int = 3) -> list:
    """One FRED series, last ~800 days. Returns [] after `retries` failures
    (FRED's CSV endpoint occasionally times out or drops a connection)."""
    if not _SERIES_ID_RE.match(series_id):
        return []
    start = ((today or date.today()) - timedelta(days=_LOOKBACK_DAYS)).isoformat()
    for attempt in range(retries):
        try:
            r = requests.get(_FRED_CSV, params={"id": series_id, "cosd": start},
                             headers=_HEADERS, timeout=20)
            r.raise_for_status()
            rows = parse_fred_csv(r.text)
            if rows:
                return rows
        except requests.RequestException:
            pass
        time.sleep(1.5 * (attempt + 1))
    return []


def fetch_fomc_dates() -> list:
    """Scraped FOMC meeting dates (all years on the page), [] on failure."""
    try:
        r = requests.get(_FOMC_URL, headers=_HEADERS, timeout=15)
        r.raise_for_status()
        return parse_fomc_dates(r.text)
    except requests.RequestException:
        return []


def fetch_dashboard(api_key: str = None) -> dict:
    """
    Pull every indicator (in parallel) plus the FOMC calendar.
    Returns {'indicators': [row...], 'fomc': {...}|None, 'errors': [ids]},
    where each row carries label/group/timing/unit/freq + summarize() output.
    """
    with ThreadPoolExecutor(max_workers=4) as pool:
        fetched = list(pool.map(lambda ind: fetch_series(ind[0]), INDICATORS))
    fomc_dates = fetch_fomc_dates()
    releases = next_release_dates(api_key)

    rows, errors = [], []
    for (sid, label, group, timing, kind, unit, freq), series in zip(INDICATORS, fetched):
        s = summarize(series, kind)
        if s is None:
            errors.append(sid)
            continue
        rows.append({"id": sid, "label": label, "group": group, "timing": timing,
                     "unit": unit, "freq": freq,
                     "next_release": releases.get(sid), **s})
    return {"indicators": rows, "fomc": next_fomc(fomc_dates), "errors": errors}
