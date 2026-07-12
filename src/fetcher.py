"""
Data sources
────────────
Price data   →  Tiingo end-of-day API (free, all tickers, no rate limits)
                API key in TIINGO_API_KEY env var / .env file

Financials   →  SEC EDGAR company facts API (XBRL)
                Completely free, no API key, no rate limits
                Authoritative source for all US public company filings

Ratios       →  Computed from price × EDGAR fundamentals
                market_cap, EV, PE, P/B, EV/EBITDA, EV/Revenue,
                beta (1Y daily returns vs. SPY), dividend yield
"""
import math
import os
import re
import time
from datetime import datetime, date, timedelta

import requests

from src import db

def _load_env():
    """Read .env and inject missing keys into os.environ. Uses inspect so the
    path is always correct regardless of how Streamlit sets __file__."""
    import inspect
    from pathlib import Path
    this_dir = Path(inspect.getfile(_load_env)).resolve().parent  # always the src/ dir
    candidates = [
        this_dir.parent / ".env",   # project root
        this_dir / ".env",           # src/ (fallback)
        Path.cwd() / ".env",         # wherever process was launched from
    ]
    for env_path in candidates:
        try:
            with open(env_path) as _f:
                for _line in _f:
                    _line = _line.strip()
                    if _line and not _line.startswith('#') and '=' in _line:
                        _k, _, _v = _line.partition('=')
                        _k, _v = _k.strip(), _v.strip()
                        # setdefault skips empty-string values; check explicitly
                        if _k and not os.environ.get(_k):
                            os.environ[_k] = _v
            return  # stop after first file found
        except OSError:
            continue
_load_env()

MAX_QUARTERS = 16
_RETRY_DELAY_BASE = 4

# ── Tiingo (price data) ───────────────────────────────────────────────────────
_TIINGO_BASE = "https://api.tiingo.com"

# ── SEC EDGAR (financial statements) ─────────────────────────────────────────
_EDGAR_FACTS_URL   = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_EDGAR_SUB_URL     = "https://data.sec.gov/submissions/CIK{cik}.json"
_EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_HEADERS     = {
    "User-Agent":      "FinancialAnalyzerApp support@finapp.dev",
    "Accept-Encoding": "gzip, deflate",
}

_cik_cache: dict = {}   # ticker → zero-padded CIK, loaded once per process


class UnknownTickerError(RuntimeError):
    """Symbol is not in the SEC's listed-company registry — permanent,
    never worth retrying."""


# ── Tiingo helpers ────────────────────────────────────────────────────────────

def _tiingo_key() -> str:
    """Return the Tiingo API key. Tries every source before giving up."""
    # 1. Environment variable (set by _load_env or the user's shell)
    key = os.environ.get("TIINGO_API_KEY", "").strip()

    # 2. Streamlit secrets (.streamlit/secrets.toml)
    if not key:
        try:
            import streamlit as st
            val = st.secrets.get("TIINGO_API_KEY") or st.secrets.get("tiingo_api_key")
            key = str(val).strip() if val else ""
        except Exception:
            pass

    if not key:
        raise RuntimeError(
            "TIINGO_API_KEY not set. Add it to your .env file: TIINGO_API_KEY=<your_key>"
        )
    return key


def _tiingo_get(path: str, params: dict = None):
    headers = {
        "Authorization":  f"Token {_tiingo_key()}",
        "Content-Type":   "application/json",
    }
    r = requests.get(f"{_TIINGO_BASE}{path}", headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _get_tiingo_prices(ticker: str) -> list:
    """One year of daily price rows from Tiingo."""
    one_year_ago = (date.today() - timedelta(days=366)).isoformat()
    today        = date.today().isoformat()
    return _tiingo_get(
        f"/tiingo/daily/{ticker}/prices",
        params={"startDate": one_year_ago, "endDate": today},
    )


_BENCHMARK = "SPY"
_benchmark_prices_cache: list = []   # fetched once per process


def _compute_beta(prices: list) -> float:
    """
    Beta vs. SPY from one year of daily adjusted closes:
    cov(stock returns, benchmark returns) / var(benchmark returns).
    Returns None when the benchmark can't be fetched or overlap is too short.
    """
    global _benchmark_prices_cache
    try:
        if not _benchmark_prices_cache:
            _benchmark_prices_cache = _get_tiingo_prices(_BENCHMARK)
        bench = _benchmark_prices_cache
    except Exception:
        return None

    def _by_day(rows):
        return {p["date"][:10]: p["adjClose"] for p in rows
                if p.get("adjClose") and p.get("date")}

    s, b = _by_day(prices), _by_day(bench)
    days = sorted(set(s) & set(b))
    if len(days) < 120:   # need a meaningful overlap for a stable estimate
        return None

    rs = [s[days[i]] / s[days[i - 1]] - 1 for i in range(1, len(days))]
    rb = [b[days[i]] / b[days[i - 1]] - 1 for i in range(1, len(days))]
    mean_s, mean_b = sum(rs) / len(rs), sum(rb) / len(rb)
    var_b = sum((x - mean_b) ** 2 for x in rb) / (len(rb) - 1)
    if var_b == 0:
        return None
    cov = sum((x - mean_s) * (y - mean_b) for x, y in zip(rs, rb)) / (len(rs) - 1)
    return round(cov / var_b, 3)


def _trim_prices(raw: list) -> list:
    """Tiingo rows → the compact {date, open, high, low, close, volume}
    format stored in the JSON files and consumed by technicals/macro."""
    def _r(v):
        return round(v, 4) if isinstance(v, (int, float)) else None

    return [
        {
            "date":   p["date"][:10],
            "open":   _r(p.get("adjOpen")),
            "high":   _r(p.get("adjHigh")),
            "low":    _r(p.get("adjLow")),
            "close":  _r(p.get("adjClose")),
            "volume": p.get("adjVolume"),
        }
        for p in raw if p.get("date")
    ]


def get_price_series(symbol: str) -> list:
    """
    Public: one year of daily rows (trimmed format) for a benchmark or
    sector ETF. Shares the SPY cache used by beta, so a process never
    fetches the benchmark twice.
    """
    global _benchmark_prices_cache
    if symbol == _BENCHMARK and _benchmark_prices_cache:
        raw = _benchmark_prices_cache
    else:
        raw = _get_tiingo_prices(symbol)
        if symbol == _BENCHMARK:
            _benchmark_prices_cache = raw
    return _trim_prices(raw)


def _get_tiingo_data(ticker: str) -> dict:
    """
    Fetch one year of daily prices from Tiingo (single request — the free
    tier is rate-limited per hour, and the meta endpoint only duplicated
    the company name EDGAR already provides).
    Returns dict with: price, week52_high, week52_low, annual_dividend
    (sum of divCash over trailing year), beta (vs. SPY), prices.
    """
    prices = _get_tiingo_prices(ticker)

    if not prices:
        raise RuntimeError(f"{ticker}: Tiingo returned no price data")

    closes        = [p["adjClose"] for p in prices if p.get("adjClose") is not None]
    annual_div    = sum(p.get("divCash", 0) or 0 for p in prices)
    price_series  = _trim_prices(prices)

    return {
        "price":          prices[-1].get("adjClose"),
        "week52_high":    max(closes) if closes else None,
        "week52_low":     min(closes) if closes else None,
        "annual_dividend": annual_div if annual_div > 0 else None,
        "beta":           _compute_beta(prices),
        "prices":         price_series,
    }


# ── EDGAR helpers ─────────────────────────────────────────────────────────────

def _get_cik(ticker: str) -> str:
    global _cik_cache
    if not _cik_cache:
        r = requests.get(_EDGAR_TICKERS_URL, headers=_EDGAR_HEADERS, timeout=30)
        r.raise_for_status()
        _cik_cache = {
            v["ticker"].upper(): str(v["cik_str"]).zfill(10)
            for v in r.json().values()
        }
    cik = _cik_cache.get(ticker.upper())
    if not cik:
        raise UnknownTickerError(f"{ticker}: not found in SEC EDGAR ticker list")
    return cik


def _get_edgar_meta(cik: str) -> dict:
    """
    Fetch company name, SIC code/description and the recent-filings index
    from EDGAR submissions (one request serves meta, the filings feed and
    the insider-transaction lookup).
    """
    r = requests.get(_EDGAR_SUB_URL.format(cik=cik), headers=_EDGAR_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return {
        "name":     data.get("name", ""),
        "industry": data.get("sicDescription", ""),
        "sic":      data.get("sic", ""),
        "recent":   data.get("filings", {}).get("recent", {}),
    }


def _filing_url(cik: str, accession: str, primary_doc: str) -> str:
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{primary_doc}")


def _recent_filings(cik: str, recent: dict, forms: set, limit: int) -> list:
    """Rows {form, date, url} for the latest filings matching `forms`."""
    out = []
    all_forms = recent.get("form", [])
    dates     = recent.get("filingDate", [])
    accs      = recent.get("accessionNumber", [])
    docs      = recent.get("primaryDocument", [])
    for i, form in enumerate(all_forms):
        if form not in forms:
            continue
        try:
            out.append({
                "form": form,
                "date": dates[i],
                "url":  _filing_url(cik, accs[i], docs[i]),
            })
        except (IndexError, ValueError):
            continue
        if len(out) >= limit:
            break
    return out


_FORM4_CODES = {
    "P": "BUY", "S": "SELL", "A": "GRANT", "M": "EXERCISE",
    "F": "TAX", "G": "GIFT", "D": "DISPOSITION", "C": "CONVERSION",
}


def _fetch_insider_transactions(cik: str, recent: dict, limit: int = 8) -> list:
    """
    Parse the latest Form 4 filings into transaction rows.
    Best-effort: unparseable filings are skipped, any failure returns
    what was collected so far — this must never break a fetch.
    """
    import xml.etree.ElementTree as ET

    filings = _recent_filings(cik, recent, {"4"}, limit)
    rows = []
    for f in filings:
        try:
            # primaryDocument for Form 4 is the XSL-rendered HTML view
            # ("xslF345X06/form4.xml"); the raw XML is the same path
            # without the xsl.../ directory component.
            xml_url = re.sub(r"/xsl[^/]+/", "/", f["url"])
            r = requests.get(xml_url, headers=_EDGAR_HEADERS, timeout=15)
            r.raise_for_status()
            root = ET.fromstring(r.content)

            name = root.findtext(".//reportingOwner/reportingOwnerId/rptOwnerName", "")
            rel  = root.find(".//reportingOwner/reportingOwnerRelationship")
            role = ""
            if rel is not None:
                role = (rel.findtext("officerTitle") or "").strip()
                if not role and (rel.findtext("isDirector") or "").strip() in ("1", "true"):
                    role = "Director"
                if not role and (rel.findtext("isTenPercentOwner") or "").strip() in ("1", "true"):
                    role = "10% Owner"

            for tx in root.findall(".//nonDerivativeTable/nonDerivativeTransaction"):
                code   = tx.findtext(".//transactionCoding/transactionCode", "")
                shares = tx.findtext(".//transactionAmounts/transactionShares/value")
                price  = tx.findtext(".//transactionAmounts/transactionPricePerShare/value")
                date   = tx.findtext(".//transactionDate/value", f["date"])
                shares = float(shares) if shares else None
                price  = float(price) if price else None
                rows.append({
                    "date":   date,
                    "name":   name,
                    "role":   role,
                    "code":   code,
                    "action": _FORM4_CODES.get(code, code),
                    "shares": shares,
                    "price":  price,
                    "value":  round(shares * price, 2) if (shares and price) else None,
                    "url":    f["url"],
                })
        except Exception:
            continue
    rows.sort(key=lambda x: x.get("date", ""), reverse=True)
    return rows


def _get_edgar_facts(cik: str) -> dict:
    url = _EDGAR_FACTS_URL.format(cik=cik)
    r = requests.get(url, headers=_EDGAR_HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


_QUARTERLY_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A"}
_ANNUAL_FORMS    = {"20-F", "20-F/A", "10-K", "10-K/A"}


def _quarterly_duration(concept: dict) -> dict:
    """
    Extract standalone quarterly (~90-day) values from a duration XBRL concept.
    Returns {end_date_str: float_value}.
    """
    result: dict = {}
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in _QUARTERLY_FORMS:
                continue
            start = dp.get("start", "")
            end   = dp.get("end", "")
            val   = dp.get("val")
            filed = dp.get("filed", "")
            if not start or not end or val is None:
                continue
            try:
                days = (datetime.strptime(end, "%Y-%m-%d") -
                        datetime.strptime(start, "%Y-%m-%d")).days
            except ValueError:
                continue
            if not (75 <= days <= 105):
                continue
            if end not in result or filed > result[end][1]:
                result[end] = (float(val), filed)
    return {k: v[0] for k, v in result.items()}


def _annual_duration(concept: dict) -> dict:
    """
    Extract annual (~365-day) values from a duration XBRL concept.
    Used as fallback for foreign filers (20-F) that don't have quarterly XBRL.
    Returns {end_date_str: float_value}.
    """
    result: dict = {}
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in _ANNUAL_FORMS:
                continue
            start = dp.get("start", "")
            end   = dp.get("end", "")
            val   = dp.get("val")
            filed = dp.get("filed", "")
            if not start or not end or val is None:
                continue
            try:
                days = (datetime.strptime(end, "%Y-%m-%d") -
                        datetime.strptime(start, "%Y-%m-%d")).days
            except ValueError:
                continue
            if not (340 <= days <= 390):
                continue
            if end not in result or filed > result[end][1]:
                result[end] = (float(val), filed)
    return {k: v[0] for k, v in result.items()}


def _quarterly_instant(concept: dict) -> dict:
    """
    Extract period-end snapshots from an instant XBRL concept (balance sheet).
    Includes 20-F filings for foreign filers.
    Returns {end_date_str: float_value}.
    """
    result: dict = {}
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in (_QUARTERLY_FORMS | _ANNUAL_FORMS):
                continue
            end   = dp.get("end", "")
            val   = dp.get("val")
            filed = dp.get("filed", "")
            if not end or val is None:
                continue
            if end not in result or filed > result[end][1]:
                result[end] = (float(val), filed)
    return {k: v[0] for k, v in result.items()}


def _fill_missing_q4(quarterly: dict, concept: dict) -> dict:
    """
    Derive standalone Q4 values that are missing from XBRL.
    10-K filings report the full fiscal year as one duration — there is no
    standalone Q4 datapoint — so quarterly series would otherwise have a hole
    every year, corrupting TTM sums and YoY comparisons.
    Q4 = FY value − sum of the three quarters inside that fiscal-year window.
    """
    annual: dict = {}   # end → (val, start, filed)
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in _ANNUAL_FORMS:
                continue
            start, end, val, filed = dp.get("start", ""), dp.get("end", ""), dp.get("val"), dp.get("filed", "")
            if not start or not end or val is None:
                continue
            try:
                days = (datetime.strptime(end, "%Y-%m-%d") -
                        datetime.strptime(start, "%Y-%m-%d")).days
            except ValueError:
                continue
            if not (340 <= days <= 390):
                continue
            if end not in annual or filed > annual[end][2]:
                annual[end] = (float(val), start, filed)

    out = dict(quarterly)
    for end, (fy_val, start, _) in annual.items():
        if end in out:
            continue
        # ISO date strings compare correctly as strings
        in_window = [v for k, v in quarterly.items() if start <= k < end]
        if len(in_window) == 3:
            out[end] = fy_val - sum(in_window)
    return out


def _ytd_quarterly(concept: dict) -> dict:
    """
    Extract quarterly values from a duration concept that is reported
    cumulatively. Cash-flow statements in 10-Qs are year-to-date (3m/6m/9m
    from the fiscal-year start; the 10-K covers 12m) — standalone quarters
    must be derived by differencing consecutive YTD values that share the
    same fiscal-year start. Directly-reported standalone quarters (~90d
    durations) are kept as-is. Returns {end_date_str: float_value}.
    """
    pts: dict = {}   # (start, end) → (val, filed)
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in (_QUARTERLY_FORMS | _ANNUAL_FORMS):
                continue
            start, end, val, filed = dp.get("start", ""), dp.get("end", ""), dp.get("val"), dp.get("filed", "")
            if not start or not end or val is None:
                continue
            try:
                days = (datetime.strptime(end, "%Y-%m-%d") -
                        datetime.strptime(start, "%Y-%m-%d")).days
            except ValueError:
                continue
            if not (60 <= days <= 400):
                continue
            key = (start, end)
            if key not in pts or filed > pts[key][1]:
                pts[key] = (float(val), filed)

    by_start: dict = {}
    for (start, end), (val, _) in pts.items():
        by_start.setdefault(start, []).append((end, val))

    result: dict = {}
    for start, series in by_start.items():
        series.sort()
        prev_end, prev_val = start, 0.0
        for end, val in series:
            try:
                chunk = (datetime.strptime(end, "%Y-%m-%d") -
                         datetime.strptime(prev_end, "%Y-%m-%d")).days
            except ValueError:
                break
            # only accept a ~one-quarter step from the previous YTD point;
            # a larger gap means an intermediate 10-Q is missing
            if 75 <= chunk <= 105:
                result.setdefault(end, val - prev_val)
            prev_end, prev_val = end, val
    return result


def _first_dur_ytd(tax: dict, *names) -> dict:
    """_first_dur variant for cumulatively-reported concepts (cash flow)."""
    merged: dict = {}
    for name in names:
        if name in tax:
            for k, v in _ytd_quarterly(tax[name]).items():
                merged.setdefault(k, v)
    if merged:
        return merged
    for name in names:
        if name in tax:
            for k, v in _annual_duration(tax[name]).items():
                merged.setdefault(k, v)
    return merged


def _first_dur(tax: dict, *names) -> dict:
    """
    Merge quarterly durations across all candidate concept names, with
    earlier-listed names winning when the same period appears twice.
    Companies migrate tags over time (e.g. Visa's revenue moved from
    "Revenues" to "RevenueFromContractWithCustomerExcludingAssessedTax"
    in 2018) — taking the first non-empty concept would return only the
    stale pre-migration years and leave every recent quarter empty.
    Falls back to annual durations (20-F filers) when no quarterly data.
    """
    merged: dict = {}
    for name in names:
        if name in tax:
            for k, v in _quarterly_duration(tax[name]).items():
                merged.setdefault(k, v)
    if merged:
        for name in names:
            if name in tax:
                merged = _fill_missing_q4(merged, tax[name])
        return merged
    for name in names:
        if name in tax:
            for k, v in _annual_duration(tax[name]).items():
                merged.setdefault(k, v)
    return merged


def _first_ins(tax: dict, *names) -> dict:
    """Merge instant values across candidate names (see _first_dur on why)."""
    merged: dict = {}
    for name in names:
        if name in tax:
            for k, v in _quarterly_instant(tax[name]).items():
                merged.setdefault(k, v)
    return merged


def _ttm(by_period: dict, n: int = 4):
    """
    Sum the n most recent non-None values from a {period: value} dict,
    provided they span ≤ ~13 months (i.e. they really are a trailing year).
    If consecutive periods are all ~1 year apart (annual-only 20-F filers),
    the latest fiscal-year value is itself the TTM.
    """
    pairs = [(p, v) for p, v in sorted(by_period.items(), reverse=True) if v is not None]
    if not pairs:
        return None
    try:
        dates = [datetime.strptime(p, "%Y-%m-%d") for p, _ in pairs]
    except ValueError:
        return None
    if len(pairs) >= 2:
        gaps = [(dates[i] - dates[i + 1]).days for i in range(len(dates) - 1)]
        if all(300 <= g <= 430 for g in gaps):
            return pairs[0][1]
    if len(pairs) < n:
        return None
    if (dates[0] - dates[n - 1]).days > 400:
        return None
    return sum(v for _, v in pairs[:n])


def _build_income(ugaap: dict, ifrs: dict) -> list:
    def _dur(*names):
        return _first_dur(ugaap, *names) or _first_dur(ifrs, *names)

    revenue = _dur(
        # US-GAAP
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueGoodsNet",
        "InterestAndNoninterestIncome",
        # IFRS
        "Revenue",
        "RevenueFromContractsWithCustomers",
    )
    gp = _dur("GrossProfit")
    # Many filers (e.g. Amazon) report cost of revenue but no GrossProfit
    # tag — derive it. Companies with no COGS line at all (Visa, banks)
    # correctly end up with no gross margin.
    cogs = _dur(
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfSales",
        "CostOfServices",
    )
    op = _dur(
        "OperatingIncomeLoss",
        "ProfitLossFromOperatingActivities",
    )
    ni = _dur(
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "ProfitLoss",
        "ProfitLossAttributableToOwnersOfParent",
    )
    ie = _dur(
        "InterestExpense",
        "InterestExpenseNonoperating",
        "InterestAndDebtExpense",
        "InterestExpenseDebt",
        "FinanceCosts",
        "BorrowingCostsRecognisedAsExpense",
    )
    da = _dur(
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
        "AdjustmentsForDepreciationExpense",
        "AdjustmentsForAmortisationExpense",
    )

    all_periods = sorted(set(revenue) | set(ni), reverse=True)[:MAX_QUARTERS]
    result = []
    for period in all_periods:
        o = op.get(period)
        d = da.get(period)
        ebitda = (o + d) if (o is not None and d is not None) else None
        g = gp.get(period)
        if g is None:
            r, c = revenue.get(period), cogs.get(period)
            if r is not None and c is not None:
                g = r - c
        result.append({
            "period":                    period,
            "revenue":                   revenue.get(period),
            "gross_profit":              g,
            "operating_income":          o,
            "net_income":                ni.get(period),
            "ebitda":                    ebitda,
            "interest_expense":          ie.get(period),
            "depreciation_amortization": d,
        })
    return result


def _build_balance(ugaap: dict, ifrs: dict) -> list:
    def _ins(*names):
        return _first_ins(ugaap, *names) or _first_ins(ifrs, *names)

    assets = _ins("Assets")
    liab   = _ins("Liabilities")
    equity = _ins(
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "EquityAttributableToOwnersOfParent",
    )
    cash = _ins(
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
        "CashCashEquivalentsAndFederalFundsSold",
        "CashAndCashEquivalents",
    )
    debt  = _ins(
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtNoncurrent",
        "LongtermBorrowings",
        "NotesPayable",   # REITs (e.g. Realty Income) tag debt this way
    )
    cur_a = _ins("AssetsCurrent",     "CurrentAssets")
    cur_l = _ins("LiabilitiesCurrent","CurrentLiabilities")
    inv   = _ins("InventoryNet", "Inventories")

    all_periods = sorted(set(assets) | set(equity), reverse=True)[:MAX_QUARTERS]
    result = []
    for period in all_periods:
        a = assets.get(period)
        e = equity.get(period)
        l = liab.get(period)
        if l is None and a is not None and e is not None:
            l = a - e
        result.append({
            "period":              period,
            "total_assets":        a,
            "total_liabilities":   l,
            "equity":              e,
            "cash":                cash.get(period),
            "total_debt":          debt.get(period),
            "current_assets":      cur_a.get(period),
            "current_liabilities": cur_l.get(period),
            "inventory":           inv.get(period),
        })

    # Some filers (e.g. CAT) report total debt only in the annual 10-K —
    # the 10-Q figure is dimensional and dropped from companyfacts. Debt
    # moves slowly; carry the nearest older value forward (≤ ~13 months)
    # rather than leaving interim quarters empty.
    for i, row in enumerate(result):
        if row["total_debt"] is not None:
            continue
        try:
            cur = datetime.strptime(row["period"], "%Y-%m-%d")
        except ValueError:
            continue
        for older in result[i + 1:]:
            if older["total_debt"] is None:
                continue
            try:
                gap = (cur - datetime.strptime(older["period"], "%Y-%m-%d")).days
            except ValueError:
                break
            if 0 < gap <= 400:
                row["total_debt"] = older["total_debt"]
            break
    return result


def _build_cashflow(ugaap: dict, ifrs: dict) -> list:
    # Cash-flow statements are cumulative YTD in 10-Qs — use the
    # differencing extractor, not the standalone-quarter one.
    def _dur(*names):
        return _first_dur_ytd(ugaap, *names) or _first_dur_ytd(ifrs, *names)

    op_cf  = _dur(
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities",
    )
    inv_cf = _dur(
        "NetCashProvidedByUsedInInvestingActivities",
        "CashFlowsFromUsedInInvestingActivities",
    )
    fin_cf = _dur(
        "NetCashProvidedByUsedInFinancingActivities",
        "CashFlowsFromUsedInFinancingActivities",
    )
    # EDGAR reports capex as a positive outflow; negate to match convention (FCF = OCF + capex)
    capex_raw = _dur(
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireProductiveAssets",
        "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
        # IFRS filers (e.g. SAP) often report capex + intangibles combined
        "PurchaseOfPropertyPlantAndEquipmentIntangibleAssetsOtherThanGoodwillInvestmentPropertyAndOtherNoncurrentAssets",
    )

    all_periods = sorted(set(op_cf), reverse=True)[:MAX_QUARTERS]
    result = []
    for period in all_periods:
        oc     = op_cf.get(period)
        cx_raw = capex_raw.get(period)
        cx     = -cx_raw if cx_raw is not None else None
        fcf    = (oc + cx) if (oc is not None and cx is not None) else None
        result.append({
            "period":         period,
            "operating_cf":   oc,
            "investing_cf":   inv_cf.get(period),
            "financing_cf":   fin_cf.get(period),
            "capex":          cx,
            "free_cash_flow": fcf,
        })
    return result


def _compute_market(price, tiingo: dict, income: list, balance: list,
                    ugaap: dict, ifrs: dict, dei: dict = None) -> dict:
    """Derive market/valuation metrics from Tiingo price + EDGAR fundamentals."""

    # Shares outstanding — most companies only report this under the dei
    # namespace (cover-page fact), so check it alongside us-gaap/ifrs.
    shares_data = (
        _first_ins(dei or {}, "EntityCommonStockSharesOutstanding") or
        _first_ins(ugaap, "CommonStockSharesOutstanding", "SharesOutstanding") or
        _first_ins(ifrs,  "NumberOfSharesOutstanding", "WeightedAverageShares")
    )
    shares = shares_data.get(max(shares_data)) if shares_data else None

    # Dual-class filers (e.g. META) report the cover-page share count only
    # per class with dimensions, which companyfacts drops — fall back to the
    # latest quarter's weighted-average diluted shares.
    if shares is None:
        wavg = (
            _first_dur(ugaap, "WeightedAverageNumberOfDilutedSharesOutstanding",
                       "WeightedAverageNumberOfSharesOutstandingBasic") or
            _first_dur(ifrs, "WeightedAverageShares",
                       "AdjustedWeightedAverageShares")
        )
        shares = wavg.get(max(wavg)) if wavg else None

    market_cap = (price * shares) if (price and shares) else None

    # Latest balance sheet for debt / cash / equity
    latest_bal = balance[0] if balance else {}
    debt  = latest_bal.get("total_debt")
    cash  = latest_bal.get("cash")
    equity = latest_bal.get("equity")

    ev = None
    if market_cap is not None:
        ev = market_cap + (debt or 0) - (cash or 0)

    # TTM income figures (last 4 quarters)
    ni_by_p  = {r["period"]: r["net_income"]  for r in income if r.get("net_income")  is not None}
    rev_by_p = {r["period"]: r["revenue"]     for r in income if r.get("revenue")     is not None}
    eb_by_p  = {r["period"]: r["ebitda"]      for r in income if r.get("ebitda")      is not None}

    ttm_ni     = _ttm(ni_by_p)
    ttm_rev    = _ttm(rev_by_p)
    ttm_ebitda = _ttm(eb_by_p)

    pe_trailing = None
    if price and ttm_ni and shares and ttm_ni > 0:
        eps = ttm_ni / shares
        pe_trailing = round(price / eps, 2) if eps else None

    pb_ratio     = round(market_cap / equity, 4) if (market_cap and equity and equity > 0) else None
    ev_ebitda    = round(ev / ttm_ebitda, 4)     if (ev and ttm_ebitda and ttm_ebitda > 0) else None
    ev_revenue   = round(ev / ttm_rev, 4)        if (ev and ttm_rev and ttm_rev > 0) else None

    annual_div   = tiingo.get("annual_dividend")
    # Stored in % (e.g. 0.42 for 0.42%) — display and legacy data expect percent, not a fraction
    div_yield    = round(annual_div / price * 100, 2) if (annual_div and price and price > 0) else None

    return {
        "market_cap":         market_cap,
        "enterprise_value":   ev,
        "shares_outstanding": shares,
        "price":              price,
        "pe_trailing":        pe_trailing,
        "pe_forward":         None,
        "pb_ratio":           pb_ratio,
        "ev_ebitda_info":     ev_ebitda,
        "ev_revenue_info":    ev_revenue,
        "dividend_yield":     div_yield,
        "beta":               tiingo.get("beta"),
        "week52_high":        tiingo.get("week52_high"),
        "week52_low":         tiingo.get("week52_low"),
    }


# ── Public API ────────────────────────────────────────────────────────────────

def _retry(fn, ticker: str, retries: int, delay_base: int = 4, status_callback=None) -> dict:
    """Call fn(ticker) up to `retries` times with exponential back-off.
    UnknownTickerError is permanent and re-raised immediately."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn(ticker)
        except UnknownTickerError:
            raise
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                delay = delay_base * 2 ** attempt
                if status_callback:
                    status_callback(attempt, delay, e)
                time.sleep(delay)
    raise last_exc


def fetch_only(ticker: str, _retries: int = 3, status_callback=None) -> dict:
    """Fetch and return a data dict. No DB writes."""
    return _retry(_fetch_raw, ticker.upper(), _retries,
                  delay_base=_RETRY_DELAY_BASE, status_callback=status_callback)


def fetch_and_store(ticker: str, _retries: int = 3) -> dict:
    """Fetch and persist to SQLite."""
    return _retry(_fetch_and_store, ticker.upper(), _retries, delay_base=8)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_raw(ticker: str) -> dict:
    # ── EDGAR: verify ticker first (free, no rate limit) ─────────────
    cik = _get_cik(ticker)

    # ── Tiingo: price + 52w range ─────────────────────────────────────
    # Degrade gracefully: EDGAR fundamentals are still worth showing when
    # the price API is rate-limited (free tier, hourly quota).
    price_error = None
    try:
        tiingo = _get_tiingo_data(ticker)
    except Exception as e:
        price_error = f"{type(e).__name__}: {e}"
        tiingo = {"price": None, "week52_high": None, "week52_low": None,
                  "annual_dividend": None, "beta": None, "prices": []}
    price = tiingo["price"]

    # ── EDGAR: company meta + XBRL statements ────────────────────────
    edgar_meta   = _get_edgar_meta(cik)
    facts        = _get_edgar_facts(cik)
    fact_ns      = facts.get("facts", {})
    ugaap        = fact_ns.get("us-gaap",   {})
    ifrs         = fact_ns.get("ifrs-full", {})
    dei          = fact_ns.get("dei",       {})

    income   = _build_income(ugaap, ifrs)
    balance  = _build_balance(ugaap, ifrs)
    cashflow = _build_cashflow(ugaap, ifrs)
    market   = _compute_market(price, tiingo, income, balance, ugaap, ifrs, dei)

    from src.utils import classify_sector

    # EDGAR names are ALL CAPS ("VISA INC.") — title-case for display
    edgar_name = (edgar_meta.get("name") or ticker).title()
    company = {
        "ticker":       ticker,
        "name":         edgar_name,
        "industry":     edgar_meta.get("industry", ""),
        "sic":          edgar_meta.get("sic", ""),
        "currency":     "USD",
        "exchange":     "",
        "last_updated": str(date.today()),
    }
    bucket = classify_sector(company)
    company["sector"] = bucket.replace("_", " ").title() if bucket != "general" else ""

    recent = edgar_meta.get("recent", {})
    filings = _recent_filings(
        cik, recent,
        {"10-K", "10-Q", "8-K", "10-K/A", "10-Q/A", "20-F", "20-F/A", "6-K"},  # 20-F/6-K: foreign filers
        12,
    )
    try:
        insider = _fetch_insider_transactions(cik, recent)
    except Exception:
        insider = []

    return {
        "company":  company,
        "market":   market,
        "income":   income,
        "balance":  balance,
        "cashflow": cashflow,
        "prices":   tiingo.get("prices", []),
        "filings":  filings,
        "insider":  insider,
        "price_data_error": price_error,
    }


def _fetch_and_store(ticker: str) -> dict:
    data = _fetch_raw(ticker)

    conn = db.get_conn()
    try:
        db.upsert_company(ticker, {
            "longName": data["company"]["name"],
            "sector":   data["company"]["sector"],
            "industry": data["company"]["industry"],
            "currency": data["company"]["currency"],
            "exchange": data["company"]["exchange"],
        }, conn=conn)
        db.upsert_market_data(ticker, data["market"], conn=conn)
        for row in data["income"]:
            db.upsert_income(ticker, row["period"], row, conn=conn)
        for row in data["balance"]:
            db.upsert_balance(ticker, row["period"], row, conn=conn)
        for row in data["cashflow"]:
            db.upsert_cashflow(ticker, row["period"], row, conn=conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return data
