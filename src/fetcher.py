"""
Data sources
────────────
Market data  →  Financial Modeling Prep (FMP) /stable endpoints
                No rate limits, API key in FMP_API_KEY env var / .env file

Financials   →  SEC EDGAR company facts API (XBRL)
                Completely free, no API key, no rate limits
                Authoritative source — all FMP/Yahoo data originates here
"""
import math
import os
import time
from datetime import datetime, date

import requests

from src import db

# Load .env so FMP_API_KEY is available without shell exports
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MAX_QUARTERS = 16
_RETRY_DELAY_BASE = 4

# ── FMP (market data) ─────────────────────────────────────────────────────────
_FMP_BASE = "https://financialmodelingprep.com/stable"

# ── SEC EDGAR (financial statements) ─────────────────────────────────────────
_EDGAR_FACTS_URL   = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_HEADERS     = {
    "User-Agent":      "FinancialAnalyzerApp support@finapp.dev",
    "Accept-Encoding": "gzip, deflate",
}

_cik_cache: dict = {}   # ticker → zero-padded CIK, loaded once per process


# ── FMP helpers ───────────────────────────────────────────────────────────────

def _fmp_key() -> str:
    key = os.environ.get("FMP_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FMP_API_KEY not set. Add it to your .env file: FMP_API_KEY=<your_key>"
        )
    return key


def _fmp_get(path: str, params: dict = None):
    url = f"{_FMP_BASE}{path}"
    p = {"apikey": _fmp_key()}
    if params:
        p.update(params)
    r = requests.get(url, params=p, timeout=15)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        msg = data.get("Error Message") or data.get("error")
        if msg:
            raise RuntimeError(str(msg))
    return data


def _safe(d: dict, *keys):
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            continue
    return None


# ── EDGAR helpers ─────────────────────────────────────────────────────────────

def _get_cik(ticker: str) -> str:
    """Return zero-padded 10-digit CIK for a ticker, loading the map on first call."""
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
        raise RuntimeError(f"{ticker}: not found in SEC EDGAR ticker list")
    return cik


def _get_edgar_facts(cik: str) -> dict:
    url = _EDGAR_FACTS_URL.format(cik=cik)
    r = requests.get(url, headers=_EDGAR_HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def _quarterly_duration(concept: dict) -> dict:
    """
    Extract standalone quarterly (~90-day) values from a duration XBRL concept.
    Income-statement and cash-flow items are duration concepts.
    Returns {end_date_str: float_value}, most-recent filing wins per period.
    """
    result: dict = {}
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in ("10-Q", "10-K", "10-Q/A", "10-K/A"):
                continue
            start  = dp.get("start", "")
            end    = dp.get("end", "")
            val    = dp.get("val")
            filed  = dp.get("filed", "")
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


def _quarterly_instant(concept: dict) -> dict:
    """
    Extract period-end snapshots from an instant XBRL concept.
    Balance-sheet items are instant concepts.
    Returns {end_date_str: float_value}, most-recent filing wins per date.
    """
    result: dict = {}
    for unit_vals in concept.get("units", {}).values():
        for dp in unit_vals:
            if dp.get("form") not in ("10-Q", "10-K", "10-Q/A", "10-K/A"):
                continue
            end   = dp.get("end", "")
            val   = dp.get("val")
            filed = dp.get("filed", "")
            if not end or val is None:
                continue
            if end not in result or filed > result[end][1]:
                result[end] = (float(val), filed)
    return {k: v[0] for k, v in result.items()}


def _first_dur(ugaap: dict, *names) -> dict:
    """Try XBRL concept names in order; return first non-empty duration result."""
    for name in names:
        if name in ugaap:
            vals = _quarterly_duration(ugaap[name])
            if vals:
                return vals
    return {}


def _first_ins(ugaap: dict, *names) -> dict:
    """Try XBRL concept names in order; return first non-empty instant result."""
    for name in names:
        if name in ugaap:
            vals = _quarterly_instant(ugaap[name])
            if vals:
                return vals
    return {}


def _build_income(ugaap: dict) -> list:
    revenue = _first_dur(ugaap,
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueGoodsNet",
        "InterestAndNoninterestIncome",
    )
    gp = _first_dur(ugaap, "GrossProfit")
    op = _first_dur(ugaap, "OperatingIncomeLoss")
    ni = _first_dur(ugaap,
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "ProfitLoss",
    )
    ie = _first_dur(ugaap,
        "InterestExpense",
        "InterestAndDebtExpense",
        "InterestExpenseDebt",
    )
    da = _first_dur(ugaap,
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
    )

    all_periods = sorted(set(revenue) | set(ni), reverse=True)[:MAX_QUARTERS]
    result = []
    for period in all_periods:
        o = op.get(period)
        d = da.get(period)
        ebitda = (o + d) if (o is not None and d is not None) else None
        result.append({
            "period":                    period,
            "revenue":                   revenue.get(period),
            "gross_profit":              gp.get(period),
            "operating_income":          o,
            "net_income":                ni.get(period),
            "ebitda":                    ebitda,
            "interest_expense":          ie.get(period),
            "depreciation_amortization": d,
        })
    return result


def _build_balance(ugaap: dict) -> list:
    assets = _first_ins(ugaap, "Assets")
    liab   = _first_ins(ugaap, "Liabilities")
    equity = _first_ins(ugaap,
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    )
    cash = _first_ins(ugaap,
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
        "CashCashEquivalentsAndFederalFundsSold",
    )
    debt   = _first_ins(ugaap,
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtNoncurrent",
    )
    cur_a  = _first_ins(ugaap, "AssetsCurrent")
    cur_l  = _first_ins(ugaap, "LiabilitiesCurrent")
    inv    = _first_ins(ugaap, "InventoryNet", "Inventories")

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
    return result


def _build_cashflow(ugaap: dict) -> list:
    op_cf  = _first_dur(ugaap, "NetCashProvidedByUsedInOperatingActivities")
    inv_cf = _first_dur(ugaap, "NetCashProvidedByUsedInInvestingActivities")
    fin_cf = _first_dur(ugaap, "NetCashProvidedByUsedInFinancingActivities")
    # EDGAR reports capex as positive (cash outflow); negate to match convention
    capex_raw = _first_dur(ugaap,
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireProductiveAssets",
    )

    all_periods = sorted(set(op_cf), reverse=True)[:MAX_QUARTERS]
    result = []
    for period in all_periods:
        oc = op_cf.get(period)
        cx_raw = capex_raw.get(period)
        cx = -cx_raw if cx_raw is not None else None   # store as negative
        fcf = (oc + cx) if (oc is not None and cx is not None) else None
        result.append({
            "period":         period,
            "operating_cf":   oc,
            "investing_cf":   inv_cf.get(period),
            "financing_cf":   fin_cf.get(period),
            "capex":          cx,
            "free_cash_flow": fcf,
        })
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def _retry(fn, ticker: str, retries: int, delay_base: int = 4, status_callback=None) -> dict:
    """Call fn(ticker) up to `retries` times with exponential back-off."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn(ticker)
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
    """Fetch market data from FMP and statements from EDGAR."""
    sym = {"symbol": ticker}

    # ── Market data via FMP ───────────────────────────────────────────
    profile_list = _fmp_get("/profile", sym)
    if not profile_list:
        raise RuntimeError(f"{ticker}: FMP returned no data — check the ticker symbol")
    profile = profile_list[0]

    quote_list = _fmp_get("/quote", sym)
    quote = quote_list[0] if quote_list else {}

    ratios_list = _fmp_get("/ratios-ttm", sym)
    ratios = ratios_list[0] if ratios_list else {}

    km_list = _fmp_get("/key-metrics-ttm", sym)
    km = km_list[0] if km_list else {}

    price = _safe(quote, "price") or _safe(profile, "price")
    last_div = _safe(profile, "lastDividend")
    div_yield = (last_div / price) if (price and last_div and price > 0) else None

    company = {
        "ticker":       ticker,
        "name":         profile.get("companyName", ticker),
        "sector":       profile.get("sector", ""),
        "industry":     profile.get("industry", ""),
        "currency":     profile.get("currency", ""),
        "exchange":     profile.get("exchange", ""),
        "last_updated": str(date.today()),
    }

    market = {
        "market_cap":         _safe(quote,  "marketCap"),
        "enterprise_value":   _safe(km,     "enterpriseValueTTM"),
        "shares_outstanding": None,  # filled below from EDGAR
        "price":              price,
        "pe_trailing":        _safe(ratios, "priceToEarningsRatioTTM"),
        "pe_forward":         None,  # not on free FMP tier
        "pb_ratio":           _safe(ratios, "priceToBookRatioTTM"),
        "ev_ebitda_info":     _safe(km,     "evToEBITDATTM"),
        "ev_revenue_info":    _safe(km,     "evToSalesTTM"),
        "dividend_yield":     div_yield,
        "beta":               _safe(profile, "beta"),
        "week52_high":        _safe(quote,  "yearHigh"),
        "week52_low":         _safe(quote,  "yearLow"),
    }

    # ── Financial statements via SEC EDGAR ────────────────────────────
    cik   = _get_cik(ticker)
    facts = _get_edgar_facts(cik)
    ugaap = facts.get("facts", {}).get("us-gaap", {})

    income   = _build_income(ugaap)
    balance  = _build_balance(ugaap)
    cashflow = _build_cashflow(ugaap)

    # Shares outstanding from most recent balance sheet (CommonStockSharesOutstanding)
    shares_data = _first_ins(ugaap,
        "CommonStockSharesOutstanding",
        "SharesOutstanding",
    )
    if shares_data:
        latest = max(shares_data.keys())
        market["shares_outstanding"] = shares_data[latest]

    return {
        "company":  company,
        "market":   market,
        "income":   income,
        "balance":  balance,
        "cashflow": cashflow,
    }


def _fetch_and_store(ticker: str) -> dict:
    """Fetch via _fetch_raw then persist everything to SQLite in one transaction."""
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
