"""
Data sources
────────────
Price data   →  Tiingo end-of-day API (free, all tickers, no rate limits)
                API key in TIINGO_API_KEY env var / .env file

Financials   →  SEC EDGAR company facts API (XBRL)
                Completely free, no API key, no rate limits
                Authoritative source for all US public company filings

Ratios       →  Computed from price × EDGAR fundamentals
                market_cap, EV, PE, P/B, EV/EBITDA, EV/Revenue
"""
import math
import os
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


def _get_tiingo_data(ticker: str) -> dict:
    """
    Fetch meta + one year of daily prices from Tiingo.
    Returns dict with: name, exchange, price, week52_high, week52_low,
    annual_dividend (sum of divCash over trailing year).
    """
    meta = _tiingo_get(f"/tiingo/daily/{ticker}")

    one_year_ago = (date.today() - timedelta(days=366)).isoformat()
    today        = date.today().isoformat()
    prices = _tiingo_get(
        f"/tiingo/daily/{ticker}/prices",
        params={"startDate": one_year_ago, "endDate": today},
    )

    if not prices:
        raise RuntimeError(f"{ticker}: Tiingo returned no price data")

    closes        = [p["adjClose"] for p in prices if p.get("adjClose") is not None]
    annual_div    = sum(p.get("divCash", 0) or 0 for p in prices)

    return {
        "name":           meta.get("name", ticker),
        "exchange":       meta.get("exchangeCode", ""),
        "price":          prices[-1].get("adjClose"),
        "week52_high":    max(closes) if closes else None,
        "week52_low":     min(closes) if closes else None,
        "annual_dividend": annual_div if annual_div > 0 else None,
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
        raise RuntimeError(f"{ticker}: not found in SEC EDGAR ticker list")
    return cik


def _get_edgar_meta(cik: str) -> dict:
    """Fetch company name and SIC industry description from EDGAR submissions."""
    r = requests.get(_EDGAR_SUB_URL.format(cik=cik), headers=_EDGAR_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return {
        "name":     data.get("name", ""),
        "industry": data.get("sicDescription", ""),
    }


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


def _first_dur(tax: dict, *names) -> dict:
    """Try quarterly duration first; fall back to annual (for 20-F filers)."""
    for name in names:
        if name in tax:
            vals = _quarterly_duration(tax[name])
            if vals:
                return vals
    for name in names:
        if name in tax:
            vals = _annual_duration(tax[name])
            if vals:
                return vals
    return {}


def _first_ins(tax: dict, *names) -> dict:
    for name in names:
        if name in tax:
            vals = _quarterly_instant(tax[name])
            if vals:
                return vals
    return {}


def _ttm(by_period: dict, n: int = 4):
    """Sum the n most recent non-None values from a {period: value} dict."""
    vals = [v for _, v in sorted(by_period.items(), reverse=True) if v is not None][:n]
    return sum(vals) if len(vals) == n else None


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
    return result


def _build_cashflow(ugaap: dict, ifrs: dict) -> list:
    def _dur(*names):
        return _first_dur(ugaap, *names) or _first_dur(ifrs, *names)

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
                    ugaap: dict, ifrs: dict) -> dict:
    """Derive market/valuation metrics from Tiingo price + EDGAR fundamentals."""

    # Shares outstanding — prefer EDGAR instant value
    shares_data = (
        _first_ins(ugaap, "CommonStockSharesOutstanding", "SharesOutstanding") or
        _first_ins(ifrs,  "NumberOfSharesOutstanding", "WeightedAverageShares")
    )
    shares = shares_data.get(max(shares_data)) if shares_data else None

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
    div_yield    = (annual_div / price) if (annual_div and price and price > 0) else None

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
        "beta":               None,
        "week52_high":        tiingo.get("week52_high"),
        "week52_low":         tiingo.get("week52_low"),
    }


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
    # ── Tiingo: price + 52w range ─────────────────────────────────────
    tiingo = _get_tiingo_data(ticker)
    price  = tiingo["price"]

    # ── EDGAR: company meta + XBRL statements ────────────────────────
    cik          = _get_cik(ticker)
    edgar_meta   = _get_edgar_meta(cik)
    facts        = _get_edgar_facts(cik)
    fact_ns      = facts.get("facts", {})
    ugaap        = fact_ns.get("us-gaap",   {})
    ifrs         = fact_ns.get("ifrs-full", {})

    income   = _build_income(ugaap, ifrs)
    balance  = _build_balance(ugaap, ifrs)
    cashflow = _build_cashflow(ugaap, ifrs)
    market   = _compute_market(price, tiingo, income, balance, ugaap, ifrs)

    company = {
        "ticker":       ticker,
        "name":         tiingo["name"] or edgar_meta.get("name", ticker),
        "sector":       "",
        "industry":     edgar_meta.get("industry", ""),
        "currency":     "USD",
        "exchange":     tiingo["exchange"],
        "last_updated": str(date.today()),
    }

    return {
        "company":  company,
        "market":   market,
        "income":   income,
        "balance":  balance,
        "cashflow": cashflow,
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
