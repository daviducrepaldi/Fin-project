import math
import os
import time
from datetime import date

import requests

from src import db

MAX_QUARTERS = 16  # ~4 years

_RETRY_DELAY_BASE = 4
_FMP_BASE = "https://financialmodelingprep.com/api/v3"


def _api_key() -> str:
    key = os.environ.get("FMP_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FMP_API_KEY environment variable is not set. "
            "Get a free key at https://financialmodelingprep.com/register"
        )
    return key


def _get(path: str, params: dict = None):
    """GET a FMP endpoint; raise RuntimeError on non-2xx or API-level error."""
    url = f"{_FMP_BASE}{path}"
    p = {"apikey": _api_key()}
    if params:
        p.update(params)
    resp = requests.get(url, params=p, timeout=15)
    resp.raise_for_status()
    data = resp.json()
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
    """
    Fetch from FMP and return a data dict. No DB writes, no disk writes.
    Use this for tickers that should not be persisted locally.
    """
    return _retry(_fetch_raw, ticker.upper(), _retries,
                  delay_base=_RETRY_DELAY_BASE, status_callback=status_callback)


def fetch_and_store(ticker: str, _retries: int = 3) -> dict:
    """
    Fetch from FMP and persist to SQLite. Returns the same data dict as fetch_only.
    Use this for pre-loaded tickers that should be stored in the DB and JSON files.
    """
    return _retry(_fetch_and_store, ticker.upper(), _retries, delay_base=8)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_raw(ticker: str) -> dict:
    """Core FMP fetch. Returns the structured data dict without touching the DB."""

    # ── Company profile + live quote ─────────────────────────────────
    profile_list = _get(f"/profile/{ticker}")
    if not profile_list:
        raise RuntimeError(f"{ticker}: no data returned — invalid ticker or API limit reached")
    profile = profile_list[0]

    quote_list = _get(f"/quote/{ticker}")
    quote = quote_list[0] if quote_list else {}

    # Latest-quarter key metrics (EV, ratios, dividend yield)
    km_list = _get(f"/key-metrics/{ticker}", {"period": "quarter", "limit": 1})
    km = km_list[0] if km_list else {}

    company = {
        'ticker':       ticker,
        'name':         profile.get('companyName', ticker),
        'sector':       profile.get('sector', ''),
        'industry':     profile.get('industry', ''),
        'currency':     profile.get('currency', ''),
        'exchange':     profile.get('exchangeShortName', ''),
        'last_updated': str(date.today()),
    }

    market = {
        'market_cap':         _safe(quote,   'marketCap'),
        'enterprise_value':   _safe(km,      'enterpriseValue'),
        'shares_outstanding': _safe(km,      'sharesOutstanding'),
        'price':              _safe(quote,   'price'),
        'pe_trailing':        _safe(quote,   'pe'),
        'pe_forward':         _safe(km,      'forwardPE'),
        'pb_ratio':           _safe(km,      'pbRatio'),
        'ev_ebitda_info':     _safe(km,      'evToEbitda'),
        'ev_revenue_info':    _safe(km,      'evToSales'),
        'dividend_yield':     _safe(km,      'dividendYield'),
        'beta':               _safe(profile, 'beta'),
        'week52_high':        _safe(quote,   'yearHigh'),
        'week52_low':         _safe(quote,   'yearLow'),
    }

    # ── Income statement (quarterly) ──────────────────────────────────
    income = []
    for row in _get(f"/income-statement/{ticker}", {"period": "quarter", "limit": MAX_QUARTERS}):
        period = str(row.get('date', ''))[:10]
        op = _safe(row, 'operatingIncome')
        da = _safe(row, 'depreciationAndAmortization')
        eb = _safe(row, 'ebitda')
        if eb is None and op is not None and da is not None:
            eb = op + da
        income.append({
            'period':                    period,
            'revenue':                   _safe(row, 'revenue'),
            'gross_profit':              _safe(row, 'grossProfit'),
            'operating_income':          op,
            'net_income':                _safe(row, 'netIncome'),
            'ebitda':                    eb,
            'interest_expense':          _safe(row, 'interestExpense'),
            'depreciation_amortization': da,
        })

    # ── Balance sheet (quarterly) ─────────────────────────────────────
    balance = []
    for row in _get(f"/balance-sheet-statement/{ticker}", {"period": "quarter", "limit": MAX_QUARTERS}):
        period = str(row.get('date', ''))[:10]
        assets = _safe(row, 'totalAssets')
        equity = _safe(row, 'totalStockholdersEquity')
        liab   = _safe(row, 'totalLiabilities')
        if liab is None and assets is not None and equity is not None:
            liab = assets - equity
        balance.append({
            'period':              period,
            'total_assets':        assets,
            'total_liabilities':   liab,
            'equity':              equity,
            'cash':                _safe(row, 'cashAndCashEquivalents'),
            'total_debt':          _safe(row, 'totalDebt'),
            'current_assets':      _safe(row, 'totalCurrentAssets'),
            'current_liabilities': _safe(row, 'totalCurrentLiabilities'),
            'inventory':           _safe(row, 'inventory'),
        })

    # ── Cash flow (quarterly) ─────────────────────────────────────────
    cashflow = []
    for row in _get(f"/cash-flow-statement/{ticker}", {"period": "quarter", "limit": MAX_QUARTERS}):
        period = str(row.get('date', ''))[:10]
        op_cf = _safe(row, 'operatingCashFlow')
        capex = _safe(row, 'capitalExpenditure')
        fcf   = _safe(row, 'freeCashFlow')
        if fcf is None and op_cf is not None and capex is not None:
            fcf = op_cf + capex
        cashflow.append({
            'period':         period,
            'operating_cf':   op_cf,
            'investing_cf':   _safe(row, 'investingCashFlow'),
            'financing_cf':   _safe(row, 'financingCashFlow'),
            'capex':          capex,
            'free_cash_flow': fcf,
        })

    return {
        'company':  company,
        'market':   market,
        'income':   income,
        'balance':  balance,
        'cashflow': cashflow,
    }


def _fetch_and_store(ticker: str) -> dict:
    """Fetch via _fetch_raw then persist everything to SQLite in one transaction."""
    data = _fetch_raw(ticker)

    conn = db.get_conn()
    try:
        db.upsert_company(ticker, {
            'longName': data['company']['name'],
            'sector':   data['company']['sector'],
            'industry': data['company']['industry'],
            'currency': data['company']['currency'],
            'exchange': data['company']['exchange'],
        }, conn=conn)
        db.upsert_market_data(ticker, data['market'], conn=conn)
        for row in data['income']:
            db.upsert_income(ticker, row['period'], row, conn=conn)
        for row in data['balance']:
            db.upsert_balance(ticker, row['period'], row, conn=conn)
        for row in data['cashflow']:
            db.upsert_cashflow(ticker, row['period'], row, conn=conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return data
