import math
import time
from datetime import date

import yfinance as yf

from src import db

MAX_QUARTERS = 16  # ~4 years

_RETRY_DELAY_BASE = 4
_RATE_LIMIT_DELAY = 60   # seconds to wait when Yahoo rate-limits us


def _safe(d: dict, *keys):
    """Get first non-None/NaN numeric value from a dict by key."""
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


def _sv(series, *names):
    """Get first non-NaN numeric value from a pandas Series by row name."""
    for name in names:
        if name in series.index:
            try:
                f = float(series[name])
                return None if math.isnan(f) else f
            except (TypeError, ValueError):
                pass
    return None


# ── Public API ────────────────────────────────────────────────────────────────

def _is_rate_limited(exc: Exception) -> bool:
    name = type(exc).__name__
    msg  = str(exc).lower()
    return 'ratelimit' in name.lower() or 'too many requests' in msg or '429' in msg


def _retry(fn, ticker: str, retries: int, delay_base: int = 4, status_callback=None) -> dict:
    """Call fn(ticker) up to `retries` times with exponential back-off.
    Rate-limit errors use a fixed long delay instead of the short base delay."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn(ticker)
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                delay = _RATE_LIMIT_DELAY if _is_rate_limited(e) else delay_base * 2 ** attempt
                if status_callback:
                    status_callback(attempt, delay, e)
                time.sleep(delay)
    raise last_exc


def fetch_only(ticker: str, _retries: int = 3, status_callback=None) -> dict:
    """
    Fetch from yfinance and return a data dict. No DB writes, no disk writes.
    Use this for tickers that should not be persisted locally.
    """
    return _retry(_fetch_raw, ticker.upper(), _retries,
                  delay_base=_RETRY_DELAY_BASE, status_callback=status_callback)


def fetch_and_store(ticker: str, _retries: int = 3) -> dict:
    """
    Fetch from yfinance and persist to SQLite. Returns the same data dict as fetch_only.
    Use this for pre-loaded tickers that should be stored in the DB and JSON files.
    """
    return _retry(_fetch_and_store, ticker.upper(), _retries, delay_base=8)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_raw(ticker: str) -> dict:
    """Core yfinance fetch. Returns the structured data dict without touching the DB."""
    t = yf.Ticker(ticker)
    info = t.info

    # Detect invalid ticker or empty response
    if not info.get('longName') and not info.get('shortName') and not info.get('symbol'):
        raise RuntimeError(f"{ticker}: no data returned — invalid ticker or rate-limited")

    company = {
        'ticker':       ticker,
        'name':         info.get('longName') or info.get('shortName', ticker),
        'sector':       info.get('sector', ''),
        'industry':     info.get('industry', ''),
        'currency':     info.get('currency', ''),
        'exchange':     info.get('exchange', ''),
        'last_updated': str(date.today()),
    }

    market = {
        'market_cap':         _safe(info, 'marketCap'),
        'enterprise_value':   _safe(info, 'enterpriseValue'),
        'shares_outstanding': _safe(info, 'sharesOutstanding', 'impliedSharesOutstanding'),
        'price':              _safe(info, 'currentPrice', 'regularMarketPrice'),
        'pe_trailing':        _safe(info, 'trailingPE'),
        'pe_forward':         _safe(info, 'forwardPE'),
        'pb_ratio':           _safe(info, 'priceToBook'),
        'ev_ebitda_info':     _safe(info, 'enterpriseToEbitda'),
        'ev_revenue_info':    _safe(info, 'enterpriseToRevenue'),
        'dividend_yield':     _safe(info, 'dividendYield'),
        'beta':               _safe(info, 'beta'),
        'week52_high':        _safe(info, 'fiftyTwoWeekHigh'),
        'week52_low':         _safe(info, 'fiftyTwoWeekLow'),
    }

    # ── Income statement (quarterly) ──────────────────────────────────
    income = []
    try:
        inc_df = t.quarterly_income_stmt
        if inc_df is not None and not inc_df.empty:
            for col in sorted(inc_df.columns, reverse=True)[:MAX_QUARTERS]:
                s = inc_df[col]
                period = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
                op = _sv(s, 'Operating Income', 'EBIT')
                da = _sv(s, 'Reconciled Depreciation', 'Depreciation And Amortization',
                             'Depreciation Amortization Depletion')
                eb = _sv(s, 'EBITDA', 'Normalized EBITDA')
                if eb is None and op is not None and da is not None:
                    eb = op + da
                income.append({
                    'period':                    period,
                    'revenue':                   _sv(s, 'Total Revenue'),
                    'gross_profit':              _sv(s, 'Gross Profit'),
                    'operating_income':          op,
                    'net_income':                _sv(s, 'Net Income', 'Net Income Common Stockholders'),
                    'ebitda':                    eb,
                    'interest_expense':          _sv(s, 'Interest Expense'),
                    'depreciation_amortization': da,
                })
    except Exception:
        pass

    # ── Balance sheet (quarterly) ─────────────────────────────────────
    balance = []
    try:
        bal_df = t.quarterly_balance_sheet
        if bal_df is not None and not bal_df.empty:
            for col in sorted(bal_df.columns, reverse=True)[:MAX_QUARTERS]:
                s = bal_df[col]
                period = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
                assets = _sv(s, 'Total Assets')
                equity = _sv(s, 'Common Stock Equity', 'Stockholders Equity',
                                'Total Equity Gross Minority Interest')
                liab   = _sv(s, 'Total Liabilities Net Minority Interest', 'Total Liabilities')
                if liab is None and assets is not None and equity is not None:
                    liab = assets - equity
                balance.append({
                    'period':              period,
                    'total_assets':        assets,
                    'total_liabilities':   liab,
                    'equity':              equity,
                    'cash':                _sv(s, 'Cash And Cash Equivalents',
                                               'Cash Cash Equivalents And Federal Funds Sold'),
                    'total_debt':          _sv(s, 'Total Debt'),
                    'current_assets':      _sv(s, 'Current Assets'),
                    'current_liabilities': _sv(s, 'Current Liabilities'),
                    'inventory':           _sv(s, 'Inventory'),
                })
    except Exception:
        pass

    # ── Cash flow (quarterly) ─────────────────────────────────────────
    cashflow = []
    try:
        cf_df = t.quarterly_cashflow
        if cf_df is not None and not cf_df.empty:
            for col in sorted(cf_df.columns, reverse=True)[:MAX_QUARTERS]:
                s = cf_df[col]
                period = str(col.date()) if hasattr(col, 'date') else str(col)[:10]
                op_cf = _sv(s, 'Operating Cash Flow', 'Cash Flow From Continuing Operating Activities')
                capex = _sv(s, 'Capital Expenditure')
                fcf   = _sv(s, 'Free Cash Flow')
                if fcf is None and op_cf is not None and capex is not None:
                    fcf = op_cf + capex
                cashflow.append({
                    'period':         period,
                    'operating_cf':   op_cf,
                    'investing_cf':   _sv(s, 'Investing Cash Flow',
                                          'Cash Flow From Continuing Investing Activities'),
                    'financing_cf':   _sv(s, 'Financing Cash Flow',
                                          'Cash Flow From Continuing Financing Activities'),
                    'capex':          capex,
                    'free_cash_flow': fcf,
                })
    except Exception:
        pass

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
