import math
import time
from datetime import date
import yfinance as yf
from src import db

MAX_QUARTERS = 16  # ~4 years

# Delay constants — Yahoo Finance throttles .info heavily; named here for easy tuning.
_DELAY_AFTER_INFO = 5        # Extra breathing room after the .info call
_DELAY_BETWEEN_CALLS = 3     # Pause between each subsequent yfinance DataFrame fetch
_RETRY_DELAY_BASE = 4        # Base for fetch_only (UI path): 4s, 8s between retries

# curl_cffi impersonates a real browser (TLS fingerprint + headers) which Yahoo Finance
# now requires to avoid 429s — especially on shared IPs like Streamlit Cloud.
try:
    from curl_cffi import requests as curl_requests
    _session = curl_requests.Session(impersonate="chrome124")
except ImportError:
    import requests
    _session = requests.Session()
    _session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })


def _get_df(ticker_obj, *attrs):
    """Try multiple attribute names and return the first non-empty DataFrame."""
    for attr in attrs:
        df = getattr(ticker_obj, attr, None)
        if df is not None and not df.empty:
            return df
    return None


def _val(series, *keys):
    """Extract a float from a Series by trying multiple index keys. Returns None on miss/NaN."""
    for key in keys:
        if key in series.index:
            v = series[key]
            try:
                f = float(v)
                if math.isnan(f):
                    return None
                return f
            except (TypeError, ValueError):
                return None
    return None


def _info_val(info, *keys):
    """
    Safe get from info dict. Returns None for missing keys, None values, or NaN floats.
    Returns 0.0 for genuine zero values — callers must use `is not None` checks, not truthiness.
    """
    for key in keys:
        v = info.get(key)
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
    """Call fn(ticker) up to `retries` times, sleeping delay_base * 2**attempt seconds
    between attempts. If status_callback is provided it is called as
    status_callback(attempt, delay, exc) before each sleep so callers can surface
    retry progress to the UI. Raises the last exception if every attempt fails."""
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


def fetch_only(ticker: str, _retries: int = 2, status_callback=None) -> dict:
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
    """
    Core yfinance fetch. Builds and returns the structured data dict without
    touching the database or filesystem.
    """
    t = yf.Ticker(ticker, session=_session)
    info = t.info or {}
    time.sleep(_DELAY_AFTER_INFO)

    # Yahoo Finance returns a near-empty dict (e.g. {'trailingPegRatio': None}) when
    # rate-limited rather than raising — detect this and raise so _retry can back off.
    if not info.get('shortName') and not info.get('longName') and not info.get('symbol'):
        raise RuntimeError(f"Empty info for {ticker} — likely rate-limited by Yahoo Finance")

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
        'market_cap':         _info_val(info, 'marketCap'),
        'enterprise_value':   _info_val(info, 'enterpriseValue'),
        'shares_outstanding': _info_val(info, 'sharesOutstanding'),
        'price':              _info_val(info, 'currentPrice', 'regularMarketPrice'),
        'pe_trailing':        _info_val(info, 'trailingPE'),
        'pe_forward':         _info_val(info, 'forwardPE'),
        'pb_ratio':           _info_val(info, 'priceToBook'),
        'ev_ebitda_info':     _info_val(info, 'enterpriseToEbitda'),
        'ev_revenue_info':    _info_val(info, 'enterpriseToRevenue'),
        'dividend_yield':     _info_val(info, 'dividendYield'),
        'beta':               _info_val(info, 'beta'),
        'week52_high':        _info_val(info, 'fiftyTwoWeekHigh'),
        'week52_low':         _info_val(info, 'fiftyTwoWeekLow'),
    }

    # ── Income statement (quarterly) ──────────────────────────────
    income = []
    inc_df = _get_df(t, 'quarterly_income_stmt', 'quarterly_financials')
    time.sleep(_DELAY_BETWEEN_CALLS)
    if inc_df is not None:
        for col in list(inc_df.columns)[:MAX_QUARTERS]:
            period = str(col.date())
            s = inc_df[col]
            ebitda = _val(s, 'EBITDA', 'Normalized EBITDA')
            da = _val(s, 'Reconciled Depreciation',
                      'Depreciation And Amortization',
                      'Depreciation Amortization Depletion')
            op = _val(s, 'Operating Income', 'EBIT')
            if ebitda is None and op is not None and da is not None:
                ebitda = op + da
            income.append({
                'period':                    period,
                'revenue':                   _val(s, 'Total Revenue'),
                'gross_profit':              _val(s, 'Gross Profit'),
                'operating_income':          op,
                'net_income':                _val(s, 'Net Income', 'Net Income Common Stockholders'),
                'ebitda':                    ebitda,
                'interest_expense':          _val(s, 'Interest Expense',
                                                  'Interest Expense Non Operating'),
                'depreciation_amortization': da,
            })

    # ── Balance sheet (quarterly) ─────────────────────────────────
    balance = []
    bal_df = _get_df(t, 'quarterly_balance_sheet')
    time.sleep(_DELAY_BETWEEN_CALLS)
    if bal_df is not None:
        for col in list(bal_df.columns)[:MAX_QUARTERS]:
            period = str(col.date())
            s = bal_df[col]
            total_assets = _val(s, 'Total Assets')
            equity = _val(s, 'Stockholders Equity', 'Common Stock Equity',
                          'Total Equity Gross Minority Interest')
            total_liab = _val(s, 'Total Liabilities Net Minority Interest', 'Total Liab')
            if total_liab is None and total_assets is not None and equity is not None:
                total_liab = total_assets - equity
            balance.append({
                'period':              period,
                'total_assets':        total_assets,
                'total_liabilities':   total_liab,
                'equity':              equity,
                'cash':                _val(s, 'Cash And Cash Equivalents',
                                            'Cash Cash Equivalents And Short Term Investments'),
                'total_debt':          _val(s, 'Total Debt',
                                            'Long Term Debt And Capital Lease Obligation'),
                'current_assets':      _val(s, 'Current Assets'),
                'current_liabilities': _val(s, 'Current Liabilities'),
                'inventory':           _val(s, 'Inventory'),
            })

    # ── Cash flow (quarterly) ─────────────────────────────────────
    cashflow = []
    cf_df = _get_df(t, 'quarterly_cash_flow', 'quarterly_cashflow')
    time.sleep(_DELAY_BETWEEN_CALLS)
    if cf_df is not None:
        for col in list(cf_df.columns)[:MAX_QUARTERS]:
            period = str(col.date())
            s = cf_df[col]
            op_cf = _val(s, 'Operating Cash Flow',
                         'Cash Flow From Continuing Operating Activities')
            capex = _val(s, 'Capital Expenditure', 'Purchase Of Ppe')
            fcf = _val(s, 'Free Cash Flow')
            if fcf is None and op_cf is not None and capex is not None:
                fcf = op_cf + capex   # capex is negative in yfinance
            cashflow.append({
                'period':         period,
                'operating_cf':   op_cf,
                'investing_cf':   _val(s, 'Investing Cash Flow',
                                       'Cash Flow From Continuing Investing Activities'),
                'financing_cf':   _val(s, 'Financing Cash Flow',
                                       'Cash Flow From Continuing Financing Activities'),
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
        # upsert_company expects yfinance-style info keys; adapt from our structured dict
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
