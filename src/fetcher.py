import math
import yfinance as yf
from src import db

MAX_QUARTERS = 16  # ~4 years


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
    """Safe get from info dict, returning None for missing/NaN/0 (0 is valid for some fields)."""
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


def fetch_and_store(ticker: str) -> dict:
    ticker = ticker.upper()
    t = yf.Ticker(ticker)
    info = t.info or {}

    # ── Company metadata ─────────────────────────────────────────
    db.upsert_company(ticker, info)

    # ── Market / valuation data ───────────────────────────────────
    market_row = {
        'market_cap':        _info_val(info, 'marketCap'),
        'enterprise_value':  _info_val(info, 'enterpriseValue'),
        'shares_outstanding': _info_val(info, 'sharesOutstanding'),
        'price':             _info_val(info, 'currentPrice', 'regularMarketPrice'),
        'pe_trailing':       _info_val(info, 'trailingPE'),
        'pe_forward':        _info_val(info, 'forwardPE'),
        'pb_ratio':          _info_val(info, 'priceToBook'),
        'ev_ebitda_info':    _info_val(info, 'enterpriseToEbitda'),
        'ev_revenue_info':   _info_val(info, 'enterpriseToRevenue'),
        'dividend_yield':    _info_val(info, 'dividendYield'),
        'beta':              _info_val(info, 'beta'),
        'week52_high':       _info_val(info, 'fiftyTwoWeekHigh'),
        'week52_low':        _info_val(info, 'fiftyTwoWeekLow'),
    }
    db.upsert_market_data(ticker, market_row)

    # ── Income statement (quarterly) ──────────────────────────────
    inc_df = _get_df(t, 'quarterly_income_stmt', 'quarterly_financials')
    if inc_df is not None:
        cols = list(inc_df.columns)[:MAX_QUARTERS]
        for col in cols:
            period = str(col.date())
            s = inc_df[col]
            ebitda = _val(s, 'EBITDA', 'Normalized EBITDA')
            da = _val(s, 'Reconciled Depreciation',
                      'Depreciation And Amortization',
                      'Depreciation Amortization Depletion')
            op = _val(s, 'Operating Income', 'EBIT')
            if ebitda is None and op is not None and da is not None:
                ebitda = op + da
            row = {
                'revenue':          _val(s, 'Total Revenue'),
                'gross_profit':     _val(s, 'Gross Profit'),
                'operating_income': op,
                'net_income':       _val(s, 'Net Income', 'Net Income Common Stockholders'),
                'ebitda':           ebitda,
                'interest_expense': _val(s, 'Interest Expense',
                                         'Interest Expense Non Operating'),
                'depreciation_amortization': da,
            }
            db.upsert_income(ticker, period, row)

    # ── Balance sheet (quarterly) ─────────────────────────────────
    bal_df = _get_df(t, 'quarterly_balance_sheet')
    if bal_df is not None:
        cols = list(bal_df.columns)[:MAX_QUARTERS]
        for col in cols:
            period = str(col.date())
            s = bal_df[col]
            total_assets = _val(s, 'Total Assets')
            equity = _val(s, 'Stockholders Equity', 'Common Stock Equity',
                          'Total Equity Gross Minority Interest')
            total_liab = _val(s, 'Total Liabilities Net Minority Interest', 'Total Liab')
            if total_liab is None and total_assets is not None and equity is not None:
                total_liab = total_assets - equity
            row = {
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
            }
            db.upsert_balance(ticker, period, row)

    # ── Cash flow (quarterly) ─────────────────────────────────────
    cf_df = _get_df(t, 'quarterly_cash_flow', 'quarterly_cashflow')
    if cf_df is not None:
        cols = list(cf_df.columns)[:MAX_QUARTERS]
        for col in cols:
            period = str(col.date())
            s = cf_df[col]
            op_cf = _val(s, 'Operating Cash Flow',
                         'Cash Flow From Continuing Operating Activities')
            capex = _val(s, 'Capital Expenditure', 'Purchase Of Ppe')
            fcf = _val(s, 'Free Cash Flow')
            if fcf is None and op_cf is not None and capex is not None:
                fcf = op_cf + capex   # capex is negative in yfinance
            row = {
                'operating_cf': op_cf,
                'investing_cf': _val(s, 'Investing Cash Flow',
                                     'Cash Flow From Continuing Investing Activities'),
                'financing_cf': _val(s, 'Financing Cash Flow',
                                     'Cash Flow From Continuing Financing Activities'),
                'capex':        capex,
                'free_cash_flow': fcf,
            }
            db.upsert_cashflow(ticker, period, row)

    return db.fetch_all(ticker)
