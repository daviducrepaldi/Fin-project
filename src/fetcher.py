import math
import time
from datetime import date
from yahooquery import Ticker
from src import db

MAX_QUARTERS = 16  # ~4 years

# Delay constants
_DELAY_AFTER_INFO = 5        # Extra breathing room after the info calls
_DELAY_BETWEEN_CALLS = 3     # Pause between each subsequent financial statement fetch
_RETRY_DELAY_BASE = 4        # Base for fetch_only (UI path): 4s, 8s between retries


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
    Fetch from yahooquery and return a data dict. No DB writes, no disk writes.
    Use this for tickers that should not be persisted locally.
    """
    return _retry(_fetch_raw, ticker.upper(), _retries,
                  delay_base=_RETRY_DELAY_BASE, status_callback=status_callback)


def fetch_and_store(ticker: str, _retries: int = 3) -> dict:
    """
    Fetch from yahooquery and persist to SQLite. Returns the same data dict as fetch_only.
    Use this for pre-loaded tickers that should be stored in the DB and JSON files.
    """
    return _retry(_fetch_and_store, ticker.upper(), _retries, delay_base=8)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_raw(ticker: str) -> dict:
    """
    Core yahooquery fetch. Builds and returns the structured data dict without
    touching the database or filesystem.
    """
    t = Ticker(ticker)

    # ── Company & market data ─────────────────────────────────────
    # yahooquery returns a string (error message) instead of a dict on failure;
    # _d() coerces non-dict responses to {} so subsequent .get() calls don't crash.
    def _d(val):
        return val if isinstance(val, dict) else {}

    raw_price    = t.price.get(ticker)
    price_data   = _d(raw_price)
    detail_data  = _d(t.summary_detail.get(ticker))
    key_stats    = _d(t.key_stats.get(ticker))
    profile_data = _d(t.asset_profile.get(ticker))
    time.sleep(_DELAY_AFTER_INFO)

    # Detect empty/rate-limited response — surface the raw error string if available
    if not price_data.get('longName') and not price_data.get('shortName') and not price_data.get('symbol'):
        reason = raw_price if isinstance(raw_price, str) else "likely rate-limited or invalid ticker"
        raise RuntimeError(f"{ticker}: {reason}")

    company = {
        'ticker':       ticker,
        'name':         price_data.get('longName') or price_data.get('shortName', ticker),
        'sector':       profile_data.get('sector', ''),
        'industry':     profile_data.get('industry', ''),
        'currency':     price_data.get('currency', ''),
        'exchange':     price_data.get('exchangeName', ''),
        'last_updated': str(date.today()),
    }

    def _safe(d, *keys):
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

    market = {
        'market_cap':         _safe(price_data,  'marketCap'),
        'enterprise_value':   _safe(key_stats,   'enterpriseValue'),
        'shares_outstanding': _safe(key_stats,   'sharesOutstanding', 'impliedSharesOutstanding'),
        'price':              _safe(price_data,  'regularMarketPrice'),
        'pe_trailing':        _safe(detail_data, 'trailingPE'),
        'pe_forward':         _safe(key_stats,   'forwardPE'),
        'pb_ratio':           _safe(key_stats,   'priceToBook'),
        'ev_ebitda_info':     _safe(key_stats,   'enterpriseToEbitda'),
        'ev_revenue_info':    _safe(key_stats,   'enterpriseToRevenue'),
        'dividend_yield':     _safe(detail_data, 'dividendYield'),
        'beta':               _safe(detail_data, 'beta'),
        'week52_high':        _safe(detail_data, 'fiftyTwoWeekHigh'),
        'week52_low':         _safe(detail_data, 'fiftyTwoWeekLow'),
    }

    # ── Financial statement helpers ───────────────────────────────
    def _row_val(row, *keys):
        for k in keys:
            v = row.get(k)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
        return None

    def _parse_period(val):
        if hasattr(val, 'date'):
            return str(val.date())
        return str(val)[:10]

    # ── Income statement (quarterly) ──────────────────────────────
    income = []
    try:
        inc_df = t.income_statement(frequency='q')
        time.sleep(_DELAY_BETWEEN_CALLS)
        if inc_df is not None and not inc_df.empty and 'asOfDate' in inc_df.columns:
            for _, row in inc_df.sort_values('asOfDate', ascending=False).head(MAX_QUARTERS).iterrows():
                period = _parse_period(row['asOfDate'])
                op = _row_val(row, 'OperatingIncome', 'EBIT')
                da = _row_val(row, 'ReconciledDepreciation', 'DepreciationAmortizationDepletion')
                eb = _row_val(row, 'EBITDA', 'NormalizedEBITDA')
                if eb is None and op is not None and da is not None:
                    eb = op + da
                income.append({
                    'period':                    period,
                    'revenue':                   _row_val(row, 'TotalRevenue'),
                    'gross_profit':              _row_val(row, 'GrossProfit'),
                    'operating_income':          op,
                    'net_income':                _row_val(row, 'NetIncome', 'NetIncomeCommonStockholders'),
                    'ebitda':                    eb,
                    'interest_expense':          _row_val(row, 'InterestExpense', 'InterestExpenseNonOperating'),
                    'depreciation_amortization': da,
                })
    except Exception:
        time.sleep(_DELAY_BETWEEN_CALLS)

    # ── Balance sheet (quarterly) ─────────────────────────────────
    balance = []
    try:
        bal_df = t.balance_sheet(frequency='q')
        time.sleep(_DELAY_BETWEEN_CALLS)
        if bal_df is not None and not bal_df.empty and 'asOfDate' in bal_df.columns:
            for _, row in bal_df.sort_values('asOfDate', ascending=False).head(MAX_QUARTERS).iterrows():
                period = _parse_period(row['asOfDate'])
                assets = _row_val(row, 'TotalAssets')
                equity = _row_val(row, 'StockholdersEquity', 'CommonStockEquity',
                                  'TotalEquityGrossMinorityInterest')
                liab   = _row_val(row, 'TotalLiabilitiesNetMinorityInterest', 'TotalLiab')
                if liab is None and assets is not None and equity is not None:
                    liab = assets - equity
                balance.append({
                    'period':              period,
                    'total_assets':        assets,
                    'total_liabilities':   liab,
                    'equity':              equity,
                    'cash':                _row_val(row, 'CashAndCashEquivalents',
                                                    'CashCashEquivalentsAndShortTermInvestments'),
                    'total_debt':          _row_val(row, 'TotalDebt',
                                                    'LongTermDebtAndCapitalLeaseObligation'),
                    'current_assets':      _row_val(row, 'CurrentAssets'),
                    'current_liabilities': _row_val(row, 'CurrentLiabilities'),
                    'inventory':           _row_val(row, 'Inventory'),
                })
    except Exception:
        time.sleep(_DELAY_BETWEEN_CALLS)

    # ── Cash flow (quarterly) ─────────────────────────────────────
    cashflow = []
    try:
        cf_df = t.cash_flow(frequency='q')
        time.sleep(_DELAY_BETWEEN_CALLS)
        if cf_df is not None and not cf_df.empty and 'asOfDate' in cf_df.columns:
            for _, row in cf_df.sort_values('asOfDate', ascending=False).head(MAX_QUARTERS).iterrows():
                period = _parse_period(row['asOfDate'])
                op_cf = _row_val(row, 'OperatingCashFlow',
                                 'CashFlowFromContinuingOperatingActivities')
                capex = _row_val(row, 'CapitalExpenditure')
                fcf   = _row_val(row, 'FreeCashFlow')
                if fcf is None and op_cf is not None and capex is not None:
                    fcf = op_cf + capex
                cashflow.append({
                    'period':         period,
                    'operating_cf':   op_cf,
                    'investing_cf':   _row_val(row, 'InvestingCashFlow',
                                               'CashFlowFromContinuingInvestingActivities'),
                    'financing_cf':   _row_val(row, 'FinancingCashFlow',
                                               'CashFlowFromContinuingFinancingActivities'),
                    'capex':          capex,
                    'free_cash_flow': fcf,
                })
    except Exception:
        time.sleep(_DELAY_BETWEEN_CALLS)

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
