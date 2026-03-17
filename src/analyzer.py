"""
analyzer.py — compute quarterly ratios, TTM aggregates, and YoY trends.

Return structure from compute_ratios():
{
  'quarters':  [dict, ...]   # per-quarter ratios, newest first, up to MAX_DISPLAY
  'ttm':       dict          # trailing-twelve-month aggregates
  'trends':    [dict, ...]   # YoY changes for quarters that have a prior-year peer
  'market':    dict          # pass-through from data['market']
  'company':   dict          # pass-through from data['company']
}
"""

MAX_DISPLAY = 8   # quarters shown side-by-side in terminal


# ── helpers ───────────────────────────────────────────────────────────────────

def _div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def _pct(val, decimals=1):
    return round(val * 100, decimals) if val is not None else None


def _ttm(rows, field, n=4):
    """Sum the `n` most-recent non-None values. Returns None if fewer than n found."""
    vals = [r[field] for r in rows if r.get(field) is not None][:n]
    return sum(vals) if len(vals) == n else None


def _arrow(pct):
    if pct is None:
        return ''
    if pct >= 15:  return '↑↑'
    if pct >= 3:   return '↑'
    if pct > -3:   return '→'
    if pct > -15:  return '↓'
    return '↓↓'


def _yoy_pct(current, prior):
    if current is None or prior is None or prior == 0:
        return None
    return round((current - prior) / abs(prior) * 100, 1)


def _yoy_bps(current_pct, prior_pct):
    """Both inputs are already in % (e.g. 46.3). Returns bps difference."""
    if current_pct is None or prior_pct is None:
        return None
    return round((current_pct - prior_pct) * 100, 0)


# ── per-quarter ratio computation ────────────────────────────────────────────

def _ratios_for_quarter(inc, bal, cf):
    """
    Compute ratios from one quarter's income, balance, cashflow row dicts.
    All margin/return values are in % (e.g. 46.3, not 0.463).
    """
    rev  = inc.get('revenue')
    gp   = inc.get('gross_profit')
    op   = inc.get('operating_income')
    ni   = inc.get('net_income')
    ebitda = inc.get('ebitda')
    interest = inc.get('interest_expense')

    assets   = bal.get('total_assets')
    equity   = bal.get('equity')
    debt     = bal.get('total_debt')
    cash     = bal.get('cash')
    cur_a    = bal.get('current_assets')
    cur_l    = bal.get('current_liabilities')
    inv      = bal.get('inventory')

    op_cf = cf.get('operating_cf')
    fcf   = cf.get('free_cash_flow')

    quick_assets = None
    if cur_a is not None:
        quick_assets = cur_a - (inv or 0)

    # Interest coverage: EBIT / |interest_expense|
    int_cov = None
    if op is not None and interest is not None and interest != 0:
        int_cov = round(op / abs(interest), 2)

    net_debt = None
    if debt is not None:
        net_debt = debt - (cash or 0)

    return {
        # Scale
        'revenue':       rev,
        'net_income':    ni,
        'free_cash_flow': fcf,
        # Profitability (%)
        'gross_margin':     _pct(_div(gp, rev)),
        'op_margin':        _pct(_div(op, rev)),
        'ebitda_margin':    _pct(_div(ebitda, rev)),
        'net_margin':       _pct(_div(ni, rev)),
        'fcf_margin':       _pct(_div(fcf, rev)),
        'op_cf_margin':     _pct(_div(op_cf, rev)),
        # Returns (point-in-time equity/assets — TTM version computed separately)
        'roe':  _pct(_div(ni, equity)),
        'roa':  _pct(_div(ni, assets)),
        # Liquidity
        'current_ratio': round(_div(cur_a, cur_l), 2) if _div(cur_a, cur_l) else None,
        'quick_ratio':   round(_div(quick_assets, cur_l), 2) if _div(quick_assets, cur_l) else None,
        # Leverage
        'debt_to_equity':   round(_div(debt, equity), 2) if _div(debt, equity) else None,
        'net_debt':         net_debt,
        'interest_coverage': int_cov,
    }


# ── TTM ratios ────────────────────────────────────────────────────────────────

def _ttm_ratios(income_rows, balance_rows, cashflow_rows, market):
    """
    Compute trailing-twelve-month ratios.
    Flow items (income, CF): sum of last 4 quarters.
    Stock items (balance sheet): most recent quarter.
    """
    ttm_rev    = _ttm(income_rows, 'revenue')
    ttm_gp     = _ttm(income_rows, 'gross_profit')
    ttm_op     = _ttm(income_rows, 'operating_income')
    ttm_ni     = _ttm(income_rows, 'net_income')
    ttm_ebitda = _ttm(income_rows, 'ebitda')
    ttm_int    = _ttm(income_rows, 'interest_expense')
    ttm_opcf   = _ttm(cashflow_rows, 'operating_cf')
    ttm_fcf    = _ttm(cashflow_rows, 'free_cash_flow')

    bal = balance_rows[0] if balance_rows else {}
    equity  = bal.get('equity')
    assets  = bal.get('total_assets')
    debt    = bal.get('total_debt')
    cash    = bal.get('cash')
    cur_a   = bal.get('current_assets')
    cur_l   = bal.get('current_liabilities')
    inv     = bal.get('inventory')

    quick_assets = None
    if cur_a is not None:
        quick_assets = cur_a - (inv or 0)

    int_cov = None
    if ttm_op is not None and ttm_int is not None and ttm_int != 0:
        int_cov = round(ttm_op / abs(ttm_int), 2)

    net_debt = (debt - (cash or 0)) if debt is not None else None

    # Calculated EV/EBITDA
    ev = market.get('enterprise_value')
    ev_ebitda_calc = round(_div(ev, ttm_ebitda), 2) if (ev and ttm_ebitda) else None
    ev_rev_calc    = round(_div(ev, ttm_rev), 2) if (ev and ttm_rev) else None

    return {
        'revenue':      ttm_rev,
        'net_income':   ttm_ni,
        'free_cash_flow': ttm_fcf,
        'ebitda':       ttm_ebitda,
        # Margins
        'gross_margin':  _pct(_div(ttm_gp, ttm_rev)),
        'op_margin':     _pct(_div(ttm_op, ttm_rev)),
        'ebitda_margin': _pct(_div(ttm_ebitda, ttm_rev)),
        'net_margin':    _pct(_div(ttm_ni, ttm_rev)),
        'fcf_margin':    _pct(_div(ttm_fcf, ttm_rev)),
        'op_cf_margin':  _pct(_div(ttm_opcf, ttm_rev)),
        # Returns (TTM NI / latest balance sheet)
        'roe': _pct(_div(ttm_ni, equity)),
        'roa': _pct(_div(ttm_ni, assets)),
        # Liquidity (latest quarter)
        'current_ratio': round(_div(cur_a, cur_l), 2) if _div(cur_a, cur_l) else None,
        'quick_ratio':   round(_div(quick_assets, cur_l), 2) if _div(quick_assets, cur_l) else None,
        # Leverage (latest quarter)
        'debt_to_equity':    round(_div(debt, equity), 2) if _div(debt, equity) else None,
        'net_debt':          net_debt,
        'interest_coverage': int_cov,
        # Valuation (calculated)
        'ev_ebitda_calc': ev_ebitda_calc,
        'ev_rev_calc':    ev_rev_calc,
    }


# ── YoY trends ────────────────────────────────────────────────────────────────

def _compute_trends(quarterly_ratios):
    """
    For each quarter that has a same-quarter-prior-year peer (index i vs i+4),
    compute YoY % change for flow items and bps change for margin items.
    Returns a list aligned with quarterly_ratios (None entries where no prior-year data).
    """
    trends = []
    n = len(quarterly_ratios)
    for i, q in enumerate(quarterly_ratios):
        if i + 4 >= n:
            trends.append(None)
            continue
        prev = quarterly_ratios[i + 4]
        rev_yoy  = _yoy_pct(q.get('revenue'), prev.get('revenue'))
        fcf_yoy  = _yoy_pct(q.get('free_cash_flow'), prev.get('free_cash_flow'))
        gm_bps   = _yoy_bps(q.get('gross_margin'),  prev.get('gross_margin'))
        op_bps   = _yoy_bps(q.get('op_margin'),     prev.get('op_margin'))
        ni_bps   = _yoy_bps(q.get('net_margin'),    prev.get('net_margin'))
        eb_bps   = _yoy_bps(q.get('ebitda_margin'), prev.get('ebitda_margin'))
        trends.append({
            'period':       q['period'],
            'rev_yoy_pct':  rev_yoy,  'rev_arrow':  _arrow(rev_yoy),
            'fcf_yoy_pct':  fcf_yoy,  'fcf_arrow':  _arrow(fcf_yoy),
            'gm_bps':       gm_bps,   'gm_arrow':   _arrow(gm_bps / 100 if gm_bps else None),
            'op_bps':       op_bps,   'op_arrow':   _arrow(op_bps / 100 if op_bps else None),
            'ni_bps':       ni_bps,   'ni_arrow':   _arrow(ni_bps / 100 if ni_bps else None),
            'eb_bps':       eb_bps,   'eb_arrow':   _arrow(eb_bps / 100 if eb_bps else None),
        })
    return trends


# ── main entry point ─────────────────────────────────────────────────────────

def compute_ratios(data: dict) -> dict:
    income_rows   = data.get('income', [])
    balance_rows  = data.get('balance', [])
    cashflow_rows = data.get('cashflow', [])
    market        = data.get('market', {})

    # Align rows by period
    income_by_p  = {r['period']: r for r in income_rows}
    balance_by_p = {r['period']: r for r in balance_rows}
    cf_by_p      = {r['period']: r for r in cashflow_rows}

    all_periods = sorted(
        set(income_by_p) | set(balance_by_p) | set(cf_by_p),
        reverse=True
    )

    quarterly = []
    for p in all_periods:
        inc = income_by_p.get(p, {})
        bal = balance_by_p.get(p, {})
        cf  = cf_by_p.get(p, {})
        r = _ratios_for_quarter(inc, bal, cf)
        r['period'] = p
        quarterly.append(r)

    ttm = _ttm_ratios(income_rows, balance_rows, cashflow_rows, market)
    trends = _compute_trends(quarterly)

    return {
        'quarters': quarterly[:MAX_DISPLAY],
        'all_quarters': quarterly,          # for CSV export
        'trends':   trends[:MAX_DISPLAY],
        'ttm':      ttm,
        'market':   market,
        'company':  data.get('company', {}),
    }
