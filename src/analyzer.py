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


def _round_ratio(a, b, ndigits=2):
    """Compute a/b and round. Returns None if either operand is None or b is 0."""
    v = _div(a, b)
    return round(v, ndigits) if v is not None else None


def _pct(val, decimals=1):
    return round(val * 100, decimals) if val is not None else None


def _ttm(rows, field, n=4):
    """Sum the `n` most-recent non-None values. Returns None if fewer than n found."""
    vals = [r.get(field) for r in rows if r.get(field) is not None][:n]
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
        'current_ratio': _round_ratio(cur_a, cur_l),
        'quick_ratio':   _round_ratio(quick_assets, cur_l),
        # Leverage
        'debt_to_equity':   _round_ratio(debt, equity),
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
    ev_ebitda_calc = _round_ratio(ev, ttm_ebitda)
    ev_rev_calc    = _round_ratio(ev, ttm_rev)

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
        'current_ratio': _round_ratio(cur_a, cur_l),
        'quick_ratio':   _round_ratio(quick_assets, cur_l),
        # Leverage (latest quarter)
        'debt_to_equity':    _round_ratio(debt, equity),
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


# ── rating helpers ────────────────────────────────────────────────────────────

def _score_bracket(value, brackets):
    """Lower-is-better scoring. brackets: [(threshold, pts)] sorted ascending by threshold."""
    for threshold, pts in brackets:
        if value <= threshold:
            return pts
    return 0


def _score_bracket_high(value, brackets):
    """Higher-is-better scoring. brackets: [(threshold, pts)] sorted descending by threshold."""
    for threshold, pts in brackets:
        if value >= threshold:
            return pts
    return 0


# ── Buy/Hold/Sell rating ──────────────────────────────────────────────────────

def compute_rating(result: dict) -> dict:
    """
    Compute a Buy/Hold/Sell rating from an already-computed result dict.

    Scoring model (100 pts total):
      Valuation     30 pts  — P/E trailing (12), EV/EBITDA (10), P/B (8)
      Profitability 30 pts  — Net Margin TTM (15), ROE TTM (15)
      Growth        25 pts  — Revenue YoY % (most recent quarter with data)
      Health        15 pts  — Current Ratio (8), Debt/Equity (7)

    Thresholds: score >= 65 → BUY, >= 40 → HOLD, < 40 → SELL
    Missing inputs are skipped and the component score is proportionally rescaled.
    """
    _DISCLAIMER = "Quantitative signal for educational purposes only. Not financial advice."

    if not result:
        return {"rating": "N/A", "score": None, "breakdown": {}, "disclaimer": _DISCLAIMER, "data_quality": "none"}

    market = result.get("market") or {}
    ttm    = result.get("ttm") or {}
    trends = result.get("trends") or []

    # ── Valuation (30 pts: P/E=12, EV/EBITDA=10, P/B=8) ──────────────────────
    pe = market.get("pe_trailing")
    ev = market.get("ev_ebitda_info")
    pb = market.get("pb_ratio")

    pe_score = ev_score = pb_score = None
    if pe is not None:
        pe_score = 0 if pe < 0 else _score_bracket(pe, [(12, 12), (18, 10), (25, 7), (35, 4), (50, 2)])
    if ev is not None and ev >= 0:
        ev_score = _score_bracket(ev, [(8, 10), (12, 8), (18, 5), (25, 2)])
    if pb is not None and pb >= 0:
        pb_score = _score_bracket(pb, [(1.5, 8), (3, 6), (5, 3), (10, 1)])

    val_raw = val_avail = 0
    for score, max_pts in [(pe_score, 12), (ev_score, 10), (pb_score, 8)]:
        if score is not None:
            val_raw   += score
            val_avail += max_pts
    val_component = (val_raw / val_avail * 30) if val_avail > 0 else None

    # ── Profitability (30 pts: net_margin=15, roe=15) ─────────────────────────
    nm  = ttm.get("net_margin")
    roe = ttm.get("roe")

    nm_score = roe_score = None
    if nm is not None:
        nm_score = 0 if nm < 0 else _score_bracket_high(nm, [(25, 15), (15, 12), (8, 8), (3, 4), (0, 1)])
    if roe is not None:
        roe_score = 0 if roe < 0 else _score_bracket_high(roe, [(30, 15), (20, 12), (12, 8), (5, 4), (0, 1)])

    prof_raw = prof_avail = 0
    for score, max_pts in [(nm_score, 15), (roe_score, 15)]:
        if score is not None:
            prof_raw   += score
            prof_avail += max_pts
    prof_component = (prof_raw / prof_avail * 30) if prof_avail > 0 else None

    # ── Growth (25 pts: revenue YoY %) ────────────────────────────────────────
    rev_yoy = None
    for t in trends:
        if t is not None and t.get("rev_yoy_pct") is not None:
            rev_yoy = t["rev_yoy_pct"]
            break

    if rev_yoy is None:
        growth_component = 12.0   # neutral — no data, don't penalise
    elif rev_yoy >= 25:  growth_component = 25.0
    elif rev_yoy >= 15:  growth_component = 20.0
    elif rev_yoy >= 8:   growth_component = 15.0
    elif rev_yoy >= 3:   growth_component = 10.0
    elif rev_yoy >= 0:   growth_component = 6.0
    elif rev_yoy >= -5:  growth_component = 3.0
    else:                growth_component = 0.0

    # ── Financial Health (15 pts: current_ratio=8, d/e=7) ────────────────────
    cr = ttm.get("current_ratio")
    de = ttm.get("debt_to_equity")

    cr_score = de_score = None
    if cr is not None:
        cr_score = _score_bracket_high(cr, [(2, 8), (1.5, 6), (1, 3)])  # below 1 → 0
    if de is not None:
        de_score = 0 if de < 0 else _score_bracket(de, [(0.3, 7), (0.8, 5), (1.5, 3), (3.0, 1)])

    health_raw = health_avail = 0
    for score, max_pts in [(cr_score, 8), (de_score, 7)]:
        if score is not None:
            health_raw   += score
            health_avail += max_pts
    health_component = (health_raw / health_avail * 15) if health_avail > 0 else None

    # ── Aggregate ─────────────────────────────────────────────────────────────
    _components = [
        ("valuation",     val_component,    30),
        ("profitability", prof_component,   30),
        ("growth",        growth_component, 25),
        ("health",        health_component, 15),
    ]

    none_count  = sum(1 for _, v, _ in _components if v is None)
    total_score = sum(v if v is not None else max_pts * 0.5 for _, v, max_pts in _components)

    if   none_count == 0: data_quality = "full"
    elif none_count <= 2: data_quality = "partial"
    elif none_count == 3: data_quality = "minimal"
    else:                 data_quality = "none"

    if data_quality == "none":
        return {"rating": "N/A", "score": None, "breakdown": {}, "disclaimer": _DISCLAIMER, "data_quality": "none"}

    total_score = round(total_score, 1)
    if   total_score >= 65: rating = "BUY"
    elif total_score >= 40: rating = "HOLD"
    else:                   rating = "SELL"

    breakdown = {
        "valuation":     {"score": round(val_component    if val_component    is not None else 15.0, 1), "max": 30, "label": "Valuation"},
        "profitability": {"score": round(prof_component   if prof_component   is not None else 15.0, 1), "max": 30, "label": "Profitability"},
        "growth":        {"score": round(growth_component,                                             1), "max": 25, "label": "Growth"},
        "health":        {"score": round(health_component if health_component is not None else 7.5,   1), "max": 15, "label": "Financial Health"},
    }

    return {
        "rating":       rating,
        "score":        total_score,
        "breakdown":    breakdown,
        "disclaimer":   _DISCLAIMER,
        "data_quality": data_quality,
    }
