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

from datetime import datetime

MAX_DISPLAY = 8   # quarters shown side-by-side in terminal


def _parse_period(period_str):
    try:
        return datetime.strptime(period_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None


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
    """
    Sum the `n` most-recent non-None values, but only if they actually form a
    trailing-twelve-month window (span ≤ ~13 months). Non-contiguous quarters
    (e.g. a missing filing) would silently produce a >12-month "TTM" otherwise.
    Annual-only filers (consecutive periods ~1 year apart) use the latest value
    directly — a fiscal year already is a trailing twelve months.
    """
    pairs = [(r['period'], r[field]) for r in rows
             if r.get(field) is not None and r.get('period')]
    pairs.sort(reverse=True)
    dates = [_parse_period(p) for p, _ in pairs]
    if len(pairs) >= 2 and all(dates):
        gaps = [(dates[i] - dates[i + 1]).days for i in range(len(dates) - 1)]
        if all(300 <= g <= 430 for g in gaps):
            return pairs[0][1]   # annual rows — latest FY value is the TTM
    if len(pairs) < n:
        return None
    newest, oldest = _parse_period(pairs[0][0]), _parse_period(pairs[n - 1][0])
    if newest and oldest and (newest - oldest).days > 400:
        return None   # quarters aren't contiguous — refuse to fake a TTM
    return sum(v for _, v in pairs[:n])


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
    For each quarter that has a same-quarter-prior-year peer, compute YoY %
    change for flow items and bps change for margin items. The peer is found
    by date (period ~1 year earlier), not by index offset — quarterly data can
    have gaps, so "4 rows back" is not always the same quarter a year ago.
    Returns a list aligned with quarterly_ratios (None entries where no prior-year data).
    """
    trends = []
    dated = [(_parse_period(q.get('period')), q) for q in quarterly_ratios]
    for q in quarterly_ratios:
        cur_date = _parse_period(q.get('period'))
        prev = None
        if cur_date:
            for p_date, p_q in dated:
                if p_date and 330 <= (cur_date - p_date).days <= 430:
                    prev = p_q
                    break
        if prev is None:
            trends.append(None)
            continue
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
            'gm_bps':       gm_bps,   'gm_arrow':   _arrow(gm_bps / 100 if gm_bps is not None else None),
            'op_bps':       op_bps,   'op_arrow':   _arrow(op_bps / 100 if op_bps is not None else None),
            'ni_bps':       ni_bps,   'ni_arrow':   _arrow(ni_bps / 100 if ni_bps is not None else None),
            'eb_bps':       eb_bps,   'eb_arrow':   _arrow(eb_bps / 100 if eb_bps is not None else None),
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

# Sector-relative valuation thresholds. A P/E of 25 is expensive for a bank
# and unremarkable for a semiconductor company; each bucket gets its own
# brackets. 'skip' marks metrics that are meaningless for the sector
# (EV/EBITDA and FCF yield for financials — debt is their raw material).
_VAL_PROFILES = {
    'general': {
        'pe':   [(12, 8), (18, 6), (25, 4), (35, 2), (50, 1)],
        'ev':   [(8, 6), (12, 5), (18, 3), (25, 1)],
        'fcfy': [(8, 7), (5, 5), (3, 3), (1.5, 2), (0, 1)],
        'pb':   [(1.5, 4), (3, 3), (5, 2), (10, 1)],
    },
    'technology': {
        'pe':   [(18, 8), (25, 6), (35, 4), (45, 2), (60, 1)],
        'ev':   [(12, 6), (16, 5), (22, 3), (30, 1)],
        'fcfy': [(6, 7), (4, 5), (2.5, 3), (1, 2), (0, 1)],
        'pb':   [(5, 4), (8, 3), (12, 2), (20, 1)],
    },
    'healthcare': {
        'pe':   [(16, 8), (22, 6), (30, 4), (40, 2), (55, 1)],
        'ev':   [(10, 6), (14, 5), (20, 3), (28, 1)],
        'fcfy': [(7, 7), (4.5, 5), (2.5, 3), (1, 2), (0, 1)],
        'pb':   [(3, 4), (5, 3), (8, 2), (14, 1)],
    },
    'financials': {
        'pe':   [(10, 8), (14, 6), (18, 4), (25, 2), (35, 1)],
        'ev':   'skip',
        'fcfy': 'skip',
        'pb':   [(1.0, 4), (1.5, 3), (2.5, 2), (4, 1)],
    },
    'real_estate': {
        'pe':   [(14, 8), (20, 6), (28, 4), (38, 2), (50, 1)],
        'ev':   [(14, 6), (18, 5), (24, 3), (32, 1)],
        'fcfy': 'skip',   # capex-heavy; FFO would be the right lens, not FCF
        'pb':   [(1.2, 4), (2, 3), (3, 2), (5, 1)],
    },
    'utilities': {
        'pe':   [(14, 8), (18, 6), (22, 4), (28, 2), (35, 1)],
        'ev':   [(9, 6), (12, 5), (15, 3), (20, 1)],
        'fcfy': [(6, 7), (4, 5), (2, 3), (0.5, 2), (0, 1)],
        'pb':   [(1.3, 4), (2, 3), (3, 2), (5, 1)],
    },
    'energy': {
        'pe':   [(8, 8), (12, 6), (16, 4), (25, 2), (35, 1)],
        'ev':   [(5, 6), (7, 5), (10, 3), (14, 1)],
        'fcfy': [(10, 7), (7, 5), (4, 3), (2, 2), (0, 1)],
        'pb':   [(1.2, 4), (2, 3), (3, 2), (5, 1)],
    },
    'consumer': {
        'pe':   [(14, 8), (20, 6), (28, 4), (38, 2), (50, 1)],
        'ev':   [(9, 6), (13, 5), (18, 3), (25, 1)],
        'fcfy': [(7, 7), (4.5, 5), (2.5, 3), (1, 2), (0, 1)],
        'pb':   [(2.5, 4), (4.5, 3), (7, 2), (12, 1)],
    },
}


def compute_rating(result: dict) -> dict:
    """
    Compute a Buy/Hold/Sell rating from an already-computed result dict.

    Scoring model (100 pts total):
      Valuation     25 pts  — P/E trailing (8), EV/EBITDA (6), FCF yield (7), P/B (4)
                              scored against sector-relative thresholds
      Profitability 25 pts  — Net Margin TTM (10), ROE TTM (10), margin trend (5)
      Growth        20 pts  — Revenue YoY %, averaged over up to 4 recent quarters
                              (a single quarter is too noisy to drive the score)
      Health        15 pts  — Current Ratio (6), Debt/Equity (6), Interest Coverage (3)
      Momentum      15 pts  — position of price within the 52-week range

    Thresholds: score >= 65 → BUY, >= 40 → HOLD, < 40 → SELL
    Missing inputs are skipped and the component score is proportionally rescaled.
    """
    from src.utils import classify_sector
    _DISCLAIMER = "Quantitative signal for educational purposes only. Not financial advice."

    if not result:
        return {"rating": "N/A", "score": None, "breakdown": {}, "sector_profile": None, "disclaimer": _DISCLAIMER, "data_quality": "none"}

    market  = result.get("market") or {}
    ttm     = result.get("ttm") or {}
    trends  = result.get("trends") or []
    company = result.get("company") or {}

    sector = classify_sector(company)
    profile = _VAL_PROFILES.get(sector, _VAL_PROFILES['general'])

    # ── Valuation (25 pts: P/E=8, EV/EBITDA=6, FCF yield=7, P/B=4) ───────────
    pe = market.get("pe_trailing")
    ev = market.get("ev_ebitda_info")
    if ev is None:
        ev = ttm.get("ev_ebitda_calc")   # fall back to the value we computed ourselves
    pb = market.get("pb_ratio")

    # FCF yield (TTM FCF / market cap, in %) — valuation anchored to actual cash
    # generation, harder to distort with accounting choices than P/E.
    fcf_ttm = ttm.get("free_cash_flow")
    mcap    = market.get("market_cap")
    fcf_yield = (fcf_ttm / mcap * 100) if (fcf_ttm is not None and mcap) else None

    pe_score = ev_score = pb_score = fcfy_score = None
    if pe is not None:
        pe_score = 0 if pe < 0 else _score_bracket(pe, profile['pe'])
    if ev is not None and ev >= 0 and profile['ev'] != 'skip':
        ev_score = _score_bracket(ev, profile['ev'])
    if pb is not None and pb >= 0:
        pb_score = _score_bracket(pb, profile['pb'])
    if fcf_yield is not None and profile['fcfy'] != 'skip':
        fcfy_score = 0 if fcf_yield < 0 else _score_bracket_high(fcf_yield, profile['fcfy'])

    val_raw = val_avail = 0
    for score, max_pts in [(pe_score, 8), (ev_score, 6), (fcfy_score, 7), (pb_score, 4)]:
        if score is not None:
            val_raw   += score
            val_avail += max_pts
    val_component = (val_raw / val_avail * 25) if val_avail > 0 else None

    # ── Profitability (25 pts: net_margin=10, roe=10, margin trend=5) ─────────
    nm  = ttm.get("net_margin")
    roe = ttm.get("roe")

    # Margin trend: average YoY operating-margin change (bps) over recent
    # quarters — rewards expanding margins, penalises compression.
    op_bps_vals = [t["op_bps"] for t in trends
                   if t is not None and t.get("op_bps") is not None][:4]
    op_bps_avg = sum(op_bps_vals) / len(op_bps_vals) if op_bps_vals else None

    nm_score = roe_score = trend_score = None
    if nm is not None:
        nm_score = 0 if nm < 0 else _score_bracket_high(nm, [(25, 10), (15, 8), (8, 5), (3, 3), (0, 1)])
    if roe is not None:
        roe_score = 0 if roe < 0 else _score_bracket_high(roe, [(30, 10), (20, 8), (12, 5), (5, 3), (0, 1)])
    if op_bps_avg is not None:
        trend_score = _score_bracket_high(op_bps_avg, [(150, 5), (50, 4), (-50, 3), (-150, 1)])

    prof_raw = prof_avail = 0
    for score, max_pts in [(nm_score, 10), (roe_score, 10), (trend_score, 5)]:
        if score is not None:
            prof_raw   += score
            prof_avail += max_pts
    prof_component = (prof_raw / prof_avail * 25) if prof_avail > 0 else None

    # ── Growth (20 pts: revenue YoY %, averaged over recent quarters) ─────────
    # One quarter's YoY is noisy (one-off charges, seasonality quirks); average
    # up to the 4 most recent quarters that have a prior-year comparison.
    yoy_vals = [t["rev_yoy_pct"] for t in trends
                if t is not None and t.get("rev_yoy_pct") is not None][:4]
    rev_yoy = round(sum(yoy_vals) / len(yoy_vals), 1) if yoy_vals else None

    if rev_yoy is None:
        growth_component = 10.0   # neutral — no data, don't penalise
    elif rev_yoy >= 25:  growth_component = 20.0
    elif rev_yoy >= 15:  growth_component = 16.0
    elif rev_yoy >= 8:   growth_component = 12.0
    elif rev_yoy >= 3:   growth_component = 8.0
    elif rev_yoy >= 0:   growth_component = 5.0
    elif rev_yoy >= -5:  growth_component = 2.0
    else:                growth_component = 0.0

    # ── Financial Health (15 pts: current_ratio=6, d/e=6, int. coverage=3) ───
    cr = ttm.get("current_ratio")
    de = ttm.get("debt_to_equity")
    ic = ttm.get("interest_coverage")

    cr_score = de_score = ic_score = None
    if cr is not None:
        cr_score = _score_bracket_high(cr, [(2, 6), (1.5, 4), (1, 2)])  # below 1 → 0
    if de is not None:
        de_score = 0 if de < 0 else _score_bracket(de, [(0.3, 6), (0.8, 4), (1.5, 2), (3.0, 1)])
    if ic is not None:
        ic_score = 0 if ic < 0 else _score_bracket_high(ic, [(10, 3), (5, 2), (2, 1)])

    health_raw = health_avail = 0
    for score, max_pts in [(cr_score, 6), (de_score, 6), (ic_score, 3)]:
        if score is not None:
            health_raw   += score
            health_avail += max_pts
    health_component = (health_raw / health_avail * 15) if health_avail > 0 else None

    # ── Momentum (15 pts: price position within the 52-week range) ───────────
    price = market.get("price")
    hi    = market.get("week52_high")
    lo    = market.get("week52_low")

    momentum_component = None
    if price is not None and hi is not None and lo is not None and hi > lo:
        pos = max(0.0, min(1.0, (price - lo) / (hi - lo)))
        momentum_component = float(_score_bracket_high(
            pos, [(0.8, 15), (0.6, 12), (0.4, 8), (0.2, 4), (0.0, 1)]))

    # ── Aggregate ─────────────────────────────────────────────────────────────
    _components = [
        ("valuation",     val_component,      25),
        ("profitability", prof_component,     25),
        ("growth",        growth_component,   20),
        ("health",        health_component,   15),
        ("momentum",      momentum_component, 15),
    ]

    none_count  = sum(1 for _, v, _ in _components if v is None)
    total_score = sum(v if v is not None else max_pts * 0.5 for _, v, max_pts in _components)

    if   none_count == 0: data_quality = "full"
    elif none_count <= 2: data_quality = "partial"
    elif none_count <= 4: data_quality = "minimal"
    else:                 data_quality = "none"

    if data_quality == "none":
        return {"rating": "N/A", "score": None, "breakdown": {}, "sector_profile": None, "disclaimer": _DISCLAIMER, "data_quality": "none"}

    total_score = round(total_score, 1)
    if   total_score >= 65: rating = "BUY"
    elif total_score >= 40: rating = "HOLD"
    else:                   rating = "SELL"

    breakdown = {
        "valuation":     {"score": round(val_component      if val_component      is not None else 12.5, 1), "max": 25, "label": "Valuation"},
        "profitability": {"score": round(prof_component     if prof_component     is not None else 12.5, 1), "max": 25, "label": "Profitability"},
        "growth":        {"score": round(growth_component,                                                1), "max": 20, "label": "Growth"},
        "health":        {"score": round(health_component   if health_component   is not None else 7.5,  1), "max": 15, "label": "Financial Health"},
        "momentum":      {"score": round(momentum_component if momentum_component is not None else 7.5,  1), "max": 15, "label": "Momentum (52W)"},
    }

    return {
        "rating":         rating,
        "score":          total_score,
        "breakdown":      breakdown,
        "sector_profile": sector,
        "disclaimer":     _DISCLAIMER,
        "data_quality":   data_quality,
    }
