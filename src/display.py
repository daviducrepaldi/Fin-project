import shutil
from tabulate import tabulate

# ── formatting helpers ────────────────────────────────────────────────────────

def _fmt_large(val, unit='$'):
    """Format a large number with T/B/M suffix."""
    if val is None:
        return 'N/A'
    if abs(val) >= 1e12:
        return f"{unit}{val/1e12:.2f}T"
    if abs(val) >= 1e9:
        return f"{unit}{val/1e9:.2f}B"
    if abs(val) >= 1e6:
        return f"{unit}{val/1e6:.0f}M"
    return f"{unit}{val:,.0f}"


def _fmt(val, suffix='', prefix=''):
    if val is None:
        return 'N/A'
    if isinstance(val, float):
        return f"{prefix}{val:,.1f}{suffix}"
    if isinstance(val, int):
        return f"{prefix}{val:,}{suffix}"
    return str(val)


def _fmt_bps(bps):
    if bps is None:
        return 'N/A'
    sign = '+' if bps >= 0 else ''
    return f"{sign}{int(bps):,}bps"


def _fmt_yoy(pct, arrow):
    if pct is None:
        return 'N/A'
    sign = '+' if pct >= 0 else ''
    return f"{sign}{pct:.1f}% {arrow}"


def _fmt_bps_arrow(bps, arrow):
    if bps is None:
        return 'N/A'
    sign = '+' if bps >= 0 else ''
    return f"{sign}{int(bps):,}bps {arrow}"


def _label_col(label, vals):
    """Return a row: [label, val1, val2, ...]."""
    return [label] + list(vals)


def _col_headers(quarters, include_ttm=True):
    """Build column headers like ['TTM', 'Q3\'24', 'Q2\'24', ...]."""
    headers = ['TTM'] if include_ttm else []
    for q in quarters:
        p = q['period']           # e.g. '2024-09-28'
        year = p[2:4]             # '24'
        month = int(p[5:7])
        qnum = (month - 1) // 3 + 1
        headers.append(f"Q{qnum}'{year}")
    return headers


def _limit_quarters(quarters, trends, term_width):
    """Trim number of quarters displayed based on terminal width."""
    # Rough estimate: label col ~22 chars, TTM ~10, each quarter ~9
    available = term_width - 22 - 10
    max_q = max(1, min(len(quarters), available // 9))
    return quarters[:max_q], trends[:max_q]


# ── section printers ──────────────────────────────────────────────────────────

def _print_market(market, company):
    if not market:
        return
    mc   = _fmt_large(market.get('market_cap'))
    ev   = _fmt_large(market.get('enterprise_value'))
    price = _fmt(market.get('price'), prefix='$')
    pe_t  = _fmt(market.get('pe_trailing'), suffix='x')
    pe_f  = _fmt(market.get('pe_forward'),  suffix='x')
    pb    = _fmt(market.get('pb_ratio'),    suffix='x')
    ev_eb = _fmt(market.get('ev_ebitda_info'), suffix='x')
    ev_rv = _fmt(market.get('ev_revenue_info'), suffix='x')
    hi    = _fmt(market.get('week52_high'), prefix='$')
    lo    = _fmt(market.get('week52_low'),  prefix='$')
    beta  = _fmt(market.get('beta'))
    dy    = _fmt(market.get('dividend_yield'), suffix='%') \
            if market.get('dividend_yield') else 'N/A'

    rows = [
        ['Market Cap',    mc,    'Price',          price],
        ['Enterprise Val', ev,   'P/E (TTM)',       pe_t],
        ['EV/EBITDA',     ev_eb, 'P/E (Fwd)',       pe_f],
        ['EV/Revenue',    ev_rv, 'P/B',             pb],
        ['52W High',      hi,    '52W Low',         lo],
        ['Beta',          beta,  'Div. Yield',      dy],
    ]
    # Filter out rows where all values are N/A
    rows = [r for r in rows if not all(v == 'N/A' for v in [r[1], r[3]])]
    if rows:
        print(tabulate(rows, tablefmt='plain'))


def _print_section(title, rows, headers):
    rows = [r for r in rows if any(v not in ('N/A', '') for v in r[1:])]
    if not rows:
        return
    print(f"\n  {title}")
    print(tabulate(rows, headers=[''] + headers, tablefmt='simple',
                   colalign=('left',) + ('right',) * len(headers)))


# ── main display functions ────────────────────────────────────────────────────

def print_analysis(ticker: str, result: dict):
    company  = result['company']
    market   = result['market']
    ttm      = result['ttm']
    quarters = result['quarters']
    trends   = result['trends']

    name     = company.get('name', ticker)
    sector   = company.get('sector', '')
    industry = company.get('industry', '')
    currency = company.get('currency', 'USD')
    exch     = company.get('exchange', '')

    term_w = shutil.get_terminal_size((120, 40)).columns
    quarters, trends = _limit_quarters(quarters, trends, term_w)

    sep = '=' * min(70, term_w)
    print(f"\n{sep}")
    print(f"  {ticker}  —  {name}")
    if sector:
        print(f"  {sector}  |  {industry}")
    if currency or exch:
        print(f"  Currency: {currency}  |  Exchange: {exch}")
    print(sep)

    # Market data
    if market:
        print(f"\n  Market Data")
        _print_market(market, company)

    if not quarters:
        print("\n  No quarterly data available.\n")
        return

    headers = _col_headers(quarters, include_ttm=True)
    q_vals  = lambda key: [_fmt(ttm.get(key))] + [_fmt(q.get(key)) for q in quarters]
    q_large = lambda key: [_fmt_large(ttm.get(key))] + [_fmt_large(q.get(key)) for q in quarters]

    # ── Scale ──────────────────────────────────────────────────────
    _print_section('Scale', [
        _label_col('Revenue',        q_large('revenue')),
        _label_col('Net Income',     q_large('net_income')),
        _label_col('Free Cash Flow', q_large('free_cash_flow')),
    ], headers)

    # ── Profitability ──────────────────────────────────────────────
    _print_section('Profitability (%)', [
        _label_col('Gross Margin',    q_vals('gross_margin')),
        _label_col('Op. Margin',      q_vals('op_margin')),
        _label_col('EBITDA Margin',   q_vals('ebitda_margin')),
        _label_col('Net Margin',      q_vals('net_margin')),
        _label_col('FCF Margin',      q_vals('fcf_margin')),
        _label_col('Op. CF Margin',   q_vals('op_cf_margin')),
        _label_col('ROE',             q_vals('roe')),
        _label_col('ROA',             q_vals('roa')),
    ], headers)

    # ── Liquidity & Leverage (balance sheet — no TTM) ─────────────
    q_bal_headers = _col_headers(quarters, include_ttm=False)
    q_bal = lambda key: [_fmt(q.get(key)) for q in quarters]
    q_bal_neg = lambda key: [_fmt(q.get(key)) for q in quarters]

    _print_section('Liquidity & Leverage', [
        _label_col('Current Ratio',    q_bal('current_ratio')),
        _label_col('Quick Ratio',      q_bal('quick_ratio')),
        _label_col('Debt / Equity',    q_bal('debt_to_equity')),
        _label_col('Net Debt',         [_fmt_large(q.get('net_debt')) for q in quarters]),
        _label_col('Interest Coverage', [_fmt(q.get('interest_coverage'), suffix='x')
                                         for q in quarters]),
    ], q_bal_headers)

    # ── YoY Trends ─────────────────────────────────────────────────
    trend_quarters = [(t, q) for t, q in zip(trends, quarters) if t is not None]
    if trend_quarters:
        t_headers = [_col_headers([q], include_ttm=False)[0] for _, q in trend_quarters]
        trend_rows = [
            ['Revenue YoY']     + [_fmt_yoy(t['rev_yoy_pct'], t['rev_arrow']) for t, _ in trend_quarters],
            ['Gross Margin Δ']  + [_fmt_bps_arrow(t['gm_bps'], t['gm_arrow']) for t, _ in trend_quarters],
            ['Op. Margin Δ']    + [_fmt_bps_arrow(t['op_bps'], t['op_arrow']) for t, _ in trend_quarters],
            ['EBITDA Margin Δ'] + [_fmt_bps_arrow(t['eb_bps'], t['eb_arrow']) for t, _ in trend_quarters],
            ['Net Margin Δ']    + [_fmt_bps_arrow(t['ni_bps'], t['ni_arrow']) for t, _ in trend_quarters],
            ['FCF YoY']         + [_fmt_yoy(t['fcf_yoy_pct'], t['fcf_arrow']) for t, _ in trend_quarters],
        ]
        trend_rows = [r for r in trend_rows if any(v != 'N/A' for v in r[1:])]
        if trend_rows:
            print(f"\n  YoY Trends  (vs. same quarter prior year)")
            print(tabulate(trend_rows, headers=[''] + t_headers, tablefmt='simple',
                           colalign=('left',) + ('right',) * len(t_headers)))

    # ── Valuation (calculated) ─────────────────────────────────────
    ev_eb_calc = ttm.get('ev_ebitda_calc')
    ev_rv_calc = ttm.get('ev_rev_calc')
    if ev_eb_calc or ev_rv_calc:
        print(f"\n  Valuation (Calculated, TTM)")
        val_rows = []
        if ev_eb_calc:
            ev_eb_info = market.get('ev_ebitda_info')
            val_rows.append(['EV / EBITDA',
                             f"{ev_eb_calc:.1f}x (calc)",
                             f"{ev_eb_info:.1f}x (info)" if ev_eb_info else ''])
        if ev_rv_calc:
            ev_rv_info = market.get('ev_revenue_info')
            val_rows.append(['EV / Revenue',
                             f"{ev_rv_calc:.1f}x (calc)",
                             f"{ev_rv_info:.1f}x (info)" if ev_rv_info else ''])
        print(tabulate(val_rows, tablefmt='plain'))

    print()


def print_comparison(tickers: list, all_results: dict):
    """Print side-by-side TTM comparison for multiple tickers."""
    term_w = shutil.get_terminal_size((120, 40)).columns
    sep = '=' * min(70, term_w)

    print(f"\n{sep}")
    print(f"  COMPARISON: {' | '.join(tickers)}")
    print(f"  Trailing Twelve Months + Latest Quarter (Balance Sheet)")
    print(sep)

    def row(label, vals):
        return [label] + list(vals)

    # Market Data
    mkt_rows = []
    for label, key, fmt_fn in [
        ('Market Cap',    'market_cap',       lambda v: _fmt_large(v)),
        ('P/E (TTM)',     'pe_trailing',      lambda v: _fmt(v, suffix='x')),
        ('P/E (Fwd)',     'pe_forward',       lambda v: _fmt(v, suffix='x')),
        ('EV/EBITDA',     'ev_ebitda_info',   lambda v: _fmt(v, suffix='x')),
        ('EV/Revenue',    'ev_revenue_info',  lambda v: _fmt(v, suffix='x')),
        ('P/B',           'pb_ratio',         lambda v: _fmt(v, suffix='x')),
        ('Beta',          'beta',             lambda v: _fmt(v)),
    ]:
        vals = [fmt_fn(all_results[t]['market'].get(key)) for t in tickers]
        if any(v != 'N/A' for v in vals):
            mkt_rows.append(row(label, vals))

    if mkt_rows:
        print(f"\n  Market Data")
        print(tabulate(mkt_rows, headers=[''] + tickers, tablefmt='simple',
                       colalign=('left',) + ('right',) * len(tickers)))

    # Scale (TTM, $B)
    scale_rows = []
    for label, key in [('Revenue', 'revenue'), ('Net Income', 'net_income'),
                        ('Free Cash Flow', 'free_cash_flow'), ('EBITDA', 'ebitda')]:
        vals = [_fmt_large(all_results[t]['ttm'].get(key)) for t in tickers]
        if any(v != 'N/A' for v in vals):
            scale_rows.append(row(label, vals))
    if scale_rows:
        print(f"\n  Scale (TTM)")
        print(tabulate(scale_rows, headers=[''] + tickers, tablefmt='simple',
                       colalign=('left',) + ('right',) * len(tickers)))

    # Profitability (TTM, %)
    prof_rows = []
    for label, key in [
        ('Gross Margin %', 'gross_margin'), ('Op. Margin %', 'op_margin'),
        ('EBITDA Margin %', 'ebitda_margin'), ('Net Margin %', 'net_margin'),
        ('FCF Margin %', 'fcf_margin'), ('ROE %', 'roe'), ('ROA %', 'roa'),
    ]:
        vals = [_fmt(all_results[t]['ttm'].get(key)) for t in tickers]
        if any(v != 'N/A' for v in vals):
            prof_rows.append(row(label, vals))
    if prof_rows:
        print(f"\n  Profitability (TTM, %)")
        print(tabulate(prof_rows, headers=[''] + tickers, tablefmt='simple',
                       colalign=('left',) + ('right',) * len(tickers)))

    # Liquidity & Leverage (latest quarter)
    lev_rows = []
    for label, key, fmt_fn in [
        ('Current Ratio',     'current_ratio',     lambda v: _fmt(v)),
        ('Quick Ratio',       'quick_ratio',        lambda v: _fmt(v)),
        ('Debt / Equity',     'debt_to_equity',     lambda v: _fmt(v)),
        ('Net Debt',          'net_debt',           lambda v: _fmt_large(v)),
        ('Interest Coverage', 'interest_coverage',  lambda v: _fmt(v, suffix='x')),
    ]:
        vals = [fmt_fn(all_results[t]['ttm'].get(key)) for t in tickers]
        if any(v != 'N/A' for v in vals):
            lev_rows.append(row(label, vals))
    if lev_rows:
        print(f"\n  Liquidity & Leverage (Latest Quarter)")
        print(tabulate(lev_rows, headers=[''] + tickers, tablefmt='simple',
                       colalign=('left',) + ('right',) * len(tickers)))

    # Valuation calculated
    val_rows = []
    for label, key, fmt_fn in [
        ('EV/EBITDA (calc)', 'ev_ebitda_calc', lambda v: _fmt(v, suffix='x')),
        ('EV/Revenue (calc)', 'ev_rev_calc',   lambda v: _fmt(v, suffix='x')),
    ]:
        vals = [fmt_fn(all_results[t]['ttm'].get(key)) for t in tickers]
        if any(v != 'N/A' for v in vals):
            val_rows.append(row(label, vals))
    if val_rows:
        print(f"\n  Valuation (Calculated, TTM)")
        print(tabulate(val_rows, headers=[''] + tickers, tablefmt='simple',
                       colalign=('left',) + ('right',) * len(tickers)))

    print()
