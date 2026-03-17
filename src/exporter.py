import csv
import os

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'exports')


def _ensure_dir():
    os.makedirs(EXPORTS_DIR, exist_ok=True)


def _clean(v):
    """Convert None to empty string for CSV output."""
    return '' if v is None else v


def export_ticker(ticker: str, data: dict, result: dict):
    """
    Write two CSV files:
      exports/{ticker}_financials.csv  — raw quarterly data (income + balance + CF joined by period)
      exports/{ticker}_ratios.csv      — per-quarter computed ratios
    """
    _ensure_dir()

    # ── Raw financials ────────────────────────────────────────────
    inc_by_p = {r['period']: r for r in data.get('income', [])}
    bal_by_p = {r['period']: r for r in data.get('balance', [])}
    cf_by_p  = {r['period']: r for r in data.get('cashflow', [])}
    all_periods = sorted(set(inc_by_p) | set(bal_by_p) | set(cf_by_p), reverse=True)

    fin_path = os.path.join(EXPORTS_DIR, f"{ticker}_financials.csv")
    fin_headers = [
        'period',
        'revenue', 'gross_profit', 'operating_income', 'net_income',
        'ebitda', 'interest_expense', 'depreciation_amortization',
        'total_assets', 'total_liabilities', 'equity', 'cash', 'total_debt',
        'current_assets', 'current_liabilities', 'inventory',
        'operating_cf', 'investing_cf', 'financing_cf', 'capex', 'free_cash_flow',
    ]
    with open(fin_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fin_headers, extrasaction='ignore')
        w.writeheader()
        for p in all_periods:
            row = {'period': p}
            row.update({k: _clean(v) for k, v in inc_by_p.get(p, {}).items()})
            row.update({k: _clean(v) for k, v in bal_by_p.get(p, {}).items()})
            row.update({k: _clean(v) for k, v in cf_by_p.get(p, {}).items()})
            w.writerow(row)

    # ── Ratios ────────────────────────────────────────────────────
    ratio_path = os.path.join(EXPORTS_DIR, f"{ticker}_ratios.csv")
    ratio_headers = [
        'period',
        'revenue', 'net_income', 'free_cash_flow',
        'gross_margin', 'op_margin', 'ebitda_margin', 'net_margin',
        'fcf_margin', 'op_cf_margin', 'roe', 'roa',
        'current_ratio', 'quick_ratio', 'debt_to_equity', 'net_debt', 'interest_coverage',
    ]
    with open(ratio_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=ratio_headers, extrasaction='ignore')
        w.writeheader()
        # TTM row first
        ttm_row = {'period': 'TTM'}
        ttm_row.update({k: _clean(result['ttm'].get(k)) for k in ratio_headers if k != 'period'})
        w.writerow(ttm_row)
        for q in result.get('all_quarters', result.get('quarters', [])):
            row = {k: _clean(q.get(k)) for k in ratio_headers}
            w.writerow(row)

    return fin_path, ratio_path


def export_comparison(tickers: list, all_results: dict):
    """
    Write exports/comparison_{tickers}.csv — TTM side-by-side for all tickers.
    """
    _ensure_dir()
    name = '_'.join(tickers)
    path = os.path.join(EXPORTS_DIR, f"comparison_{name}.csv")

    metrics = [
        ('market_cap',        'Market Cap',       'market'),
        ('pe_trailing',       'P/E (TTM)',         'market'),
        ('pe_forward',        'P/E (Fwd)',         'market'),
        ('ev_ebitda_info',    'EV/EBITDA (info)',  'market'),
        ('pb_ratio',          'P/B',               'market'),
        ('beta',              'Beta',              'market'),
        ('revenue',           'Revenue (TTM)',      'ttm'),
        ('net_income',        'Net Income (TTM)',   'ttm'),
        ('free_cash_flow',    'FCF (TTM)',          'ttm'),
        ('ebitda',            'EBITDA (TTM)',       'ttm'),
        ('gross_margin',      'Gross Margin %',    'ttm'),
        ('op_margin',         'Op. Margin %',      'ttm'),
        ('ebitda_margin',     'EBITDA Margin %',   'ttm'),
        ('net_margin',        'Net Margin %',      'ttm'),
        ('fcf_margin',        'FCF Margin %',      'ttm'),
        ('roe',               'ROE %',             'ttm'),
        ('roa',               'ROA %',             'ttm'),
        ('current_ratio',     'Current Ratio',     'ttm'),
        ('quick_ratio',       'Quick Ratio',       'ttm'),
        ('debt_to_equity',    'Debt / Equity',     'ttm'),
        ('net_debt',          'Net Debt',          'ttm'),
        ('interest_coverage', 'Interest Coverage', 'ttm'),
        ('ev_ebitda_calc',    'EV/EBITDA (calc)',  'ttm'),
        ('ev_rev_calc',       'EV/Revenue (calc)', 'ttm'),
    ]

    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['Metric'] + tickers)
        for key, label, source in metrics:
            row = [label]
            for t in tickers:
                val = all_results[t][source].get(key)
                row.append(_clean(val))
            w.writerow(row)

    return path
