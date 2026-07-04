"""
tests/test_analyzer.py — Unit tests for src/analyzer.py

Uses a fully synthetic 5-quarter dataset so no network access or DB is needed.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.analyzer import compute_ratios, compute_rating


# ── Synthetic data fixture ────────────────────────────────────────────────────

def _make_data():
    """
    Build a minimal but realistic synthetic `data` dict with 5 quarters.
    Quarters are newest-first.

    Revenue:       1.10B, 1.05B, 1.00B, 0.95B, 0.90B
    Gross profit:  40 % of revenue
    Op income:     20 % of revenue
    Net income:    10 % of revenue
    EBITDA:        25 % of revenue
    Interest exp:  -20 M (negative = expense)
    Balance sheet: static — equity 5B, assets 8B, debt 1B, cash 200M,
                   current_assets 2B, current_liabilities 1B, inventory 200M
    Cash flows:    op_cf = 15% of rev, FCF = 12% of rev
    """
    PERIODS = [
        '2024-09-28',
        '2024-06-29',
        '2024-03-30',
        '2023-12-31',
        '2023-09-30',
    ]
    REVENUES = [
        1_100_000_000,
        1_050_000_000,
        1_000_000_000,
          950_000_000,
          900_000_000,
    ]

    income = []
    balance = []
    cashflow = []

    for period, rev in zip(PERIODS, REVENUES):
        income.append({
            'period':           period,
            'revenue':          rev,
            'gross_profit':     rev * 0.40,
            'operating_income': rev * 0.20,
            'net_income':       rev * 0.10,
            'ebitda':           rev * 0.25,
            'interest_expense': -20_000_000,
        })
        balance.append({
            'period':               period,
            'total_assets':         8_000_000_000,
            'total_liabilities':    3_000_000_000,
            'equity':               5_000_000_000,
            'cash':                   200_000_000,
            'total_debt':           1_000_000_000,
            'current_assets':       2_000_000_000,
            'current_liabilities':  1_000_000_000,
            'inventory':              200_000_000,
        })
        cashflow.append({
            'period':          period,
            'operating_cf':    rev * 0.15,
            'investing_cf':    -rev * 0.03,
            'financing_cf':    -rev * 0.02,
            'capex':           -rev * 0.03,
            'free_cash_flow':  rev * 0.12,
        })

    return {
        'income':   income,
        'balance':  balance,
        'cashflow': cashflow,
        'market': {
            'market_cap':       50_000_000_000,
            'enterprise_value': 51_000_000_000,
            'pe_trailing':      22.5,
            'pe_forward':       18.0,
            'pb_ratio':         3.5,
            'ev_ebitda_info':   14.0,
            'beta':             1.1,
            'price':            50.0,
            'week52_high':      60.0,
            'week52_low':       35.0,
        },
        'company': {
            'ticker': 'TEST',
            'name':   'Test Corp',
            'sector': 'Technology',
        },
    }


# ── compute_ratios — top-level structure ─────────────────────────────────────

class TestComputeRatiosStructure:
    def setup_method(self):
        self.data   = _make_data()
        self.result = compute_ratios(self.data)

    def test_required_keys_present(self):
        for key in ('quarters', 'ttm', 'trends', 'market', 'company'):
            assert key in self.result, f"Missing key: {key}"

    def test_quarters_is_list(self):
        assert isinstance(self.result['quarters'], list)

    def test_ttm_is_dict(self):
        assert isinstance(self.result['ttm'], dict)

    def test_trends_is_list(self):
        assert isinstance(self.result['trends'], list)

    def test_market_passthrough(self):
        assert self.result['market'] == self.data['market']

    def test_company_passthrough(self):
        assert self.result['company'] == self.data['company']

    def test_quarters_count_at_most_max_display(self):
        # MAX_DISPLAY = 8; we have 5 quarters, so all 5 should appear
        assert len(self.result['quarters']) == 5

    def test_each_quarter_has_period(self):
        for q in self.result['quarters']:
            assert 'period' in q

    def test_quarters_newest_first(self):
        periods = [q['period'] for q in self.result['quarters']]
        assert periods == sorted(periods, reverse=True)


# ── compute_ratios — TTM values ───────────────────────────────────────────────

class TestComputeRatiosTTM:
    def setup_method(self):
        self.data   = _make_data()
        self.result = compute_ratios(self.data)
        self.ttm    = self.result['ttm']

    def test_ttm_revenue_is_sum_of_last_four_quarters(self):
        # 1.10B + 1.05B + 1.00B + 0.95B = 4.10B
        expected = 1_100_000_000 + 1_050_000_000 + 1_000_000_000 + 950_000_000
        assert self.ttm['revenue'] == expected

    def test_ttm_net_income_is_sum_of_last_four_quarters(self):
        revs = [1_100_000_000, 1_050_000_000, 1_000_000_000, 950_000_000]
        expected = sum(r * 0.10 for r in revs)
        assert abs(self.ttm['net_income'] - expected) < 1  # float tolerance

    def test_ttm_gross_margin_is_40_pct(self):
        # gross_profit = 40% of revenue for every quarter → TTM margin = 40%
        assert self.ttm['gross_margin'] == 40.0

    def test_ttm_op_margin_is_20_pct(self):
        assert self.ttm['op_margin'] == 20.0

    def test_ttm_net_margin_is_10_pct(self):
        assert self.ttm['net_margin'] == 10.0

    def test_ttm_has_current_ratio(self):
        # current_assets / current_liabilities = 2B / 1B = 2.0
        assert self.ttm['current_ratio'] == 2.0

    def test_ttm_has_quick_ratio(self):
        # (current_assets - inventory) / current_liabilities
        # = (2B - 200M) / 1B = 1.8
        assert self.ttm['quick_ratio'] == 1.8

    def test_ttm_debt_to_equity(self):
        # 1B / 5B = 0.2
        assert self.ttm['debt_to_equity'] == 0.2

    def test_ttm_net_debt(self):
        # debt - cash = 1B - 200M = 800M
        assert self.ttm['net_debt'] == 800_000_000

    def test_ttm_roe(self):
        revs = [1_100_000_000, 1_050_000_000, 1_000_000_000, 950_000_000]
        ttm_ni = sum(r * 0.10 for r in revs)
        equity = 5_000_000_000
        expected_roe = round(ttm_ni / equity * 100, 1)
        assert self.ttm['roe'] == expected_roe


# ── compute_ratios — per-quarter gross margin ─────────────────────────────────

class TestComputeRatiosQuarterlyRatios:
    def setup_method(self):
        self.data   = _make_data()
        self.result = compute_ratios(self.data)

    def test_newest_quarter_gross_margin(self):
        newest = self.result['quarters'][0]
        # gross_profit = 40% of revenue → gross_margin = 40.0
        assert newest['gross_margin'] == 40.0

    def test_newest_quarter_revenue(self):
        newest = self.result['quarters'][0]
        assert newest['revenue'] == 1_100_000_000

    def test_newest_quarter_net_margin(self):
        newest = self.result['quarters'][0]
        assert newest['net_margin'] == 10.0

    def test_newest_quarter_current_ratio(self):
        newest = self.result['quarters'][0]
        assert newest['current_ratio'] == 2.0

    def test_all_quarters_have_gross_margin(self):
        for q in self.result['quarters']:
            assert q.get('gross_margin') == 40.0

    def test_periods_are_strings(self):
        for q in self.result['quarters']:
            assert isinstance(q['period'], str)


# ── compute_ratios — trends ───────────────────────────────────────────────────

class TestComputeRatiosTrends:
    def setup_method(self):
        self.data   = _make_data()
        self.result = compute_ratios(self.data)

    def test_trends_length_matches_quarters(self):
        assert len(self.result['trends']) == len(self.result['quarters'])

    def test_last_trend_is_none_when_no_year_ago_peer(self):
        # _compute_trends: entry at index i is None when i+4 >= n.
        # With 5 quarters (n=5), index 4: 4+4=8 >= 5 → no year-ago peer → None.
        assert self.result['trends'][4] is None

    def test_trend_with_year_ago_peer(self):
        # Quarter at index 0 (2024-09-28) has its year-ago at index 4 (2023-09-30).
        # i=0, i+4=4, n=5 → 4 < 5 → a trend is computed (not None).
        trend = self.result['trends'][0]
        assert trend is not None

    def test_trend_has_required_keys(self):
        trend = self.result['trends'][0]
        for key in ('period', 'rev_yoy_pct', 'rev_arrow', 'gm_bps', 'op_bps'):
            assert key in trend

    def test_rev_yoy_positive(self):
        # Q3-2024 revenue (1.1B) vs Q3-2023 (0.9B) → ~22.2% growth
        trend = self.result['trends'][0]
        assert trend['rev_yoy_pct'] > 0

    def test_rev_yoy_value(self):
        # (1.1B - 0.9B) / 0.9B * 100 = 22.2%
        trend = self.result['trends'][0]
        expected = round((1_100_000_000 - 900_000_000) / 900_000_000 * 100, 1)
        assert trend['rev_yoy_pct'] == expected


# ── compute_rating ────────────────────────────────────────────────────────────

class TestComputeRating:
    def setup_method(self):
        data   = _make_data()
        self.result = compute_ratios(data)

    def test_rating_has_required_keys(self):
        rating_out = compute_rating(self.result)
        for key in ('rating', 'score', 'breakdown', 'disclaimer'):
            assert key in rating_out

    def test_rating_is_valid_string(self):
        rating_out = compute_rating(self.result)
        assert rating_out['rating'] in ('BUY', 'HOLD', 'SELL', 'N/A')

    def test_score_is_numeric(self):
        rating_out = compute_rating(self.result)
        assert isinstance(rating_out['score'], (int, float))

    def test_score_in_range(self):
        rating_out = compute_rating(self.result)
        assert 0 <= rating_out['score'] <= 100

    def test_breakdown_has_four_components(self):
        rating_out = compute_rating(self.result)
        breakdown = rating_out['breakdown']
        for key in ('valuation', 'profitability', 'growth', 'health'):
            assert key in breakdown

    def test_breakdown_scores_have_score_and_max(self):
        rating_out = compute_rating(self.result)
        for comp in rating_out['breakdown'].values():
            assert 'score' in comp
            assert 'max' in comp

    def test_disclaimer_is_string(self):
        rating_out = compute_rating(self.result)
        assert isinstance(rating_out['disclaimer'], str)
        assert len(rating_out['disclaimer']) > 0

    def test_empty_result_returns_na(self):
        rating_out = compute_rating({})
        assert rating_out['rating'] == 'N/A'
        assert rating_out['score'] is None

    def test_none_result_returns_na(self):
        rating_out = compute_rating(None)
        assert rating_out['rating'] == 'N/A'

    def test_data_quality_present(self):
        rating_out = compute_rating(self.result)
        assert 'data_quality' in rating_out
        assert rating_out['data_quality'] in ('full', 'partial', 'minimal', 'none')

    def test_high_quality_data_produces_full_quality(self):
        # Our synthetic data has market + ttm values, so quality should be full
        rating_out = compute_rating(self.result)
        assert rating_out['data_quality'] == 'full'


# ── regression tests: gaps in quarterly data ─────────────────────────────────

def _quarter(period, rev):
    """One aligned income/balance/cashflow row set for a given period."""
    income = {
        'period': period, 'revenue': rev, 'gross_profit': rev * 0.40,
        'operating_income': rev * 0.20, 'net_income': rev * 0.10,
        'ebitda': rev * 0.25, 'interest_expense': -20_000_000,
    }
    balance = {
        'period': period, 'total_assets': 8e9, 'total_liabilities': 3e9,
        'equity': 5e9, 'cash': 2e8, 'total_debt': 1e9,
        'current_assets': 2e9, 'current_liabilities': 1e9, 'inventory': 2e8,
    }
    cashflow = {
        'period': period, 'operating_cf': rev * 0.15, 'investing_cf': -rev * 0.03,
        'financing_cf': -rev * 0.02, 'capex': -rev * 0.03, 'free_cash_flow': rev * 0.12,
    }
    return income, balance, cashflow


def _data_from_periods(period_revs):
    income, balance, cashflow = [], [], []
    for period, rev in period_revs:
        i, b, c = _quarter(period, rev)
        income.append(i); balance.append(b); cashflow.append(c)
    return {'income': income, 'balance': balance, 'cashflow': cashflow,
            'market': {}, 'company': {}}


class TestTrendsWithGaps:
    def test_yoy_peer_found_by_date_despite_missing_quarter(self):
        # Q2'24 is missing, so the year-ago peer of 2024-12-31 is only 3 rows
        # back — an index-offset (i+4) lookup would miss it or pick Q3'23.
        data = _data_from_periods([
            ('2024-12-31', 1_200_000_000),
            ('2024-09-30', 1_100_000_000),
            ('2024-03-31', 1_000_000_000),
            ('2023-12-31', 1_000_000_000),
            ('2023-09-30',   900_000_000),
        ])
        result = compute_ratios(data)
        trend = result['trends'][0]
        assert trend is not None
        # 1.2B vs 1.0B a year earlier → +20.0%
        assert trend['rev_yoy_pct'] == 20.0

    def test_no_false_peer_when_prior_year_missing(self):
        data = _data_from_periods([
            ('2024-12-31', 1_200_000_000),
            ('2024-09-30', 1_100_000_000),
        ])
        result = compute_ratios(data)
        assert result['trends'][0] is None

    def test_zero_bps_change_gets_flat_arrow(self):
        # Identical margins YoY → 0 bps; must render '→', not '' (old truthiness bug)
        data = _data_from_periods([
            ('2024-12-31', 1_000_000_000),
            ('2023-12-31', 1_000_000_000),
        ])
        result = compute_ratios(data)
        trend = result['trends'][0]
        assert trend['gm_bps'] == 0
        assert trend['gm_arrow'] == '→'


class TestTTMContiguity:
    def test_ttm_none_when_quarters_not_contiguous(self):
        # 4 quarters spanning >2 years must not be summed as a "TTM"
        data = _data_from_periods([
            ('2024-12-31', 1_000_000_000),
            ('2024-09-30', 1_000_000_000),
            ('2023-03-31', 1_000_000_000),
            ('2022-12-31', 1_000_000_000),
        ])
        result = compute_ratios(data)
        assert result['ttm']['revenue'] is None

    def test_ttm_uses_latest_value_for_annual_only_filers(self):
        # Consecutive periods ~1 year apart → rows are fiscal years; the most
        # recent FY value already is a trailing twelve months.
        data = _data_from_periods([
            ('2024-12-31', 4_000_000_000),
            ('2023-12-31', 3_600_000_000),
            ('2022-12-31', 3_200_000_000),
        ])
        result = compute_ratios(data)
        assert result['ttm']['revenue'] == 4_000_000_000


class TestRatingGrowthSmoothing:
    def test_growth_uses_average_of_recent_quarters(self):
        # Two YoY datapoints: +30% and +10% → average 20% → 16 pts, not the
        # 20 pts a single +30% quarter would earn.
        data = _data_from_periods([
            ('2024-12-31', 1_300_000_000),
            ('2024-09-30', 1_100_000_000),
            ('2024-06-30', 1_000_000_000),
            ('2024-03-31', 1_000_000_000),
            ('2023-12-31', 1_000_000_000),
            ('2023-09-30', 1_000_000_000),
        ])
        result = compute_ratios(data)
        rating = compute_rating(result)
        assert rating['breakdown']['growth']['score'] == 16.0
        assert rating['breakdown']['growth']['max'] == 20


# ── sector-relative rating & momentum ────────────────────────────────────────

class TestSectorClassification:
    def test_sic_code_takes_priority(self):
        from src.utils import classify_sector
        assert classify_sector({'sic': '6022'}) == 'financials'
        assert classify_sector({'sic': '3674'}) == 'technology'
        assert classify_sector({'sic': '2911'}) == 'energy'
        assert classify_sector({'sic': '4911'}) == 'utilities'
        assert classify_sector({'sic': '6324'}) == 'healthcare'   # UNH-style medical plans
        assert classify_sector({'sic': '6798'}) == 'real_estate'  # REITs
        assert classify_sector({'sic': '6022'}) == 'financials'   # still banks

    def test_text_fallback_for_legacy_data(self):
        from src.utils import classify_sector
        assert classify_sector({'sector': 'Financial Services', 'industry': 'Banks'}) == 'financials'
        assert classify_sector({'sector': 'Technology', 'industry': 'Consumer Electronics'}) == 'technology'
        assert classify_sector({'sector': '', 'industry': ''}) == 'general'

    def test_unknown_sic_falls_back_to_text(self):
        from src.utils import classify_sector
        assert classify_sector({'sic': '0100', 'sector': 'Energy'}) == 'energy'


class TestSectorRelativeRating:
    def test_financials_skip_ev_and_fcf_yield(self):
        # A bank with a P/E of 12 and P/B of 1.2 should score decently on
        # valuation even though EV/EBITDA and FCF yield are excluded.
        data = _make_data()
        data['company'] = {'ticker': 'BANK', 'sector': 'Financial Services', 'industry': 'Banks'}
        data['market']['pe_trailing'] = 12.0
        data['market']['pb_ratio'] = 1.2
        result = compute_ratios(data)
        rating = compute_rating(result)
        assert rating['sector_profile'] == 'financials'
        # pe 12 → 6/8, pb 1.2 → 3/4 ⇒ (6+3)/(8+4)*25 = 18.75 → 18.8
        assert rating['breakdown']['valuation']['score'] == 18.8

    def test_same_metrics_score_differently_by_sector(self):
        data_tech = _make_data()
        data_gen  = _make_data()
        data_gen['company'] = {'ticker': 'GEN', 'sector': '', 'industry': ''}
        r_tech = compute_rating(compute_ratios(data_tech))
        r_gen  = compute_rating(compute_ratios(data_gen))
        # P/E 22.5 is mid-range for tech but expensive for the general profile
        assert r_tech['breakdown']['valuation']['score'] > r_gen['breakdown']['valuation']['score']


class TestMomentum:
    def test_momentum_from_52w_position(self):
        data = _make_data()
        # price 50 in a 35–60 range → position 0.6 → 12/15
        rating = compute_rating(compute_ratios(data))
        assert rating['breakdown']['momentum']['score'] == 12.0
        assert rating['breakdown']['momentum']['max'] == 15

    def test_momentum_near_low(self):
        data = _make_data()
        data['market']['price'] = 36.0   # position 0.04 → 1 pt
        rating = compute_rating(compute_ratios(data))
        assert rating['breakdown']['momentum']['score'] == 1.0

    def test_missing_52w_data_degrades_quality_not_crash(self):
        data = _make_data()
        for k in ('price', 'week52_high', 'week52_low'):
            data['market'].pop(k, None)
        rating = compute_rating(compute_ratios(data))
        assert rating['rating'] in ('BUY', 'HOLD', 'SELL')
        assert rating['data_quality'] == 'partial'
