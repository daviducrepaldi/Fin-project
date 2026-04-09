"""
tests/test_exporter.py — Unit tests for src/exporter.py

Uses pytest's tmp_path fixture and monkeypatches EXPORTS_DIR so no real
filesystem side-effects occur outside the temp directory.
"""

import csv
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import src.exporter as exporter_module
from src.exporter import export_ticker, export_comparison


# ── Synthetic data helpers ────────────────────────────────────────────────────

def _make_data():
    """Minimal synthetic `data` dict (3 quarters, newest first)."""
    PERIODS = ['2024-09-28', '2024-06-29', '2024-03-30']
    REVENUES = [1_100_000_000, 1_050_000_000, 1_000_000_000]

    income   = []
    balance  = []
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
            'depreciation_amortization': rev * 0.05,
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
        },
        'company': {'ticker': 'TEST', 'name': 'Test Corp', 'sector': 'Technology'},
    }


def _make_result():
    """Minimal synthetic result dict (as would come from compute_ratios)."""
    # Build a small all_quarters list
    quarters = []
    for period, rev in [('2024-09-28', 1_100_000_000),
                        ('2024-06-29', 1_050_000_000),
                        ('2024-03-30', 1_000_000_000)]:
        quarters.append({
            'period':           period,
            'revenue':          rev,
            'net_income':       rev * 0.10,
            'free_cash_flow':   rev * 0.12,
            'gross_margin':     40.0,
            'op_margin':        20.0,
            'ebitda_margin':    25.0,
            'net_margin':       10.0,
            'fcf_margin':       12.0,
            'op_cf_margin':     15.0,
            'roe':              8.2,
            'roa':              5.1,
            'current_ratio':    2.0,
            'quick_ratio':      1.8,
            'debt_to_equity':   0.2,
            'net_debt':         800_000_000,
            'interest_coverage': 55.0,
        })

    ttm = {
        'revenue':          4_100_000_000,
        'net_income':         410_000_000,
        'free_cash_flow':     492_000_000,
        'ebitda':           1_025_000_000,
        'gross_margin':     40.0,
        'op_margin':        20.0,
        'ebitda_margin':    25.0,
        'net_margin':       10.0,
        'fcf_margin':       12.0,
        'op_cf_margin':     15.0,
        'roe':              8.2,
        'roa':              5.1,
        'current_ratio':    2.0,
        'quick_ratio':      1.8,
        'debt_to_equity':   0.2,
        'net_debt':         800_000_000,
        'interest_coverage': 55.0,
        'ev_ebitda_calc':   49.8,
        'ev_rev_calc':      12.4,
    }

    return {
        'quarters':     quarters,
        'all_quarters': quarters,
        'ttm':          ttm,
        'trends':       [None, None, None],
        'market':       _make_data()['market'],
        'company':      _make_data()['company'],
    }


# ── export_ticker ─────────────────────────────────────────────────────────────

class TestExportTicker:
    def test_creates_financials_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        data   = _make_data()
        result = _make_result()
        fin_path, ratio_path = export_ticker('TEST', data, result)

        assert os.path.exists(fin_path)

    def test_creates_ratios_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        data   = _make_data()
        result = _make_result()
        fin_path, ratio_path = export_ticker('TEST', data, result)

        assert os.path.exists(ratio_path)

    def test_financials_csv_filename(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('AAPL', _make_data(), _make_result())
        assert os.path.basename(fin_path) == 'AAPL_financials.csv'

    def test_ratios_csv_filename(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        _, ratio_path = export_ticker('AAPL', _make_data(), _make_result())
        assert os.path.basename(ratio_path) == 'AAPL_ratios.csv'

    def test_financials_csv_has_period_header(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('TEST', _make_data(), _make_result())
        with open(fin_path, newline='') as f:
            reader = csv.DictReader(f)
            assert 'period' in reader.fieldnames

    def test_financials_csv_has_revenue_header(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('TEST', _make_data(), _make_result())
        with open(fin_path, newline='') as f:
            reader = csv.DictReader(f)
            assert 'revenue' in reader.fieldnames

    def test_financials_csv_expected_headers(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('TEST', _make_data(), _make_result())
        expected_headers = [
            'period', 'revenue', 'gross_profit', 'operating_income', 'net_income',
            'ebitda', 'interest_expense', 'depreciation_amortization',
            'total_assets', 'total_liabilities', 'equity', 'cash', 'total_debt',
            'current_assets', 'current_liabilities', 'inventory',
            'operating_cf', 'investing_cf', 'financing_cf', 'capex', 'free_cash_flow',
        ]
        with open(fin_path, newline='') as f:
            reader = csv.DictReader(f)
            for h in expected_headers:
                assert h in reader.fieldnames, f"Missing header: {h}"

    def test_ratios_csv_expected_headers(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        _, ratio_path = export_ticker('TEST', _make_data(), _make_result())
        expected_headers = [
            'period', 'revenue', 'net_income', 'free_cash_flow',
            'gross_margin', 'op_margin', 'net_margin', 'current_ratio',
            'debt_to_equity', 'interest_coverage',
        ]
        with open(ratio_path, newline='') as f:
            reader = csv.DictReader(f)
            for h in expected_headers:
                assert h in reader.fieldnames, f"Missing header: {h}"

    def test_financials_csv_row_count(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('TEST', _make_data(), _make_result())
        with open(fin_path, newline='') as f:
            rows = list(csv.DictReader(f))
        # 3 periods in synthetic data
        assert len(rows) == 3

    def test_ratios_csv_first_row_is_ttm(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        _, ratio_path = export_ticker('TEST', _make_data(), _make_result())
        with open(ratio_path, newline='') as f:
            rows = list(csv.DictReader(f))
        assert rows[0]['period'] == 'TTM'

    def test_ratios_csv_ttm_revenue(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        _, ratio_path = export_ticker('TEST', _make_data(), _make_result())
        with open(ratio_path, newline='') as f:
            rows = list(csv.DictReader(f))
        ttm_row = rows[0]
        assert float(ttm_row['revenue']) == 4_100_000_000

    def test_financials_csv_periods_newest_first(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        fin_path, _ = export_ticker('TEST', _make_data(), _make_result())
        with open(fin_path, newline='') as f:
            rows = list(csv.DictReader(f))
        periods = [r['period'] for r in rows]
        assert periods == sorted(periods, reverse=True)


# ── export_comparison ─────────────────────────────────────────────────────────

class TestExportComparison:
    def _all_results(self):
        result = _make_result()
        # Build all_results dict for two tickers
        return {
            'AAPL': result,
            'MSFT': result,
        }

    def test_creates_comparison_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        assert os.path.exists(path)

    def test_comparison_filename(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        assert os.path.basename(path) == 'comparison_AAPL_MSFT.csv'

    def test_comparison_has_header_row_with_tickers(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
        assert 'AAPL' in header
        assert 'MSFT' in header

    def test_comparison_first_column_is_metric(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header[0] == 'Metric'

    def test_comparison_has_revenue_row(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            rows = list(csv.reader(f))
        metric_labels = [r[0] for r in rows]
        assert 'Revenue (TTM)' in metric_labels

    def test_comparison_has_pe_row(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            rows = list(csv.reader(f))
        metric_labels = [r[0] for r in rows]
        assert 'P/E (TTM)' in metric_labels

    def test_comparison_data_values_present(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            rows = list(csv.reader(f))
        # Find Revenue (TTM) row
        rev_row = next(r for r in rows if r[0] == 'Revenue (TTM)')
        # Both tickers should have non-empty revenue
        assert rev_row[1] != ''
        assert rev_row[2] != ''

    def test_single_ticker_comparison(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        all_results = {'GOOG': _make_result()}
        path = export_comparison(['GOOG'], all_results)
        assert os.path.exists(path)
        assert 'GOOG' in os.path.basename(path)

    def test_comparison_row_count(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter_module, 'EXPORTS_DIR', str(tmp_path))
        path = export_comparison(['AAPL', 'MSFT'], self._all_results())
        with open(path, newline='') as f:
            rows = list(csv.reader(f))
        # 1 header + 24 metric rows (as defined in exporter.py)
        assert len(rows) == 25
