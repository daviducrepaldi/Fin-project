"""
tests/test_fetcher.py — Unit tests for src/fetcher.py pure helpers.

Only tests the offline data-shaping logic (no network access).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.fetcher import _fill_missing_q4, _quarterly_duration, _ttm


def _dp(start, end, val, form, filed):
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed}


def _concept(datapoints):
    return {"units": {"USD": datapoints}}


class TestFillMissingQ4:
    def test_q4_derived_from_annual_minus_three_quarters(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-04-01", "2024-06-30", 110, "10-Q", "2024-07-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            # 10-K reports the full year — no standalone Q4 datapoint
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        assert "2024-12-31" not in quarterly
        filled = _fill_missing_q4(quarterly, concept)
        assert filled["2024-12-31"] == 480 - (100 + 110 + 120)

    def test_no_derivation_when_a_quarter_is_missing(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        filled = _fill_missing_q4(quarterly, concept)
        # Only 2 quarters inside the FY window — a "Q4" would silently absorb
        # the missing quarter's value, so nothing must be derived.
        assert "2024-12-31" not in filled

    def test_existing_q4_not_overwritten(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-04-01", "2024-06-30", 110, "10-Q", "2024-07-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            _dp("2024-10-01", "2024-12-31", 140, "10-Q", "2025-01-15"),
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        filled = _fill_missing_q4(quarterly, concept)
        assert filled["2024-12-31"] == 140.0


class TestFetcherTTM:
    def test_sums_four_contiguous_quarters(self):
        by_p = {"2024-03-31": 100, "2024-06-30": 110,
                "2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) == 470

    def test_none_when_quarters_span_more_than_a_year(self):
        by_p = {"2022-12-31": 100, "2023-03-31": 110,
                "2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) is None

    def test_annual_only_filer_uses_latest_fiscal_year(self):
        by_p = {"2022-12-31": 300, "2023-12-31": 350, "2024-12-31": 400}
        assert _ttm(by_p) == 400

    def test_none_when_fewer_than_four_quarters(self):
        by_p = {"2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) is None
