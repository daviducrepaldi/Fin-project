"""
tests/test_technicals.py — Unit tests for src/technicals.py (pure, offline).
"""

import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.technicals import compute_technicals, reverse_dcf, _dcf_value


def _series(n=260, daily_growth=0.001, start=100.0):
    """Synthetic price rows: close grows at a constant daily rate."""
    d0 = date(2025, 1, 1)
    rows = []
    px = start
    for i in range(n):
        px *= (1 + daily_growth) if i else 1
        rows.append({
            "date":   (d0 + timedelta(days=i)).isoformat(),
            "open":   px, "high": px * 1.01, "low": px * 0.99,
            "close":  round(px, 6), "volume": 1000 + i,
        })
    return rows


class TestComputeTechnicals:
    def setup_method(self):
        self.tech = compute_technicals(_series())

    def test_returns_none_when_too_short(self):
        assert compute_technicals(_series(n=30)) is None
        assert compute_technicals([]) is None

    def test_one_month_return(self):
        # constant 0.1%/day growth → 21-day return = 1.001^21 - 1 ≈ 2.1%
        assert self.tech["return_1m"] == round(((1.001 ** 21) - 1) * 100, 1)

    def test_one_year_return_capped_at_251_trading_days(self):
        # 260 rows available → the 1Y window is still 251 trading days
        assert self.tech["return_1y"] == round(((1.001 ** 251) - 1) * 100, 1)

    def test_one_year_return_uses_available_history_when_shorter(self):
        tech = compute_technicals(_series(n=100))
        assert tech["return_1y"] == round(((1.001 ** 99) - 1) * 100, 1)

    def test_constant_growth_has_zero_volatility_and_drawdown(self):
        assert self.tech["ann_vol_pct"] == 0.0
        assert self.tech["max_drawdown_pct"] == 0.0

    def test_drawdown_detected(self):
        rows = _series(n=100, daily_growth=0.0)
        for r in rows[60:]:                     # 30% crash from day 60
            r["close"] = r["close"] * 0.7
        tech = compute_technicals(rows)
        assert tech["max_drawdown_pct"] == -30.0

    def test_sma_alignment_and_warmup(self):
        assert len(self.tech["sma50"]) == len(self.tech["dates"])
        assert self.tech["sma50"][48] is None       # warm-up
        assert self.tech["sma50"][49] is not None
        # rising series → last close above both SMAs
        assert self.tech["last_close"] > self.tech["sma50"][-1] > self.tech["sma200"][-1]
        assert self.tech["above_sma200"] is True


class TestReverseDcf:
    def test_round_trip_recovers_known_growth(self):
        fcf, g = 1_000_000_000, 0.08
        mcap = _dcf_value(fcf, g, 0.10, 0.025, 10)
        out = reverse_dcf(fcf, mcap)
        assert abs(out["implied_growth_pct"] - 8.0) < 0.1
        assert out["clamped"] is False

    def test_negative_fcf_returns_none(self):
        assert reverse_dcf(-5e9, 100e9) is None
        assert reverse_dcf(None, 100e9) is None
        assert reverse_dcf(5e9, None) is None

    def test_absurd_valuation_clamps(self):
        # market cap 1000x FCF → implied growth pinned at the +60% bound
        out = reverse_dcf(1e9, 1e12)
        assert out["implied_growth_pct"] == 60.0
        assert out["clamped"] is True

    def test_sensitivity_grid_shape(self):
        out = reverse_dcf(1e9, 30e9)
        assert len(out["sensitivity"]) == 3
        for row in out["sensitivity"]:
            assert {"growth_pct", "dr_8", "dr_10", "dr_12"} <= set(row)
        # higher discount rate → lower fair value
        mid = out["sensitivity"][1]
        assert mid["dr_8"] > mid["dr_10"] > mid["dr_12"]
