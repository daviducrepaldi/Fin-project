"""
tests/test_economy.py — Unit tests for src/economy.py (pure parts, offline).
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src import economy


def _rows(vals, start_year=2025):
    return [(f"{start_year + i // 12}-{i % 12 + 1:02d}-01", v) for i, v in enumerate(vals)]


class TestParseFredCsv:
    def test_parses_and_drops_missing(self):
        text = "observation_date,DGS10\n2026-09-28,5.24\n2026-09-29,.\n2026-09-30,5.29\n"
        assert economy.parse_fred_csv(text) == [("2026-09-28", 5.24), ("2026-09-30", 5.29)]

    def test_sorts_oldest_first(self):
        text = "d,x\n2026-02-01,2\n2026-01-01,1\n"
        assert [r[0] for r in economy.parse_fred_csv(text)] == ["2026-01-01", "2026-02-01"]

    def test_empty(self):
        assert economy.parse_fred_csv("") == []


class TestSummarize:
    def test_level(self):
        s = economy.summarize(_rows([4.0, 4.1, 4.3]), "level")
        assert s["value"] == 4.3 and s["prior"] == 4.1 and s["change"] == 0.2

    def test_diff(self):
        s = economy.summarize(_rows([100, 110, 135]), "diff")
        assert s["value"] == 25 and s["prior"] == 10 and s["change"] == 15

    def test_mom(self):
        s = economy.summarize(_rows([100, 110, 99]), "mom")
        assert s["value"] == -10.0 and s["prior"] == 10.0

    def test_yoy_needs_13_points(self):
        assert economy.summarize(_rows(list(range(1, 13))), "yoy") is None
        vals = [100.0] * 12 + [103.0, 106.0]
        s = economy.summarize(_rows(vals), "yoy")
        assert s["value"] == 6.0 and s["prior"] == 3.0 and s["change"] == 3.0

    def test_empty_and_short(self):
        assert economy.summarize([], "level") is None
        assert economy.summarize(_rows([5.0]), "diff") is None

    def test_single_level_has_no_prior(self):
        s = economy.summarize(_rows([5.0]), "level")
        assert s["prior"] is None and s["change"] is None


_FOMC_HTML = '''
<a id="1">2026 FOMC Meetings</a>
<div class="fomc-meeting__month col"><strong>January</strong></div>
<div class="fomc-meeting__date col">27-28</div>
<div class="fomc-meeting__month col"><strong>March</strong></div>
<div class="fomc-meeting__date col">17-18*</div>
<div class="fomc-meeting__month col"><strong>October</strong></div>
<div class="fomc-meeting__date col">27-28</div>
<a id="2">2025 FOMC Meetings</a>
<div class="fomc-meeting__month col"><strong>December</strong></div>
<div class="fomc-meeting__date col">9-10</div>
<a id="3">2027 FOMC Meetings</a>
<div class="fomc-meeting__month col"><strong>Apr/May</strong></div>
<div class="fomc-meeting__date col">28-29</div>
'''


class TestFomc:
    def test_parse_dates(self):
        assert economy.parse_fomc_dates(_FOMC_HTML) == [
            "2025-12-10", "2026-01-28", "2026-03-18", "2026-10-28", "2027-05-29"]

    def test_next_fomc(self):
        dates = economy.parse_fomc_dates(_FOMC_HTML)
        n = economy.next_fomc(dates, today=date(2026, 10, 1))
        assert n == {"decision_date": "2026-10-28", "days_away": 27}

    def test_next_fomc_same_day_counts(self):
        n = economy.next_fomc(["2026-10-28"], today=date(2026, 10, 28))
        assert n["days_away"] == 0

    def test_next_fomc_none_left(self):
        assert economy.next_fomc(["2025-01-01"], today=date(2026, 1, 1)) is None


def test_fetch_series_rejects_bad_id():
    assert economy.fetch_series("../etc/passwd") == []
    assert economy.fetch_series("a b") == []
