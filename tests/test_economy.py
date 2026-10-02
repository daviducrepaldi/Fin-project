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


class TestReleaseDates:
    def test_no_key_returns_empty(self):
        assert economy.next_release_dates(None) == {}

    def test_maps_series_through_release_ids(self, monkeypatch):
        monkeypatch.setattr(economy, "_next_date_for_release",
                            lambda rid, key, today: {10: "2026-10-14", 50: "2026-10-02"}.get(rid))
        out = economy.next_release_dates("k", today=date(2026, 10, 1))
        assert out["CPIAUCSL"] == "2026-10-14"
        assert out["PAYEMS"] == out["UNRATE"] == "2026-10-02"
        assert "GDP" not in out and "HOUST" not in out   # unresolved releases omitted


class TestParseFredJson:
    def test_parses_and_drops_dots(self):
        payload = {"observations": [{"date": "2026-02-01", "value": "2"},
                                    {"date": "2026-01-01", "value": "."},
                                    {"date": "2026-01-02", "value": "1.5"}]}
        assert economy.parse_fred_json(payload) == [("2026-01-02", 1.5), ("2026-02-01", 2.0)]

    def test_garbage(self):
        assert economy.parse_fred_json({}) == []
        assert economy.parse_fred_json({"observations": [{"x": 1}]}) == []


class TestDeadline:
    def test_dashboard_returns_within_deadline_when_fred_hangs(self, monkeypatch):
        import time
        monkeypatch.setattr(economy, "fetch_series", lambda *a, **k: time.sleep(3) or [])
        monkeypatch.setattr(economy, "fetch_fomc_dates", lambda: time.sleep(3) or [])
        t = time.time()
        d = economy.fetch_dashboard(None, deadline=0.4)
        assert time.time() - t < 2
        assert d["indicators"] == []
        assert len(d["errors"]) == len(economy.INDICATORS)


class TestSnapshot:
    def _live(self, ids):
        rows = [{"id": i[0], "label": i[1], "group": i[2]} for i in economy.INDICATORS if i[0] in ids]
        return {"indicators": rows, "fomc_dates": [], "errors": [], "fetched_at": "2026-10-01T10:00:00"}

    def test_merge_fills_missing_from_snapshot_in_order(self):
        snap = self._live({i[0] for i in economy.INDICATORS})
        snap["fomc_dates"] = ["2026-10-28"]
        live = self._live({"UNRATE"})
        m = economy.merge_with_snapshot(live, snap)
        assert [r["id"] for r in m["indicators"]] == [i[0] for i in economy.INDICATORS]
        assert "UNRATE" not in m["stale_ids"] and len(m["stale_ids"]) == len(economy.INDICATORS) - 1
        assert m["fomc_dates"] == ["2026-10-28"] and m["errors"] == []

    def test_merge_without_snapshot_keeps_errors(self):
        live = self._live({"UNRATE"})
        live["errors"] = ["CPIAUCSL"]
        m = economy.merge_with_snapshot(live, None)
        assert m["stale_ids"] == [] and m["errors"] == ["CPIAUCSL"]

    def test_roundtrip_and_freshness(self, tmp_path):
        from datetime import datetime
        dash = self._live({"UNRATE"})
        dash["fetched_at"] = datetime.now().isoformat(timespec="seconds")
        path = tmp_path / "_economy.json"
        assert economy.write_snapshot(path, dash)
        snap = economy.read_snapshot(path)
        assert snap["indicators"][0]["id"] == "UNRATE"
        assert economy.snapshot_is_fresh(snap)
        assert not economy.snapshot_is_fresh({"fetched_at": "2020-01-01T00:00:00"})
        assert not economy.snapshot_is_fresh(None)

    def test_read_missing_or_bad(self, tmp_path):
        assert economy.read_snapshot(tmp_path / "nope.json") is None
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        assert economy.read_snapshot(bad) is None


class TestAssess:
    def test_lower_is_better(self):
        assert economy.assess("CPIAUCSL", 2.1) == "good"
        assert economy.assess("CPIAUCSL", 3.0) == "watch"
        assert economy.assess("CPIAUCSL", 3.71) == "bad"

    def test_higher_is_better(self):
        assert economy.assess("A191RL1Q225SBEA", 2.2) == "good"
        assert economy.assess("A191RL1Q225SBEA", 1.0) == "watch"
        assert economy.assess("A191RL1Q225SBEA", -0.5) == "bad"
        assert economy.assess("GACDFSA066MSFRBPHI", 37.8) == "good"
        assert economy.assess("GACDFSA066MSFRBPHI", -20) == "bad"

    def test_informational_and_unknown(self):
        assert economy.assess("DGS10", 5.29) is None
        assert economy.assess("NOPE", 1) is None
        assert economy.assess("UNRATE", None) is None

    def test_every_indicator_has_a_benchmark(self):
        assert {i[0] for i in economy.INDICATORS} == set(economy.BENCHMARKS)
