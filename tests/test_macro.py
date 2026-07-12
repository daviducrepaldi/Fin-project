"""
tests/test_macro.py — Unit tests for src/macro.py (pure parts, offline).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src import macro
from src.utils import classify_sector


def _prices(closes, start_day=1):
    return [{"date": f"2026-06-{start_day + i:02d}", "close": c}
            for i, c in enumerate(closes)]


class TestPriceChange:
    def test_up_day(self):
        last, chg = macro.price_change(_prices([100.0, 102.5]))
        assert last == 102.5
        assert chg == 2.5

    def test_down_day(self):
        _, chg = macro.price_change(_prices([200.0, 190.0]))
        assert chg == -5.0

    def test_sorts_by_date(self):
        rows = list(reversed(_prices([100.0, 110.0])))
        last, chg = macro.price_change(rows)
        assert last == 110.0 and chg == 10.0

    def test_insufficient_data(self):
        assert macro.price_change(_prices([100.0])) is None
        assert macro.price_change([]) is None
        assert macro.price_change(None) is None

    def test_skips_null_closes(self):
        rows = _prices([100.0, 105.0]) + [{"date": "2026-06-03", "close": None}]
        last, chg = macro.price_change(rows)
        assert last == 105.0 and chg == 5.0


class TestIndexedWindow:
    def test_rebased_to_100(self):
        dates, values = macro.indexed_window(_prices([50.0, 55.0, 60.0]))
        assert values[0] == 100.0
        assert values[-1] == 120.0
        assert len(dates) == len(values) == 3

    def test_window_trims_to_days(self):
        rows = _prices([float(100 + i) for i in range(10)])
        dates, values = macro.indexed_window(rows, days=5)
        assert len(values) == 5
        assert values[0] == 100.0          # rebased at the window start

    def test_none_when_too_short(self):
        assert macro.indexed_window(_prices([100.0])) is None
        assert macro.indexed_window([]) is None


_YAHOO_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Yahoo Finance</title>
  <item>
    <title>Apple ships new thing</title>
    <link>https://finance.yahoo.com/news/apple-thing.html</link>
    <pubDate>Mon, 06 Jul 2026 12:34:56 +0000</pubDate>
  </item>
  <item>
    <title>No link — should be skipped</title>
  </item>
  <item>
    <title>Second story</title>
    <link>https://finance.yahoo.com/news/second.html</link>
    <pubDate>Sun, 05 Jul 2026 08:00:00 GMT</pubDate>
  </item>
</channel></rss>"""

_GOOGLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AAPL stock rallies - Reuters</title>
    <link>https://news.google.com/articles/abc</link>
    <pubDate>Tue, 07 Jul 2026 09:00:00 GMT</pubDate>
    <source url="https://reuters.com">Reuters</source>
  </item>
</channel></rss>"""


class TestParseRss:
    def test_yahoo_shape(self):
        items = macro.parse_rss(_YAHOO_RSS, default_source="Yahoo Finance")
        assert len(items) == 2                       # linkless item skipped
        assert items[0] == {
            "date":   "2026-07-06",
            "title":  "Apple ships new thing",
            "source": "Yahoo Finance",
            "url":    "https://finance.yahoo.com/news/apple-thing.html",
        }
        assert items[1]["date"] == "2026-07-05"      # GMT date format variant

    def test_google_source_tag_wins_over_default(self):
        items = macro.parse_rss(_GOOGLE_RSS, default_source="Google News")
        assert items[0]["source"] == "Reuters"

    def test_limit(self):
        assert len(macro.parse_rss(_YAHOO_RSS, limit=1)) == 1

    def test_malformed_xml(self):
        assert macro.parse_rss("this is not xml <<<") == []
        assert macro.parse_rss("") == []


class TestFetchNews:
    def test_invalid_ticker_short_circuits_without_network(self):
        # regex guard rejects these before any HTTP request is attempted
        assert macro.fetch_news("../etc/passwd") == []
        assert macro.fetch_news("aapl") == []
        assert macro.fetch_news("") == []
        assert macro.fetch_news(None) == []


class TestSectorEtfs:
    def test_every_sector_bucket_has_an_etf_except_general(self):
        buckets = {'financials', 'real_estate', 'technology', 'healthcare',
                   'energy', 'utilities', 'consumer'}
        assert set(macro.SECTOR_ETFS) == buckets

    def test_classifier_output_maps(self):
        sector = classify_sector({'sic': '7372'})    # prepackaged software
        assert macro.SECTOR_ETFS[sector] == "XLK"
