"""
macro.py — market-context helpers: sector ETF mapping, tape/day-change math,
indexed performance windows, and per-ticker headlines from free RSS feeds.

Everything here is pure and offline-testable except fetch_news, which makes
one keyless HTTP GET (Yahoo Finance RSS, Google News fallback) — no API key,
no quota.
"""

import re
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

# SPDR sector fund for each classify_sector() bucket. 'general' has no
# meaningful sector proxy — callers compare against SPY alone.
SECTOR_ETFS = {
    "technology":  "XLK",
    "financials":  "XLF",
    "real_estate": "XLRE",
    "healthcare":  "XLV",
    "energy":      "XLE",
    "utilities":   "XLU",
    "consumer":    "XLY",
}

MARKET_BENCHMARK = "SPY"

_TICKER_RE = re.compile(r'^[A-Z]{1,10}([.-][A-Z]{1,4})?$')

_NEWS_FEEDS = [
    ("https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US",
     "Yahoo Finance"),
    ("https://news.google.com/rss/search?q={t}%20stock&hl=en-US&gl=US&ceid=US:en",
     "Google News"),
]
_NEWS_HEADERS = {"User-Agent": "Mozilla/5.0 (FinancialAnalyzerApp)"}


def _sorted_closes(prices: list) -> list:
    """[(date, close), ...] oldest-first, rows without a close dropped."""
    rows = [(p.get("date"), p.get("close")) for p in (prices or [])
            if p.get("close") is not None and p.get("date")]
    rows.sort()
    return rows


def price_change(prices: list):
    """(last_close, day_change_pct) from a daily series; None if <2 closes."""
    closes = _sorted_closes(prices)
    if len(closes) < 2 or not closes[-2][1]:
        return None
    prev, last = closes[-2][1], closes[-1][1]
    return last, round((last / prev - 1) * 100, 2)


def indexed_window(prices: list, days: int = 126):
    """
    (dates, values) over the trailing `days` closes, rebased to 100 at the
    window start — for overlaying series with different price levels.
    Returns None when there are fewer than 2 usable closes.
    """
    closes = _sorted_closes(prices)[-days:]
    if len(closes) < 2 or not closes[0][1]:
        return None
    base = closes[0][1]
    return ([d for d, _ in closes],
            [round(c / base * 100, 2) for _, c in closes])


def _rss_date(raw) -> str:
    """RFC-822 pubDate → 'YYYY-MM-DD'; falls back to the raw prefix."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw[:16]


def parse_rss(xml_text: str, limit: int = 10, default_source: str = "") -> list:
    """
    Parse RSS 2.0 <item> entries into [{date, title, source, url}, ...].
    Skips items without a title or an http(s) link; [] on unparseable XML.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link.startswith("http"):
            continue
        items.append({
            "date":   _rss_date(item.findtext("pubDate")),
            "title":  title,
            "source": (item.findtext("source") or default_source).strip(),
            "url":    link,
        })
        if len(items) >= limit:
            break
    return items


def fetch_news(ticker: str, limit: int = 10) -> list:
    """
    Latest headlines for a ticker from free keyless RSS feeds.
    Tries Yahoo Finance first, then Google News; [] when both fail.
    """
    if not _TICKER_RE.match(ticker or ""):
        return []
    for url_tpl, default_source in _NEWS_FEEDS:
        try:
            r = requests.get(url_tpl.format(t=ticker),
                             headers=_NEWS_HEADERS, timeout=10)
            r.raise_for_status()
            items = parse_rss(r.text, limit=limit, default_source=default_source)
            if items:
                return items
        except Exception:
            continue
    return []
