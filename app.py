import html
import json
import os
import re
import time
from pathlib import Path

# Load .env before any other imports so API keys are available immediately
def _load_env():
    candidates = [
        Path(__file__).resolve().parent / ".env",  # project root
        Path.cwd() / ".env",                        # wherever streamlit was launched from
    ]
    for env_path in candidates:
        try:
            with open(env_path) as _f:
                for _line in _f:
                    _line = _line.strip()
                    if _line and not _line.startswith('#') and '=' in _line:
                        _k, _, _v = _line.partition('=')
                        os.environ.setdefault(_k.strip(), _v.strip())
            break
        except OSError:
            continue
_load_env()

from datetime import datetime, timedelta

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from src import db, fetcher, analyzer, technicals, macro
from src.utils import period_to_quarter_label, clean_for_json, classify_sector

# db.init_db() intentionally not called here — the web app reads from JSON files,
# not SQLite. Init is only needed by the CLI (main.py).

AVAILABLE_TICKERS = ['AAPL', 'AMZN', 'GOOGL', 'JPM', 'META', 'MSFT', 'NVDA', 'TSLA']
DATA_DIR = Path(__file__).parent / 'data'

st.set_page_config(
    page_title="Financial Terminal",
    page_icon="▶",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Bloomberg Terminal theme ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');

:root {
    --bg:      #0a0a0a;
    --surface: #111111;
    --raised:  #1a1a1a;
    --border:  #2a2a2a;
    --orange:  #ff6600;
    --green:   #00cc44;
    --red:     #ff3333;
    --text:    #e0e0e0;
    --dim:     #888888;
    --font:    'IBM Plex Mono', 'Courier New', monospace;
}

/* ── App shell ── */
.stApp { background: var(--bg) !important; color: var(--text); font-family: var(--font); }
.block-container {
    /* clear Streamlit's 60px fixed header — content under it is invisible */
    padding-top: 4rem !important;
    padding-bottom: 0.5rem !important;
    background: var(--bg) !important;
    max-width: 100% !important;
}
header[data-testid="stHeader"] { background: var(--bg) !important; }
/* Font inheritance from root — no !important here so that icon spans
   with their own explicit font-family declaration can still override it */
.stApp { font-family: var(--font); }
/* Explicit !important only on safe leaf/content elements that never
   contain Material Icon text glyphs — span and div intentionally omitted */
p, h1, h2, h3, h4, h5, h6,
input, select, textarea,
td, th, li, label, caption, small {
    font-family: var(--font) !important;
}
/* Streamlit-specific content containers (no icon children) */
[data-testid="stMarkdownContainer"],
[data-testid="stMetricValue"],
[data-testid="stMetricLabel"],
[data-testid="stCaptionContainer"],
[data-testid="stDataFrameResizable"] {
    font-family: var(--font) !important;
}

/* ── Headings ── */
h1, h2, h3, h4, h5 {
    color: var(--orange) !important;
    font-family: var(--font) !important;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-weight: 600;
    margin-bottom: 0.3rem !important;
}

/* ── Body text ── */
p, li { color: var(--text); line-height: 1.5; font-size: 0.84rem; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] td,
[data-testid="stSidebar"] th { font-family: var(--font) !important; }
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div { color: var(--text); }
[data-testid="stSidebarContent"] { padding: 0.8rem 0.8rem; }

/* Text input — style only the input element; kill the BaseWeb wrapper's own
   border so the box doesn't render doubled */
[data-testid="stTextInput"] [data-baseweb="input"],
[data-testid="stTextInput"] [data-baseweb="base-input"] {
    background: var(--bg) !important;
    border: none !important;
    box-shadow: none !important;
}
/* "Press Enter to submit form" hint overlaps the typed text — hide it */
[data-testid="InputInstructions"] { display: none !important; }
[data-testid="stTextInput"] input {
    background: var(--bg) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-size: 0.82rem;
    padding: 0.3rem 0.5rem;
    border-radius: 2px !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: var(--orange) !important;
    box-shadow: none !important;
    outline: none;
}
[data-testid="stTextInput"] label { color: var(--orange) !important; font-size: 0.7rem; letter-spacing: 0.08em; text-transform: uppercase; }

/* ── Buttons ── */
[data-testid="stButton"] > button[kind="primary"],
[data-testid="stFormSubmitButton"] > button,
[data-testid="stButton"] > button[data-testid="baseButton-primary"] {
    background: var(--orange) !important;
    color: #000 !important;
    border: none !important;
    font-family: var(--font) !important;
    font-weight: 600;
    font-size: 0.76rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.35rem 0.8rem;
    border-radius: 2px !important;
}
[data-testid="stButton"] > button[kind="primary"]:hover,
[data-testid="stFormSubmitButton"] > button:hover { background: #cc5200 !important; }
[data-testid="stButton"] > button:not([kind="primary"]) {
    background: transparent !important;
    border: 1px solid var(--border) !important;
    color: var(--dim) !important;
    font-family: var(--font) !important;
    font-size: 0.74rem;
    padding: 0.3rem 0.8rem;
    border-radius: 2px !important;
}
[data-testid="stButton"] > button:not([kind="primary"]):hover {
    border-color: var(--orange) !important;
    color: var(--orange) !important;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    gap: 0;
}
[data-testid="stTabs"] button[role="tab"] {
    background: transparent !important;
    color: var(--dim);
    font-family: var(--font) !important;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.07em;
    text-transform: uppercase;
    border-bottom: 2px solid transparent;
    padding: 0.4rem 1rem;
}
[data-testid="stTabs"] button[role="tab"]:hover { color: var(--text); }
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    color: var(--orange) !important;
    border-bottom-color: var(--orange) !important;
    background: transparent !important;
}
[data-testid="stTabContent"] { background: var(--bg); padding-top: 0.6rem; }

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-top: 2px solid var(--orange) !important;
    border-radius: 0 !important;
    padding: 0.35rem 0.55rem !important;
}
[data-testid="stMetric"] label {
    color: var(--orange) !important;
    font-family: var(--font) !important;
    font-size: 0.62rem !important;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}
[data-testid="stMetricValue"] {
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-size: 1.1rem !important;
    font-weight: 600;
}
[data-testid="stMetricDelta"] {
    font-family: var(--font) !important;
    font-size: 0.72rem !important;
}

/* ── DataFrames ── */
[data-testid="stDataFrame"] {
    background: var(--surface);
    border: 1px solid var(--border) !important;
    border-radius: 0 !important;
}
[data-testid="stDataFrame"] th {
    background: var(--raised) !important;
    color: var(--orange) !important;
    font-family: var(--font) !important;
    font-size: 0.62rem !important;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    padding: 0.2rem 0.45rem !important;
    border-bottom: 1px solid var(--border);
}
[data-testid="stDataFrame"] td {
    font-family: var(--font) !important;
    font-size: 0.76rem !important;
    color: var(--text) !important;
    padding: 0.18rem 0.45rem !important;
    background: var(--surface) !important;
}
[data-testid="stDataFrame"] tr:nth-child(even) td { background: var(--raised) !important; }

/* ── Dividers ── */
hr { border-color: var(--border) !important; margin: 0.4rem 0 !important; }

/* ── Captions ── */
[data-testid="stCaptionContainer"],
.stCaption,
small { color: var(--dim) !important; font-family: var(--font) !important; font-size: 0.69rem !important; }

/* ── Expander ── */
[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: 0 !important;
    background: var(--surface) !important;
}
[data-testid="stExpander"] summary {
    color: var(--orange) !important;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}
[data-testid="stExpander"] summary:hover { color: var(--text) !important; }

/* ── Info / Warning / Error alerts ── */
[data-testid="stAlert"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 0 !important;
    font-size: 0.78rem;
}

/* ── Spinner ── */
.stSpinner > div { border-top-color: var(--orange) !important; }

/* ── Status boxes ── */
[data-testid="stStatusWidget"] { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: 0 !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 0; }
::-webkit-scrollbar-thumb:hover { background: var(--orange); }
</style>
""", unsafe_allow_html=True)

# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_large(v):
    if v is None: return "N/A"
    if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:  return f"${v/1e6:.0f}M"
    return f"${v:,.0f}"

def _fmt_x(v, dec=1):
    return f"{v:.{dec}f}x" if v is not None else "N/A"

def _fmt_pct(v):
    return f"{v:.1f}%" if v is not None else "N/A"

_qlabel = period_to_quarter_label

# ── Bloomberg Terminal chart palette ─────────────────────────────────────────
_C_BLUE   = "#4d9de0"
_C_GREEN  = "#00cc44"
_C_AMBER  = "#ff6600"
_C_PURPLE = "#cc88ff"
_C_RED    = "#ff3333"

def _chart_theme(**overrides) -> dict:
    """Base Plotly layout kwargs for the Bloomberg Terminal aesthetic."""
    base = dict(
        paper_bgcolor="#111111",
        plot_bgcolor="#0a0a0a",
        font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=10),
        title=dict(
            text="",   # explicit empty text — Plotly renders a missing key as "undefined"
            font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#ff6600", size=12),
            x=0, xanchor="left", pad=dict(l=4),
        ),
        xaxis=dict(
            gridcolor="#1e1e1e", linecolor="#2a2a2a",
            tickfont=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9),
            zeroline=False,
        ),
        yaxis=dict(
            gridcolor="#1e1e1e", linecolor="#2a2a2a",
            tickfont=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9),
            zeroline=False,
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9),
        ),
        margin=dict(t=36, b=16, l=4, r=4),
    )
    base.update(overrides)
    return base

def _section_header(label: str):
    """Render a Bloomberg-style orange section label with bottom border."""
    st.markdown(
        f'<div style="color:#ff6600;font-family:\'IBM Plex Mono\',monospace;'
        f'font-size:0.68rem;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;'
        f'padding:0.15rem 0;border-bottom:1px solid #2a2a2a;margin:0.3rem 0 0.45rem;">── {label}</div>',
        unsafe_allow_html=True,
    )


# ── static data loading ───────────────────────────────────────────────────────

def _load_file(ticker: str):
    """Load pre-fetched data from data/{TICKER}.json. Returns None if not found."""
    path = (DATA_DIR / f'{ticker}.json').resolve()
    if path.parent != DATA_DIR.resolve():
        return None  # path escaped data/ — should never happen after ticker validation
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save_file(ticker: str, data: dict):
    """Write data back to data/{TICKER}.json (works locally; silent no-op on Cloud)."""
    path = (DATA_DIR / f'{ticker}.json').resolve()
    if path.parent != DATA_DIR.resolve():
        return  # path escaped data/ — refuse to write
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(clean_for_json(data), f, indent=2)


# ── macro context (SPY / sector ETFs / news) ─────────────────────────────────

def _prices_fresh(series, max_age_days: int = 5) -> bool:
    """True when the newest row is recent enough to skip a live refresh
    (5 days tolerates weekends + market holidays)."""
    if not series:
        return False
    last = max((p.get("date") or "") for p in series)
    try:
        return (datetime.now() - datetime.strptime(last, "%Y-%m-%d")).days <= max_age_days
    except ValueError:
        return False


def _get_macro_prices(symbol: str):
    """
    Daily series for SPY / a sector ETF. Priority: session cache → fresh
    disk file → live Tiingo (persisted back) → stale disk file → None.
    `symbol` only ever comes from macro.SECTOR_ETFS / MARKET_BENCHMARK,
    never from user input.
    """
    cache = st.session_state.setdefault('macro_cache', {})
    if symbol in cache:
        return cache[symbol]

    path = (DATA_DIR / f'_macro_{symbol}.json').resolve()
    stored = None
    if path.parent == DATA_DIR.resolve():
        try:
            with open(path) as f:
                stored = json.load(f)
        except (OSError, json.JSONDecodeError):
            stored = None

    series = stored if _prices_fresh(stored) else None
    if series is None:
        try:
            series = fetcher.get_price_series(symbol)
            if path.parent == DATA_DIR.resolve():
                try:
                    with open(path, 'w') as f:
                        json.dump(clean_for_json(series), f)
                except OSError:
                    pass   # Cloud filesystem is read-only — that's fine
        except Exception:
            series = stored   # stale beats nothing

    cache[symbol] = series
    return series


def _get_news(ticker: str) -> list:
    """Headlines for a ticker, fetched once per session (keyless RSS)."""
    cache = st.session_state.setdefault('news_cache', {})
    if ticker not in cache:
        cache[ticker] = macro.fetch_news(ticker, limit=12)
    return cache[ticker]


def _render_ticker_tape():
    """NYSE-style scrolling tape built from the pre-loaded JSON files —
    zero API calls; shows each ticker's last close vs the prior day."""
    items = []
    for t in AVAILABLE_TICKERS:
        data = _load_file(t)
        pc = macro.price_change((data or {}).get("prices"))
        if pc:
            items.append((t, pc[0], pc[1]))
    if not items:
        return

    cells = []
    for t, px, chg in items:
        color = _C_GREEN if chg >= 0 else _C_RED
        arrow = "▲" if chg >= 0 else "▼"
        cells.append(
            f'<span class="tape-item">{html.escape(t)} '
            f'<span style="color:#e0e0e0;">{px:,.2f}</span> '
            f'<span style="color:{color};">{arrow} {abs(chg):.2f}%</span></span>'
        )
    strip = "".join(cells)
    st.markdown(f"""
<style>
.tape-wrap {{ overflow:hidden; white-space:nowrap; background:#111111;
  border-top:1px solid #2a2a2a; border-bottom:1px solid #2a2a2a;
  padding:0.35rem 0; margin-bottom:0.6rem; }}
.tape-move {{ display:inline-block; white-space:nowrap;
  animation: tape-scroll 45s linear infinite; }}
.tape-wrap:hover .tape-move {{ animation-play-state: paused; }}
.tape-item {{ font-family:'IBM Plex Mono','Courier New',monospace;
  font-size:0.8rem; color:#ff6600; letter-spacing:0.04em; padding:0 1.4rem; }}
@keyframes tape-scroll {{ from {{ transform: translateX(0); }}
  to {{ transform: translateX(-50%); }} }}
</style>
<div class="tape-wrap"><div class="tape-move">{strip}{strip}</div></div>
""", unsafe_allow_html=True)


def _render_market_backdrop():
    """S&P 500 regime snapshot on the home screen (SPY daily series)."""
    spy = _get_macro_prices(macro.MARKET_BENCHMARK)
    tech = technicals.compute_technicals(spy) if spy else None
    if not tech:
        return

    _section_header("MARKET BACKDROP — S&P 500 (SPY)")
    s1, s2, s3, s4, s5, s6 = st.columns(6)
    s1.metric("SPY Last", f"${tech['last_close']:,.2f}")
    s2.metric("1M Return", _fmt_signed_pct(tech["return_1m"]))
    s3.metric("3M Return", _fmt_signed_pct(tech["return_3m"]))
    s4.metric("1Y Return", _fmt_signed_pct(tech["return_1y"]))
    s5.metric("Ann. Vol", _fmt_pct(tech["ann_vol_pct"]))
    s6.metric("Trend", "ABOVE 200D ▲" if tech["above_sma200"] else "BELOW 200D ▼")

    fig = go.Figure()
    fig.add_scatter(x=tech["dates"], y=tech["closes"], name="SPY",
                    mode="lines", line=dict(color=_C_GREEN, width=1.6))
    fig.add_scatter(x=tech["dates"], y=tech["sma50"], name="SMA 50",
                    mode="lines", line=dict(color=_C_AMBER, width=1.2))
    fig.add_scatter(x=tech["dates"], y=tech["sma200"], name="SMA 200",
                    mode="lines", line=dict(color=_C_BLUE, width=1.2))
    fig.update_layout(**_chart_theme(
        title="S&P 500 — 1Y DAILY (SPY, ADJUSTED)", height=260,
        legend=dict(orientation="h", y=1.08, x=1, xanchor="right",
                    bgcolor="rgba(0,0,0,0)",
                    font=dict(family="IBM Plex Mono, 'Courier New', monospace",
                              color="#888888", size=9)),
    ))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "End-of-day data. Trend flag compares the last close to the 200-day "
        "moving average — a common risk-on / risk-off proxy."
    )


def _price_warning(ticker: str, data: dict):
    """Info message when EDGAR fundamentals loaded but price data didn't."""
    err = (data or {}).get("price_data_error")
    if not err:
        return None
    if "429" in err:
        return (
            f"**{ticker}**: price data is rate-limited right now (free Tiingo tier, "
            "hourly quota) — showing SEC fundamentals only. Valuation, price chart "
            "and momentum will appear once the limit resets."
        )
    return f"**{ticker}**: price data unavailable — showing SEC fundamentals only."


def _fail_hint(exc) -> str:
    if "429" in str(exc):
        return " Price-data rate limit reached (free tier) — it resets within the hour."
    return ""


def _get_ticker(ticker: str, force_refresh: bool = False):
    """
    Returns ((data, result), warning_msg).
    Priority:
      • session_state cache (skipped if force_refresh)
      • force_refresh=True  → live yfinance; saves to disk only for AVAILABLE_TICKERS
      • static file exists  → load data/{TICKER}.json
      • static file missing → live yfinance fetch (any ticker, never saved to disk)
      • all fail            → error
    """
    cache = st.session_state.setdefault('ticker_cache', {})

    if not force_refresh and ticker in cache:
        return cache[ticker], None

    if force_refresh:
        try:
            with st.spinner(f"Fetching live data for {ticker}…"):
                data = fetcher.fetch_and_store(ticker) if ticker in AVAILABLE_TICKERS else fetcher.fetch_only(ticker)
            result = analyzer.compute_ratios(data)
            cache[ticker] = (data, result)
            if ticker in AVAILABLE_TICKERS and not data.get("price_data_error"):
                try:
                    _save_file(ticker, data)
                except Exception:
                    pass   # Cloud filesystem is read-only — that's fine
            return (data, result), _price_warning(ticker, data)
        except fetcher.UnknownTickerError:
            return None, (
                f"**{ticker}** is not a valid ticker — no listed company with that "
                f"symbol in the SEC registry. Check the spelling (e.g. AAPL, not APPL)."
            )
        except Exception as e:
            print(f"FETCH ERROR [force_refresh] {ticker}: {type(e).__name__}: {e}")
            data = _load_file(ticker)
            if data:
                result = analyzer.compute_ratios(data)
                cache[ticker] = (data, result)
                last = data.get('company', {}).get('last_updated', 'unknown date')
                return (data, result), (
                    f"Live refresh failed for **{ticker}** — showing previously "
                    f"saved data (as of {last})."
                )
            if ticker in AVAILABLE_TICKERS:
                return None, f"Live fetch failed for **{ticker}** and no cached data found. Run `python prefetch_data.py --tickers {ticker}` to rebuild it."
            print(f"FETCH ERROR [force_refresh fallback] {ticker}: {type(e).__name__}: {e}")
            return None, f"Live fetch failed for **{ticker}**.{_fail_hint(e)} Wait a moment and try again."

    # Default: load from static file
    data = _load_file(ticker)
    if data:
        result = analyzer.compute_ratios(data)
        cache[ticker] = (data, result)
        return (data, result), None

    # Not in static data — live fetch via fetch_only (no DB, no disk write)
    try:
        status_box = st.status(f"Fetching live data for {ticker}…", expanded=True)

        def _on_retry(attempt, delay, exc):
            status_box.write(
                f"Attempt {attempt + 1} failed ({type(exc).__name__}). Retrying in {delay}s…"
            )

        with status_box:
            data = fetcher.fetch_only(ticker, status_callback=_on_retry)
            status_box.update(label=f"{ticker} fetched successfully", state="complete")

        result = analyzer.compute_ratios(data)
        cache[ticker] = (data, result)
        return (data, result), _price_warning(ticker, data)
    except fetcher.UnknownTickerError:
        try:
            status_box.update(label=f"{ticker}: not a valid ticker", state="error")
        except Exception:
            pass
        return None, (
            f"**{ticker}** is not a valid ticker — no listed company with that "
            f"symbol in the SEC registry. Check the spelling (e.g. AAPL, not APPL)."
        )
    except Exception as e:
        print(f"FETCH ERROR [analyze] {ticker}: {type(e).__name__}: {e}")
        available = '  ·  '.join(AVAILABLE_TICKERS)
        return None, (
            f"Could not fetch **{ticker}**.{_fail_hint(e)} Wait a moment and try again.\n\n"
            f"Pre-loaded (instant): {available}"
        )


# ── market intel tab ──────────────────────────────────────────────────────────

def _fmt_signed_pct(v):
    if v is None:
        return "N/A"
    return f"{'+' if v >= 0 else ''}{v:.1f}%"


def _render_relative_performance(ticker: str, prices: list, data: dict):
    """Stock vs its sector SPDR fund vs SPY over 6 months, rebased to 100."""
    sector = classify_sector(data.get("company") or {})
    etf_sym = macro.SECTOR_ETFS.get(sector)

    series = [(ticker, prices, _C_AMBER)]
    if etf_sym:
        etf = _get_macro_prices(etf_sym)
        if etf:
            series.append((etf_sym, etf, _C_PURPLE))
    spy = _get_macro_prices(macro.MARKET_BENCHMARK)
    if spy:
        series.append((macro.MARKET_BENCHMARK, spy, _C_BLUE))
    if len(series) < 2:
        return   # no benchmark data available — nothing to compare against

    windows = [(name, macro.indexed_window(pr, days=126), color)
               for name, pr, color in series]
    windows = [w for w in windows if w[1]]
    if len(windows) < 2:
        return

    _section_header("RELATIVE PERFORMANCE — 6M (INDEXED TO 100)")
    cols = st.columns(len(windows))
    fig = go.Figure()
    for col, (name, (dates, values), color) in zip(cols, windows):
        col.metric(f"{name} 6M", _fmt_signed_pct(round(values[-1] - 100, 1)))
        fig.add_scatter(x=dates, y=values, name=name, mode="lines",
                        line=dict(color=color, width=1.6))
    fig.update_layout(**_chart_theme(
        title=f"{html.escape(ticker)} VS SECTOR VS MARKET", height=280,
        legend=dict(orientation="h", y=1.08, x=1, xanchor="right",
                    bgcolor="rgba(0,0,0,0)",
                    font=dict(family="IBM Plex Mono, 'Courier New', monospace",
                              color="#888888", size=9)),
    ))
    st.plotly_chart(fig, use_container_width=True)
    if etf_sym:
        st.caption(
            f"Sector proxy: {etf_sym} (SPDR {sector.replace('_', ' ')} fund). "
            "Tells you whether the stock is moving on its own merits or riding "
            "its sector."
        )
    else:
        st.caption("No sector fund mapped for this company — comparing against SPY only.")


def _render_market_intel(ticker: str, data: dict, result: dict):
    """Render the MARKET INTEL sub-tab: price action, reverse DCF,
    insider activity and the EDGAR filings feed."""
    prices  = data.get("prices") or []
    filings = data.get("filings") or []
    insider = data.get("insider") or []
    market  = result.get("market") or {}
    ttm     = result.get("ttm") or {}
    trends  = result.get("trends") or []

    if not prices and not filings:
        st.info(
            "Market intel (price history, insider activity, filings) is not in "
            "this ticker's cached data yet — click **🔄 REFRESH LIVE DATA** to fetch it."
        )

    # ── price action ──────────────────────────────────────────────
    tech = technicals.compute_technicals(prices) if prices else None
    if tech:
        _section_header("PRICE ACTION — 1Y DAILY")

        dist_high = None
        if market.get("price") and market.get("week52_high"):
            dist_high = (market["price"] / market["week52_high"] - 1) * 100

        s1, s2, s3, s4, s5, s6, s7 = st.columns(7)
        s1.metric("1M Return",  _fmt_signed_pct(tech["return_1m"]))
        s2.metric("3M Return",  _fmt_signed_pct(tech["return_3m"]))
        s3.metric("6M Return",  _fmt_signed_pct(tech["return_6m"]))
        s4.metric("1Y Return",  _fmt_signed_pct(tech["return_1y"]))
        s5.metric("Ann. Vol",   _fmt_pct(tech["ann_vol_pct"]))
        s6.metric("Max Drawdown", _fmt_signed_pct(tech["max_drawdown_pct"]))
        s7.metric("vs 52W High", _fmt_signed_pct(dist_high))

        fig = go.Figure()
        fig.add_candlestick(
            x=[p["date"] for p in prices],
            open=[p.get("open") for p in prices],
            high=[p.get("high") for p in prices],
            low=[p.get("low") for p in prices],
            close=[p.get("close") for p in prices],
            name="OHLC",
            increasing_line_color=_C_GREEN, decreasing_line_color=_C_RED,
            increasing_fillcolor=_C_GREEN, decreasing_fillcolor=_C_RED,
        )
        fig.add_scatter(x=tech["dates"], y=tech["sma50"], name="SMA 50",
                        mode="lines", line=dict(color=_C_AMBER, width=1.4))
        fig.add_scatter(x=tech["dates"], y=tech["sma200"], name="SMA 200",
                        mode="lines", line=dict(color=_C_BLUE, width=1.4))
        theme = _chart_theme(
            title=f"{html.escape(ticker)} — DAILY (ADJUSTED)",
            height=420,
            legend=dict(orientation="h", y=1.06, x=1, xanchor="right",
                        bgcolor="rgba(0,0,0,0)",
                        font=dict(family="IBM Plex Mono, 'Courier New', monospace",
                                  color="#888888", size=9)),
        )
        theme["xaxis"]["rangeslider"] = dict(visible=False)
        fig.update_layout(**theme)
        st.plotly_chart(fig, use_container_width=True)

        vols = [p.get("volume") for p in prices]
        if any(v is not None for v in vols):
            vfig = go.Figure(go.Bar(
                x=[p["date"] for p in prices], y=vols, name="Volume",
                marker_color=[
                    _C_GREEN if (p.get("close") or 0) >= (p.get("open") or 0) else _C_RED
                    for p in prices
                ],
            ))
            vfig.update_layout(**_chart_theme(
                title="VOLUME", height=140,
                margin=dict(t=24, b=8, l=4, r=4), showlegend=False,
            ))
            st.plotly_chart(vfig, use_container_width=True)

    # ── relative performance vs sector & market ──────────────────
    if tech:
        _render_relative_performance(ticker, prices, data)

    # ── reverse DCF ───────────────────────────────────────────────
    _section_header("IMPLIED EXPECTATIONS (REVERSE DCF)")
    rd = technicals.reverse_dcf(ttm.get("free_cash_flow"), market.get("market_cap"))
    if rd:
        yoy_vals = [t["rev_yoy_pct"] for t in trends
                    if t is not None and t.get("rev_yoy_pct") is not None][:4]
        actual_growth = round(sum(yoy_vals) / len(yoy_vals), 1) if yoy_vals else None
        fcf_yield = None
        if ttm.get("free_cash_flow") and market.get("market_cap"):
            fcf_yield = ttm["free_cash_flow"] / market["market_cap"] * 100

        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Implied FCF Growth (10Y)",
                  f"{rd['implied_growth_pct']:.1f}%/yr" + ("+" if rd["clamped"] else ""))
        d2.metric("Actual Revenue Growth", _fmt_signed_pct(actual_growth))
        d3.metric("TTM FCF", _fmt_large(ttm.get("free_cash_flow")))
        d4.metric("FCF Yield", _fmt_pct(round(fcf_yield, 2) if fcf_yield is not None else None))

        sens_rows = []
        for row in rd["sensitivity"]:
            sens_rows.append({
                "FCF Growth": f"{row['growth_pct']:.1f}%/yr",
                "Fair Value @8% DR":  _fmt_large(row.get("dr_8")),
                "Fair Value @10% DR": _fmt_large(row.get("dr_10")),
                "Fair Value @12% DR": _fmt_large(row.get("dr_12")),
            })
        st.dataframe(pd.DataFrame(sens_rows), use_container_width=True, hide_index=True)
        st.caption(
            f"To justify today's market cap ({_fmt_large(market.get('market_cap'))}), TTM free cash "
            f"flow must grow ~{rd['implied_growth_pct']:.1f}%/yr for {rd['years']} years "
            f"(then {rd['terminal_growth_pct']}% forever, {rd['discount_rate_pct']}% discount rate). "
            "Simplified model — compares levered FCF to equity value, ignores net debt and dilution. "
            "Not financial advice."
        )
    else:
        st.info("Reverse DCF unavailable — needs positive TTM free cash flow and a market cap.")

    # ── insider activity ──────────────────────────────────────────
    _section_header("INSIDER ACTIVITY (FORM 4)")
    if insider:
        buys  = [t for t in insider if t.get("code") == "P"]
        sells = [t for t in insider if t.get("code") == "S"]
        buy_val  = sum(t["value"] or 0 for t in buys)
        sell_val = sum(t["value"] or 0 for t in sells)
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace;font-size:0.78rem;color:#e0e0e0;'
            f'margin-bottom:0.4rem;">RECENT FILINGS: '
            f'<span style="color:{_C_GREEN};">{len(buys)} OPEN-MARKET BUYS ({_fmt_large(buy_val)})</span>'
            f'<span style="color:#888;"> · </span>'
            f'<span style="color:{_C_RED};">{len(sells)} SALES ({_fmt_large(sell_val)})</span></div>',
            unsafe_allow_html=True,
        )
        ins_rows = [{
            "Date":    t.get("date", ""),
            "Insider": t.get("name", ""),
            "Role":    t.get("role", ""),
            "Action":  t.get("action", ""),
            "Shares":  f"{t['shares']:,.0f}" if t.get("shares") is not None else "N/A",
            "Price":   f"${t['price']:,.2f}" if t.get("price") is not None else "N/A",
            "Value":   _fmt_large(t.get("value")) if t.get("value") is not None else "N/A",
        } for t in insider]
        st.dataframe(pd.DataFrame(ins_rows), use_container_width=True, hide_index=True)
        st.caption(
            "Latest Form 4 filings from SEC EDGAR. EXERCISE/GRANT/TAX rows are routine "
            "compensation mechanics — open-market BUYs are the strongest signal."
        )
    elif filings or prices:
        st.info("No recent Form 4 (insider) filings found for this ticker.")

    # ── filings & events ──────────────────────────────────────────
    _section_header("FILINGS & EVENTS")
    if filings:
        last_report = next((f for f in filings if f["form"] in ("10-Q", "10-K", "20-F")), None)
        if last_report:
            try:
                nxt = datetime.strptime(last_report["date"], "%Y-%m-%d") + timedelta(days=91)
                e1, e2 = st.columns(2)
                e1.metric("Last Report Filed", f"{last_report['date']} ({last_report['form']})")
                e2.metric("Est. Next Earnings", f"~{nxt.strftime('%Y-%m-%d')}")
            except ValueError:
                pass
        fil_df = pd.DataFrame([{
            "Form": f["form"], "Filed": f["date"], "Link": f["url"],
        } for f in filings])
        st.dataframe(
            fil_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Link": st.column_config.LinkColumn("EDGAR", display_text="OPEN ↗"),
            },
        )
        st.caption(
            "Next-earnings date is an estimate (last quarterly filing + ~91 days) — "
            "companies announce results before filing the 10-Q/10-K."
        )

    # ── news ──────────────────────────────────────────────────────
    _section_header("NEWS")
    news = _get_news(ticker)
    if news:
        news_df = pd.DataFrame([{
            "Date":     n["date"],
            "Headline": n["title"],
            "Source":   n["source"],
            "Link":     n["url"],
        } for n in news])
        st.dataframe(
            news_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Headline": st.column_config.TextColumn(width="large"),
                "Link":     st.column_config.LinkColumn("Open", display_text="READ ↗"),
            },
        )
        st.caption(
            "Headlines from free public RSS feeds (Yahoo Finance / Google News) — "
            "press coverage, not vetted. Cross-check against the filings feed above: "
            "8-Ks are what the company legally disclosed."
        )
    else:
        st.info("No recent headlines found — news feeds may be unreachable right now.")


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        '<div style="font-family:\'IBM Plex Mono\',monospace;font-size:0.92rem;'
        'font-weight:600;color:#ff6600;letter-spacing:0.1em;text-transform:uppercase;'
        'padding:0.1rem 0 0.4rem;">▶ FINANCIAL TERMINAL</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    with st.form("ticker_form", border=False):
        tickers_input = st.text_input(
            "TICKER(S)",
            value="AAPL",
            placeholder="AAPL MSFT GOOGL",
            help="Space-separated. Available: " + ", ".join(AVAILABLE_TICKERS),
        )
        analyze_btn = st.form_submit_button("ANALYZE", type="primary", use_container_width=True)
    refresh_btn = st.button(
        "🔄 REFRESH LIVE DATA",
        use_container_width=True,
        help="Fetch latest data from live sources. May be slow or rate-limited.",
    )

    st.divider()
    st.caption(
        "PRE-LOADED (INSTANT):\n" + "  ·  ".join(AVAILABLE_TICKERS) +
        "\n\nAny valid ticker works — others are fetched live."
    )

MAX_TICKERS = 5
# Letters with an optional class suffix (BRK.B / BRK-B). No slashes and no way
# to form "..", so nothing can escape data/ in the file loader.
TICKER_RE = re.compile(r'^[A-Z]{1,10}([.-][A-Z]{1,4})?$')
# split on spaces, commas or semicolons — "AAPL, MSFT" must not silently
# drop AAPL (the comma made the token fail validation and killed comparison)
raw_tokens = [t.strip().upper() for t in re.split(r'[,;\s]+', tickers_input) if t.strip()]
tickers = [t for t in raw_tokens if TICKER_RE.match(t)]
dropped = [t for t in raw_tokens if not TICKER_RE.match(t)]
if dropped:
    st.warning("Ignored invalid ticker(s): " + ", ".join(dropped[:5]))
if len(tickers) > MAX_TICKERS:
    st.warning(f"Showing first {MAX_TICKERS} tickers only.")
    tickers = tickers[:MAX_TICKERS]


# ── main ──────────────────────────────────────────────────────────────────────

# Track which tickers are "active" and whether a refresh was requested
if analyze_btn:
    st.session_state['active_tickers'] = tickers
    st.session_state.pop('force_refresh', None)

if refresh_btn:
    # Clear session cache for current tickers so they re-fetch
    for t in st.session_state.get('active_tickers', tickers):
        st.session_state.get('ticker_cache', {}).pop(t, None)
    st.session_state['active_tickers'] = st.session_state.get('active_tickers', tickers)
    st.session_state['force_refresh'] = True

active_tickers = st.session_state.get('active_tickers', [])
force_refresh  = st.session_state.pop('force_refresh', False)

if not active_tickers:
    # ── Terminal welcome screen ────────────────────────────────────────────────
    _render_ticker_tape()
    st.markdown("""
<div style="font-family:'IBM Plex Mono','Courier New',monospace;padding:0.5rem 0 0.8rem;">
  <div style="color:#ff6600;font-size:1.5rem;font-weight:600;letter-spacing:0.06em;
              text-transform:uppercase;line-height:1.2;margin-bottom:0.3rem;">
    ▶ FINANCIAL TERMINAL
  </div>
  <div style="color:#888;font-size:0.76rem;margin-bottom:0.6rem;letter-spacing:0.04em;">
    EQUITY ANALYSIS SYSTEM · LIVE MARKET DATA
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown(
        '<div style="color:#e0e0e0;font-size:0.8rem;font-family:\'IBM Plex Mono\',monospace;">'
        'Enter one or more tickers in the sidebar and click <span style="color:#ff6600;">ANALYZE</span>.<br><br>'
        '<span style="color:#ff6600;">FEATURES:</span> Quarterly financials · TTM aggregates · Margin trends · '
        'YoY growth · Liquidity &amp; leverage · Valuation multiples · Multi-ticker comparison · '
        'Price action &amp; insider intel · Sector-relative performance · News feed<br><br>'
        f'<span style="color:#ff6600;">PRE-LOADED:</span> {", ".join(AVAILABLE_TICKERS)}'
        '</div>',
        unsafe_allow_html=True,
    )

    _render_market_backdrop()

    st.markdown('<div style="color:#888;font-size:0.68rem;font-family:\'IBM Plex Mono\',monospace;margin-top:0.6rem;letter-spacing:0.06em;">PREVIEW — RUN AN ANALYSIS TO POPULATE</div>', unsafe_allow_html=True)

    # ── Decorative ghost preview grid ─────────────────────────────────────────
    _quarters = ["Q1'23", "Q2'23", "Q3'23", "Q4'23", "Q1'24", "Q2'24", "Q3'24", "Q4'24"]
    _ghost_specs = [
        ("REVENUE (4Y)",   "bar",  _C_BLUE,   [82,91,78,117,96,85,103,143]),
        ("NET INCOME",     "bar",  _C_GREEN,  [20,24,19,33,26,22,28,42]),
        ("GROSS MARGIN %", "line", _C_AMBER,  [43,44,42,46,45,44,46,48]),
        ("EPS GROWTH",     "line", _C_PURPLE, [8,11,7,15,10,9,13,18]),
        ("P/E MULTIPLE",   "line", _C_BLUE,   [28,27,30,25,29,31,28,26]),
        ("DEBT / EQUITY",  "line", _C_RED,    [1.8,1.7,1.9,1.6,1.7,1.8,1.5,1.6]),
    ]

    row1_cols = st.columns(3)
    row2_cols = st.columns(3)
    _all_cols  = row1_cols + row2_cols

    for col, (label, kind, color, values) in zip(_all_cols, _ghost_specs):
        fig = go.Figure()
        if kind == "bar":
            fig.add_bar(x=_quarters, y=values, marker_color=color, opacity=0.4)
        else:
            fig.add_scatter(
                x=_quarters, y=values, mode="lines+markers",
                line=dict(color=color, width=2), opacity=0.4,
            )
        fig.update_layout(**_chart_theme(
            title=dict(text=label, font=dict(size=11, color="#666666",
                       family="IBM Plex Mono, 'Courier New', monospace"), x=0, xanchor="left", pad=dict(l=4)),
            height=200,
            margin=dict(t=30, b=4, l=4, r=4),
            xaxis=dict(showticklabels=False, showgrid=False, zeroline=False, linecolor="#1e1e1e"),
            yaxis=dict(showticklabels=False, showgrid=False, zeroline=False, linecolor="#1e1e1e"),
            showlegend=False,
        ))
        with col:
            st.plotly_chart(fig, use_container_width=True)

    st.stop()

# Load all tickers
all_data, all_results = {}, {}
for i, ticker in enumerate(active_tickers):
    if force_refresh and i > 0:
        time.sleep(4)   # space out live requests for multi-ticker
    result_tuple, warning = _get_ticker(ticker, force_refresh=force_refresh)
    if warning:
        if result_tuple:
            st.info(warning)
        else:
            st.error(warning)
            continue
    if result_tuple:
        data, result = result_tuple
        all_data[ticker] = data
        all_results[ticker] = result

if not all_results:
    st.stop()

# Build tab list: one per ticker + Comparison if >1
tab_labels = list(all_results.keys())
if len(tab_labels) > 1:
    tab_labels.append("⚖ COMPARISON")

tabs = st.tabs(tab_labels)


# ── per-ticker tab ────────────────────────────────────────────────────────────

for tab_idx, ticker in enumerate(all_results.keys()):
    with tabs[tab_idx]:
        result   = all_results[ticker]
        company  = result["company"]
        market   = result["market"]
        ttm      = result["ttm"]
        quarters = result["quarters"]
        trends   = result["trends"]

        name     = company.get("name", ticker)
        sector   = company.get("sector", "")
        industry = company.get("industry", "")

        # Escape all API-sourced / user-input strings before HTML interpolation.
        s_ticker   = html.escape(ticker)
        s_name     = html.escape(name)
        s_sector   = html.escape(sector)
        s_industry = html.escape(industry)
        s_currency = html.escape(company.get("currency", "USD"))

        # ── header ────────────────────────────────────────────────
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace;">'
            f'<span style="color:#ff6600;font-size:1.05rem;font-weight:600;'
            f'letter-spacing:0.06em;text-transform:uppercase;">{s_ticker}</span>'
            f'<span style="color:#888;font-size:0.82rem;margin-left:0.6rem;">— {s_name}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if sector:
            st.markdown(
                f'<div style="color:#888;font-size:0.68rem;font-family:\'IBM Plex Mono\',monospace;'
                f'letter-spacing:0.04em;margin-bottom:0.1rem;">'
                f'{s_sector} · {s_industry} · {s_currency}</div>',
                unsafe_allow_html=True,
            )

        # ── market snapshot panel header ───────────────────────────
        st.markdown(
            f'<div style="background:#111;border:1px solid #2a2a2a;border-top:2px solid #ff6600;'
            f'padding:0.2rem 0.6rem;font-family:\'IBM Plex Mono\',monospace;font-size:0.62rem;'
            f'color:#ff6600;letter-spacing:0.1em;text-transform:uppercase;margin:0.3rem 0 0.2rem;">'
            f'MARKET DATA · {s_ticker}</div>',
            unsafe_allow_html=True,
        )

        # ── market snapshot metrics ────────────────────────────────
        if market:
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Market Cap",    _fmt_large(market.get("market_cap")))
            c2.metric("Price",         f"${market.get('price'):.2f}" if market.get("price") is not None else "N/A")
            c3.metric("P/E (TTM)",     _fmt_x(market.get("pe_trailing")))
            c4.metric("P/E (Fwd)",     _fmt_x(market.get("pe_forward")))
            c5.metric("EV/EBITDA",     _fmt_x(market.get("ev_ebitda_info")))
            c6.metric("P/B",           _fmt_x(market.get("pb_ratio")))

        st.divider()

        # ── rating banner ──────────────────────────────────────────────────────
        try:
            _rating = analyzer.compute_rating(result)
            if _rating and _rating.get("rating") != "N/A":
                _r, _score = _rating["rating"], _rating["score"]
                _profile = _rating.get("sector_profile") or "general"
                _profile_html = (
                    f'<span style="color:#666;font-size:0.68rem;margin-left:0.6rem;">'
                    f'VS {html.escape(_profile.replace("_", " ").upper())} THRESHOLDS</span>'
                )
                _caution_html = (
                    '<span style="color:#666;font-size:0.72rem;">'
                    '(LIMITED DATA — TREAT WITH CAUTION)</span>'
                    if _rating.get("data_quality") == "minimal" else ""
                )
                _color_map = {
                    "BUY":  "#00cc44",
                    "HOLD": "#ff6600",
                    "SELL": "#ff3333",
                }
                _accent = _color_map.get(_r, "#888888")
                st.markdown(f"""
<div style="background:#111;border:1px solid #2a2a2a;border-left:3px solid {_accent};
            padding:0.5rem 0.9rem;margin:0.4rem 0 1rem;
            font-family:'IBM Plex Mono',monospace;line-height:1.6;">
  <span style="color:{_accent};font-size:0.88rem;font-weight:600;letter-spacing:0.12em;">{_r}</span>
  <span style="color:#888;font-size:0.78rem;margin:0 0.6rem;">·</span>
  <span style="color:#e0e0e0;font-size:0.78rem;">SCORE:&nbsp;<span style="color:{_accent};font-weight:600;">{_score:.1f}</span>/100</span>
  {_profile_html}
  {_caution_html}
</div>
""", unsafe_allow_html=True)
                _section_header("SCORE BREAKDOWN")
                _rows = [
                    {"Component": v["label"], "Score": f"{v['score']:.1f}",
                     "Max": str(v["max"]), "%": f"{v['score'] / v['max'] * 100:.0f}%"}
                    for v in _rating["breakdown"].values()
                ]
                st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)
                st.caption(_rating["disclaimer"])
        except Exception as _e:
            print(f"RATING ERROR {ticker}: {type(_e).__name__}: {_e}")
            st.warning("Rating unavailable for this ticker.")

        tab_fund, tab_intel = st.tabs(["FUNDAMENTALS", "MARKET INTEL"])

        with tab_intel:
            _render_market_intel(ticker, all_data.get(ticker, {}), result)

        if not quarters:
            with tab_fund:
                st.info("No quarterly data available for this ticker.")
            continue

        with tab_fund:

            # ── build DataFrame ────────────────────────────────────────
            df = pd.DataFrame(quarters)
            df["label"] = df["period"].apply(_qlabel)
            df = df.sort_values("period")   # oldest → newest for charts

            # ── charts row 1: Revenue/NI + Margins ────────────────────
            col_l, col_r = st.columns(2)

            with col_l:
                fig = go.Figure()
                if df["revenue"].notna().any():
                    fig.add_bar(
                        x=df["label"], y=df["revenue"] / 1e9,
                        name="Revenue", marker_color=_C_BLUE,
                    )
                if df["net_income"].notna().any():
                    fig.add_bar(
                        x=df["label"], y=df["net_income"] / 1e9,
                        name="Net Income", marker_color=_C_GREEN,
                    )
                fig.update_layout(**_chart_theme(
                    title="REVENUE & NET INCOME ($B)",
                    barmode="group",
                    height=340,
                    legend=dict(orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)",
                                font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9)),
                    margin=dict(t=36, b=48, l=4, r=4),
                ))
                st.plotly_chart(fig, use_container_width=True)

            with col_r:
                fig = go.Figure()
                for col, name_l, color in [
                    ("gross_margin", "Gross Margin", _C_BLUE),
                    ("op_margin",    "Op. Margin",   _C_AMBER),
                    ("net_margin",   "Net Margin",   _C_GREEN),
                    ("fcf_margin",   "FCF Margin",   _C_PURPLE),
                ]:
                    if col in df.columns and df[col].notna().any():
                        fig.add_scatter(
                            x=df["label"], y=df[col], name=name_l,
                            mode="lines+markers", line=dict(color=color, width=2),
                        )
                fig.update_layout(**_chart_theme(
                    title="MARGIN TRENDS (%)",
                    height=340,
                    legend=dict(orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)",
                                font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9)),
                    margin=dict(t=36, b=48, l=4, r=4),
                ))
                st.plotly_chart(fig, use_container_width=True)

            # ── charts row 2: FCF + Liquidity/Leverage ─────────────────
            col_l2, col_r2 = st.columns(2)

            with col_l2:
                fig = go.Figure()
                if df["free_cash_flow"].notna().any():
                    colors = [
                        _C_GREEN if (v or 0) >= 0 else _C_RED
                        for v in df["free_cash_flow"]
                    ]
                    fig.add_bar(
                        x=df["label"], y=df["free_cash_flow"] / 1e9,
                        name="FCF", marker_color=colors,
                    )
                fig.update_layout(**_chart_theme(
                    title="FREE CASH FLOW ($B)",
                    height=340,
                    margin=dict(t=36, b=48, l=4, r=4),
                ))
                st.plotly_chart(fig, use_container_width=True)

            with col_r2:
                fig = go.Figure()
                for col, name_l, color in [
                    ("current_ratio",   "Current Ratio", _C_BLUE),
                    ("quick_ratio",     "Quick Ratio",   _C_AMBER),
                    ("debt_to_equity",  "D/E Ratio",     _C_RED),
                ]:
                    if col in df.columns and df[col].notna().any():
                        fig.add_scatter(
                            x=df["label"], y=df[col], name=name_l,
                            mode="lines+markers", line=dict(color=color, width=2),
                        )
                fig.update_layout(**_chart_theme(
                    title="LIQUIDITY & LEVERAGE",
                    height=340,
                    legend=dict(orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)",
                                font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9)),
                    margin=dict(t=36, b=48, l=4, r=4),
                ))
                st.plotly_chart(fig, use_container_width=True)

            # ── TTM summary ────────────────────────────────────────────
            st.divider()
            _section_header("TRAILING TWELVE MONTHS (TTM)")
            t1, t2, t3, t4, t5, t6 = st.columns(6)
            t1.metric("Revenue",      _fmt_large(ttm.get("revenue")))
            t2.metric("Net Income",   _fmt_large(ttm.get("net_income")))
            t3.metric("Free CF",      _fmt_large(ttm.get("free_cash_flow")))
            t4.metric("Gross Margin", _fmt_pct(ttm.get("gross_margin")))
            t5.metric("Net Margin",   _fmt_pct(ttm.get("net_margin")))
            t6.metric("ROE",          _fmt_pct(ttm.get("roe")))

            # ── quarterly ratio table ──────────────────────────────────
            st.divider()
            _section_header("QUARTERLY RATIOS")
            display_cols = {
                "label":           "Period",
                "gross_margin":    "Gross Margin %",
                "op_margin":       "Op. Margin %",
                "net_margin":      "Net Margin %",
                "fcf_margin":      "FCF Margin %",
                "roe":             "ROE %",
                "roa":             "ROA %",
                "current_ratio":   "Current Ratio",
                "quick_ratio":     "Quick Ratio",
                "debt_to_equity":  "D/E",
                "interest_coverage": "Int. Coverage",
            }
            # Sort by the real date, not the "Q3'24" label — string-sorting labels
            # groups all Q4s together regardless of year.
            table_df = (
                df.sort_values("period", ascending=False)[list(display_cols.keys())]
                .rename(columns=display_cols)
                .reset_index(drop=True)
            )
            # All-None columns arrive as object dtype and can render as the literal
            # string "None" in st.dataframe; coerce to float so missing values are
            # real NaN and na_rep applies.
            for _c in table_df.columns:
                if _c != "Period":
                    table_df[_c] = pd.to_numeric(table_df[_c], errors="coerce")
            # Hide metrics the company doesn't report at all (e.g. Visa has no
            # cost-of-goods line, so gross margin is undefined, not missing).
            table_df = table_df.drop(columns=[
                c for c in table_df.columns
                if c != "Period" and table_df[c].isna().all()
            ])
            st.dataframe(
                table_df.style.format(
                    {c: "{:.1f}" for c in table_df.columns if c != "Period"},
                    na_rep="N/A",
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(
                "Quarters are labeled by calendar period end. Companies with offset "
                "fiscal years (e.g. AAPL's year ends in September) show fiscal quarters "
                "under the calendar label."
            )

            # ── YoY trends ─────────────────────────────────────────────
            trend_pairs = [(t, q) for t, q in zip(trends, quarters) if t is not None]
            if trend_pairs:
                st.divider()
                _section_header("YOY TRENDS (VS. SAME QUARTER PRIOR YEAR)")
                trend_rows = []
                for t_row, q_row in trend_pairs:
                    r_yoy = t_row.get("rev_yoy_pct")
                    fcf_yoy = t_row.get("fcf_yoy_pct")
                    trend_rows.append({
                        "Quarter":         _qlabel(q_row["period"]),
                        "Revenue YoY":     f"{'+' if (r_yoy or 0)>=0 else ''}{r_yoy:.1f}% {t_row.get('rev_arrow','')}" if r_yoy is not None else "N/A",
                        "Gross Margin Δ":  f"{'+' if (t_row.get('gm_bps') or 0)>=0 else ''}{int(t_row['gm_bps']):,}bps {t_row.get('gm_arrow','')}" if t_row.get("gm_bps") is not None else "N/A",
                        "Op. Margin Δ":    f"{'+' if (t_row.get('op_bps') or 0)>=0 else ''}{int(t_row['op_bps']):,}bps {t_row.get('op_arrow','')}" if t_row.get("op_bps") is not None else "N/A",
                        "Net Margin Δ":    f"{'+' if (t_row.get('ni_bps') or 0)>=0 else ''}{int(t_row['ni_bps']):,}bps {t_row.get('ni_arrow','')}" if t_row.get("ni_bps") is not None else "N/A",
                        "FCF YoY":         f"{'+' if (fcf_yoy or 0)>=0 else ''}{fcf_yoy:.1f}% {t_row.get('fcf_arrow','')}" if fcf_yoy is not None else "N/A",
                    })
                st.dataframe(pd.DataFrame(trend_rows), use_container_width=True, hide_index=True)


# ── comparison tab ────────────────────────────────────────────────────────────

if len(all_results) > 1:
    with tabs[-1]:
        ticker_list = list(all_results.keys())
        s_ticker_list = " · ".join(html.escape(t) for t in ticker_list)
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace;">'
            f'<span style="color:#ff6600;font-size:1.0rem;font-weight:600;'
            f'letter-spacing:0.06em;text-transform:uppercase;">COMPARISON: {s_ticker_list}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div style="color:#888;font-size:0.68rem;font-family:\'IBM Plex Mono\',monospace;margin-bottom:0.4rem;">'
            'TRAILING TWELVE MONTHS (FLOW ITEMS) + LATEST QUARTER (BALANCE SHEET)</div>',
            unsafe_allow_html=True,
        )

        # ── valuation bar charts ───────────────────────────────────
        st.divider()
        _section_header("VALUATION MULTIPLES")

        def _comp_bar(metric_key, title, source="market", fmt_fn=None):
            vals = []
            for t in ticker_list:
                src = all_results[t][source]
                vals.append(src.get(metric_key))
            if all(v is None for v in vals):
                return
            fig = go.Figure(go.Bar(
                x=ticker_list,
                y=[v or 0 for v in vals],
                text=[fmt_fn(v) if fmt_fn else str(v) for v in vals],
                textposition="outside",
                textfont=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#e0e0e0", size=10),
                marker_color=[
                    [_C_BLUE, _C_GREEN, _C_AMBER, _C_PURPLE][i % 4]
                    for i in range(len(ticker_list))
                ],
            ))
            fig.update_layout(**_chart_theme(
                title=title,
                height=300,
                margin=dict(t=36, b=20, l=4, r=4),
            ))
            return fig

        vc1, vc2, vc3 = st.columns(3)
        figs = [
            _comp_bar("pe_trailing",    "P/E (TTM)",    "market", _fmt_x),
            _comp_bar("ev_ebitda_info", "EV/EBITDA",   "market", _fmt_x),
            _comp_bar("pb_ratio",       "P/B",          "market", _fmt_x),
        ]
        for col, fig in zip([vc1, vc2, vc3], figs):
            if fig:
                col.plotly_chart(fig, use_container_width=True)

        # ── margin comparison ──────────────────────────────────────
        _section_header("PROFITABILITY (TTM %)")
        margin_metrics = [
            ("gross_margin",  "Gross Margin"),
            ("op_margin",     "Op. Margin"),
            ("net_margin",    "Net Margin"),
            ("fcf_margin",    "FCF Margin"),
            ("roe",           "ROE"),
            ("roa",           "ROA"),
        ]
        fig = go.Figure()
        colors = [_C_BLUE, _C_GREEN, _C_AMBER, _C_PURPLE]
        for i, t in enumerate(ticker_list):
            ttm = all_results[t]["ttm"]
            fig.add_bar(
                name=t,
                x=[label for _, label in margin_metrics],
                y=[ttm.get(key) for key, _ in margin_metrics],
                marker_color=colors[i % len(colors)],
            )
        fig.update_layout(**_chart_theme(
            barmode="group",
            height=360,
            legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)",
                        font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9)),
            margin=dict(t=36, b=40, l=4, r=4),
        ))
        st.plotly_chart(fig, use_container_width=True)

        # ── scale comparison ───────────────────────────────────────
        _section_header("SCALE (TTM, $B)")
        scale_metrics = [
            ("revenue",       "Revenue"),
            ("net_income",    "Net Income"),
            ("free_cash_flow","FCF"),
            ("ebitda",        "EBITDA"),
        ]
        fig = go.Figure()
        for i, t in enumerate(ticker_list):
            ttm = all_results[t]["ttm"]
            fig.add_bar(
                name=t,
                x=[label for _, label in scale_metrics],
                y=[(ttm.get(key) or 0) / 1e9 for key, _ in scale_metrics],
                marker_color=colors[i % len(colors)],
            )
        fig.update_layout(**_chart_theme(
            barmode="group",
            height=360,
            legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)",
                        font=dict(family="IBM Plex Mono, 'Courier New', monospace", color="#888888", size=9)),
            margin=dict(t=36, b=40, l=4, r=4),
        ))
        st.plotly_chart(fig, use_container_width=True)

        # ── summary table ──────────────────────────────────────────
        _section_header("FULL COMPARISON TABLE")
        rows = []
        sections = [
            ("Market Cap",         lambda t: _fmt_large(all_results[t]["market"].get("market_cap"))),
            ("Price",              lambda t: f"${all_results[t]['market'].get('price'):.2f}" if all_results[t]['market'].get('price') is not None else "N/A"),
            ("P/E (TTM)",          lambda t: _fmt_x(all_results[t]["market"].get("pe_trailing"))),
            ("P/E (Fwd)",          lambda t: _fmt_x(all_results[t]["market"].get("pe_forward"))),
            ("EV/EBITDA (info)",   lambda t: _fmt_x(all_results[t]["market"].get("ev_ebitda_info"))),
            ("EV/EBITDA (calc)",   lambda t: _fmt_x(all_results[t]["ttm"].get("ev_ebitda_calc"))),
            ("P/B",                lambda t: _fmt_x(all_results[t]["market"].get("pb_ratio"))),
            ("Beta",               lambda t: f"{all_results[t]['market'].get('beta'):.2f}" if all_results[t]['market'].get('beta') is not None else "N/A"),
            ("---",                None),
            ("Revenue (TTM)",      lambda t: _fmt_large(all_results[t]["ttm"].get("revenue"))),
            ("Net Income (TTM)",   lambda t: _fmt_large(all_results[t]["ttm"].get("net_income"))),
            ("FCF (TTM)",          lambda t: _fmt_large(all_results[t]["ttm"].get("free_cash_flow"))),
            ("---",                None),
            ("Gross Margin %",     lambda t: _fmt_pct(all_results[t]["ttm"].get("gross_margin"))),
            ("Op. Margin %",       lambda t: _fmt_pct(all_results[t]["ttm"].get("op_margin"))),
            ("Net Margin %",       lambda t: _fmt_pct(all_results[t]["ttm"].get("net_margin"))),
            ("FCF Margin %",       lambda t: _fmt_pct(all_results[t]["ttm"].get("fcf_margin"))),
            ("ROE %",              lambda t: _fmt_pct(all_results[t]["ttm"].get("roe"))),
            ("ROA %",              lambda t: _fmt_pct(all_results[t]["ttm"].get("roa"))),
            ("---",                None),
            ("Current Ratio",      lambda t: f"{all_results[t]['ttm'].get('current_ratio'):.2f}" if all_results[t]['ttm'].get('current_ratio') is not None else "N/A"),
            ("Quick Ratio",        lambda t: f"{all_results[t]['ttm'].get('quick_ratio'):.2f}" if all_results[t]['ttm'].get('quick_ratio') is not None else "N/A"),
            ("D/E Ratio",          lambda t: f"{all_results[t]['ttm'].get('debt_to_equity'):.2f}" if all_results[t]['ttm'].get('debt_to_equity') is not None else "N/A"),
            ("Interest Coverage",  lambda t: _fmt_x(all_results[t]["ttm"].get("interest_coverage"))),
        ]
        for label, fn in sections:
            if label == "---":
                rows.append({"Metric": "—"} | {t: "" for t in ticker_list})
            else:
                rows.append({"Metric": label} | {t: fn(t) for t in ticker_list})

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
