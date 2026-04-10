import json
import os
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

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from src import db, fetcher, analyzer
from src.utils import period_to_quarter_label, clean_for_json

db.init_db()   # ensure SQLite tables exist (no-op if already created)

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
    padding-top: 0.75rem !important;
    padding-bottom: 0.5rem !important;
    background: var(--bg) !important;
    max-width: 100% !important;
}
* { font-family: var(--font) !important; }

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
[data-testid="stSidebar"] * { font-family: var(--font) !important; }
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div { color: var(--text); }
[data-testid="stSidebarContent"] { padding: 0.8rem 0.8rem; }

/* Text input */
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
[data-testid="stButton"] > button[kind="primary"]:hover { background: #cc5200 !important; }
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
    path = DATA_DIR / f'{ticker}.json'
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save_file(ticker: str, data: dict):
    """Write data back to data/{TICKER}.json (works locally; silent no-op on Cloud)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / f'{ticker}.json', 'w') as f:
        json.dump(clean_for_json(data), f, indent=2)


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
            if ticker in AVAILABLE_TICKERS:
                try:
                    _save_file(ticker, data)
                except Exception:
                    pass   # Cloud filesystem is read-only — that's fine
            return (data, result), None
        except Exception as e:
            print(f"FETCH ERROR [force_refresh] {ticker}: {type(e).__name__}: {e}")
            data = _load_file(ticker)
            if data:
                result = analyzer.compute_ratios(data)
                cache[ticker] = (data, result)
                return (data, result), None   # silent fallback to pre-fetched JSON
            if ticker in AVAILABLE_TICKERS:
                return None, f"Live fetch failed for **{ticker}** and no cached data found. Run `python prefetch_data.py --tickers {ticker}` to rebuild it."
            reason = str(e) if str(e) else type(e).__name__
            return None, (
                f"Live fetch failed for **{ticker}**.\n\n"
                f"**Reason:** {reason}\n\nWait a moment and try again."
            )

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
        return (data, result), None
    except Exception as e:
        print(f"FETCH ERROR [analyze] {ticker}: {type(e).__name__}: {e}")
        available = '  ·  '.join(AVAILABLE_TICKERS)
        reason = str(e) if str(e) else type(e).__name__
        return None, (
            f"Could not fetch **{ticker}**.\n\n"
            f"**Reason:** {reason}\n\n"
            f"Wait a moment and try again.\n\nPre-loaded (instant): {available}"
        )


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        '<div style="font-family:\'IBM Plex Mono\',monospace;font-size:0.92rem;'
        'font-weight:600;color:#ff6600;letter-spacing:0.1em;text-transform:uppercase;'
        'padding:0.1rem 0 0.4rem;">▶ FINANCIAL TERMINAL</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    tickers_input = st.text_input(
        "TICKER(S)",
        value="AAPL",
        placeholder="AAPL MSFT GOOGL",
        help="Space-separated. Available: " + ", ".join(AVAILABLE_TICKERS),
    )
    analyze_btn = st.button("ANALYZE", type="primary", use_container_width=True)
    refresh_btn = st.button(
        "↺ REFRESH LIVE DATA",
        use_container_width=True,
        help="Fetch latest data from yfinance. May be slow or rate-limited.",
    )

    st.divider()
    st.caption(
        "PRE-LOADED (INSTANT):\n" + "  ·  ".join(AVAILABLE_TICKERS) +
        "\n\nAny valid ticker works — others are fetched live."
    )

MAX_TICKERS = 5
tickers = [t.strip().upper() for t in tickers_input.split() if t.strip()]
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
    st.markdown("""
<div style="font-family:'IBM Plex Mono','Courier New',monospace;padding:0.5rem 0 0.8rem;">
  <div style="color:#ff6600;font-size:1.5rem;font-weight:600;letter-spacing:0.06em;
              text-transform:uppercase;line-height:1.2;margin-bottom:0.3rem;">
    ▶ FINANCIAL TERMINAL
  </div>
  <div style="color:#888;font-size:0.76rem;margin-bottom:0.6rem;letter-spacing:0.04em;">
    EQUITY ANALYSIS SYSTEM · POWERED BY SEC EDGAR + TIINGO
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown(
        '<div style="color:#e0e0e0;font-size:0.8rem;font-family:\'IBM Plex Mono\',monospace;">'
        'Enter one or more tickers in the sidebar and click <span style="color:#ff6600;">ANALYZE</span>.<br><br>'
        '<span style="color:#ff6600;">FEATURES:</span> Quarterly financials · TTM aggregates · Margin trends · '
        'YoY growth · Liquidity &amp; leverage · Valuation multiples · Multi-ticker comparison<br><br>'
        f'<span style="color:#ff6600;">PRE-LOADED:</span> {", ".join(AVAILABLE_TICKERS)}'
        '</div>',
        unsafe_allow_html=True,
    )

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

        # ── header ────────────────────────────────────────────────
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace;">'
            f'<span style="color:#ff6600;font-size:1.05rem;font-weight:600;'
            f'letter-spacing:0.06em;text-transform:uppercase;">{ticker}</span>'
            f'<span style="color:#888;font-size:0.82rem;margin-left:0.6rem;">— {name}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if sector:
            st.markdown(
                f'<div style="color:#888;font-size:0.68rem;font-family:\'IBM Plex Mono\',monospace;'
                f'letter-spacing:0.04em;margin-bottom:0.1rem;">'
                f'{sector} · {industry} · {company.get("currency","USD")}</div>',
                unsafe_allow_html=True,
            )

        # ── market snapshot panel header ───────────────────────────
        st.markdown(
            f'<div style="background:#111;border:1px solid #2a2a2a;border-top:2px solid #ff6600;'
            f'padding:0.2rem 0.6rem;font-family:\'IBM Plex Mono\',monospace;font-size:0.62rem;'
            f'color:#ff6600;letter-spacing:0.1em;text-transform:uppercase;margin:0.3rem 0 0.2rem;">'
            f'MARKET DATA · {ticker}</div>',
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
            padding:0.4rem 0.8rem;margin:0.3rem 0 0.5rem;
            font-family:'IBM Plex Mono',monospace;
            display:flex;align-items:center;gap:1.8rem;">
  <span style="color:{_accent};font-size:0.88rem;font-weight:600;
               letter-spacing:0.12em;">{_r}</span>
  <span style="color:#e0e0e0;font-size:0.78rem;">
    SCORE: <span style="color:{_accent};font-weight:600;">{_score:.1f}</span>/100
  </span>
  {_caution_html}
</div>
""", unsafe_allow_html=True)
                with st.expander("SCORE BREAKDOWN"):
                    _rows = [
                        {"Component": v["label"], "Score": f"{v['score']:.1f}",
                         "Max": str(v["max"]), "%": f"{v['score'] / v['max'] * 100:.0f}%"}
                        for v in _rating["breakdown"].values()
                    ]
                    st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)
                    st.caption(_rating["disclaimer"])
        except Exception as _e:
            st.warning(f"Rating error: {_e}")

        if not quarters:
            st.info("No quarterly data available for this ticker.")
            continue

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
        table_df = df[list(display_cols.keys())].rename(columns=display_cols)
        table_df = table_df.sort_values("Period", ascending=False).reset_index(drop=True)
        st.dataframe(
            table_df.style.format(
                {c: "{:.1f}" for c in display_cols.values() if c != "Period"},
                na_rep="N/A",
            ),
            use_container_width=True,
            hide_index=True,
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
        st.markdown(
            f'<div style="font-family:\'IBM Plex Mono\',monospace;">'
            f'<span style="color:#ff6600;font-size:1.0rem;font-weight:600;'
            f'letter-spacing:0.06em;text-transform:uppercase;">COMPARISON: {" · ".join(ticker_list)}</span>'
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
                marker_color=[_C_BLUE, _C_GREEN, _C_AMBER, _C_PURPLE][:len(ticker_list)],
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
