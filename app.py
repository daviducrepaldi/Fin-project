import json
import math
import os
import time
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from src import fetcher, analyzer

AVAILABLE_TICKERS = ['AAPL', 'AMZN', 'GOOGL', 'JPM', 'META', 'MSFT', 'NVDA', 'TSLA']
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

st.set_page_config(
    page_title="Financial Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

def _qlabel(period_str):
    """'2024-09-28' → \"Q3'24\" """
    y = period_str[2:4]
    m = int(period_str[5:7])
    return f"Q{(m-1)//3+1}'{y}"


# ── static data loading ───────────────────────────────────────────────────────

def _load_file(ticker: str):
    """Load pre-fetched data from data/{TICKER}.json. Returns None if not found."""
    path = os.path.join(DATA_DIR, f'{ticker}.json')
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save_file(ticker: str, data: dict):
    """Write data back to data/{TICKER}.json (works locally; silent no-op on Cloud)."""
    def _clean(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):  return [_clean(v) for v in obj]
        return obj
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, f'{ticker}.json'), 'w') as f:
        json.dump(_clean(data), f, indent=2)


def _get_ticker(ticker: str, force_refresh: bool = False):
    """
    Returns ((data, result), warning_msg).
    Priority:
      • session_state cache (skipped if force_refresh)
      • force_refresh=True → live yfinance, falls back to static on failure
      • default → data/{TICKER}.json
      • not found → friendly error listing available tickers
    """
    cache = st.session_state.setdefault('ticker_cache', {})

    if not force_refresh and ticker in cache:
        return cache[ticker], None

    if force_refresh:
        try:
            with st.spinner(f"Fetching live data for {ticker}…"):
                data = fetcher.fetch_and_store(ticker)
            result = analyzer.compute_ratios(data)
            cache[ticker] = (data, result)
            try:
                _save_file(ticker, data)
            except Exception:
                pass   # Cloud filesystem is read-only — that's fine
            return (data, result), None
        except Exception as e:
            data = _load_file(ticker)
            if data:
                result = analyzer.compute_ratios(data)
                cache[ticker] = (data, result)
                return (data, result), f"Live fetch failed — showing static data for **{ticker}**."
            return None, f"Live fetch failed and **{ticker}** has no static data."

    # Default: load from static file
    data = _load_file(ticker)
    if data:
        result = analyzer.compute_ratios(data)
        cache[ticker] = (data, result)
        return (data, result), None

    available = '  ·  '.join(AVAILABLE_TICKERS)
    return None, f"**{ticker}** is not in the local dataset.\n\nAvailable tickers: {available}"


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📊 Financial Analyzer")
    st.caption("Powered by yfinance · Streamlit")
    st.divider()

    tickers_input = st.text_input(
        "Ticker(s)",
        value="AAPL",
        placeholder="AAPL MSFT GOOGL",
        help="Space-separated. Available: " + ", ".join(AVAILABLE_TICKERS),
    )
    analyze_btn = st.button("Analyze", type="primary", use_container_width=True)
    refresh_btn = st.button(
        "🔄 Refresh live data",
        use_container_width=True,
        help="Fetch latest data from yfinance. May be slow or rate-limited.",
    )

    st.divider()
    st.caption(
        "**Pre-loaded tickers:**\n" + "  ·  ".join(AVAILABLE_TICKERS) +
        "\n\nOther tickers require a live fetch via the Refresh button."
    )

tickers = [t.strip().upper() for t in tickers_input.split() if t.strip()]


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
    st.markdown("## 📊 Financial Statement Analyzer")
    st.markdown(
        "Enter one or more tickers in the sidebar and click **Analyze**.\n\n"
        "**Features:** Quarterly financials · TTM aggregates · Margin trends · "
        "YoY growth · Liquidity & leverage · Valuation multiples · Multi-ticker comparison\n\n"
        f"**Pre-loaded:** {', '.join(AVAILABLE_TICKERS)}"
    )
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
    tab_labels.append("⚖️ Comparison")

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
        st.markdown(f"## {ticker} — {name}")
        if sector:
            st.caption(f"{sector}  ·  {industry}  ·  {company.get('currency','USD')}")

        # ── market snapshot metrics ────────────────────────────────
        if market:
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Market Cap",    _fmt_large(market.get("market_cap")))
            c2.metric("Price",         f"${market.get('price'):.2f}" if market.get("price") else "N/A")
            c3.metric("P/E (TTM)",     _fmt_x(market.get("pe_trailing")))
            c4.metric("P/E (Fwd)",     _fmt_x(market.get("pe_forward")))
            c5.metric("EV/EBITDA",     _fmt_x(market.get("ev_ebitda_info")))
            c6.metric("P/B",           _fmt_x(market.get("pb_ratio")))

        st.divider()

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
                    name="Revenue", marker_color="#4C8BE2",
                )
            if df["net_income"].notna().any():
                fig.add_bar(
                    x=df["label"], y=df["net_income"] / 1e9,
                    name="Net Income", marker_color="#34C785",
                )
            fig.update_layout(
                title="Revenue & Net Income ($B)", barmode="group",
                height=360, legend=dict(orientation="h", y=-0.25),
                margin=dict(t=40, b=60),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            fig = go.Figure()
            for col, name_l, color in [
                ("gross_margin", "Gross Margin", "#4C8BE2"),
                ("op_margin",    "Op. Margin",   "#FF9800"),
                ("net_margin",   "Net Margin",   "#34C785"),
                ("fcf_margin",   "FCF Margin",   "#AB47BC"),
            ]:
                if col in df.columns and df[col].notna().any():
                    fig.add_scatter(
                        x=df["label"], y=df[col], name=name_l,
                        mode="lines+markers", line=dict(color=color, width=2),
                    )
            fig.update_layout(
                title="Margin Trends (%)", height=360,
                legend=dict(orientation="h", y=-0.25),
                margin=dict(t=40, b=60),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── charts row 2: FCF + Liquidity/Leverage ─────────────────
        col_l2, col_r2 = st.columns(2)

        with col_l2:
            fig = go.Figure()
            if df["free_cash_flow"].notna().any():
                colors = [
                    "#34C785" if (v or 0) >= 0 else "#EF5350"
                    for v in df["free_cash_flow"]
                ]
                fig.add_bar(
                    x=df["label"], y=df["free_cash_flow"] / 1e9,
                    name="FCF", marker_color=colors,
                )
            fig.update_layout(
                title="Free Cash Flow ($B)", height=360,
                margin=dict(t=40, b=60),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r2:
            fig = go.Figure()
            for col, name_l, color in [
                ("current_ratio",   "Current Ratio", "#4C8BE2"),
                ("quick_ratio",     "Quick Ratio",   "#FF9800"),
                ("debt_to_equity",  "D/E Ratio",     "#EF5350"),
            ]:
                if col in df.columns and df[col].notna().any():
                    fig.add_scatter(
                        x=df["label"], y=df[col], name=name_l,
                        mode="lines+markers", line=dict(color=color, width=2),
                    )
            fig.update_layout(
                title="Liquidity & Leverage", height=360,
                legend=dict(orientation="h", y=-0.25),
                margin=dict(t=40, b=60),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── TTM summary ────────────────────────────────────────────
        st.divider()
        st.markdown("### Trailing Twelve Months (TTM)")
        t1, t2, t3, t4, t5, t6 = st.columns(6)
        t1.metric("Revenue",      _fmt_large(ttm.get("revenue")))
        t2.metric("Net Income",   _fmt_large(ttm.get("net_income")))
        t3.metric("Free CF",      _fmt_large(ttm.get("free_cash_flow")))
        t4.metric("Gross Margin", _fmt_pct(ttm.get("gross_margin")))
        t5.metric("Net Margin",   _fmt_pct(ttm.get("net_margin")))
        t6.metric("ROE",          _fmt_pct(ttm.get("roe")))

        # ── quarterly ratio table ──────────────────────────────────
        st.divider()
        st.markdown("### Quarterly Ratios")
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
            st.markdown("### YoY Trends (vs. same quarter prior year)")
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
        st.markdown(f"## Comparison: {' · '.join(ticker_list)}")
        st.caption("Trailing Twelve Months (flow items) + Latest Quarter (balance sheet)")

        # ── valuation bar charts ───────────────────────────────────
        st.divider()
        st.markdown("### Valuation Multiples")

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
                marker_color=["#4C8BE2", "#34C785", "#FF9800", "#AB47BC"][:len(ticker_list)],
            ))
            fig.update_layout(title=title, height=300, margin=dict(t=40, b=20))
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
        st.markdown("### Profitability (TTM %)")
        margin_metrics = [
            ("gross_margin",  "Gross Margin"),
            ("op_margin",     "Op. Margin"),
            ("net_margin",    "Net Margin"),
            ("fcf_margin",    "FCF Margin"),
            ("roe",           "ROE"),
            ("roa",           "ROA"),
        ]
        fig = go.Figure()
        colors = ["#4C8BE2", "#34C785", "#FF9800", "#AB47BC"]
        for i, t in enumerate(ticker_list):
            ttm = all_results[t]["ttm"]
            fig.add_bar(
                name=t,
                x=[label for _, label in margin_metrics],
                y=[ttm.get(key) for key, _ in margin_metrics],
                marker_color=colors[i % len(colors)],
            )
        fig.update_layout(barmode="group", height=380, legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)

        # ── scale comparison ───────────────────────────────────────
        st.markdown("### Scale (TTM, $B)")
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
        fig.update_layout(barmode="group", height=380, legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)

        # ── summary table ──────────────────────────────────────────
        st.markdown("### Full Comparison Table")
        rows = []
        sections = [
            ("Market Cap",         lambda t: _fmt_large(all_results[t]["market"].get("market_cap"))),
            ("Price",              lambda t: f"${all_results[t]['market'].get('price'):.2f}" if all_results[t]['market'].get('price') else "N/A"),
            ("P/E (TTM)",          lambda t: _fmt_x(all_results[t]["market"].get("pe_trailing"))),
            ("P/E (Fwd)",          lambda t: _fmt_x(all_results[t]["market"].get("pe_forward"))),
            ("EV/EBITDA (info)",   lambda t: _fmt_x(all_results[t]["market"].get("ev_ebitda_info"))),
            ("EV/EBITDA (calc)",   lambda t: _fmt_x(all_results[t]["ttm"].get("ev_ebitda_calc"))),
            ("P/B",                lambda t: _fmt_x(all_results[t]["market"].get("pb_ratio"))),
            ("Beta",               lambda t: f"{all_results[t]['market'].get('beta'):.2f}" if all_results[t]['market'].get('beta') else "N/A"),
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
            ("Current Ratio",      lambda t: f"{all_results[t]['ttm'].get('current_ratio'):.2f}" if all_results[t]['ttm'].get('current_ratio') else "N/A"),
            ("Quick Ratio",        lambda t: f"{all_results[t]['ttm'].get('quick_ratio'):.2f}" if all_results[t]['ttm'].get('quick_ratio') else "N/A"),
            ("D/E Ratio",          lambda t: f"{all_results[t]['ttm'].get('debt_to_equity'):.2f}" if all_results[t]['ttm'].get('debt_to_equity') else "N/A"),
            ("Interest Coverage",  lambda t: _fmt_x(all_results[t]["ttm"].get("interest_coverage"))),
        ]
        for label, fn in sections:
            if label == "---":
                rows.append({"Metric": "—"} | {t: "" for t in ticker_list})
            else:
                rows.append({"Metric": label} | {t: fn(t) for t in ticker_list})

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
