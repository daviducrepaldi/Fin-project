"""
technicals.py — pure price-series analytics and reverse DCF.

No I/O here: everything operates on the `prices` list produced by
fetcher (`[{date, open, high, low, close, volume}, ...]`, oldest first)
so it is unit-testable offline.
"""

import math

TRADING_DAYS = 252


def _closes(prices: list) -> list:
    return [(p["date"], p["close"]) for p in prices
            if p.get("close") is not None and p.get("date")]


def _pct_return(closes: list, lookback: int):
    """% return from `lookback` trading days ago to the last close."""
    if len(closes) <= lookback:
        return None
    start, end = closes[-1 - lookback][1], closes[-1][1]
    if not start:
        return None
    return round((end / start - 1) * 100, 1)


def compute_technicals(prices: list):
    """
    Compute return/risk stats and moving averages from daily prices.
    Returns None when there is not enough history (< 40 closes).
    """
    closes = _closes(sorted(prices, key=lambda p: p.get("date", "")))
    if len(closes) < 40:
        return None

    values = [c for _, c in closes]
    dates  = [d for d, _ in closes]

    # Daily returns → annualized volatility
    rets = [values[i] / values[i - 1] - 1 for i in range(1, len(values))
            if values[i - 1]]
    ann_vol = None
    if len(rets) >= 20:
        mean = sum(rets) / len(rets)
        var  = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        ann_vol = round(math.sqrt(var) * math.sqrt(TRADING_DAYS) * 100, 1)

    # Max drawdown (peak-to-trough, %)
    peak, max_dd = values[0], 0.0
    for v in values:
        peak = max(peak, v)
        if peak:
            max_dd = min(max_dd, v / peak - 1)
    max_drawdown = round(max_dd * 100, 1)

    def _sma(n):
        """SMA series aligned with `dates`; None until n points exist."""
        out, running = [], 0.0
        for i, v in enumerate(values):
            running += v
            if i >= n:
                running -= values[i - n]
            out.append(round(running / n, 4) if i >= n - 1 else None)
        return out

    sma50, sma200 = _sma(50), _sma(200)
    last = values[-1]

    return {
        "dates":         dates,
        "closes":        values,
        "sma50":         sma50,
        "sma200":        sma200,
        "last_close":    last,
        "return_1m":     _pct_return(closes, 21),
        "return_3m":     _pct_return(closes, 63),
        "return_6m":     _pct_return(closes, 126),
        # a "1 year" Tiingo series is ~251 rows; use the oldest close available
        "return_1y":     _pct_return(closes, min(TRADING_DAYS - 1, len(closes) - 1)),
        "ann_vol_pct":   ann_vol,
        "max_drawdown_pct": max_drawdown,
        "above_sma200":  (last > sma200[-1]) if sma200[-1] else None,
    }


# ── Reverse DCF ───────────────────────────────────────────────────────────────

def _dcf_value(fcf: float, growth: float, discount: float,
               terminal_growth: float, years: int) -> float:
    """PV of `years` of FCF growing at `growth`, plus Gordon terminal value."""
    pv, cash = 0.0, fcf
    for t in range(1, years + 1):
        cash *= (1 + growth)
        pv += cash / (1 + discount) ** t
    terminal = cash * (1 + terminal_growth) / (discount - terminal_growth)
    pv += terminal / (1 + discount) ** years
    return pv


def reverse_dcf(fcf_ttm, market_cap, discount_rate=0.10,
                terminal_growth=0.025, years=10):
    """
    Solve for the FCF growth rate implied by the current market cap:
    the g such that a `years`-year DCF of TTM FCF equals market cap.

    Simplification: compares our FCF (operating CF − capex, a levered-ish
    figure) directly against equity value, ignoring net debt and dilution.
    Good enough to answer "how much growth is priced in?", not a fair-value
    model. Returns None when FCF ≤ 0 or market cap is missing.
    """
    if not fcf_ttm or fcf_ttm <= 0 or not market_cap or market_cap <= 0:
        return None
    if discount_rate <= terminal_growth:
        return None

    lo, hi = -0.50, 0.60
    # Market cap outside the solvable band → clamp to the boundary
    if _dcf_value(fcf_ttm, lo, discount_rate, terminal_growth, years) >= market_cap:
        implied = lo
    elif _dcf_value(fcf_ttm, hi, discount_rate, terminal_growth, years) <= market_cap:
        implied = hi
    else:
        for _ in range(80):
            mid = (lo + hi) / 2
            if _dcf_value(fcf_ttm, mid, discount_rate, terminal_growth, years) < market_cap:
                lo = mid
            else:
                hi = mid
        implied = (lo + hi) / 2

    # Sensitivity: fair value at implied growth ±5pp, across discount rates
    growth_cases   = [implied - 0.05, implied, implied + 0.05]
    discount_cases = [0.08, 0.10, 0.12]
    grid = []
    for g in growth_cases:
        row = {"growth_pct": round(g * 100, 1)}
        for d in discount_cases:
            if d > terminal_growth:
                row[f"dr_{int(d * 100)}"] = _dcf_value(
                    fcf_ttm, g, d, terminal_growth, years)
            else:
                row[f"dr_{int(d * 100)}"] = None
        grid.append(row)

    return {
        "implied_growth_pct": round(implied * 100, 1),
        "clamped":            implied in (-0.50, 0.60),
        "discount_rate_pct":  round(discount_rate * 100, 1),
        "terminal_growth_pct": round(terminal_growth * 100, 1),
        "years":              years,
        "sensitivity":        grid,
    }
