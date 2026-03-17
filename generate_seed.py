#!/usr/bin/env python3
"""
Run locally to regenerate seed_data.json:
    python3 generate_seed.py
Commits the result so Streamlit Cloud has fallback data on cold starts.
"""
import json
from src import db, fetcher

SEED_TICKERS = ['AAPL', 'MSFT', 'GOOGL']

def _clean(obj):
    """Recursively replace NaN/Inf with None for JSON serialisation."""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj

db.init_db()
seed = {}
for ticker in SEED_TICKERS:
    print(f"Fetching {ticker}…", flush=True)
    try:
        data = fetcher.fetch_and_store(ticker)
        seed[ticker] = _clean(data)
        print(f"  {ticker}: {len(data['income'])} income quarters stored")
    except Exception as e:
        print(f"  {ticker}: FAILED — {e}")

with open('seed_data.json', 'w') as f:
    json.dump(seed, f)

print(f"\nWrote seed_data.json with {list(seed.keys())}")
