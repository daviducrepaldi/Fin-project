#!/usr/bin/env python3
"""
Diagnostic script — run directly to see exactly what yahooquery returns for a ticker.
Usage: python3 debug_fetch.py BAC
"""
import sys
import time
from yahooquery import Ticker

ticker = sys.argv[1] if len(sys.argv) > 1 else "BAC"
print(f"\n=== Testing yahooquery for {ticker} ===\n")

for attempt in range(3):
    print(f"--- Attempt {attempt + 1} ---")
    try:
        t = Ticker(ticker, timeout=15)

        price = t.price.get(ticker)
        print(f"  price type : {type(price).__name__}")
        print(f"  price value: {repr(price)[:300]}")

        detail = t.summary_detail.get(ticker)
        print(f"  detail type: {type(detail).__name__}")
        print(f"  detail val : {repr(detail)[:200]}")

        if isinstance(price, dict) and price.get('regularMarketPrice'):
            print(f"\n  SUCCESS: {price.get('longName')} @ {price['regularMarketPrice']}")
            break
        else:
            print(f"\n  NOT a valid dict response — sleeping 8s before retry")
            time.sleep(8)

    except Exception as e:
        print(f"  EXCEPTION: {type(e).__name__}: {e}")
        time.sleep(8)

print("\n=== Done ===")
