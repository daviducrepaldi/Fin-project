import math


def period_to_quarter_label(period_str: str) -> str:
    """Convert '2024-09-28' to \"Q3'24\"."""
    year = period_str[2:4]
    month = int(period_str[5:7])
    return f"Q{(month - 1) // 3 + 1}'{year}"


def classify_sector(company: dict) -> str:
    """
    Coarse sector bucket for sector-relative rating thresholds.
    Works from the SEC SIC code when present (new EDGAR fetches) and falls
    back to sector/industry text (legacy yfinance-era JSON files).
    Returns one of: financials, real_estate, technology, healthcare,
    energy, utilities, consumer, general.
    """
    sic = company.get('sic')
    if sic:
        try:
            s = int(sic)
        except (ValueError, TypeError):
            s = None
        if s is not None:
            if 6000 <= s <= 6499 or 6700 <= s <= 6799:
                return 'financials'
            if 6500 <= s <= 6599:
                return 'real_estate'
            if 4900 <= s <= 4999:
                return 'utilities'
            if 1300 <= s <= 1399 or 2900 <= s <= 2999:
                return 'energy'
            if 2833 <= s <= 2836 or 3841 <= s <= 3851 or 8000 <= s <= 8099:
                return 'healthcare'
            if 3570 <= s <= 3699 or 7370 <= s <= 7379:
                return 'technology'
            if 5200 <= s <= 5999 or 2000 <= s <= 2199:
                return 'consumer'

    keyword_map = [
        ('financials',  ('bank', 'insurance', 'financial', 'capital market', 'credit', 'asset management', 'broker')),
        ('real_estate', ('reit', 'real estate')),
        ('technology',  ('software', 'semiconductor', 'technology', 'internet', 'computer', 'electronic', 'it services')),
        ('healthcare',  ('pharma', 'biotech', 'health', 'medical', 'drug', 'life sciences')),
        ('energy',      ('oil', 'gas', 'energy', 'petroleum', 'coal', 'drilling')),
        ('utilities',   ('utility', 'utilities', 'electric services', 'power generation', 'water supply')),
        ('consumer',    ('retail', 'consumer', 'food', 'beverage', 'apparel', 'restaurant', 'grocery', 'automobile', 'auto manufact')),
    ]
    # The sector field is authoritative when present ("Consumer Cyclical" beats
    # an "Internet Retail" industry string); industry is only a fallback.
    for text in (str(company.get('sector', '')).lower(),
                 str(company.get('industry', '')).lower()):
        if not text.strip():
            continue
        for bucket, keywords in keyword_map:
            if any(k in text for k in keywords):
                return bucket
    return 'general'


def clean_for_json(obj):
    """Recursively replace NaN/Inf floats with None for JSON serialisation."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj
