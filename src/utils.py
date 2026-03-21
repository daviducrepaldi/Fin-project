import math


def period_to_quarter_label(period_str: str) -> str:
    """Convert '2024-09-28' to \"Q3'24\"."""
    year = period_str[2:4]
    month = int(period_str[5:7])
    return f"Q{(month - 1) // 3 + 1}'{year}"


def clean_for_json(obj):
    """Recursively replace NaN/Inf floats with None for JSON serialisation."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj
