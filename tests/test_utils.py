"""
tests/test_utils.py — Unit tests for src/utils.py
"""

import math
import sys
import os

# Ensure the project root is on the path so `src` is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils import period_to_quarter_label, clean_for_json


# ── period_to_quarter_label ───────────────────────────────────────────────────

class TestPeriodToQuarterLabel:
    def test_q1_march(self):
        # March → Q1
        assert period_to_quarter_label('2024-03-30') == "Q1'24"

    def test_q1_january(self):
        assert period_to_quarter_label('2024-01-15') == "Q1'24"

    def test_q2_june(self):
        assert period_to_quarter_label('2024-06-29') == "Q2'24"

    def test_q2_april(self):
        assert period_to_quarter_label('2024-04-01') == "Q2'24"

    def test_q3_september(self):
        assert period_to_quarter_label('2024-09-28') == "Q3'24"

    def test_q3_july(self):
        assert period_to_quarter_label('2024-07-31') == "Q3'24"

    def test_q4_december(self):
        assert period_to_quarter_label('2023-12-31') == "Q4'23"

    def test_q4_october(self):
        assert period_to_quarter_label('2023-10-01') == "Q4'23"

    def test_two_digit_year_preserved(self):
        # Year digits come from characters [2:4] of the string
        label = period_to_quarter_label('2022-09-24')
        assert label == "Q3'22"

    def test_boundary_month_3(self):
        # Month 3 is still Q1
        assert period_to_quarter_label('2025-03-01') == "Q1'25"

    def test_boundary_month_4(self):
        # Month 4 is Q2
        assert period_to_quarter_label('2025-04-30') == "Q2'25"

    def test_boundary_month_6(self):
        assert period_to_quarter_label('2025-06-30') == "Q2'25"

    def test_boundary_month_7(self):
        assert period_to_quarter_label('2025-07-01') == "Q3'25"

    def test_boundary_month_9(self):
        assert period_to_quarter_label('2025-09-30') == "Q3'25"

    def test_boundary_month_10(self):
        assert period_to_quarter_label('2025-10-01') == "Q4'25"


# ── clean_for_json ────────────────────────────────────────────────────────────

class TestCleanForJson:
    def test_nan_becomes_none(self):
        assert clean_for_json(float('nan')) is None

    def test_inf_becomes_none(self):
        assert clean_for_json(float('inf')) is None

    def test_neg_inf_becomes_none(self):
        assert clean_for_json(float('-inf')) is None

    def test_normal_float_unchanged(self):
        assert clean_for_json(3.14) == 3.14

    def test_zero_float_unchanged(self):
        assert clean_for_json(0.0) == 0.0

    def test_integer_unchanged(self):
        assert clean_for_json(42) == 42

    def test_string_unchanged(self):
        assert clean_for_json("hello") == "hello"

    def test_none_unchanged(self):
        assert clean_for_json(None) is None

    def test_bool_unchanged(self):
        assert clean_for_json(True) is True

    def test_list_with_nan(self):
        result = clean_for_json([1.0, float('nan'), 3.0])
        assert result == [1.0, None, 3.0]

    def test_list_with_inf(self):
        result = clean_for_json([float('inf'), 2.5])
        assert result == [None, 2.5]

    def test_nested_dict_with_nan(self):
        data = {'a': 1.0, 'b': float('nan'), 'c': {'d': float('inf'), 'e': 5.0}}
        result = clean_for_json(data)
        assert result == {'a': 1.0, 'b': None, 'c': {'d': None, 'e': 5.0}}

    def test_dict_with_normal_values(self):
        data = {'x': 1, 'y': 2.5, 'z': 'text'}
        assert clean_for_json(data) == {'x': 1, 'y': 2.5, 'z': 'text'}

    def test_list_of_dicts(self):
        data = [{'v': float('nan')}, {'v': 99.9}]
        result = clean_for_json(data)
        assert result == [{'v': None}, {'v': 99.9}]

    def test_deeply_nested(self):
        data = {'outer': {'inner': [float('nan'), {'deep': float('inf')}]}}
        result = clean_for_json(data)
        assert result == {'outer': {'inner': [None, {'deep': None}]}}

    def test_empty_dict(self):
        assert clean_for_json({}) == {}

    def test_empty_list(self):
        assert clean_for_json([]) == []
