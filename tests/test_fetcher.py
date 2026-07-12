"""
tests/test_fetcher.py — Unit tests for src/fetcher.py pure helpers.

Only tests the offline data-shaping logic (no network access).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.fetcher import _fill_missing_q4, _quarterly_duration, _ttm


def _dp(start, end, val, form, filed):
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed}


def _concept(datapoints):
    return {"units": {"USD": datapoints}}


class TestFillMissingQ4:
    def test_q4_derived_from_annual_minus_three_quarters(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-04-01", "2024-06-30", 110, "10-Q", "2024-07-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            # 10-K reports the full year — no standalone Q4 datapoint
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        assert "2024-12-31" not in quarterly
        filled = _fill_missing_q4(quarterly, concept)
        assert filled["2024-12-31"] == 480 - (100 + 110 + 120)

    def test_no_derivation_when_a_quarter_is_missing(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        filled = _fill_missing_q4(quarterly, concept)
        # Only 2 quarters inside the FY window — a "Q4" would silently absorb
        # the missing quarter's value, so nothing must be derived.
        assert "2024-12-31" not in filled

    def test_existing_q4_not_overwritten(self):
        concept = _concept([
            _dp("2024-01-01", "2024-03-31", 100, "10-Q", "2024-04-15"),
            _dp("2024-04-01", "2024-06-30", 110, "10-Q", "2024-07-15"),
            _dp("2024-07-01", "2024-09-30", 120, "10-Q", "2024-10-15"),
            _dp("2024-10-01", "2024-12-31", 140, "10-Q", "2025-01-15"),
            _dp("2024-01-01", "2024-12-31", 480, "10-K", "2025-02-15"),
        ])
        quarterly = _quarterly_duration(concept)
        filled = _fill_missing_q4(quarterly, concept)
        assert filled["2024-12-31"] == 140.0


class TestFetcherTTM:
    def test_sums_four_contiguous_quarters(self):
        by_p = {"2024-03-31": 100, "2024-06-30": 110,
                "2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) == 470

    def test_none_when_quarters_span_more_than_a_year(self):
        by_p = {"2022-12-31": 100, "2023-03-31": 110,
                "2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) is None

    def test_annual_only_filer_uses_latest_fiscal_year(self):
        by_p = {"2022-12-31": 300, "2023-12-31": 350, "2024-12-31": 400}
        assert _ttm(by_p) == 400

    def test_none_when_fewer_than_four_quarters(self):
        by_p = {"2024-09-30": 120, "2024-12-31": 140}
        assert _ttm(by_p) is None


class TestConceptMigrationMerge:
    def test_periods_merged_across_renamed_concepts(self):
        # Visa-style tag migration: old years under "Revenues", recent years
        # under "RevenueFromContractWithCustomerExcludingAssessedTax".
        # First-non-empty selection would return only the stale years.
        from src.fetcher import _first_dur
        tax = {
            "Revenues": _concept([
                _dp("2017-01-01", "2017-03-31", 100, "10-Q", "2017-04-15"),
            ]),
            "RevenueFromContractWithCustomerExcludingAssessedTax": _concept([
                _dp("2025-01-01", "2025-03-31", 900, "10-Q", "2025-04-15"),
            ]),
        }
        merged = _first_dur(tax, "Revenues",
                            "RevenueFromContractWithCustomerExcludingAssessedTax")
        assert merged["2017-03-31"] == 100.0
        assert merged["2025-03-31"] == 900.0

    def test_earlier_name_wins_on_period_conflict(self):
        from src.fetcher import _first_dur
        tax = {
            "Revenues": _concept([
                _dp("2025-01-01", "2025-03-31", 111, "10-Q", "2025-04-15"),
            ]),
            "SalesRevenueNet": _concept([
                _dp("2025-01-01", "2025-03-31", 222, "10-Q", "2025-04-15"),
            ]),
        }
        merged = _first_dur(tax, "Revenues", "SalesRevenueNet")
        assert merged["2025-03-31"] == 111.0

    def test_instant_concepts_merged_too(self):
        from src.fetcher import _first_ins
        old = {"end": "2017-03-31", "val": 10, "form": "10-Q", "filed": "2017-04-15"}
        new = {"end": "2025-03-31", "val": 90, "form": "10-Q", "filed": "2025-04-15"}
        tax = {
            "StockholdersEquity": _concept([old]),
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": _concept([new]),
        }
        merged = _first_ins(tax, "StockholdersEquity",
                            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
        assert merged == {"2017-03-31": 10.0, "2025-03-31": 90.0}


class TestGrossProfitDerivation:
    def test_gross_profit_derived_from_revenue_minus_cogs(self):
        # Amazon-style filer: cost of revenue reported, GrossProfit tag absent
        from src.fetcher import _build_income
        ugaap = {
            "Revenues": _concept([
                _dp("2025-01-01", "2025-03-31", 1000, "10-Q", "2025-04-15"),
            ]),
            "NetIncomeLoss": _concept([
                _dp("2025-01-01", "2025-03-31", 100, "10-Q", "2025-04-15"),
            ]),
            "CostOfGoodsAndServicesSold": _concept([
                _dp("2025-01-01", "2025-03-31", 600, "10-Q", "2025-04-15"),
            ]),
        }
        rows = _build_income(ugaap, {})
        assert rows[0]["gross_profit"] == 400.0

    def test_reported_gross_profit_preferred_over_derivation(self):
        from src.fetcher import _build_income
        ugaap = {
            "Revenues": _concept([
                _dp("2025-01-01", "2025-03-31", 1000, "10-Q", "2025-04-15"),
            ]),
            "NetIncomeLoss": _concept([
                _dp("2025-01-01", "2025-03-31", 100, "10-Q", "2025-04-15"),
            ]),
            "GrossProfit": _concept([
                _dp("2025-01-01", "2025-03-31", 450, "10-Q", "2025-04-15"),
            ]),
            "CostOfGoodsAndServicesSold": _concept([
                _dp("2025-01-01", "2025-03-31", 600, "10-Q", "2025-04-15"),
            ]),
        }
        rows = _build_income(ugaap, {})
        assert rows[0]["gross_profit"] == 450.0

    def test_no_cogs_means_no_gross_profit(self):
        # Visa-style filer: no COGS line at all → gross profit stays None
        from src.fetcher import _build_income
        ugaap = {
            "Revenues": _concept([
                _dp("2025-01-01", "2025-03-31", 1000, "10-Q", "2025-04-15"),
            ]),
            "NetIncomeLoss": _concept([
                _dp("2025-01-01", "2025-03-31", 100, "10-Q", "2025-04-15"),
            ]),
        }
        rows = _build_income(ugaap, {})
        assert rows[0]["gross_profit"] is None


class TestRecentFilings:
    def _recent(self):
        return {
            "form":            ["8-K", "4", "10-Q", "3", "10-K"],
            "filingDate":      ["2026-06-01", "2026-05-20", "2026-05-01", "2026-04-15", "2026-02-01"],
            "accessionNumber": ["0001-26-000005", "0001-26-000004", "0001-26-000003",
                                "0001-26-000002", "0001-26-000001"],
            "primaryDocument": ["ev.htm", "xslF345X06/form4.xml", "q.htm", "own.xml", "k.htm"],
        }

    def test_filters_forms_and_builds_urls(self):
        from src.fetcher import _recent_filings
        rows = _recent_filings("0000001403", self._recent(), {"10-Q", "10-K", "8-K"}, 10)
        assert [r["form"] for r in rows] == ["8-K", "10-Q", "10-K"]
        assert rows[0]["url"] == (
            "https://www.sec.gov/Archives/edgar/data/1403/000126000005/ev.htm")

    def test_limit_respected(self):
        from src.fetcher import _recent_filings
        rows = _recent_filings("0000001403", self._recent(), {"10-Q", "10-K", "8-K"}, 1)
        assert len(rows) == 1


_FORM4_XML = b"""<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>DOE JANE</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <officerTitle></officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-06-01</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>50.5</value></transactionPricePerShare>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-06-02</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>500</value></transactionShares>
        <transactionPricePerShare><value>52</value></transactionPricePerShare>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


class TestInsiderTransactions:
    def test_parses_form4_transactions(self, monkeypatch):
        import src.fetcher as fetcher_mod

        captured_urls = []

        class FakeResp:
            content = _FORM4_XML
            def raise_for_status(self): pass

        def fake_get(url, **kwargs):
            captured_urls.append(url)
            return FakeResp()

        monkeypatch.setattr(fetcher_mod.requests, "get", fake_get)

        recent = {
            "form":            ["4"],
            "filingDate":      ["2026-06-03"],
            "accessionNumber": ["0001-26-000009"],
            "primaryDocument": ["xslF345X06/form4.xml"],
        }
        rows = fetcher_mod._fetch_insider_transactions("0000001403", recent)

        # xsl rendering prefix stripped so the raw XML is fetched
        assert captured_urls[0].endswith("/000126000009/form4.xml")
        assert "xsl" not in captured_urls[0]

        assert len(rows) == 2
        sell, buy = rows[0], rows[1]           # sorted newest first
        assert buy["name"] == "DOE JANE"
        assert buy["role"] == "Director"
        assert buy["action"] == "BUY" and buy["shares"] == 1000.0
        assert buy["value"] == 50500.0
        assert sell["action"] == "SELL" and sell["price"] == 52.0

    def test_fetch_failure_yields_empty_list(self, monkeypatch):
        import src.fetcher as fetcher_mod

        def boom(url, **kwargs):
            raise RuntimeError("network down")

        monkeypatch.setattr(fetcher_mod.requests, "get", boom)
        recent = {
            "form": ["4"], "filingDate": ["2026-06-03"],
            "accessionNumber": ["0001-26-000009"],
            "primaryDocument": ["form4.xml"],
        }
        assert fetcher_mod._fetch_insider_transactions("0000001403", recent) == []


class TestYtdQuarterly:
    def test_ytd_series_differenced_into_quarters(self):
        # Apple-style: cash flow reported cumulatively from fiscal-year start
        from src.fetcher import _ytd_quarterly
        concept = _concept([
            _dp("2024-09-29", "2024-12-28", 100, "10-Q", "2025-01-30"),  # Q1 (3m)
            _dp("2024-09-29", "2025-03-29", 180, "10-Q", "2025-05-01"),  # 6m YTD
            _dp("2024-09-29", "2025-06-28", 250, "10-Q", "2025-07-31"),  # 9m YTD
            _dp("2024-09-29", "2025-09-27", 340, "10-K", "2025-10-30"),  # FY (12m)
        ])
        q = _ytd_quarterly(concept)
        assert q["2024-12-28"] == 100.0          # Q1 as reported
        assert q["2025-03-29"] == 80.0           # 180 - 100
        assert q["2025-06-28"] == 70.0           # 250 - 180
        assert q["2025-09-27"] == 90.0           # 340 - 250

    def test_standalone_quarters_kept_as_is(self):
        from src.fetcher import _ytd_quarterly
        concept = _concept([
            _dp("2025-01-01", "2025-03-31", 55, "10-Q", "2025-04-30"),
        ])
        assert _ytd_quarterly(concept) == {"2025-03-31": 55.0}

    def test_gap_in_ytd_series_stops_differencing(self):
        # 6m YTD missing: FY minus 9m works, but 9m minus Q1 (a 6-month
        # chunk) must NOT be emitted as a "quarter"
        from src.fetcher import _ytd_quarterly
        concept = _concept([
            _dp("2024-09-29", "2024-12-28", 100, "10-Q", "2025-01-30"),
            _dp("2024-09-29", "2025-06-28", 250, "10-Q", "2025-07-31"),
            _dp("2024-09-29", "2025-09-27", 340, "10-K", "2025-10-30"),
        ])
        q = _ytd_quarterly(concept)
        assert q == {"2024-12-28": 100.0, "2025-09-27": 90.0}


class TestUnknownTickerFailsFast:
    def test_retry_does_not_retry_unknown_ticker(self):
        from src.fetcher import _retry, UnknownTickerError
        calls = {"n": 0}

        def fn(ticker):
            calls["n"] += 1
            raise UnknownTickerError(f"{ticker}: not found")

        import pytest
        with pytest.raises(UnknownTickerError):
            _retry(fn, "TABLE", retries=3, delay_base=0)
        assert calls["n"] == 1   # no pointless retries with sleeps

    def test_retry_does_not_retry_no_fundamentals(self):
        from src.fetcher import _retry, NoFundamentalsError
        calls = {"n": 0}

        def fn(ticker):
            calls["n"] += 1
            raise NoFundamentalsError(f"{ticker}: no companyfacts (ETF?)")

        import pytest
        with pytest.raises(NoFundamentalsError):
            _retry(fn, "SPY", retries=3, delay_base=0)
        assert calls["n"] == 1

    def test_transient_errors_still_retry(self):
        from src.fetcher import _retry
        calls = {"n": 0}

        def fn(ticker):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("transient")
            return {"ok": True}

        assert _retry(fn, "AAPL", retries=3, delay_base=0) == {"ok": True}
        assert calls["n"] == 3
