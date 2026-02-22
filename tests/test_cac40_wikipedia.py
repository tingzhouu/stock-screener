from __future__ import annotations

from stock_screener.constituents.cac40_wikipedia import _to_yahoo_paris_ticker


def test_to_yahoo_paris_ticker_adds_paris_suffix_when_missing() -> None:
    assert _to_yahoo_paris_ticker("MC") == "MC.PA"


def test_to_yahoo_paris_ticker_keeps_existing_exchange_suffix() -> None:
    assert _to_yahoo_paris_ticker("MT.AS") == "MT.AS"
    assert _to_yahoo_paris_ticker("$MT.AS") == "MT.AS"


def test_to_yahoo_paris_ticker_keeps_paris_suffix() -> None:
    assert _to_yahoo_paris_ticker("OR.PA") == "OR.PA"


def test_to_yahoo_paris_ticker_handles_empty_nan_values() -> None:
    assert _to_yahoo_paris_ticker("") is None
    assert _to_yahoo_paris_ticker("nan") is None
