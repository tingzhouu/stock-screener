from __future__ import annotations

import sys
import types

from stock_screener.providers.yfinance_provider import YFinanceProvider


def test_get_pe_ratios_uses_trailing_pe(monkeypatch) -> None:
    class FakeTicker:
        def __init__(self, ticker: str):
            self.info = {"trailingPE": 18.456, "forwardPE": 12.34}

    fake_yf = types.SimpleNamespace(Ticker=FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    provider = YFinanceProvider()
    out = provider.get_pe_ratios(["AAA"])

    assert out["AAA"] == 18.456


def test_get_pe_ratios_does_not_fallback_to_forward_pe(monkeypatch) -> None:
    class FakeTicker:
        def __init__(self, ticker: str):
            self.info = {"forwardPE": 12.34}

    fake_yf = types.SimpleNamespace(Ticker=FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    provider = YFinanceProvider()
    out = provider.get_pe_ratios(["AAA"])

    assert out["AAA"] is None


def test_get_pe_ratios_handles_ticker_errors(monkeypatch) -> None:
    class FakeTicker:
        def __init__(self, ticker: str):
            raise RuntimeError("boom")

    fake_yf = types.SimpleNamespace(Ticker=FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    provider = YFinanceProvider()
    out = provider.get_pe_ratios(["AAA"])

    assert out["AAA"] is None
