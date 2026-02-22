from __future__ import annotations

import pytest

from stock_screener.constituents import service
from stock_screener.models import Constituent


def test_get_index_constituents_cac40_calls_loader(monkeypatch) -> None:
    expected = [Constituent("MC.PA", "LVMH")]

    def _fake() -> list[Constituent]:
        return expected

    monkeypatch.setattr(service, "get_cac40_constituents", _fake)
    assert service.get_index_constituents("CAC40") == expected


def test_get_index_constituents_cac40_uses_fallback_on_error(monkeypatch) -> None:
    def _boom() -> list[Constituent]:
        raise RuntimeError("boom")

    monkeypatch.setattr(service, "get_cac40_constituents", _boom)
    out = service.get_index_constituents("CAC40")
    assert any(m.ticker == "MC.PA" for m in out)


def test_get_index_constituents_nikkei225_raises_on_error(monkeypatch) -> None:
    def _boom() -> list[Constituent]:
        raise RuntimeError("boom")

    monkeypatch.setattr(service, "get_nikkei225_constituents", _boom)
    with pytest.raises(RuntimeError, match="boom"):
        service.get_index_constituents("NIKKEI225")


def test_get_supported_indices_includes_nikkei225() -> None:
    supported = service.get_supported_indices()
    assert "NIKKEI225" in supported
