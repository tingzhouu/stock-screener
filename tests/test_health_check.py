from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd

from stock_screener.health_check import run_constituent_health_check
from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider


class FakeProvider(MarketDataProvider):
    def __init__(self, members: Dict[str, List[Constituent]]):
        self.members = members

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        return self.members.get(index_code, [])

    def get_price_history(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        return {}


def _mk_members(prefix: str, count: int) -> List[Constituent]:
    return [Constituent(ticker=f"{prefix}{i}", company=f"C{i}") for i in range(count)]


def test_health_check_ok_updates_state(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    provider = FakeProvider(
        {
            "SP500": _mk_members("S", 500),
            "STI": _mk_members("T", 30),
            "HSI": _mk_members("H", 80),
        }
    )
    ok, payload = run_constituent_health_check(
        indices=["SP500", "STI", "HSI"],
        provider=provider,
        state_file=str(state_file),
        max_change_pct=0.5,
    )

    assert ok
    assert payload["ok"] is True
    assert payload["state_updated"] is True
    assert state_file.exists()


def test_health_check_detects_abnormal_change(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"SP500": [f"S{i}" for i in range(500)]}))

    provider = FakeProvider({"SP500": _mk_members("X", 500)})
    ok, payload = run_constituent_health_check(
        indices=["SP500"],
        provider=provider,
        state_file=str(state_file),
        max_change_pct=0.10,
    )

    assert not ok
    assert payload["state_updated"] is False
    assert payload["indices"][0]["ok"] is False
    assert any("abnormal_constituent_change" in err for err in payload["indices"][0]["errors"])
