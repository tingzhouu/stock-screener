from __future__ import annotations

import json
from datetime import date
from typing import Dict, List

import pandas as pd

from stock_screener.cli import main, parse_args
from stock_screener.constituents.service import get_supported_indices
from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider


class FakeProvider(MarketDataProvider):
    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        return [Constituent("AAA", "Alpha"), Constituent("BBB", "Beta")]

    def get_price_history(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        return {
            "AAA": pd.DataFrame(
                {"Close": [100.0, 60.0]},
                index=pd.to_datetime(["2025-11-20", "2026-02-20"]),
            ),
            "BBB": pd.DataFrame(
                {"Close": [100.0, 95.0]},
                index=pd.to_datetime(["2025-11-20", "2026-02-20"]),
            ),
        }


def test_parse_args_defaults_to_all_supported_indices() -> None:
    args = parse_args([])
    assert args.indices is None


def test_main_defaults_indices_when_omitted(capsys) -> None:
    rc = main(
        [
            "--as-of-date",
            "2026-02-20",
            "--min-history-days",
            "2",
            "--include-skips",
        ],
        provider=FakeProvider(),
    )
    captured = capsys.readouterr()

    assert rc == 0
    payload = json.loads(captured.out.strip())
    assert payload["meta"]["indices"] == get_supported_indices()
    assert payload["meta"]["hit_count"] == len(get_supported_indices())


def test_main_outputs_json(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "--indices",
            "SP500",
            "--as-of-date",
            "2026-02-20",
            "--min-history-days",
            "2",
            "--include-skips",
        ],
        provider=FakeProvider(),
    )
    captured = capsys.readouterr()

    assert rc == 0
    payload = json.loads(captured.out.strip())
    assert "meta" in payload
    assert "hits" in payload
    assert "skips" in payload
    assert payload["meta"]["hit_count"] == 1


def test_print_constituents_mode(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        ["--indices", "SP500", "--print-constituents", "--print-constituents-limit", "1"],
        provider=FakeProvider(),
    )
    captured = capsys.readouterr()

    assert rc == 0
    payload = json.loads(captured.out.strip())
    assert "constituents" in payload
    assert len(payload["constituents"]) == 1
