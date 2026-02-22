from __future__ import annotations

from datetime import date
from typing import Dict, List

import pandas as pd

from stock_screener.models import Constituent, ScreenConfig
from stock_screener.providers.base import MarketDataProvider
from stock_screener.screener import screen_index


class FakeProvider(MarketDataProvider):
    def __init__(self, members: List[Constituent], prices: Dict[str, pd.DataFrame]):
        self._members = members
        self._prices = prices

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        return self._members

    def get_price_history(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        return {t: self._prices.get(t, pd.DataFrame(columns=["Close"])) for t in tickers}


def test_screen_index_returns_hit_and_skip() -> None:
    members = [
        Constituent("AAA", "Alpha"),
        Constituent("BBB", "Beta"),
    ]
    prices = {
        "AAA": pd.DataFrame(
            {"Close": [100.0, 60.0]},
            index=pd.to_datetime(["2025-11-20", "2026-02-20"]),
        ),
        "BBB": pd.DataFrame({"Close": [10.0]}, index=pd.to_datetime(["2026-02-20"])),
    }
    provider = FakeProvider(members, prices)
    cfg = ScreenConfig(
        indices=["SP500"],
        threshold_pct=-0.30,
        lookback_months=3,
        mode="point_to_point",
        as_of_date=date(2026, 2, 20),
        min_history_days=2,
        price_basis="raw",
    )

    hits, skips = screen_index(provider, "SP500", cfg)

    assert len(hits) == 1
    assert hits[0].ticker == "AAA"
    assert len(skips) == 1
    assert skips[0].ticker == "BBB"
    assert skips[0].reason == "insufficient_history"
