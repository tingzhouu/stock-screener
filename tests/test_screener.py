from __future__ import annotations

from datetime import date

import pandas as pd

from stock_screener.screener import (
    prepare_price_df,
    screen_ticker_drawdown,
    screen_ticker_point_to_point,
)


def test_prepare_price_df_adjusted_prefers_adj_close() -> None:
    idx = pd.to_datetime(["2026-01-01", "2026-01-02"])
    df = pd.DataFrame({"Close": [100.0, 90.0], "Adj Close": [99.0, 89.0]}, index=idx)

    out = prepare_price_df(df, price_basis="adjusted")

    assert list(out.columns) == ["Close"]
    assert out.iloc[-1]["Close"] == 89.0


def test_screen_ticker_point_to_point_hit() -> None:
    idx = pd.to_datetime(["2025-11-20", "2026-02-20"])
    df = pd.DataFrame({"Close": [100.0, 60.0]}, index=idx)

    hit, status = screen_ticker_point_to_point(
        df=df,
        as_of=date(2026, 2, 20),
        lookback_months=3,
        threshold_pct=-0.30,
        price_basis="raw",
    )

    assert status is None
    assert hit is not None
    assert hit.mode == "point_to_point"
    assert hit.pct_change == -0.4


def test_screen_ticker_drawdown_hit() -> None:
    idx = pd.to_datetime(["2025-12-01", "2026-01-15", "2026-02-20"])
    df = pd.DataFrame({"Close": [100.0, 120.0, 80.0]}, index=idx)

    hit, status = screen_ticker_drawdown(
        df=df,
        as_of=date(2026, 2, 20),
        lookback_months=3,
        threshold_pct=-0.20,
        price_basis="raw",
    )

    assert status is None
    assert hit is not None
    assert hit.mode == "drawdown"
    assert hit.peak_close == 120.0
    assert hit.pct_change == (80.0 / 120.0) - 1.0
