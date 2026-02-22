from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from stock_screener.models import HitRow, ScreenConfig, SkipRow
from stock_screener.providers.base import MarketDataProvider


def to_date(d: Optional[str]) -> Optional[date]:
    if d is None:
        return None
    return datetime.strptime(d, "%Y-%m-%d").date()


def resolve_as_of_date(as_of: Optional[date]) -> date:
    return as_of or date.today()


def compute_window_dates(as_of: date, lookback_months: int) -> Tuple[date, date]:
    end_date = as_of
    ref_target = pd.Timestamp(as_of) - pd.DateOffset(months=lookback_months)
    start_date = (ref_target - pd.DateOffset(days=45)).date()
    return start_date, end_date


def prepare_price_df(df: pd.DataFrame, price_basis: str = "raw") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Close"])

    out = df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index)

    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]

    if price_basis == "adjusted":
        if "Adj Close" in out.columns:
            out = out[["Adj Close"]].rename(columns={"Adj Close": "Close"})
        elif "Close" in out.columns:
            out = out[["Close"]]
        else:
            raise ValueError("Price DataFrame must contain 'Adj Close' or 'Close'.")
    else:
        if "Close" not in out.columns:
            raise ValueError("Price DataFrame must contain 'Close' column.")
        out = out[["Close"]]

    return out.dropna()


def latest_row(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, float]]:
    if df.empty:
        return None
    ts = df.index[-1]
    return ts, float(df.iloc[-1]["Close"])


def prior_or_same_row(df: pd.DataFrame, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    if df.empty:
        return None
    eligible = df[df.index <= target]
    if eligible.empty:
        return None
    ts = eligible.index[-1]
    return ts, float(eligible.iloc[-1]["Close"])


def screen_ticker_point_to_point(
    df: pd.DataFrame,
    as_of: date,
    lookback_months: int,
    threshold_pct: float,
    price_basis: str = "raw",
) -> Tuple[Optional[HitRow], Optional[str]]:
    df = prepare_price_df(df, price_basis=price_basis)
    if len(df) < 2:
        return None, "missing_or_insufficient_data"

    latest = latest_row(df)
    if latest is None:
        return None, "missing_latest_close"
    latest_ts, latest_close = latest

    ref_target = pd.Timestamp(as_of) - pd.DateOffset(months=lookback_months)
    ref = prior_or_same_row(df, ref_target)
    if ref is None:
        return None, "missing_reference_close"
    ref_ts, ref_close = ref

    if ref_close <= 0:
        return None, "invalid_reference_close"

    pct_change = (latest_close / ref_close) - 1.0
    if pct_change <= threshold_pct:
        return (
            HitRow(
                run_date=as_of.isoformat(),
                index="",
                ticker="",
                company=None,
                mode="point_to_point",
                ref_date=ref_ts.date().isoformat(),
                ref_close=ref_close,
                latest_date=latest_ts.date().isoformat(),
                latest_close=latest_close,
                pct_change=pct_change,
            ),
            None,
        )

    return None, "not_hit"


def screen_ticker_drawdown(
    df: pd.DataFrame,
    as_of: date,
    lookback_months: int,
    threshold_pct: float,
    price_basis: str = "raw",
) -> Tuple[Optional[HitRow], Optional[str]]:
    df = prepare_price_df(df, price_basis=price_basis)
    if len(df) < 2:
        return None, "missing_or_insufficient_data"

    window_start = pd.Timestamp(as_of) - pd.DateOffset(months=lookback_months)
    window = df[df.index >= window_start]
    if window.empty:
        return None, "missing_window_data"

    latest = latest_row(window)
    if latest is None:
        return None, "missing_latest_close"
    latest_ts, latest_close = latest

    peak_idx = window["Close"].idxmax()
    peak_close = float(window.loc[peak_idx, "Close"])
    if peak_close <= 0:
        return None, "invalid_peak_close"

    pct_change = (latest_close / peak_close) - 1.0
    if pct_change <= threshold_pct:
        return (
            HitRow(
                run_date=as_of.isoformat(),
                index="",
                ticker="",
                company=None,
                mode="drawdown",
                ref_date=None,
                ref_close=None,
                latest_date=latest_ts.date().isoformat(),
                latest_close=latest_close,
                pct_change=pct_change,
                peak_date=pd.Timestamp(peak_idx).date().isoformat(),
                peak_close=peak_close,
            ),
            None,
        )

    return None, "not_hit"


def screen_index(
    provider: MarketDataProvider,
    index_code: str,
    cfg: ScreenConfig,
) -> Tuple[List[HitRow], List[SkipRow]]:
    as_of = resolve_as_of_date(cfg.as_of_date)
    start_date, end_date = compute_window_dates(as_of, cfg.lookback_months)

    members = provider.get_index_constituents(index_code)
    if not members:
        return [], [SkipRow(as_of.isoformat(), index_code, "", None, "no_constituents")]

    tickers = [m.ticker for m in members]
    company_map = {m.ticker: m.company for m in members}
    price_map: Dict[str, pd.DataFrame] = provider.get_price_history(tickers, start_date, end_date)

    hits: List[HitRow] = []
    skips: List[SkipRow] = []

    for ticker in tickers:
        raw_df = price_map.get(ticker)
        try:
            df = (
                prepare_price_df(raw_df, price_basis=cfg.price_basis)
                if raw_df is not None
                else pd.DataFrame(columns=["Close"])
            )
        except Exception as e:
            skips.append(SkipRow(as_of.isoformat(), index_code, ticker, company_map.get(ticker), f"bad_price_data:{e}"))
            continue

        if len(df) < cfg.min_history_days:
            skips.append(SkipRow(as_of.isoformat(), index_code, ticker, company_map.get(ticker), "insufficient_history"))
            continue

        if cfg.mode == "point_to_point":
            hit, status = screen_ticker_point_to_point(
                df, as_of, cfg.lookback_months, cfg.threshold_pct, price_basis=cfg.price_basis
            )
        elif cfg.mode == "drawdown":
            hit, status = screen_ticker_drawdown(
                df, as_of, cfg.lookback_months, cfg.threshold_pct, price_basis=cfg.price_basis
            )
        else:
            raise ValueError(f"Unsupported mode: {cfg.mode}")

        if hit:
            hit.index = index_code
            hit.ticker = ticker
            hit.company = company_map.get(ticker)
            hits.append(hit)
        elif status and status != "not_hit":
            skips.append(SkipRow(as_of.isoformat(), index_code, ticker, company_map.get(ticker), status))

    return hits, skips
