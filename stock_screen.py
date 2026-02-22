#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Optional, Iterable, List, Dict, Tuple
from io import StringIO

import pandas as pd


# -----------------------------
# Models
# -----------------------------

@dataclass
class ScreenConfig:
    indices: List[str]
    threshold_pct: float = -0.30
    lookback_months: int = 3
    mode: str = "point_to_point"  # or "drawdown"
    as_of_date: Optional[date] = None
    min_history_days: int = 60
    price_basis: str = "raw"  # "raw" or "adjusted"


@dataclass
class Constituent:
    ticker: str
    company: Optional[str] = None


@dataclass
class HitRow:
    run_date: str
    index: str
    ticker: str
    company: Optional[str]
    mode: str
    ref_date: Optional[str]
    ref_close: Optional[float]
    latest_date: Optional[str]
    latest_close: Optional[float]
    pct_change: Optional[float]
    peak_date: Optional[str] = None
    peak_close: Optional[float] = None


@dataclass
class SkipRow:
    run_date: str
    index: str
    ticker: str
    company: Optional[str]
    reason: str


# -----------------------------
# Provider interface (plug in later)
# -----------------------------

class MarketDataProvider:
    """
    Implement these methods for your chosen provider (yfinance/EODHD/FMP/etc.)
    """

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        raise NotImplementedError

    def get_price_history(
        self,
        tickers: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, pd.DataFrame]:
        """
        Return mapping ticker -> DataFrame with:
          index: DatetimeIndex (trading dates)
          columns: ['Close']
        """
        raise NotImplementedError


# -----------------------------
# Example stub provider (replace later)
# -----------------------------

class StubProvider(MarketDataProvider):
    """
    For development/testing only.
    Hardcoded sample tickers and no price data implementation.
    """

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        samples = {
            "SP500": [Constituent("AAPL", "Apple"), Constituent("TSLA", "Tesla")],
            "STI": [Constituent("D05.SI", "DBS"), Constituent("O39.SI", "OCBC")],
            "HSI": [Constituent("0700.HK", "Tencent"), Constituent("9988.HK", "Alibaba")],
        }
        return samples.get(index_code, [])

    def get_price_history(
        self,
        tickers: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, pd.DataFrame]:
        # Replace with real provider logic.
        # Returning empty frames forces "missing_data" skips but validates pipeline behavior.
        return {t: pd.DataFrame(columns=["Close"]) for t in tickers}

# -----------------------------
# Yahoo Finance (YFinance) provider
# -----------------------------

class YFinanceProvider(MarketDataProvider):
    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        if index_code == "SP500":
            import requests

            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            }

            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()

            tables = pd.read_html(StringIO(resp.text))
            df = tables[0]

            out: List[Constituent] = []
            for _, row in df.iterrows():
                ticker = str(row["Symbol"]).strip().replace(".", "-")  # BRK.B -> BRK-B for yfinance
                company = str(row["Security"]).strip()
                out.append(Constituent(ticker=ticker, company=company))
            return out
        
        # Temporary hardcoded subsets for proof-of-logic.
        # We'll replace SP500 with full constituents next.
        samples = {
            # "SP500": [
            #     Constituent("TTD", "The Trade Desk"),
            #     Constituent("AAPL", "Apple"),
            #     Constituent("TSLA", "Tesla"),
            #     Constituent("NVDA", "NVIDIA"),
            #     Constituent("INTC", "Intel"),
            # ],
            "STI": [
                Constituent("D05.SI", "DBS"),
                Constituent("O39.SI", "OCBC"),
                Constituent("U11.SI", "UOB"),
            ],
            "HSI": [
                Constituent("0700.HK", "Tencent"),
                Constituent("9988.HK", "Alibaba"),
                Constituent("1299.HK", "AIA"),
            ],
        }
        return samples.get(index_code, [])

    def get_price_history(
        self,
        tickers: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, pd.DataFrame]:
        import yfinance as yf

        # yfinance end date is exclusive-ish in practice; add a day buffer
        dl_end = pd.Timestamp(end_date) + pd.Timedelta(days=1)

        result: Dict[str, pd.DataFrame] = {}

        try:
            data = yf.download(
                tickers=tickers,
                start=start_date.isoformat(),
                end=dl_end.date().isoformat(),
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
            )
        except Exception:
            # If bulk download fails, fall back to per-ticker
            data = None

        if data is not None and not data.empty:
            # Multi-ticker format usually has MultiIndex columns: (ticker, field)
            if isinstance(data.columns, pd.MultiIndex):
                for t in tickers:
                    try:
                        tdf = data[t].copy()
                        cols = [c for c in ["Close", "Adj Close"] if c in tdf.columns]
                        if cols:
                            result[t] = tdf[cols].dropna(how="all")
                        else:
                            result[t] = pd.DataFrame(columns=["Close"])
                    except Exception:
                        result[t] = pd.DataFrame(columns=["Close"])
            else:
                # Single ticker shape
                if len(tickers) == 1:
                    cols = [c for c in ["Close", "Adj Close"] if c in data.columns]
                    if cols:
                        result[tickers[0]] = data[cols].dropna(how="all")

        # Fill missing tickers with per-ticker fallback
        missing = [t for t in tickers if t not in result]
        for t in missing:
            try:
                tdf = yf.download(
                    t,
                    start=start_date.isoformat(),
                    end=dl_end.date().isoformat(),
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )
                if not tdf.empty:
                    cols = [c for c in ["Close", "Adj Close"] if c in tdf.columns]
                    if cols:
                        result[t] = tdf[cols].dropna(how="all")
                    else:
                        result[t] = pd.DataFrame(columns=["Close"])
                else:
                    result[t] = pd.DataFrame(columns=["Close"])
            except Exception:
                result[t] = pd.DataFrame(columns=["Close"])

        return result

# -----------------------------
# Date / price helpers
# -----------------------------

def to_date(d: Optional[str]) -> Optional[date]:
    if d is None:
        return None
    return datetime.strptime(d, "%Y-%m-%d").date()


def resolve_as_of_date(as_of: Optional[date]) -> date:
    # We use "today" if not provided. In production, you may prefer "last completed market day".
    return as_of or date.today()


def compute_window_dates(as_of: date, lookback_months: int) -> Tuple[date, date]:
    # Fetch extra buffer so we can locate a prior trading day around the reference date.
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
        # Prefer Adj Close if available; otherwise fall back to Close
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

    out = out.dropna()
    return out


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


# -----------------------------
# Core screening logic
# -----------------------------

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
        return HitRow(
            run_date=as_of.isoformat(),
            index="",  # filled by caller
            ticker="",
            company=None,
            mode="point_to_point",
            ref_date=ref_ts.date().isoformat(),
            ref_close=ref_close,
            latest_date=latest_ts.date().isoformat(),
            latest_close=latest_close,
            pct_change=pct_change,
        ), None

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
        return HitRow(
            run_date=as_of.isoformat(),
            index="",  # filled by caller
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
        ), None

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
    price_map = provider.get_price_history(tickers, start_date, end_date)

    hits: List[HitRow] = []
    skips: List[SkipRow] = []

    for ticker in tickers:
        raw_df = price_map.get(ticker)
        try:
            df = prepare_price_df(raw_df, price_basis=cfg.price_basis) if raw_df is not None else pd.DataFrame(columns=["Close"])
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
        else:
            # Keep only actionable skips; omit not_hit to reduce noise
            if status and status != "not_hit":
                skips.append(SkipRow(as_of.isoformat(), index_code, ticker, company_map.get(ticker), status))

    return hits, skips


def write_hits_csv(path: str, hits: List[HitRow]) -> None:
    import os
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df = pd.DataFrame([asdict(r) for r in hits])
    df.to_csv(path, index=False)

# -----------------------------
# CLI / main
# -----------------------------

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Weekly stock drop screener")
    p.add_argument("--indices", nargs="+", required=True, help="Index codes, e.g. SP500 STI HSI")
    p.add_argument("--threshold", type=float, default=-0.30, help="Drop threshold as decimal, e.g. -0.30")
    p.add_argument("--lookback-months", type=int, default=3)
    p.add_argument("--mode", choices=["point_to_point", "drawdown"], default="point_to_point")
    p.add_argument("--as-of-date", type=str, default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--min-history-days", type=int, default=60)
    p.add_argument("--include-skips", action="store_true", help="Include skipped symbols in output")
    p.add_argument("--price-basis", choices=["raw", "adjusted"], default="raw")
    p.add_argument("--output-csv", type=str, default=None, help="Optional path to write hits CSV")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    cfg = ScreenConfig(
        indices=args.indices,
        threshold_pct=args.threshold,
        lookback_months=args.lookback_months,
        mode=args.mode,
        as_of_date=to_date(args.as_of_date),
        min_history_days=args.min_history_days,
        price_basis=args.price_basis,
    )

    # Replace StubProvider with your real provider implementation
    provider: MarketDataProvider = YFinanceProvider()

    all_hits: List[HitRow] = []
    all_skips: List[SkipRow] = []

    for idx in cfg.indices:
        hits, skips = screen_index(provider, idx, cfg)
        all_hits.extend(hits)
        all_skips.extend(skips)

    # Sort biggest drops first
    all_hits.sort(key=lambda r: (r.pct_change if r.pct_change is not None else 999), reverse=False)
    if args.output_csv:
        write_hits_csv(args.output_csv, all_hits)

    output = {
        "meta": {
            "run_date": resolve_as_of_date(cfg.as_of_date).isoformat(),
            "indices": cfg.indices,
            "threshold_pct": cfg.threshold_pct,
            "lookback_months": cfg.lookback_months,
            "mode": cfg.mode,
            "price_basis": cfg.price_basis,
            "hit_count": len(all_hits),
            "skip_count": len(all_skips),
        },
        "hits": [asdict(r) for r in all_hits],
    }

    if args.include_skips:
        output["skips"] = [asdict(r) for r in all_skips]

    print(
        f"[summary] run_date={resolve_as_of_date(cfg.as_of_date).isoformat()} "
        f"indices={','.join(cfg.indices)} mode={cfg.mode} price_basis={cfg.price_basis} "
        f"hits={len(all_hits)} skips={len(all_skips)}",
        file=sys.stderr,
    )
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))