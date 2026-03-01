from __future__ import annotations

from datetime import date
import math
import os
import time
from typing import Dict, List, Optional

import pandas as pd

from stock_screener.constituents.service import get_index_constituents
from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
    @staticmethod
    def _to_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return num if math.isfinite(num) else None

    @classmethod
    def _extract_pe_ratio(cls, ticker_obj: object) -> Optional[float]:
        info = getattr(ticker_obj, "info", None) or {}

        trailing_pe = cls._to_float(info.get("trailingPE"))
        if trailing_pe is not None:
            return trailing_pe

        trailing_eps = cls._to_float(info.get("trailingEps"))
        if trailing_eps not in (None, 0.0):
            for key in ("regularMarketPrice", "currentPrice", "previousClose"):
                market_price = cls._to_float(info.get(key))
                if market_price is not None:
                    return market_price / trailing_eps

            try:
                fast_info = getattr(ticker_obj, "fast_info", None) or {}
            except Exception:
                fast_info = {}
            for key in ("last_price", "regular_market_price", "previous_close"):
                market_price = cls._to_float(fast_info.get(key))
                if market_price is not None:
                    return market_price / trailing_eps

        return cls._to_float(info.get("forwardPE"))

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        return get_index_constituents(index_code)

    def get_price_history(
        self,
        tickers: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, pd.DataFrame]:
        import yfinance as yf

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
            data = None

        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                for t in tickers:
                    try:
                        tdf = data[t].copy()
                        cols = [c for c in ["Close", "Adj Close"] if c in tdf.columns]
                        result[t] = tdf[cols].dropna(how="all") if cols else pd.DataFrame(columns=["Close"])
                    except Exception:
                        result[t] = pd.DataFrame(columns=["Close"])
            elif len(tickers) == 1:
                cols = [c for c in ["Close", "Adj Close"] if c in data.columns]
                if cols:
                    result[tickers[0]] = data[cols].dropna(how="all")

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
                    result[t] = tdf[cols].dropna(how="all") if cols else pd.DataFrame(columns=["Close"])
                else:
                    result[t] = pd.DataFrame(columns=["Close"])
            except Exception:
                result[t] = pd.DataFrame(columns=["Close"])

        return result

    def get_pe_ratios(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        import yfinance as yf

        delay_seconds = self._to_float(os.getenv("YF_PE_CALL_DELAY_SECONDS"))
        if delay_seconds is None or delay_seconds < 0:
            delay_seconds = 0.1

        result: Dict[str, Optional[float]] = {t: None for t in tickers}
        for idx, ticker in enumerate(tickers):
            try:
                result[ticker] = self._extract_pe_ratio(yf.Ticker(ticker))
            except Exception:
                result[ticker] = None
            if delay_seconds > 0 and idx < len(tickers) - 1:
                time.sleep(delay_seconds)
        return result
