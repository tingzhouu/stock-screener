from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from stock_screener.constituents.service import get_index_constituents
from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
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

        result: Dict[str, Optional[float]] = {t: None for t in tickers}
        for ticker in tickers:
            try:
                info = yf.Ticker(ticker).info or {}
                pe = info.get("trailingPE")
                result[ticker] = float(pe) if pe is not None else None
            except Exception:
                result[ticker] = None
        return result
