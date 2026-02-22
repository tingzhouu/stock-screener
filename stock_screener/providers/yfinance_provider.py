from __future__ import annotations

import sys
from datetime import date
from io import StringIO
from typing import Dict, List

import pandas as pd

from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
    def _fetch_html_tables(self, url: str) -> List[pd.DataFrame]:
        import requests

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        return pd.read_html(StringIO(resp.text))

    def _get_sti_constituents(self) -> List[Constituent]:
        import re

        tables = self._fetch_html_tables("https://en.wikipedia.org/wiki/Straits_Times_Index")

        for df in tables:
            lower_cols = [str(c).strip().lower() for c in df.columns]
            has_company = any("company" in c for c in lower_cols)
            has_symbol = any(("symbol" in c) or ("ticker" in c) for c in lower_cols)
            if not (has_company and has_symbol):
                continue

            company_col = next(c for c in df.columns if "company" in str(c).strip().lower())
            symbol_col = next(
                c
                for c in df.columns
                if ("symbol" in str(c).strip().lower()) or ("ticker" in str(c).strip().lower())
            )

            out: List[Constituent] = []
            seen: set[str] = set()

            for _, row in df.iterrows():
                company = str(row[company_col]).strip()
                raw_symbol = str(row[symbol_col]).strip()
                if not company or company.lower() == "nan" or not raw_symbol or raw_symbol.lower() == "nan":
                    continue

                candidates = re.findall(r"[A-Z0-9]{2,6}", raw_symbol.upper())
                candidates = [c for c in candidates if any(ch.isdigit() for ch in c)]
                if not candidates:
                    continue

                sgx_code = candidates[0]
                ticker = f"{sgx_code}.SI"

                if ticker in seen:
                    continue
                seen.add(ticker)
                out.append(Constituent(ticker=ticker, company=company))

            if len(out) >= 20:
                return out

        raise ValueError("Could not parse STI constituents table from source")

    def _get_hsi_constituents(self) -> List[Constituent]:
        tables = self._fetch_html_tables("https://en.wikipedia.org/wiki/Hang_Seng_Index")

        for df in tables:
            lower_cols = [str(c).strip().lower() for c in df.columns]
            has_company = any(("name" in c) or ("company" in c) for c in lower_cols)
            has_symbol = any(("ticker" in c) or ("code" in c) or ("symbol" in c) for c in lower_cols)
            if not (has_company and has_symbol):
                continue

            company_col = next(
                c for c in df.columns if ("name" in str(c).strip().lower()) or ("company" in str(c).strip().lower())
            )
            symbol_col = next(
                c
                for c in df.columns
                if ("ticker" in str(c).strip().lower())
                or ("code" in str(c).strip().lower())
                or ("symbol" in str(c).strip().lower())
            )

            out: List[Constituent] = []
            for _, row in df.iterrows():
                company = str(row[company_col]).strip()
                raw_symbol = str(row[symbol_col]).strip()
                if not company or company.lower() == "nan" or not raw_symbol or raw_symbol.lower() == "nan":
                    continue

                digits = "".join(ch for ch in raw_symbol if ch.isdigit())
                if not digits:
                    continue
                ticker = f"{digits.zfill(4)}.HK"
                out.append(Constituent(ticker=ticker, company=company))

            if len(out) >= 30:
                return out

        raise ValueError("Could not parse HSI constituents table from source")

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        if index_code == "SP500":
            tables = self._fetch_html_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
            df = tables[0]

            out: List[Constituent] = []
            for _, row in df.iterrows():
                ticker = str(row["Symbol"]).strip().replace(".", "-")
                company = str(row["Security"]).strip()
                out.append(Constituent(ticker=ticker, company=company))
            return out

        if index_code == "STI":
            try:
                return self._get_sti_constituents()
            except Exception as e:
                print(f"WARN failed to load STI constituents from source: {e}", file=sys.stderr)
                return [
                    Constituent("D05.SI", "DBS"),
                    Constituent("O39.SI", "OCBC"),
                    Constituent("U11.SI", "UOB"),
                ]

        if index_code == "HSI":
            try:
                return self._get_hsi_constituents()
            except Exception as e:
                print(f"WARN failed to load HSI constituents from source: {e}", file=sys.stderr)
                return [
                    Constituent("0700.HK", "Tencent"),
                    Constituent("9988.HK", "Alibaba"),
                    Constituent("1299.HK", "AIA"),
                ]

        return []

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
