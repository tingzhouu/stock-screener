from __future__ import annotations

from typing import List

from stock_screener.constituents.utils import fetch_html_tables
from stock_screener.models import Constituent


def get_hsi_constituents() -> List[Constituent]:
    tables = fetch_html_tables("https://en.wikipedia.org/wiki/Hang_Seng_Index")

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
