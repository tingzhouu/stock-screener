from __future__ import annotations

import re
from typing import List

from stock_screener.constituents.utils import fetch_html_tables
from stock_screener.models import Constituent


def get_sti_constituents() -> List[Constituent]:
    tables = fetch_html_tables("https://en.wikipedia.org/wiki/Straits_Times_Index")

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
