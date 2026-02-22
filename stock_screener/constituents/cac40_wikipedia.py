from __future__ import annotations

import re
from typing import List

from stock_screener.constituents.utils import fetch_html_tables
from stock_screener.models import Constituent


def _to_yahoo_paris_ticker(raw_symbol: str) -> str | None:
    token = str(raw_symbol).strip().upper()
    if not token or token == "NAN":
        return None
    token = token.replace(" ", "")
    matches = re.findall(r"[A-Z0-9]+(?:\.[A-Z]{1,4})?", token)
    if not matches:
        return None

    ticker = matches[0]
    if not ticker:
        return None
    if "." in ticker:
        return ticker
    return f"{ticker}.PA"


def get_cac40_constituents() -> List[Constituent]:
    tables = fetch_html_tables("https://en.wikipedia.org/wiki/CAC_40")

    for df in tables:
        lower_cols = [str(c).strip().lower() for c in df.columns]
        has_company = any(("company" in c) or ("name" in c) for c in lower_cols)
        has_symbol = any(("ticker" in c) or ("symbol" in c) or ("epic" in c) for c in lower_cols)
        if not (has_company and has_symbol):
            continue

        company_col = next(
            c for c in df.columns if ("company" in str(c).strip().lower()) or ("name" in str(c).strip().lower())
        )
        symbol_col = next(
            c
            for c in df.columns
            if ("ticker" in str(c).strip().lower())
            or ("symbol" in str(c).strip().lower())
            or ("epic" in str(c).strip().lower())
        )

        out: List[Constituent] = []
        seen: set[str] = set()
        for _, row in df.iterrows():
            company = str(row[company_col]).strip()
            if not company or company.lower() == "nan":
                continue
            ticker = _to_yahoo_paris_ticker(str(row[symbol_col]))
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            out.append(Constituent(ticker=ticker, company=company))

        if len(out) >= 30:
            return out

    raise ValueError("Could not parse CAC40 constituents table from source")
