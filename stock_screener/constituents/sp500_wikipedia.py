from __future__ import annotations

from typing import List

from stock_screener.constituents.utils import fetch_html_tables
from stock_screener.models import Constituent


def get_sp500_constituents() -> List[Constituent]:
    tables = fetch_html_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    df = tables[0]

    out: List[Constituent] = []
    for _, row in df.iterrows():
        ticker = str(row["Symbol"]).strip().replace(".", "-")
        company = str(row["Security"]).strip()
        out.append(Constituent(ticker=ticker, company=company))
    return out
