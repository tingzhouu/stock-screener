from __future__ import annotations

import re
from typing import List

import pandas as pd
from lxml import html as lxml_html

from stock_screener.constituents.utils import fetch_html, fetch_html_tables
from stock_screener.models import Constituent


def _flatten_col_name(col: object) -> str:
    if isinstance(col, tuple):
        parts = [str(x).strip() for x in col if str(x).strip()]
        return " ".join(parts)
    return str(col).strip()


def _pick_company_col(columns: list[object], symbol_col: object | None) -> object | None:
    for c in columns:
        lower = _flatten_col_name(c).lower()
        if ("company" in lower) or ("name" in lower) or ("constituent" in lower):
            return c
    for c in columns:
        if symbol_col is not None and c == symbol_col:
            continue
        lower = _flatten_col_name(c).lower()
        if not any(k in lower for k in ("weight", "sector", "note", "remarks")):
            return c
    return None


def _pick_symbol_col(columns: list[object]) -> object | None:
    for c in columns:
        lower = _flatten_col_name(c).lower()
        if any(k in lower for k in ("ticker", "code", "symbol", "security")):
            return c
    return None


def _to_yahoo_korea_ticker(raw_symbol: str) -> str | None:
    token = str(raw_symbol).strip().upper()
    if not token or token == "NAN":
        return None

    compact = token.replace(" ", "")
    match_with_suffix = re.search(r"(\d{6})\.(KS|KQ)\b", compact)
    if match_with_suffix:
        code = match_with_suffix.group(1)
        suffix = match_with_suffix.group(2)
        return f"{code}.{suffix}"

    match_code = re.search(r"\b(\d{6})\b", token)
    if match_code:
        return f"{match_code.group(1)}.KS"

    return None


def _clean_company_name(raw: str) -> str:
    name = re.sub(r"\[[0-9]+\]", "", str(raw))
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _parse_list_constituents(page_html: str) -> List[Constituent]:
    tree = lxml_html.fromstring(page_html)
    out: List[Constituent] = []
    seen: set[str] = set()

    for li in tree.xpath("//li"):
        text = " ".join(li.xpath(".//text()"))
        if "KRX" not in text.upper():
            continue
        ticker = _to_yahoo_korea_ticker(text)
        if not ticker or ticker in seen:
            continue

        company = ""
        anchors = li.xpath("./a")
        if anchors:
            company = anchors[0].text_content().strip()

        if not company:
            company = text.split("(", 1)[0]
        company = _clean_company_name(company)
        if not company:
            company = ticker

        seen.add(ticker)
        out.append(Constituent(ticker=ticker, company=company))

    return out


def get_kospi200_constituents(min_count: int = 150) -> List[Constituent]:
    url = "https://en.wikipedia.org/wiki/KOSPI_200"
    tables = fetch_html_tables(url)

    out: List[Constituent] = []
    seen: set[str] = set()

    for df in tables:
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue

        symbol_col = _pick_symbol_col(list(df.columns))
        if symbol_col is None:
            continue
        company_col = _pick_company_col(list(df.columns), symbol_col=symbol_col)

        for _, row in df.iterrows():
            ticker = _to_yahoo_korea_ticker(str(row[symbol_col]))
            if not ticker or ticker in seen:
                continue

            company = ""
            if company_col is not None:
                company = str(row[company_col]).strip()
            if not company or company.lower() == "nan":
                company = ticker

            company = _clean_company_name(company)
            seen.add(ticker)
            out.append(Constituent(ticker=ticker, company=company))

    if len(out) >= min_count:
        return out

    list_out = _parse_list_constituents(fetch_html(url))
    if len(list_out) >= min_count:
        return list_out

    raise ValueError("Could not parse KOSPI200 constituents from table/list source")
