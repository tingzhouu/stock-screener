from __future__ import annotations

import pandas as pd

from stock_screener.constituents import kospi200_wikipedia
from stock_screener.constituents.kospi200_wikipedia import _to_yahoo_korea_ticker


def test_to_yahoo_korea_ticker_adds_korea_suffix_when_missing() -> None:
    assert _to_yahoo_korea_ticker("005930") == "005930.KS"
    assert _to_yahoo_korea_ticker("KRX:000660") == "000660.KS"


def test_to_yahoo_korea_ticker_keeps_exchange_suffix() -> None:
    assert _to_yahoo_korea_ticker("005930.KS") == "005930.KS"
    assert _to_yahoo_korea_ticker(" 035420.kq ") == "035420.KQ"


def test_to_yahoo_korea_ticker_handles_invalid_values() -> None:
    assert _to_yahoo_korea_ticker("") is None
    assert _to_yahoo_korea_ticker("nan") is None
    assert _to_yahoo_korea_ticker("Samsung") is None


def test_get_kospi200_constituents_parses_multiindex_columns(monkeypatch) -> None:
    cols = pd.MultiIndex.from_tuples(
        [
            ("Constituents", "Company"),
            ("Constituents", "Code"),
        ]
    )
    df = pd.DataFrame(
        [
            ["Samsung Electronics", "005930"],
            ["SK hynix", "000660"],
        ],
        columns=cols,
    )

    monkeypatch.setattr(kospi200_wikipedia, "fetch_html_tables", lambda _: [df])
    out = kospi200_wikipedia.get_kospi200_constituents(min_count=1)
    assert [m.ticker for m in out] == ["005930.KS", "000660.KS"]


def test_get_kospi200_constituents_aggregates_multiple_tables(monkeypatch) -> None:
    df1 = pd.DataFrame({"Company": ["A", "B"], "Code": ["100000", "100001"]})
    df2 = pd.DataFrame({"Name": ["C", "D"], "Ticker symbol": ["100002", "100003"]})

    monkeypatch.setattr(kospi200_wikipedia, "fetch_html_tables", lambda _: [df1, df2])
    out = kospi200_wikipedia.get_kospi200_constituents(min_count=4)
    assert [m.ticker for m in out] == ["100000.KS", "100001.KS", "100002.KS", "100003.KS"]


def test_get_kospi200_constituents_falls_back_to_section_list(monkeypatch) -> None:
    html = """
    <html><body>
      <ul>
        <li><a href="/wiki/Samsung_Electronics">Samsung Electronics</a> (KRX: <a>005930</a>)</li>
        <li><a href="/wiki/SK_Hynix">SK hynix</a> (KRX: <a>000660</a>)</li>
      </ul>
    </body></html>
    """
    monkeypatch.setattr(kospi200_wikipedia, "fetch_html_tables", lambda _: [])
    monkeypatch.setattr(kospi200_wikipedia, "fetch_html", lambda _: html)
    out = kospi200_wikipedia.get_kospi200_constituents(min_count=2)
    assert [m.ticker for m in out] == ["005930.KS", "000660.KS"]
    assert [m.company for m in out] == ["Samsung Electronics", "SK hynix"]
