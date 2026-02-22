from __future__ import annotations

import pandas as pd

from stock_screener.constituents import nikkei225_wikipedia
from stock_screener.constituents.nikkei225_wikipedia import _to_yahoo_tokyo_ticker


def test_to_yahoo_tokyo_ticker_adds_tokyo_suffix_when_missing() -> None:
    assert _to_yahoo_tokyo_ticker("7203") == "7203.T"
    assert _to_yahoo_tokyo_ticker("TYO:6758") == "6758.T"


def test_to_yahoo_tokyo_ticker_keeps_tokyo_suffix() -> None:
    assert _to_yahoo_tokyo_ticker("7203.T") == "7203.T"
    assert _to_yahoo_tokyo_ticker(" 6758.t ") == "6758.T"


def test_to_yahoo_tokyo_ticker_handles_invalid_values() -> None:
    assert _to_yahoo_tokyo_ticker("") is None
    assert _to_yahoo_tokyo_ticker("nan") is None
    assert _to_yahoo_tokyo_ticker("Toyota") is None


def test_get_nikkei225_constituents_parses_multiindex_columns(monkeypatch) -> None:
    cols = pd.MultiIndex.from_tuples(
        [
            ("Constituents", "Issue name"),
            ("Constituents", "Code"),
        ]
    )
    df = pd.DataFrame(
        [
            ["Toyota Motor", "7203"],
            ["Sony Group", "6758"],
        ],
        columns=cols,
    )

    monkeypatch.setattr(nikkei225_wikipedia, "fetch_html_tables", lambda _: [df])
    out = nikkei225_wikipedia.get_nikkei225_constituents(min_count=1)
    assert [m.ticker for m in out] == ["7203.T", "6758.T"]


def test_get_nikkei225_constituents_aggregates_multiple_tables(monkeypatch) -> None:
    df1 = pd.DataFrame({"Name": ["A", "B"], "Code": ["1000", "1001"]})
    df2 = pd.DataFrame({"Company": ["C", "D"], "Ticker symbol": ["1002", "1003"]})

    monkeypatch.setattr(nikkei225_wikipedia, "fetch_html_tables", lambda _: [df1, df2])
    out = nikkei225_wikipedia.get_nikkei225_constituents(min_count=4)
    assert [m.ticker for m in out] == ["1000.T", "1001.T", "1002.T", "1003.T"]


def test_get_nikkei225_constituents_falls_back_to_section_list(monkeypatch) -> None:
    html = """
    <html><body>
      <h3 id="Banking">Banking</h3>
      <ul>
        <li><a href="/wiki/Aozora_Bank">Aozora Bank</a>, Ltd. (TYO: <a>8304</a>)</li>
        <li><a href="/wiki/Chiba_Bank">The Chiba Bank</a>, Ltd. (TYO: <a>8331</a>)</li>
      </ul>
    </body></html>
    """
    monkeypatch.setattr(nikkei225_wikipedia, "fetch_html_tables", lambda _: [])
    monkeypatch.setattr(nikkei225_wikipedia, "fetch_html", lambda _: html)
    out = nikkei225_wikipedia.get_nikkei225_constituents(min_count=2)
    assert [m.ticker for m in out] == ["8304.T", "8331.T"]
    assert [m.company for m in out] == ["Aozora Bank", "The Chiba Bank"]
