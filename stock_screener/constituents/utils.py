from __future__ import annotations

from io import StringIO
from typing import List

import pandas as pd
import requests


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.text


def fetch_html_tables(url: str) -> List[pd.DataFrame]:
    return pd.read_html(StringIO(fetch_html(url)))
