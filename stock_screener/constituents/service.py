from __future__ import annotations

from typing import List

from stock_screener.constituents.hsi_wikipedia import get_hsi_constituents
from stock_screener.constituents.sp500_wikipedia import get_sp500_constituents
from stock_screener.constituents.sti_wikipedia import get_sti_constituents
from stock_screener.models import Constituent


def get_index_constituents(index_code: str) -> List[Constituent]:
    if index_code == "SP500":
        return get_sp500_constituents()

    if index_code == "STI":
        try:
            return get_sti_constituents()
        except Exception:
            return [
                Constituent("D05.SI", "DBS"),
                Constituent("O39.SI", "OCBC"),
                Constituent("U11.SI", "UOB"),
            ]

    if index_code == "HSI":
        try:
            return get_hsi_constituents()
        except Exception:
            return [
                Constituent("0700.HK", "Tencent"),
                Constituent("9988.HK", "Alibaba"),
                Constituent("1299.HK", "AIA"),
            ]

    return []
