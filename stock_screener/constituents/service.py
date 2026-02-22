from __future__ import annotations

from typing import List

from stock_screener.constituents.cac40_wikipedia import get_cac40_constituents
from stock_screener.constituents.hsi_wikipedia import get_hsi_constituents
from stock_screener.constituents.nikkei225_wikipedia import get_nikkei225_constituents
from stock_screener.constituents.sp500_wikipedia import get_sp500_constituents
from stock_screener.constituents.sti_wikipedia import get_sti_constituents
from stock_screener.models import Constituent

SUPPORTED_INDICES: tuple[str, ...] = ("SP500", "STI", "HSI", "CAC40", "NIKKEI225")


def get_supported_indices() -> List[str]:
    return list(SUPPORTED_INDICES)


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

    if index_code == "CAC40":
        try:
            return get_cac40_constituents()
        except Exception:
            return [
                Constituent("MC.PA", "LVMH"),
                Constituent("OR.PA", "L'Oreal"),
                Constituent("SAN.PA", "Sanofi"),
            ]

    if index_code == "NIKKEI225":
        return get_nikkei225_constituents()

    raise ValueError(f"Unsupported index: {index_code}")
