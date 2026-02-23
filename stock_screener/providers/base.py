from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from stock_screener.models import Constituent


class MarketDataProvider:
    """Data provider interface for constituents and historical prices."""

    def get_index_constituents(self, index_code: str) -> List[Constituent]:
        raise NotImplementedError

    def get_price_history(
        self,
        tickers: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, pd.DataFrame]:
        """
        Return mapping ticker -> DataFrame with:
          index: DatetimeIndex (trading dates)
          columns: ['Close']
        """
        raise NotImplementedError

    def get_pe_ratios(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        """Return mapping ticker -> PE ratio when available."""
        return {t: None for t in tickers}
