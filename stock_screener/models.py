from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional


@dataclass
class ScreenConfig:
    indices: List[str]
    threshold_pct: float = -0.30
    lookback_months: int = 3
    mode: str = "point_to_point"  # or "drawdown"
    as_of_date: Optional[date] = None
    min_history_days: int = 60
    price_basis: str = "raw"  # "raw" or "adjusted"


@dataclass
class Constituent:
    ticker: str
    company: Optional[str] = None


@dataclass
class HitRow:
    run_date: str
    index: str
    ticker: str
    company: Optional[str]
    mode: str
    ref_date: Optional[str]
    ref_close: Optional[float]
    latest_date: Optional[str]
    latest_close: Optional[float]
    pct_change: Optional[float]
    peak_date: Optional[str] = None
    peak_close: Optional[float] = None


@dataclass
class SkipRow:
    run_date: str
    index: str
    ticker: str
    company: Optional[str]
    reason: str
