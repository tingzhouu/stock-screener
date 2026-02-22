from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Dict, List

import pandas as pd

from stock_screener.models import HitRow, ScreenConfig, SkipRow
from stock_screener.screener import resolve_as_of_date


def write_hits_csv(path: str, hits: List[HitRow]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df = pd.DataFrame([asdict(r) for r in hits])
    df.to_csv(path, index=False)


def build_output(cfg: ScreenConfig, hits: List[HitRow], skips: List[SkipRow], include_skips: bool) -> Dict:
    output: Dict = {
        "meta": {
            "run_date": resolve_as_of_date(cfg.as_of_date).isoformat(),
            "indices": cfg.indices,
            "threshold_pct": cfg.threshold_pct,
            "lookback_months": cfg.lookback_months,
            "mode": cfg.mode,
            "price_basis": cfg.price_basis,
            "hit_count": len(hits),
            "skip_count": len(skips),
        },
        "hits": [asdict(r) for r in hits],
    }
    if include_skips:
        output["skips"] = [asdict(r) for r in skips]
    return output


def output_to_json(output: Dict) -> str:
    return json.dumps(output, ensure_ascii=False)
