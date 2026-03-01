#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from typing import Any, Optional

import yfinance as yf


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def resolve_pe(info: dict[str, Any], fast_info: dict[str, Any]) -> tuple[Optional[float], str]:
    trailing_pe = to_float(info.get("trailingPE"))
    if trailing_pe is not None:
        return trailing_pe, "info.trailingPE"
    return None, "missing"


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug Yahoo Finance PE extraction for one ticker.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. 4704.T or STLAP.PA")
    parser.add_argument(
        "--show-full",
        action="store_true",
        help="Include full info/fast_info payloads (can be very large).",
    )
    args = parser.parse_args()

    t = yf.Ticker(args.ticker)
    info = t.info or {}
    try:
        fast_info = dict(t.fast_info or {})
    except Exception:
        try:
            fast_info = t.fast_info or {}
        except Exception:
            fast_info = {}

    pe, source = resolve_pe(info, fast_info)

    summary = {
        "ticker": args.ticker,
        "resolved_pe_ratio": pe,
        "source": source,
        "fields": {
            "trailingPE": info.get("trailingPE"),
            "forwardPE": info.get("forwardPE"),
            "trailingEps": info.get("trailingEps"),
            "epsTrailingTwelveMonths": info.get("epsTrailingTwelveMonths"),
            "regularMarketPrice": info.get("regularMarketPrice"),
            "currentPrice": info.get("currentPrice"),
            "previousClose": info.get("previousClose"),
            "fast_lastPrice": fast_info.get("lastPrice"),
            "fast_regularMarketPrice": fast_info.get("regularMarketPrice"),
            "fast_previousClose": fast_info.get("previousClose"),
            "fast_last_price": fast_info.get("last_price"),
            "fast_regular_market_price": fast_info.get("regular_market_price"),
            "fast_previous_close": fast_info.get("previous_close"),
        },
    }

    if args.show_full:
        summary["info"] = info
        summary["fast_info"] = fast_info

    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
