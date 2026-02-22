from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from stock_screener.io import build_output, write_hits_csv
from stock_screener.constituents.service import get_supported_indices
from stock_screener.models import Constituent, HitRow, ScreenConfig, SkipRow
from stock_screener.providers.base import MarketDataProvider
from stock_screener.providers.yfinance_provider import YFinanceProvider
from stock_screener.screener import resolve_as_of_date, screen_index, to_date


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Weekly stock drop screener")
    p.add_argument(
        "--indices",
        nargs="*",
        default=None,
        help="Index codes, e.g. SP500 STI HSI CAC40 (defaults to all supported indices)",
    )
    p.add_argument("--threshold", type=float, default=-0.30, help="Drop threshold as decimal, e.g. -0.30")
    p.add_argument("--lookback-months", type=int, default=3)
    p.add_argument("--mode", choices=["point_to_point", "drawdown"], default="point_to_point")
    p.add_argument("--as-of-date", type=str, default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--min-history-days", type=int, default=60)
    p.add_argument("--include-skips", action="store_true", help="Include skipped symbols in output")
    p.add_argument("--price-basis", choices=["raw", "adjusted"], default="raw")
    p.add_argument("--output-csv", type=str, default=None, help="Optional path to write hits CSV")
    p.add_argument("--print-constituents", action="store_true", help="Print parsed constituents and exit")
    p.add_argument(
        "--print-constituents-limit",
        type=int,
        default=20,
        help="How many constituents to print when using --print-constituents",
    )
    return p.parse_args(argv)


def print_constituents_debug(provider: MarketDataProvider, indices: List[str], limit: int = 20) -> int:
    rows = []
    for idx in indices:
        try:
            members: List[Constituent] = provider.get_index_constituents(idx)
        except Exception as e:
            print(f"[constituents] index={idx} ERROR: {e}", file=sys.stderr)
            continue

        print(f"[constituents] index={idx} count={len(members)}", file=sys.stderr)
        for m in members[: max(0, limit)]:
            rows.append({"index": idx, "ticker": m.ticker, "company": m.company})

    print(json.dumps({"constituents": rows}, ensure_ascii=False))
    return 0


def main(argv: List[str], provider: Optional[MarketDataProvider] = None) -> int:
    args = parse_args(argv)
    indices = args.indices if args.indices else get_supported_indices()
    cfg = ScreenConfig(
        indices=indices,
        threshold_pct=args.threshold,
        lookback_months=args.lookback_months,
        mode=args.mode,
        as_of_date=to_date(args.as_of_date),
        min_history_days=args.min_history_days,
        price_basis=args.price_basis,
    )

    active_provider = provider or YFinanceProvider()
    if args.print_constituents:
        return print_constituents_debug(active_provider, cfg.indices, limit=args.print_constituents_limit)

    all_hits: List[HitRow] = []
    all_skips: List[SkipRow] = []

    for idx in cfg.indices:
        hits, skips = screen_index(active_provider, idx, cfg)
        all_hits.extend(hits)
        all_skips.extend(skips)

    all_hits.sort(key=lambda r: (r.pct_change if r.pct_change is not None else 999), reverse=False)
    if args.output_csv:
        write_hits_csv(args.output_csv, all_hits)

    output = build_output(cfg, all_hits, all_skips, include_skips=args.include_skips)
    print(
        f"[summary] run_date={resolve_as_of_date(cfg.as_of_date).isoformat()} "
        f"indices={','.join(cfg.indices)} mode={cfg.mode} price_basis={cfg.price_basis} "
        f"hits={len(all_hits)} skips={len(all_skips)}",
        file=sys.stderr,
    )
    print(json.dumps(output, ensure_ascii=False))
    return 0
