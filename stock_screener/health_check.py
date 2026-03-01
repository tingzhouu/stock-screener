from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from stock_screener.constituents.service import get_supported_indices
from stock_screener.models import Constituent
from stock_screener.providers.base import MarketDataProvider
from stock_screener.providers.yfinance_provider import YFinanceProvider

DEFAULT_COUNT_BOUNDS: Dict[str, tuple[int, int]] = {
    "SP500": (450, 550),
    "STI": (20, 40),
    "HSI": (50, 120),
    "CAC40": (35, 45),
    "NIKKEI225": (200, 250),
    "KOSPI200": (170, 230),
}

DEFAULT_SENTINELS: Dict[str, List[str]] = {
    "SP500": ["AAPL", "MSFT"],
    "STI": ["D05.SI", "O39.SI"],
    "HSI": ["0700.HK", "9988.HK"],
    "CAC40": ["MC.PA", "OR.PA"],
    "NIKKEI225": ["7203.T", "6758.T"],
    "KOSPI200": ["005930.KS", "000660.KS"],
}


@dataclass
class IndexCheck:
    index: str
    ok: bool
    count: int
    changed_pct: Optional[float]
    errors: List[str]
    warnings: List[str]


def _load_previous_state(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for idx, value in raw.items():
        if isinstance(idx, str) and isinstance(value, list):
            out[idx] = [str(x) for x in value]
    return out


def _save_state(path: Path, state: Dict[str, List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def _changed_pct(previous: List[str], current: List[str]) -> Optional[float]:
    if not previous:
        return None
    prev_set = set(previous)
    cur_set = set(current)
    if not prev_set:
        return None
    diff = len(prev_set.symmetric_difference(cur_set))
    return diff / len(prev_set)


def _validate_constituents(index: str, members: List[Constituent]) -> tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    if not members:
        errors.append("no_constituents")
        return errors, warnings

    min_count, max_count = DEFAULT_COUNT_BOUNDS.get(index, (1, 1000000))
    if not (min_count <= len(members) <= max_count):
        errors.append(f"count_out_of_range:{len(members)} expected={min_count}-{max_count}")

    tickers = [m.ticker for m in members if m.ticker]
    if len(tickers) != len(members):
        errors.append("missing_ticker")

    required = DEFAULT_SENTINELS.get(index, [])
    missing_required = [t for t in required if t not in set(tickers)]
    if missing_required:
        warnings.append(f"missing_sentinels:{','.join(missing_required)}")

    return errors, warnings


def run_constituent_health_check(
    indices: List[str],
    provider: Optional[MarketDataProvider] = None,
    state_file: str = "outputs/constituents_state.json",
    max_change_pct: float = 0.10,
) -> tuple[bool, Dict]:
    active_provider = provider or YFinanceProvider()
    prev_state = _load_previous_state(Path(state_file))
    next_state: Dict[str, List[str]] = dict(prev_state)

    checks: List[IndexCheck] = []
    overall_ok = True

    for index in indices:
        errors: List[str] = []
        warnings: List[str] = []
        count = 0
        changed_pct: Optional[float] = None

        try:
            members = active_provider.get_index_constituents(index)
            count = len(members)
            e, w = _validate_constituents(index, members)
            errors.extend(e)
            warnings.extend(w)

            current_tickers = sorted({m.ticker for m in members if m.ticker})
            changed_pct = _changed_pct(prev_state.get(index, []), current_tickers)
            if changed_pct is not None and changed_pct > max_change_pct:
                errors.append(f"abnormal_constituent_change:{changed_pct:.4f}>{max_change_pct:.4f}")

            next_state[index] = current_tickers
        except Exception as e:
            errors.append(f"fetch_error:{type(e).__name__}:{e}")

        ok = len(errors) == 0
        if not ok:
            overall_ok = False

        checks.append(
            IndexCheck(
                index=index,
                ok=ok,
                count=count,
                changed_pct=changed_pct,
                errors=errors,
                warnings=warnings,
            )
        )

    if overall_ok:
        _save_state(Path(state_file), next_state)

    payload = {
        "ok": overall_ok,
        "run_date": date.today().isoformat(),
        "state_file": state_file,
        "max_change_pct": max_change_pct,
        "indices": [asdict(c) for c in checks],
        "state_updated": overall_ok,
    }
    return overall_ok, payload


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Constituent fetch health check")
    p.add_argument(
        "--indices",
        nargs="*",
        default=None,
        help="Index codes, e.g. SP500 STI HSI CAC40 NIKKEI225 KOSPI200 (defaults to all supported indices)",
    )
    p.add_argument("--state-file", type=str, default="outputs/constituents_state.json")
    p.add_argument("--max-change-pct", type=float, default=0.10)
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    indices = args.indices if args.indices else get_supported_indices()
    ok, payload = run_constituent_health_check(
        indices=indices,
        state_file=args.state_file,
        max_change_pct=args.max_change_pct,
    )
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if ok else 1
