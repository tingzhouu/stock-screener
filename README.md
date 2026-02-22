# Stock Screener

This repository contains a CLI stock screener that finds stocks with large declines over a configurable lookback window.

It currently screens index constituents for:
- `SP500`
- `STI`
- `HSI`

The script pulls:
- index constituents from Wikipedia tables
- historical prices from Yahoo Finance (via `yfinance`)

It outputs:
- JSON to stdout (always)
- optional CSV file of hit results

## What This Repository Does

`stock_screen.py` runs a rules-based screen over index members and returns stocks that pass your drop threshold.

Two screening modes are supported:
- `point_to_point`: compare latest close vs close around `N` months ago
- `drawdown`: compare latest close vs peak close within last `N` months

A stock is a hit when:
- `pct_change <= threshold`
- example: threshold `-0.30` means "down 30% or more"

## Project Structure

- `/Users/tingzhou/Desktop/n8n-bot/stock_screener/stock_screen.py`: main CLI and screening logic
- `/Users/tingzhou/Desktop/n8n-bot/stock_screener/requirements.txt`: Python dependencies
- `/Users/tingzhou/Desktop/n8n-bot/stock_screener/outputs/`: example output CSV files

## Setup

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run tests:

```bash
pytest
```

## Usage

Basic form:

```bash
python3 stock_screen.py --indices <INDEX...> [options]
```

Show help:

```bash
python3 stock_screen.py --help
```

## Commands and Options

Required:
- `--indices INDICES [INDICES ...]`
  - Index codes to screen (for example: `SP500 STI HSI`)

Optional:
- `--threshold <float>`
  - Drop threshold as decimal (default: `-0.30`)
  - Example: `-0.10` means down 10% or more
- `--lookback-months <int>`
  - Lookback window in months (default: `3`)
- `--mode {point_to_point,drawdown}`
  - Screening mode (default: `point_to_point`)
- `--as-of-date YYYY-MM-DD`
  - Evaluate using this date instead of today
- `--min-history-days <int>`
  - Minimum historical rows required per ticker (default: `60`)
- `--price-basis {raw,adjusted}`
  - Use raw close or adjusted close (default: `raw`)
- `--include-skips`
  - Include skipped symbols and reasons in JSON output
- `--output-csv <path>`
  - Write hits to CSV at the provided path
- `--print-constituents`
  - Print parsed index constituents and exit (no screening run)
- `--print-constituents-limit <int>`
  - Max rows to print when using `--print-constituents` (default: `20`)

## Example Commands

Screen S&P 500 with defaults:

```bash
python3 stock_screen.py --indices SP500
```

Run drawdown mode for all supported indices and save CSV:

```bash
python3 stock_screen.py \
  --indices SP500 STI HSI \
  --mode drawdown \
  --threshold -0.30 \
  --lookback-months 3 \
  --output-csv outputs/all_indices_drawdown.csv
```

Use adjusted prices and include skip diagnostics:

```bash
python3 stock_screen.py \
  --indices SP500 \
  --price-basis adjusted \
  --include-skips \
  --output-csv outputs/sp500_adjusted.csv
```

Inspect parsed constituents only:

```bash
python3 stock_screen.py --indices STI HSI --print-constituents --print-constituents-limit 10
```

Run as of a fixed date:

```bash
python3 stock_screen.py --indices SP500 --as-of-date 2026-02-20
```

## Output Format

The script prints JSON to stdout with:
- `meta`: run configuration and summary counts
- `hits`: matched securities
- `skips`: optional, only when `--include-skips` is set

CSV output (when `--output-csv` is provided) contains hit rows, including:
- `run_date,index,ticker,company,mode,ref_date,ref_close,latest_date,latest_close,pct_change,peak_date,peak_close`

Notes:
- `peak_date` and `peak_close` are populated in `drawdown` mode
- `ref_date` and `ref_close` are populated in `point_to_point` mode

## Practical Notes

- Internet access is required at runtime:
  - Wikipedia (constituents)
  - Yahoo Finance (prices)
- Constituents are parsed from web tables; if table layout changes, parsing may need updates.
- If STI/HSI parsing fails, the script falls back to a small hardcoded list for those indices.
