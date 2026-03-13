#!/usr/bin/env python3
"""
Run the dedicated v6.1 backtester.

Examples:
  python3 run_v61_backtest.py --csv data/UNIUSDT_15m.csv
  python3 run_v61_backtest.py --symbol UNIUSDT --interval 15m --limit 1500
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

from gaussian_v61_bt import GaussianV61Backtest


BINANCE_URL = "https://api.binance.com/api/v3/klines"


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    candidates = ["open_time", "timestamp", "time", "date"]
    ts_col = next((c for c in candidates if c in df.columns), None)
    if ts_col is None:
        raise ValueError("CSV must include one of: open_time, timestamp, time, date")
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df = df.set_index(ts_col)
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"CSV missing required column: {col}")
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def download_ohlcv(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    resp = requests.get(BINANCE_URL, params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(
        data,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Gaussian v6.1 strategy in Python")
    parser.add_argument("--csv", type=Path, help="CSV with open,high,low,close,volume and timestamp")
    parser.add_argument("--symbol", default="UNIUSDT", help="Binance symbol for download mode")
    parser.add_argument("--interval", default="15m", help="Binance interval for download mode")
    parser.add_argument("--limit", type=int, default=1500, help="Candles to download")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    args = parser.parse_args()

    if args.csv:
        df = load_csv(args.csv)
    else:
        df = download_ohlcv(args.symbol.upper(), args.interval, args.limit)

    bt = GaussianV61Backtest()
    result = bt.run(df)

    summary = {
        k: v
        for k, v in result.items()
        if k not in {"trades", "equity_curve"}
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, ensure_ascii=False)
        print(f"\nSaved detailed result to {args.output}")


if __name__ == "__main__":
    main()
