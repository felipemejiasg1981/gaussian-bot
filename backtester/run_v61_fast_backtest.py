#!/usr/bin/env python3
"""
Runner rápido para Gaussian Trend IA Pro v6.1 en Python.

Objetivo:
  - probar muchos símbolos/timeframes sin TradingView
  - cachear OHLCV en CSV local
  - aplicar overrides de parámetros sin tocar el código
  - comparar por win rate / PF / DD / net profit / expectancy

Ejemplos:
  python3 run_v61_fast_backtest.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --intervals 15m,1h
  python3 run_v61_fast_backtest.py --symbols AZTECUSDT --intervals 15m --limit 3000 --workers 4
  python3 run_v61_fast_backtest.py --symbols UNIUSDT --intervals 15m --params-json '{"use_smc_filter": false, "adx_threshold": 20}'
"""

from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests

from gaussian_v61_bt import GaussianV61Backtest


BINANCE_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "UNIUSDT",
]


def parse_csv_list(raw: str | None, fallback: List[str]) -> List[str]:
    if not raw:
        return fallback
    items = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return items or fallback


def load_cached_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df.set_index("open_time")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def download_ohlcv(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    resp = requests.get(
        BINANCE_URL,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=20,
    )
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


def get_data(symbol: str, interval: str, limit: int, cache_dir: Path, refresh: bool) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{symbol}_{interval}_{limit}.csv"
    if cache_file.exists() and not refresh:
        return load_cached_csv(cache_file)

    df = download_ohlcv(symbol, interval, limit)
    to_save = df.reset_index().rename(columns={"index": "open_time"})
    to_save.to_csv(cache_file, index=False)
    return df


def summarize_result(result: Dict, symbol: str, interval: str, candles: int) -> Dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "candles": candles,
        "trades": result["total_trades"],
        "win_rate": result["win_rate"],
        "profit_factor": result["profit_factor"],
        "net_profit_pct": result["net_profit_pct"],
        "max_drawdown_pct": result["max_drawdown_pct"],
        "expectancy_pct": result["expectancy_pct"],
        "avg_trade_pct": result["avg_trade_pct"],
        "best_trade_pct": result.get("best_trade_pct", 0.0),
        "worst_trade_pct": result.get("worst_trade_pct", 0.0),
        "tp1_hits": result.get("tp1_hits", 0),
        "tp2_hits": result.get("tp2_hits", 0),
        "tp3_hits": result.get("tp3_hits", 0),
    }


def run_one(task: Tuple[str, str, int, str, bool, Dict]) -> Dict:
    symbol, interval, limit, cache_dir_str, refresh, params = task
    cache_dir = Path(cache_dir_str)
    df = get_data(symbol, interval, limit, cache_dir, refresh)
    bt = GaussianV61Backtest(**params)
    result = bt.run(df)
    return summarize_result(result, symbol, interval, len(df))


def execute_tasks(tasks: List[Tuple[str, str, int, str, bool, Dict]], workers: int) -> List[Tuple[Tuple[str, str, int, str, bool, Dict], Dict | Exception]]:
    results: List[Tuple[Tuple[str, str, int, str, bool, Dict], Dict | Exception]] = []

    if workers <= 1:
        for task in tasks:
            try:
                results.append((task, run_one(task)))
            except Exception as exc:
                results.append((task, exc))
        return results

    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(run_one, task): task for task in tasks}
            for future in as_completed(future_map):
                task = future_map[future]
                try:
                    results.append((task, future.result()))
                except Exception as exc:
                    results.append((task, exc))
        return results
    except PermissionError:
        for task in tasks:
            try:
                results.append((task, run_one(task)))
            except Exception as exc:
                results.append((task, exc))
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtesting local rápido para Gaussian v6.1")
    parser.add_argument("--symbols", help="Lista separada por coma, ej: BTCUSDT,ETHUSDT")
    parser.add_argument("--intervals", default="15m,1h,4h", help="Lista separada por coma, ej: 15m,1h")
    parser.add_argument("--limit", type=int, default=2000, help="Número de velas por símbolo/timeframe")
    parser.add_argument("--workers", type=int, default=max(1, min((os.cpu_count() or 2) - 1, 6)))
    parser.add_argument("--refresh", action="store_true", help="Ignora cache local y vuelve a descargar")
    parser.add_argument("--cache-dir", type=Path, default=Path(__file__).resolve().parent / "data")
    parser.add_argument("--params-json", help="Overrides JSON para GaussianV61Backtest")
    parser.add_argument("--params-file", type=Path, help="Archivo JSON con overrides")
    parser.add_argument("--sort-by", default="profit_factor", choices=["profit_factor", "net_profit_pct", "win_rate", "expectancy_pct", "max_drawdown_pct"])
    parser.add_argument("--min-trades", type=int, default=0, help="Filtra resultados con menos trades que este mínimo")
    parser.add_argument("--top", type=int, default=20, help="Cantidad de filas a imprimir")
    parser.add_argument("--output-json", type=Path, default=Path(__file__).resolve().parent / "results" / "v61_fast_results.json")
    parser.add_argument("--output-csv", type=Path, default=Path(__file__).resolve().parent / "results" / "v61_fast_results.csv")
    args = parser.parse_args()

    symbols = parse_csv_list(args.symbols, DEFAULT_SYMBOLS)
    intervals = parse_csv_list(args.intervals, ["15M", "1H", "4H"])
    intervals = [x.lower() for x in intervals]

    params: Dict = {}
    if args.params_file:
        params.update(json.loads(args.params_file.read_text(encoding="utf-8")))
    if args.params_json:
        params.update(json.loads(args.params_json))

    tasks = [
        (symbol, interval, args.limit, str(args.cache_dir), args.refresh, params)
        for symbol in symbols
        for interval in intervals
    ]

    print("=" * 78)
    print("Backtesting rápido local — Gaussian v6.1")
    print(f"Símbolos   : {len(symbols)}")
    print(f"Timeframes : {', '.join(intervals)}")
    print(f"Velas      : {args.limit}")
    print(f"Workers    : {args.workers}")
    print(f"Overrides  : {json.dumps(params, ensure_ascii=False) if params else 'ninguno'}")
    print("=" * 78)

    results: List[Dict] = []
    for task, outcome in execute_tasks(tasks, args.workers):
        symbol, interval, *_ = task
        if isinstance(outcome, Exception):
            print(f"❌ {symbol} {interval}: {outcome}")
            continue
        row = outcome
        results.append(row)
        print(
            f"✅ {symbol:12s} {interval:4s} | trades={row['trades']:3d} | WR={row['win_rate']:6.2f}% | "
            f"PF={row['profit_factor']:5.2f} | Net={row['net_profit_pct']:7.2f}% | DD={row['max_drawdown_pct']:6.2f}%"
        )

    if args.min_trades > 0:
        results = [row for row in results if row["trades"] >= args.min_trades]

    reverse = args.sort_by != "max_drawdown_pct"
    results.sort(key=lambda row: row[args.sort_by], reverse=reverse)

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output_json, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    pd.DataFrame(results).to_csv(args.output_csv, index=False)

    print()
    print(f"Resultados guardados en {args.output_json}")
    print(f"CSV guardado en {args.output_csv}")
    print()

    if not results:
        print("No hubo resultados para mostrar.")
        return

    print(f"Top {min(args.top, len(results))} por {args.sort_by}:")
    print("-" * 78)
    for row in results[: args.top]:
        print(
            f"{row['symbol']:12s} {row['interval']:4s} | trades={row['trades']:3d} | WR={row['win_rate']:6.2f}% | "
            f"PF={row['profit_factor']:5.2f} | Net={row['net_profit_pct']:7.2f}% | Exp={row['expectancy_pct']:6.3f}% | DD={row['max_drawdown_pct']:6.2f}%"
        )


if __name__ == "__main__":
    main()
