#!/usr/bin/env python3
"""
Laboratorio de estrategias local.

Permite:
  - probar múltiples estrategias Python registradas
  - barrer combinaciones de parámetros (grid search)
  - cachear datos OHLCV
  - rankear por PF, win rate, DD, expectancy o net profit

Estrategias soportadas hoy:
  - gaussian_v61
  - gaussian_legacy

La arquitectura queda lista para sumar Frost / Smooth cuando tengan port Python.
"""

from __future__ import annotations

import argparse
import inspect
import itertools
import json
import os
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

import pandas as pd
import requests

from gaussian_bt import GaussianBacktest
from gaussian_v61_bt import GaussianV61Backtest


BINANCE_URL = "https://api.binance.com/api/v3/klines"
BITGET_TICKERS_URL = "https://api.bitget.com/api/v2/mix/market/tickers"
BITGET_CANDLES_URL = "https://api.bitget.com/api/v2/mix/market/history-candles"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
DEFAULT_INTERVALS = ["15m", "1h", "4h"]

warnings.filterwarnings("ignore", category=FutureWarning)


@dataclass(frozen=True)
class StrategySpec:
    name: str
    constructor: Callable[..., Any]
    normalize: Callable[[Dict[str, Any]], Dict[str, Any]]


def normalize_v61(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trades": result.get("total_trades", 0),
        "wins": result.get("wins", 0),
        "losses": result.get("losses", 0),
        "win_rate": result.get("win_rate", 0.0),
        "profit_factor": result.get("profit_factor", 0.0),
        "net_profit_pct": result.get("net_profit_pct", 0.0),
        "max_drawdown_pct": result.get("max_drawdown_pct", 0.0),
        "expectancy_pct": result.get("expectancy_pct", 0.0),
        "avg_trade_pct": result.get("avg_trade_pct", 0.0),
        "best_trade_pct": result.get("best_trade_pct", 0.0),
        "worst_trade_pct": result.get("worst_trade_pct", 0.0),
    }


def normalize_legacy(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trades": result.get("total_trades", 0),
        "wins": result.get("wins", 0),
        "losses": result.get("losses", 0),
        "win_rate": result.get("win_rate", 0.0),
        "profit_factor": result.get("profit_factor", 0.0),
        "net_profit_pct": result.get("total_pnl", 0.0),
        "max_drawdown_pct": result.get("max_drawdown", 0.0),
        "expectancy_pct": result.get("avg_pnl", 0.0),
        "avg_trade_pct": result.get("avg_pnl", 0.0),
        "best_trade_pct": result.get("best_trade", 0.0),
        "worst_trade_pct": result.get("worst_trade", 0.0),
    }


STRATEGIES: Dict[str, StrategySpec] = {
    "gaussian_v61": StrategySpec("gaussian_v61", GaussianV61Backtest, normalize_v61),
    "gaussian_legacy": StrategySpec("gaussian_legacy", GaussianBacktest, normalize_legacy),
}


def make_gaussian_v31_profile() -> GaussianBacktest:
    return GaussianBacktest(
        length=20,
        distance=1.0,
        mode="AVG",
        atr_len=14,
        sl_buf_pct=0.002,
        max_sl_pct=0.06,
    )


def make_gaussian_v61_proxy_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=False,
        use_adx_filter=True,
        use_dmi_confirm=True,
        use_smc_filter=False,
        use_volume_absorption=False,
        use_multi_osc=False,
        use_kill_zones=False,
        use_htf_alignment=False,
        use_trend_slope_filter=False,
        use_band_width_filter=False,
        use_divergence=False,
    )


def make_gaussian_v61_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=True,
        sigma_base=3.0,
        sigma_min=5.0,
        sigma_max=18.0,
        use_adx_filter=False,
        adx_len=14,
        adx_threshold=25.0,
        use_dmi_confirm=False,
        use_smc_filter=False,
        use_volume_absorption=True,
        absorption_threshold=1.5,
        use_multi_osc=True,
        use_rsi=False,
        use_stoch=False,
        use_cci=False,
        rsi_len=14,
        stoch_len=14,
        cci_len=20,
        rsi_oversold=35.0,
        rsi_overbought=65.0,
        use_kill_zones=True,
        kz_london_open=True,
        kz_ny_open=True,
        kz_london_close=False,
        kz_asia_session=False,
        use_chop_filter=True,
        chop_len=20,
        chop_threshold=50.0,
        use_squeeze=False,
        sqz_bb_len=20,
        sqz_bb_mult=2.0,
        sqz_kc_len=20,
        sqz_kc_mult=1.5,
        sqz_lookback=8,
        use_wae=True,
        wae_sens=150,
        wae_fast_len=20,
        wae_slow_len=40,
        wae_bb_len=20,
        wae_bb_mult=2.0,
        wae_dead_zone=3.7,
        use_fisher=False,
        fisher_len=10,
        fisher_extreme=1.5,
        use_frost=True,
        min_frost_conf=2,
        frost_mode="Intraday",
        use_reentry=True,
        reentry_bars=6,
        use_htf_alignment=False,
        use_trend_slope_filter=False,
        use_band_width_filter=False,
        use_divergence=True,
        div_pivot_strength=5,
        atr_len=14,
        max_sl_pct=0.06,
        sl_buf=0.002,
        tp1_r=1.0,
        tp2_r=2.0,
        tp3_r=3.0,
        tp4_r=4.236,
        pct_tp1=30.0,
        pct_tp2=30.0,
        pct_tp3=20.0,
    )


def make_gaussian_v62_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=True,
        use_adx_filter=False,
        use_dmi_confirm=False,
        use_smc_filter=False,
        use_volume_absorption=False,
        use_multi_osc=False,
        use_kill_zones=False,
        use_htf_alignment=False,
        use_trend_slope_filter=False,
        use_band_width_filter=False,
        use_divergence=False,
    )


def make_gaussian_v62_guarded_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=True,
        use_adx_filter=True,
        adx_threshold=15.0,
        use_dmi_confirm=False,
        use_smc_filter=False,
        use_volume_absorption=False,
        use_multi_osc=False,
        use_kill_zones=False,
        use_htf_alignment=False,
        use_trend_slope_filter=True,
        min_trend_slope_atr=0.04,
        use_band_width_filter=True,
        min_band_width_atr=0.75,
        use_divergence=False,
        max_sl_pct=0.05,
    )


def make_gaussian_v75_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=True,
        use_adx_filter=True,
        use_dmi_confirm=True,
        use_smc_filter=False,
        use_volume_absorption=False,
        use_multi_osc=False,
        use_kill_zones=False,
        use_htf_alignment=False,
        use_trend_slope_filter=False,
        use_band_width_filter=False,
        use_divergence=False,
    )


def make_gaussian_v62_risk_profile() -> GaussianV61Backtest:
    return GaussianV61Backtest(
        use_adaptive_sigma=True,
        use_adx_filter=False,
        use_dmi_confirm=False,
        use_smc_filter=False,
        use_volume_absorption=False,
        use_multi_osc=False,
        use_kill_zones=False,
        use_htf_alignment=False,
        use_trend_slope_filter=False,
        use_band_width_filter=False,
        use_divergence=False,
        max_sl_pct=0.05,
        sl_buf=0.0015,
        tp1_r=0.90,
        tp2_r=1.80,
        tp3_r=2.80,
        tp4_r=3.80,
        pct_tp1=35.0,
        pct_tp2=30.0,
        pct_tp3=20.0,
    )


STRATEGIES.update(
    {
        "gaussian_v61_proxy_profile": StrategySpec("gaussian_v61_proxy_profile", make_gaussian_v61_proxy_profile, normalize_v61),
        "gaussian_v31_profile": StrategySpec("gaussian_v31_profile", make_gaussian_v31_profile, normalize_legacy),
        "gaussian_v61_profile": StrategySpec("gaussian_v61_profile", make_gaussian_v61_profile, normalize_v61),
        "gaussian_v62_profile": StrategySpec("gaussian_v62_profile", make_gaussian_v62_profile, normalize_v61),
        "gaussian_v62_guarded_profile": StrategySpec("gaussian_v62_guarded_profile", make_gaussian_v62_guarded_profile, normalize_v61),
        "gaussian_v62_risk_profile": StrategySpec("gaussian_v62_risk_profile", make_gaussian_v62_risk_profile, normalize_v61),
        "gaussian_v75_profile": StrategySpec("gaussian_v75_profile", make_gaussian_v75_profile, normalize_v61),
    }
)


def parse_csv_list(raw: str | None, fallback: List[str]) -> List[str]:
    if not raw:
        return fallback
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or fallback


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
    df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"]), unit="ms", utc=True)
    df = df.set_index("open_time")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def bitget_granularity(interval: str) -> str:
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "1w": "1W",
    }
    if interval not in mapping:
        raise ValueError(f"Intervalo no soportado en Bitget: {interval}")
    return mapping[interval]


def download_ohlcv_bitget(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    granularity = bitget_granularity(interval)
    remaining = limit
    end_time = None
    rows: List[List[str]] = []

    while remaining > 0:
        batch_limit = min(remaining, 200)
        params = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "granularity": granularity,
            "limit": batch_limit,
        }
        if end_time is not None:
            params["endTime"] = end_time

        resp = requests.get(BITGET_CANDLES_URL, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != "00000":
            raise ValueError(f"Bitget candles error para {symbol} {interval}: {payload}")

        batch = payload.get("data", [])
        if not batch:
            break

        rows = batch + rows
        oldest_ts = int(batch[0][0])
        end_time = oldest_ts - 1
        remaining -= len(batch)

        if len(batch) < batch_limit:
            break

        time.sleep(0.03)

    if not rows:
        raise ValueError(f"Sin datos Bitget para {symbol} {interval}")

    deduped: Dict[int, List[str]] = {}
    for row in rows:
        deduped[int(row[0])] = row
    ordered = [deduped[key] for key in sorted(deduped.keys())][-limit:]

    df = pd.DataFrame(
        ordered,
        columns=["open_time", "open", "high", "low", "close", "volume", "quote_volume"],
    )
    df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"]), unit="ms", utc=True)
    df = df.set_index("open_time")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def get_data(symbol: str, interval: str, limit: int, cache_dir: Path, refresh: bool, provider: str) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{provider}_{symbol}_{interval}_{limit}.csv"
    if cache_file.exists() and not refresh:
        return load_cached_csv(cache_file)
    if provider == "bitget":
        df = download_ohlcv_bitget(symbol, interval, limit)
    else:
        df = download_ohlcv(symbol, interval, limit)
    df.reset_index().to_csv(cache_file, index=False)
    return df


def fetch_top_bitget_symbols(top_n: int, min_usdt_volume: float = 0.0) -> List[str]:
    resp = requests.get(
        BITGET_TICKERS_URL,
        params={"productType": "USDT-FUTURES"},
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != "00000":
        raise ValueError(f"Bitget tickers error: {payload}")

    rows = []
    for item in payload.get("data", []):
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        try:
            usdt_volume = float(item.get("usdtVolume") or 0.0)
        except (TypeError, ValueError):
            usdt_volume = 0.0
        if usdt_volume < min_usdt_volume:
            continue
        rows.append((symbol, usdt_volume))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [symbol for symbol, _ in rows[:top_n]]


def sanitize_params(constructor: Callable[..., Any], params: Dict[str, Any]) -> Dict[str, Any]:
    sig = inspect.signature(constructor.__init__)
    valid = set(sig.parameters.keys()) - {"self"}
    return {k: v for k, v in params.items() if k in valid}


def build_param_combos(grid: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not grid:
        return [{}]

    keys = list(grid.keys())
    value_lists: List[List[Any]] = []
    for key in keys:
        value = grid[key]
        if isinstance(value, list):
            value_lists.append(value)
        else:
            value_lists.append([value])

    combos = []
    for values in itertools.product(*value_lists):
        combos.append({k: v for k, v in zip(keys, values)})
    return combos


def short_params_label(params: Dict[str, Any], max_items: int = 5) -> str:
    if not params:
        return "base"
    parts = []
    for idx, (k, v) in enumerate(sorted(params.items())):
        if idx >= max_items:
            parts.append("...")
            break
        parts.append(f"{k}={v}")
    return ", ".join(parts)


def score_row(row: Dict[str, Any]) -> float:
    pf = min(float(row["profit_factor"]), 10.0)
    wr = float(row["win_rate"]) / 100.0
    netp = float(row["net_profit_pct"])
    dd = float(row["max_drawdown_pct"])
    exp = float(row["expectancy_pct"])
    trades = float(row["trades"])
    trade_factor = min(trades / 50.0, 1.0)
    return round((pf * 2.0 + wr * 4.0 + netp * 0.25 + exp * 2.0 - dd * 0.2) * trade_factor, 4)


def run_one(task: Tuple[str, Dict[str, Any], str, str, int, str, bool, str]) -> Dict[str, Any]:
    strategy_name, params, symbol, interval, limit, cache_dir_str, refresh, provider = task
    spec = STRATEGIES[strategy_name]
    df = get_data(symbol, interval, limit, Path(cache_dir_str), refresh, provider)
    is_class_constructor = inspect.isclass(spec.constructor)
    clean_params = sanitize_params(spec.constructor, params) if is_class_constructor else {}
    engine = spec.constructor(**clean_params) if is_class_constructor else spec.constructor()
    raw = engine.run(df)
    normalized = spec.normalize(raw)
    normalized.update(
        {
            "strategy": strategy_name,
            "symbol": symbol,
            "interval": interval,
            "provider": provider,
            "candles": len(df),
            "params": clean_params,
            "params_label": short_params_label(clean_params),
        }
    )
    normalized["composite_score"] = score_row(normalized)
    return normalized


def execute_tasks(tasks: List[Tuple[str, Dict[str, Any], str, str, int, str, bool, str]], workers: int) -> List[Tuple[Tuple[str, Dict[str, Any], str, str, int, str, bool, str], Dict[str, Any] | Exception]]:
    results: List[Tuple[Tuple[str, Dict[str, Any], str, str, int, str, bool, str], Dict[str, Any] | Exception]] = []
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


def load_grid(args: argparse.Namespace, selected_strategies: List[str]) -> Dict[str, Dict[str, Any]]:
    per_strategy: Dict[str, Dict[str, Any]] = {name: {} for name in selected_strategies}

    if args.grid_file:
        payload = json.loads(args.grid_file.read_text(encoding="utf-8"))
        for name in selected_strategies:
            if name in payload and isinstance(payload[name], dict):
                per_strategy[name].update(payload[name])

    if args.grid_json:
        payload = json.loads(args.grid_json)
        if any(name in payload for name in selected_strategies):
            for name in selected_strategies:
                if name in payload and isinstance(payload[name], dict):
                    per_strategy[name].update(payload[name])
        else:
            for name in selected_strategies:
                per_strategy[name].update(payload)

    if args.params_file:
        payload = json.loads(args.params_file.read_text(encoding="utf-8"))
        if any(name in payload for name in selected_strategies):
            for name in selected_strategies:
                if name in payload and isinstance(payload[name], dict):
                    per_strategy[name].update({k: [v] for k, v in payload[name].items()})
        else:
            for name in selected_strategies:
                per_strategy[name].update({k: [v] for k, v in payload.items()})

    if args.params_json:
        payload = json.loads(args.params_json)
        if any(name in payload for name in selected_strategies):
            for name in selected_strategies:
                if name in payload and isinstance(payload[name], dict):
                    per_strategy[name].update({k: [v] for k, v in payload[name].items()})
        else:
            for name in selected_strategies:
                per_strategy[name].update({k: [v] for k, v in payload.items()})

    return per_strategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Laboratorio local de backtesting multi-estrategia")
    parser.add_argument("--strategies", default="gaussian_v61", help="Lista separada por coma o 'all'")
    parser.add_argument("--symbols", help="Lista separada por coma, ej: BTCUSDT,ETHUSDT")
    parser.add_argument("--intervals", help="Lista separada por coma, ej: 15m,1h,4h")
    parser.add_argument("--provider", default="binance", choices=["binance", "bitget"])
    parser.add_argument("--top-bitget", type=int, help="Selecciona automáticamente el top N de USDT futures por volumen en Bitget")
    parser.add_argument("--min-usdt-volume", type=float, default=0.0, help="Volumen mínimo USDT para filtrar el universo Bitget")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=max(1, min((os.cpu_count() or 2) - 1, 6)))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=Path(__file__).resolve().parent / "data")
    parser.add_argument("--params-json", help="Overrides fijos en JSON")
    parser.add_argument("--params-file", type=Path, help="Overrides fijos desde archivo JSON")
    parser.add_argument("--grid-json", help="Grid JSON. Puede ser general o por estrategia")
    parser.add_argument("--grid-file", type=Path, help="Archivo JSON con grid de parámetros")
    parser.add_argument("--sort-by", default="composite_score", choices=["composite_score", "profit_factor", "net_profit_pct", "win_rate", "expectancy_pct", "max_drawdown_pct"])
    parser.add_argument("--min-trades", type=int, default=0)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--output-json", type=Path, default=Path(__file__).resolve().parent / "results" / "strategy_lab_results.json")
    parser.add_argument("--output-csv", type=Path, default=Path(__file__).resolve().parent / "results" / "strategy_lab_results.csv")
    args = parser.parse_args()

    selected_strategies = list(STRATEGIES.keys()) if args.strategies.strip().lower() == "all" else [s.strip() for s in args.strategies.split(",") if s.strip()]
    unknown = [s for s in selected_strategies if s not in STRATEGIES]
    if unknown:
        raise ValueError(f"Estrategias no registradas: {', '.join(unknown)}")

    if args.top_bitget:
        symbols = fetch_top_bitget_symbols(args.top_bitget, args.min_usdt_volume)
        args.provider = "bitget"
    else:
        symbols = [s.upper() for s in parse_csv_list(args.symbols, DEFAULT_SYMBOLS)]
    intervals = [s.lower() for s in parse_csv_list(args.intervals, DEFAULT_INTERVALS)]
    grid_per_strategy = load_grid(args, selected_strategies)

    tasks: List[Tuple[str, Dict[str, Any], str, str, int, str, bool, str]] = []
    combo_count = 0
    for strategy_name in selected_strategies:
        combos = build_param_combos(grid_per_strategy[strategy_name])
        combo_count += len(combos)
        for combo in combos:
            for symbol in symbols:
                for interval in intervals:
                    tasks.append((strategy_name, combo, symbol, interval, args.limit, str(args.cache_dir), args.refresh, args.provider))

    print("=" * 88)
    print("Laboratorio local de estrategias")
    print(f"Estrategias : {', '.join(selected_strategies)}")
    print(f"Provider    : {args.provider}")
    print(f"Símbolos    : {', '.join(symbols)}")
    print(f"Timeframes  : {', '.join(intervals)}")
    print(f"Combos      : {combo_count}")
    print(f"Corridas    : {len(tasks)}")
    print(f"Workers     : {args.workers}")
    print("=" * 88)

    rows: List[Dict[str, Any]] = []
    for task, outcome in execute_tasks(tasks, args.workers):
        strategy_name, params, symbol, interval, *_ = task
        if isinstance(outcome, Exception):
            print(f"❌ {strategy_name:15s} {symbol:12s} {interval:4s} | {outcome}")
            continue
        row = outcome
        rows.append(row)
        print(
            f"✅ {strategy_name:15s} {symbol:12s} {interval:4s} | trades={row['trades']:3d} | "
            f"WR={row['win_rate']:6.2f}% | PF={row['profit_factor']:5.2f} | "
            f"Net={row['net_profit_pct']:7.2f}% | DD={row['max_drawdown_pct']:6.2f}% | "
            f"score={row['composite_score']:7.3f}"
        )

    if args.min_trades > 0:
        rows = [row for row in rows if row["trades"] >= args.min_trades]

    reverse = args.sort_by != "max_drawdown_pct"
    rows.sort(key=lambda row: row[args.sort_by], reverse=reverse)

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output_json, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, ensure_ascii=False)

    csv_ready = []
    for row in rows:
        flat = row.copy()
        flat["params"] = json.dumps(row["params"], ensure_ascii=False, sort_keys=True)
        csv_ready.append(flat)
    pd.DataFrame(csv_ready).to_csv(args.output_csv, index=False)

    print()
    print(f"JSON: {args.output_json}")
    print(f"CSV : {args.output_csv}")
    print()
    if not rows:
        print("No hubo resultados válidos.")
        return

    print(f"Top {min(args.top, len(rows))} por {args.sort_by}:")
    print("-" * 88)
    for row in rows[: args.top]:
        print(
            f"{row['strategy']:15s} {row['symbol']:12s} {row['interval']:4s} | trades={row['trades']:3d} | "
            f"WR={row['win_rate']:6.2f}% | PF={row['profit_factor']:5.2f} | Net={row['net_profit_pct']:7.2f}% | "
            f"Exp={row['expectancy_pct']:6.3f}% | DD={row['max_drawdown_pct']:6.2f}% | {row['params_label']}"
        )


if __name__ == "__main__":
    main()
