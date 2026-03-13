#!/usr/bin/env python3
"""
Escaner puntual para watchlists Gaussian v6.2.

Uso:
  python3 scan_v62_watchlist.py --watchlist clean
  python3 scan_v62_watchlist.py --watchlist broad --refresh
  python3 scan_v62_watchlist.py --symbols BTCUSDT,ETHUSDT,SOLUSDT
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from gaussian_v61_bt import GaussianV61Backtest
from strategy_lab import get_data


BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
DATA_DIR = BASE_DIR / "data"

WATCHLIST_FILES = {
    "clean": RESULTS_DIR / "gaussian_v62_live_watchlist_clean.json",
    "broad": RESULTS_DIR / "gaussian_v62_live_watchlist_broad.json",
}


def make_v62_engine() -> GaussianV61Backtest:
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


def load_watchlist(name: str) -> List[Dict[str, Any]]:
    path = WATCHLIST_FILES[name]
    return json.loads(path.read_text(encoding="utf-8"))


def classify_snapshot(snapshot: Dict[str, Any]) -> str:
    if not snapshot.get("ready"):
        return "NO_DATA"
    if snapshot["long_signal"]:
        return "LONG"
    if snapshot["short_signal"]:
        return "SHORT"
    dist = abs(snapshot["distance_to_trend_pct"])
    trend = snapshot["trend"]
    adx = snapshot["adx"]
    if trend == "bull" and dist <= 1.0 and (adx != adx or adx >= 18):
        return "WATCH_LONG"
    if trend == "bear" and dist <= 1.0 and (adx != adx or adx >= 18):
        return "WATCH_SHORT"
    return "NONE"


def sort_key(row: Dict[str, Any]) -> tuple:
    action_rank = {
        "LONG": 5,
        "SHORT": 5,
        "WATCH_LONG": 4,
        "WATCH_SHORT": 4,
        "NONE": 1,
        "NO_DATA": 0,
    }
    return (
        action_rank.get(row["action"], 0),
        float(row.get("priority_score", 0.0)),
        float(row.get("profit_factor", 0.0)),
        float(row.get("win_rate", 0.0)),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Escaner local de watchlists Gaussian v6.2")
    parser.add_argument("--watchlist", choices=["clean", "broad"], default="clean")
    parser.add_argument("--symbols", help="Lista custom separada por coma")
    parser.add_argument("--interval", default="15m")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--provider", default="bitget", choices=["bitget", "binance"])
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--output-json", type=Path, default=RESULTS_DIR / "gaussian_v62_watchlist_scan.json")
    args = parser.parse_args()

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        metadata = {s: {"symbol": s, "segment": "custom", "priority_score": 0.0} for s in symbols}
    else:
        items = load_watchlist(args.watchlist)
        symbols = [item["symbol"] for item in items]
        metadata = {item["symbol"]: item for item in items}

    engine = make_v62_engine()
    rows: List[Dict[str, Any]] = []

    for symbol in symbols:
        row = dict(metadata[symbol])
        try:
            df = get_data(symbol, args.interval, args.limit, DATA_DIR, args.refresh, args.provider)
            snap = engine.snapshot(df)
            row.update(snap)
            row["action"] = classify_snapshot(snap)
        except Exception as exc:
            row.update({"ready": False, "action": "ERROR", "error": str(exc)})
        rows.append(row)

    rows.sort(key=sort_key, reverse=True)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 110)
    print(f"Escaner Gaussian v6.2 | watchlist={args.watchlist} | interval={args.interval} | provider={args.provider}")
    print("=" * 110)
    print(f"{'ACTION':12} {'SYMBOL':14} {'SEGMENT':18} {'PF':>5} {'WR':>6} {'NET':>7} {'ADX':>6} {'DIST%':>7} {'TREND':>6}")
    print("-" * 110)
    for row in rows:
        print(
            f"{row.get('action','ERR'):12} "
            f"{row.get('symbol','?'):14} "
            f"{row.get('segment','?'):18} "
            f"{float(row.get('profit_factor', 0.0)):>5.2f} "
            f"{float(row.get('win_rate', 0.0)):>5.1f}% "
            f"{float(row.get('net_profit_pct', 0.0)):>6.2f}% "
            f"{float(row.get('adx', float('nan'))):>6.2f} "
            f"{float(row.get('distance_to_trend_pct', float('nan'))):>6.2f}% "
            f"{str(row.get('trend', '-')):>6}"
        )

    print()
    print(f"JSON: {args.output_json}")


if __name__ == "__main__":
    main()
