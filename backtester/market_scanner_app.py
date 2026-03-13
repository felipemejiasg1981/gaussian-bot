#!/usr/bin/env python3
"""
Escaner web cripto para comparar Gaussian v6.1 real de TradingView vs v6.2.
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from flask import Flask, jsonify, render_template, request

from strategy_lab import get_data, make_gaussian_v61_profile, make_gaussian_v62_profile


BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "scanner_history.db"

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"), static_folder=str(BASE_DIR / "static"))

DEFAULT_TOP30 = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "PEPEUSDT",
    "ICPUSDT",
    "ADAUSDT",
    "BNBUSDT",
    "LINKUSDT",
    "TRXUSDT",
    "TAOUSDT",
    "ZECUSDT",
    "FILUSDT",
    "NEARUSDT",
    "ICXUSDT",
    "HUMAUSDT",
    "AAVEUSDT",
    "SHIBUSDT",
    "RENDERUSDT",
    "WLDUSDT",
    "UNIUSDT",
    "XLMUSDT",
    "OPUSDT",
    "ONDOUSDT",
    "FARTCOINUSDT",
    "ZROUSDT",
    "BANANAS31USDT",
    "PIXELUSDT",
]

TIMEFRAME_OPTIONS = ["15m", "1h", "4h", "1d"]
VERSION_BUILDERS = {
    "v6.1": make_gaussian_v61_profile,
    "v6.2": make_gaussian_v62_profile,
}


@dataclass
class ScanRow:
    symbol: str
    timeframe: str
    version: str
    segment: str
    total_trades: int
    win_rate: float
    net_profit_pct: float
    profit_factor: float
    max_drawdown_pct: float
    expectancy_pct: float
    avg_trade_pct: float
    best_trade_pct: float
    worst_trade_pct: float
    final_equity: float
    quality_score: float = 0.0
    error: Optional[str] = None


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                asset_types_json TEXT NOT NULL,
                timeframes_json TEXT NOT NULL,
                top_n INTEGER NOT NULL,
                min_trades INTEGER NOT NULL,
                max_workers INTEGER NOT NULL,
                tested INTEGER NOT NULL,
                failed INTEGER NOT NULL,
                scan_seconds REAL NOT NULL,
                avg_winrate REAL NOT NULL,
                avg_profit REAL NOT NULL,
                avg_drawdown REAL NOT NULL,
                summary_json TEXT NOT NULL,
                result_json TEXT NOT NULL
            )
            """
        )


def save_scan_result(result: Dict, timeframes: List[str], top_n: int, min_trades: int, max_workers: int) -> int:
    summary = result["summary"]
    created_at = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        cur = conn.execute(
            """
            INSERT INTO scans (
                created_at, asset_types_json, timeframes_json, top_n, min_trades, max_workers,
                tested, failed, scan_seconds, avg_winrate, avg_profit, avg_drawdown,
                summary_json, result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                json.dumps(["crypto"], ensure_ascii=False),
                json.dumps(timeframes, ensure_ascii=False),
                top_n,
                min_trades,
                max_workers,
                summary["tested"],
                summary["failed"],
                summary["scan_seconds"],
                summary["avg_winrate"],
                summary["avg_profit"],
                summary["avg_drawdown"],
                json.dumps(summary, ensure_ascii=False),
                json.dumps(result, ensure_ascii=False),
            ),
        )
        return int(cur.lastrowid)


def list_scan_history(limit: int = 20) -> List[Dict]:
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT id, created_at, timeframes_json, top_n, min_trades,
                   tested, failed, scan_seconds, avg_winrate, avg_profit, avg_drawdown,
                   summary_json
            FROM scans
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out = []
    for row in rows:
        summary = json.loads(row["summary_json"])
        out.append(
            {
                "id": int(row["id"]),
                "created_at": row["created_at"],
                "timeframes": json.loads(row["timeframes_json"]),
                "top_n": int(row["top_n"]),
                "min_trades": int(row["min_trades"]),
                "tested": int(row["tested"]),
                "failed": int(row["failed"]),
                "scan_seconds": float(row["scan_seconds"]),
                "avg_winrate": float(row["avg_winrate"]),
                "avg_profit": float(row["avg_profit"]),
                "avg_drawdown": float(row["avg_drawdown"]),
                "universe_name": summary.get("universe_name", "custom"),
                "versions": summary.get("versions", []),
                "symbols_count": summary.get("symbols_count", 0),
            }
        )
    return out


def get_scan_result(scan_id: int) -> Optional[Dict]:
    with _db() as conn:
        row = conn.execute("SELECT result_json FROM scans WHERE id = ?", (scan_id,)).fetchone()
    if not row:
        return None
    return json.loads(row["result_json"])


def load_watchlist_file(filename: str) -> List[Dict]:
    path = RESULTS_DIR / filename
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def load_segmented_universe() -> List[Dict]:
    path = RESULTS_DIR / "bitget_segmented_universe.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def build_universes() -> tuple[Dict[str, List[str]], Dict[str, str], Dict[str, Dict]]:
    meta: Dict[str, Dict] = {}
    segment_map: Dict[str, str] = {}

    segmented = load_segmented_universe()
    for item in segmented:
        meta[item["symbol"]] = item
        segment_map[item["symbol"]] = item.get("segment", "unknown")

    clean_items = load_watchlist_file("gaussian_v62_live_watchlist_clean.json")
    broad_items = load_watchlist_file("gaussian_v62_live_watchlist_broad.json")
    for item in clean_items + broad_items:
        meta[item["symbol"]] = {**meta.get(item["symbol"], {}), **item}
        segment_map[item["symbol"]] = item.get("segment", segment_map.get(item["symbol"], "unknown"))

    universes = {
        "clean": [item["symbol"] for item in clean_items] or DEFAULT_TOP30[:12],
        "broad": [item["symbol"] for item in broad_items] or DEFAULT_TOP30[:25],
        "top30": [item["symbol"] for item in segmented[:30]] or DEFAULT_TOP30,
    }
    return universes, segment_map, meta


UNIVERSES, SEGMENT_BY_SYMBOL, META_BY_SYMBOL = build_universes()


def _to_scan_row(symbol: str, timeframe: str, version: str, result: Dict) -> ScanRow:
    return ScanRow(
        symbol=symbol,
        timeframe=timeframe,
        version=version,
        segment=SEGMENT_BY_SYMBOL.get(symbol, META_BY_SYMBOL.get(symbol, {}).get("segment", "unknown")),
        total_trades=int(result["total_trades"]),
        win_rate=float(result["win_rate"]),
        net_profit_pct=float(result["net_profit_pct"]),
        profit_factor=float(result["profit_factor"]),
        max_drawdown_pct=float(result["max_drawdown_pct"]),
        expectancy_pct=float(result["expectancy_pct"]),
        avg_trade_pct=float(result["avg_trade_pct"]),
        best_trade_pct=float(result["best_trade_pct"]),
        worst_trade_pct=float(result["worst_trade_pct"]),
        final_equity=float(result["final_equity"]),
    )


def _score_rows(rows: List[ScanRow]) -> None:
    if not rows:
        return

    def norm(values: List[float], invert: bool = False) -> List[float]:
        lo = min(values)
        hi = max(values)
        if math.isclose(lo, hi):
            return [0.5] * len(values)
        vals = [(v - lo) / (hi - lo) for v in values]
        return [1.0 - v if invert else v for v in vals]

    wr_n = norm([r.win_rate for r in rows])
    profit_n = norm([r.net_profit_pct for r in rows])
    pf_n = norm([min(r.profit_factor, 10.0) for r in rows])
    dd_n = norm([r.max_drawdown_pct for r in rows], invert=True)
    tr_n = norm([r.total_trades for r in rows])

    for i, row in enumerate(rows):
        score = (
            wr_n[i] * 0.32
            + profit_n[i] * 0.28
            + pf_n[i] * 0.20
            + dd_n[i] * 0.15
            + tr_n[i] * 0.05
        )
        row.quality_score = round(score * 100.0, 2)


def _run_one(symbol: str, timeframe: str, version: str, min_trades: int, provider: str) -> ScanRow:
    try:
        df = get_data(symbol, timeframe, 2000, DATA_DIR, False, provider)
        if len(df) < 250:
            raise ValueError("historial insuficiente")
        engine = VERSION_BUILDERS[version]()
        result = engine.run(df)
        row = _to_scan_row(symbol, timeframe, version, result)
        if row.total_trades < min_trades:
            row.error = f"menos de {min_trades} trades"
        return row
    except Exception as exc:
        return ScanRow(
            symbol=symbol,
            timeframe=timeframe,
            version=version,
            segment=SEGMENT_BY_SYMBOL.get(symbol, "unknown"),
            total_trades=0,
            win_rate=0.0,
            net_profit_pct=0.0,
            profit_factor=0.0,
            max_drawdown_pct=0.0,
            expectancy_pct=0.0,
            avg_trade_pct=0.0,
            best_trade_pct=0.0,
            worst_trade_pct=0.0,
            final_equity=0.0,
            error=str(exc),
        )


def _comparison_key(item: Dict) -> tuple:
    best_score = max(item.get("v6_1", {}).get("quality_score", 0.0), item.get("v6_2", {}).get("quality_score", 0.0))
    improvement = item.get("delta_net_profit_pct", 0.0)
    best_pf = max(item.get("v6_1", {}).get("profit_factor", 0.0), item.get("v6_2", {}).get("profit_factor", 0.0))
    return (best_score, improvement, best_pf)


def build_comparison(rows: List[ScanRow], timeframes: List[str], top_n: int) -> Dict[str, List[Dict]]:
    by_tf: Dict[str, List[Dict]] = {}
    for timeframe in timeframes:
        rows_tf = [row for row in rows if row.timeframe == timeframe]
        bucket: Dict[str, Dict] = {}
        for row in rows_tf:
            key = row.symbol
            item = bucket.setdefault(
                key,
                {
                    "symbol": row.symbol,
                    "segment": row.segment,
                    "priority_score": float(META_BY_SYMBOL.get(row.symbol, {}).get("priority_score", 0.0)),
                    "rank_hint": int(META_BY_SYMBOL.get(row.symbol, {}).get("rank", 999)),
                    "timeframe": timeframe,
                },
            )
            item["v6_1" if row.version == "v6.1" else "v6_2"] = asdict(row)

        comparison = []
        for item in bucket.values():
            v61 = item.get("v6_1", {})
            v62 = item.get("v6_2", {})
            item["delta_net_profit_pct"] = round(v62.get("net_profit_pct", 0.0) - v61.get("net_profit_pct", 0.0), 2)
            item["delta_win_rate"] = round(v62.get("win_rate", 0.0) - v61.get("win_rate", 0.0), 2)
            item["delta_profit_factor"] = round(v62.get("profit_factor", 0.0) - v61.get("profit_factor", 0.0), 2)
            comparison.append(item)

        comparison.sort(key=_comparison_key, reverse=True)
        by_tf[timeframe] = comparison[:top_n]
    return by_tf


def _version_summary(rows: List[ScanRow], version: str) -> Dict:
    subset = [row for row in rows if row.version == version]
    if not subset:
        return {
            "count": 0,
            "avg_winrate": 0.0,
            "avg_profit": 0.0,
            "avg_drawdown": 0.0,
            "avg_profit_factor": 0.0,
        }
    return {
        "count": len(subset),
        "avg_winrate": round(sum(row.win_rate for row in subset) / len(subset), 2),
        "avg_profit": round(sum(row.net_profit_pct for row in subset) / len(subset), 2),
        "avg_drawdown": round(sum(row.max_drawdown_pct for row in subset) / len(subset), 2),
        "avg_profit_factor": round(sum(row.profit_factor for row in subset) / len(subset), 2),
    }


def run_scan(universe_name: str, symbols: List[str], versions: List[str], timeframes: List[str], top_n: int, min_trades: int, max_workers: int, provider: str) -> Dict:
    started = time.time()
    scanned_rows: List[ScanRow] = []
    failures: List[Dict] = []
    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for timeframe in timeframes:
            for version in versions:
                for symbol in symbols:
                    futures.append(executor.submit(_run_one, symbol, timeframe, version, min_trades, provider))

        for future in as_completed(futures):
            row = future.result()
            if row.error:
                failures.append(
                    {
                        "symbol": row.symbol,
                        "timeframe": row.timeframe,
                        "version": row.version,
                        "error": row.error,
                    }
                )
            else:
                scanned_rows.append(row)

    _score_rows(scanned_rows)
    by_timeframe = build_comparison(scanned_rows, timeframes, top_n)

    top_v62 = sorted(
        [asdict(row) for row in scanned_rows if row.version == "v6.2"],
        key=lambda row: (row["quality_score"], row["net_profit_pct"], row["profit_factor"]),
        reverse=True,
    )[:top_n]
    top_v61 = sorted(
        [asdict(row) for row in scanned_rows if row.version == "v6.1"],
        key=lambda row: (row["quality_score"], row["net_profit_pct"], row["profit_factor"]),
        reverse=True,
    )[:top_n]

    summary = {
        "scan_seconds": round(time.time() - started, 2),
        "tested": len(scanned_rows),
        "failed": len(failures),
        "versions": versions,
        "timeframes": timeframes,
        "universe_name": universe_name,
        "symbols_count": len(symbols),
        "symbols": symbols,
        "avg_winrate": round(sum(row.win_rate for row in scanned_rows) / len(scanned_rows), 2) if scanned_rows else 0.0,
        "avg_profit": round(sum(row.net_profit_pct for row in scanned_rows) / len(scanned_rows), 2) if scanned_rows else 0.0,
        "avg_drawdown": round(sum(row.max_drawdown_pct for row in scanned_rows) / len(scanned_rows), 2) if scanned_rows else 0.0,
        "v6_1": _version_summary(scanned_rows, "v6.1"),
        "v6_2": _version_summary(scanned_rows, "v6.2"),
    }

    return {
        "summary": summary,
        "comparison": by_timeframe,
        "top_v61": top_v61,
        "top_v62": top_v62,
        "failures": failures[:200],
    }


def universe_counts() -> Dict[str, int]:
    return {name: len(symbols) for name, symbols in UNIVERSES.items()}


@app.route("/")
def index():
    counts = universe_counts()
    return render_template(
        "market_scanner.html",
        universe_counts=counts,
        timeframes=TIMEFRAME_OPTIONS,
    )


@app.route("/api/universe")
def universe():
    return jsonify(
        {
            "universes": UNIVERSES,
            "counts": universe_counts(),
            "timeframes": TIMEFRAME_OPTIONS,
            "versions": list(VERSION_BUILDERS.keys()),
        }
    )


@app.route("/api/history")
def api_history():
    limit = int(request.args.get("limit", 20))
    return jsonify({"history": list_scan_history(limit=limit)})


@app.route("/api/history/<int:scan_id>")
def api_history_detail(scan_id: int):
    result = get_scan_result(scan_id)
    if not result:
        return jsonify({"error": "scan no encontrado"}), 404
    return jsonify(result)


@app.route("/api/scan", methods=["POST"])
def api_scan():
    payload = request.get_json(silent=True) or {}
    universe_name = str(payload.get("universe_name") or "clean")
    versions = payload.get("versions") or ["v6.1", "v6.2"]
    timeframes = payload.get("timeframes") or ["15m", "1h"]
    top_n = int(payload.get("top_n", 15))
    min_trades = int(payload.get("min_trades", 20))
    max_workers = int(payload.get("max_workers", 8))
    provider = str(payload.get("provider") or "bitget")
    custom_symbols_raw = str(payload.get("custom_symbols") or "").strip()

    if custom_symbols_raw:
        symbols = [s.strip().upper() for s in custom_symbols_raw.split(",") if s.strip()]
        universe_name = "custom"
    else:
        if universe_name not in UNIVERSES:
            return jsonify({"error": f"universo no soportado: {universe_name}"}), 400
        symbols = UNIVERSES[universe_name]

    bad_versions = [version for version in versions if version not in VERSION_BUILDERS]
    if bad_versions:
        return jsonify({"error": f"versiones no soportadas: {', '.join(bad_versions)}"}), 400
    bad_tfs = [tf for tf in timeframes if tf not in TIMEFRAME_OPTIONS]
    if bad_tfs:
        return jsonify({"error": f"timeframes no soportados: {', '.join(bad_tfs)}"}), 400

    result = run_scan(universe_name, symbols, versions, timeframes, top_n, min_trades, max_workers, provider)
    scan_id = save_scan_result(result, timeframes, top_n, min_trades, max_workers)
    result["summary"]["scan_id"] = scan_id
    return jsonify(result)


init_db()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=True)
