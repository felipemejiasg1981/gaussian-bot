#!/usr/bin/env python3
import csv
import json
import math
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


BASE_URL = "https://api.bitget.com"
SYMBOLS_URL = f"{BASE_URL}/api/v2/spot/public/symbols"
CANDLES_URL = f"{BASE_URL}/api/v2/spot/market/candles"
HEADERS = {"User-Agent": "codex-bitget-metrics/1.0"}
LOOKBACK_DAYS = 365
MIN_CANDLES_FOR_RANKING = 90
MAX_WORKERS = 16
REQUEST_TIMEOUT = 20
RETRIES = 3
SSL_CONTEXT = ssl._create_unverified_context()


def get_json(url: str) -> dict:
    last_error = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(
                req,
                timeout=REQUEST_TIMEOUT,
                context=SSL_CONTEXT,
            ) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"request failed for {url}: {last_error}")


def fetch_symbols() -> list[dict]:
    payload = get_json(SYMBOLS_URL)
    if payload.get("code") != "00000":
        raise RuntimeError(f"symbols endpoint error: {payload}")
    return [
        item
        for item in payload.get("data", [])
        if item.get("quoteCoin") == "USDT" and item.get("status") == "online"
    ]


def fetch_candles(symbol: str, limit: int = LOOKBACK_DAYS) -> list[list[str]]:
    query = urllib.parse.urlencode(
        {"symbol": symbol, "granularity": "1day", "limit": str(limit)}
    )
    payload = get_json(f"{CANDLES_URL}?{query}")
    if payload.get("code") != "00000":
        raise RuntimeError(f"candles endpoint error for {symbol}: {payload}")
    candles = payload.get("data", [])
    candles.sort(key=lambda row: int(row[0]))
    return candles


def compute_metrics(symbol_info: dict) -> dict | None:
    symbol = symbol_info["symbol"]
    candles = fetch_candles(symbol)
    if len(candles) < 2:
        return None

    closes = [float(row[4]) for row in candles]
    returns = []
    equity = [1.0]
    for prev_close, close in zip(closes, closes[1:]):
        if prev_close <= 0:
            continue
        daily_return = (close / prev_close) - 1.0
        returns.append(daily_return)
        equity.append(equity[-1] * (1.0 + daily_return))

    if not returns:
        return None

    gross_profit = sum(r for r in returns if r > 0)
    gross_loss = abs(sum(r for r in returns if r < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf

    peak = equity[0]
    max_drawdown = 0.0
    for value in equity:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0
        max_drawdown = min(max_drawdown, drawdown)

    first_ts = int(candles[0][0])
    last_ts = int(candles[-1][0])
    first_close = closes[0]
    last_close = closes[-1]
    total_return = (last_close / first_close) - 1.0 if first_close > 0 else 0.0
    win_rate = sum(1 for r in returns if r > 0) / len(returns)

    return {
        "symbol": symbol,
        "baseCoin": symbol_info.get("baseCoin", ""),
        "quoteCoin": symbol_info.get("quoteCoin", ""),
        "openTime": symbol_info.get("openTime", ""),
        "candles": len(candles),
        "days_of_returns": len(returns),
        "first_date": datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc).date().isoformat(),
        "last_date": datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).date().isoformat(),
        "first_close": round(first_close, 12),
        "last_close": round(last_close, 12),
        "total_return_pct": round(total_return * 100, 4),
        "win_rate_pct": round(win_rate * 100, 4),
        "profit_factor": "inf" if math.isinf(profit_factor) else round(profit_factor, 4),
        "max_drawdown_pct": round(max_drawdown * 100, 4),
    }


def metric_key(record: dict, field: str):
    value = record[field]
    if value == "inf":
        return math.inf
    return float(value)


def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    start = time.time()
    symbols = fetch_symbols()
    results = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(compute_metrics, item): item["symbol"] for item in symbols}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                row = future.result()
                if row:
                    results.append(row)
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": symbol, "error": str(exc)})

    results.sort(key=lambda item: item["symbol"])
    ranking_pool = [row for row in results if row["candles"] >= MIN_CANDLES_FOR_RANKING]

    by_return = sorted(
        ranking_pool,
        key=lambda item: metric_key(item, "total_return_pct"),
        reverse=True,
    )
    by_win_rate = sorted(
        ranking_pool,
        key=lambda item: metric_key(item, "win_rate_pct"),
        reverse=True,
    )
    by_pf = sorted(
        ranking_pool,
        key=lambda item: metric_key(item, "profit_factor"),
        reverse=True,
    )
    by_drawdown = sorted(
        ranking_pool,
        key=lambda item: metric_key(item, "max_drawdown_pct"),
        reverse=True,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    metrics_csv = f"bitget_spot_usdt_metrics_{timestamp}.csv"
    summary_json = f"bitget_spot_usdt_summary_{timestamp}.json"
    errors_json = f"bitget_spot_usdt_errors_{timestamp}.json"

    write_csv(metrics_csv, results)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "symbols": SYMBOLS_URL,
            "candles": CANDLES_URL,
        },
        "assumptions": {
            "universe": "Bitget spot pairs with quoteCoin=USDT and status=online",
            "lookback_days": LOOKBACK_DAYS,
            "granularity": "1day",
            "ranking_min_candles": MIN_CANDLES_FOR_RANKING,
            "profitability": "last_close / first_close - 1",
            "win_rate": "share of positive daily close-to-close returns",
            "profit_factor": "sum(positive daily returns) / abs(sum(negative daily returns))",
            "max_drawdown": "worst peak-to-trough drawdown of the daily equity curve",
        },
        "counts": {
            "symbols_considered": len(symbols),
            "metrics_computed": len(results),
            "ranked_symbols": len(ranking_pool),
            "errors": len(errors),
            "elapsed_seconds": round(time.time() - start, 2),
        },
        "top_10": {
            "profitability": by_return[:10],
            "win_rate": by_win_rate[:10],
            "profit_factor": by_pf[:10],
            "max_drawdown_best": by_drawdown[:10],
        },
    }

    with open(summary_json, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    with open(errors_json, "w", encoding="utf-8") as handle:
        json.dump(errors, handle, indent=2, ensure_ascii=False)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\nmetrics_csv={metrics_csv}", file=sys.stderr)
    print(f"summary_json={summary_json}", file=sys.stderr)
    print(f"errors_json={errors_json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
