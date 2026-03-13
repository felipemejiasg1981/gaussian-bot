#!/usr/bin/env python3
"""
Gaussian Trend Backtester — Multi-Symbol Runner
Downloads data from Binance and runs backtests across symbols + timeframes.
Generates results.json for the dashboard.

Usage:
    python3 run_backtest.py
"""
import json, os, time, sys
import requests
import pandas as pd
from gaussian_bt import GaussianBacktest

# ─── CONFIG ───────────────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "DOTUSDT",
    "MATICUSDT", "ATOMUSDT", "UNIUSDT", "AAVEUSDT", "LTCUSDT"
]

TIMEFRAMES = {
    "1h":  {"interval": "1h",  "limit": 1000, "label": "1H"},
    "4h":  {"interval": "4h",  "limit": 1000, "label": "4H"},
    "1d":  {"interval": "1d",  "limit": 1000, "label": "1D"},
}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
BINANCE_URL = "https://api.binance.com/api/v3/klines"

# ─── DATA DOWNLOAD ────────────────────────────────────────────
def download_ohlcv(symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
    """Download OHLCV data from Binance public API."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        resp = requests.get(BINANCE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ⚠ Error downloading {symbol} {interval}: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("open_time", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    bt = GaussianBacktest()

    all_results = []
    total = len(SYMBOLS) * len(TIMEFRAMES)
    current = 0

    print("=" * 60)
    print("  🔮 Gaussian Trend Backtester")
    print(f"  {len(SYMBOLS)} symbols × {len(TIMEFRAMES)} timeframes = {total} tests")
    print("=" * 60)

    for symbol in SYMBOLS:
        for tf_key, tf_conf in TIMEFRAMES.items():
            current += 1
            pct = round(current / total * 100)
            print(f"  [{pct:3d}%] {symbol} {tf_conf['label']}...", end=" ", flush=True)

            df = download_ohlcv(symbol, tf_conf["interval"], tf_conf["limit"])
            if df.empty or len(df) < 100:
                print("⚠ skip (insufficient data)")
                continue

            result = bt.run(df)
            result["symbol"] = symbol.replace("USDT", "")
            result["timeframe"] = tf_conf["label"]
            result["candles"] = len(df)

            # Remove trades detail for the overview (keep equity)
            result_summary = {k: v for k, v in result.items() if k != "trades"}
            all_results.append(result_summary)

            # Color indicator
            wr = result["win_rate"]
            icon = "🟢" if wr >= 50 else "🟡" if wr >= 40 else "🔴"
            pnl = result["total_pnl"]
            print(f"{icon} WR:{wr}%  PnL:{pnl:+.1f}%  Trades:{result['total_trades']}  DD:{result['max_drawdown']:.1f}%")

            time.sleep(0.2)  # Rate limit courtesy

    # Sort by total PnL descending
    all_results.sort(key=lambda x: x["total_pnl"], reverse=True)

    # Save results
    output_file = os.path.join(OUTPUT_DIR, "results.json")
    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print()
    print("=" * 60)
    print(f"  ✅ Done! {len(all_results)} backtests completed")
    print(f"  📄 Results saved to: {output_file}")
    print(f"  🌐 Open dashboard.html in your browser to see results")
    print("=" * 60)

    # Quick summary
    if all_results:
        profitable = [r for r in all_results if r["total_pnl"] > 0]
        print(f"\n  📊 Quick Summary:")
        print(f"     Profitable: {len(profitable)}/{len(all_results)} ({round(len(profitable)/len(all_results)*100)}%)")
        avg_wr = round(sum(r["win_rate"] for r in all_results) / len(all_results), 1)
        avg_pnl = round(sum(r["total_pnl"] for r in all_results) / len(all_results), 2)
        print(f"     Avg WR: {avg_wr}%")
        print(f"     Avg PnL: {avg_pnl:+.2f}%")
        print(f"\n  🏆 Top 5:")
        for r in all_results[:5]:
            print(f"     {r['symbol']:8s} {r['timeframe']:3s}  WR:{r['win_rate']}%  PnL:{r['total_pnl']:+.1f}%")

if __name__ == "__main__":
    main()
