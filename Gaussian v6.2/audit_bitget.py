import os, sys, json
import ccxt
from pathlib import Path

BASE_DIR = Path("/Users/felipe/Desktop/Codigo/Gaussian/Gaussian v6.2")

def load_env():
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip().strip('"').strip("'")

load_env()

try:
    exchange = ccxt.bitget({
        'apiKey': os.environ.get("BITGET_API_KEY"),
        'secret': os.environ.get("BITGET_API_SECRET"),
        'password': os.environ.get("BITGET_PASSWORD") or os.environ.get("BG_PASS"),
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True,
    })
    
    print("--- ACCOUNT BALANCE ---")
    bal = exchange.fetch_balance()
    usdt_bal = bal.get('USDT', {})
    print(f"USDT Total: {usdt_bal.get('total')}")
    print(f"USDT Free:  {usdt_bal.get('free')}")
    
    print("\n--- RECENT TRADES ---")
    try:
        # fetch historical orders or trades
        # CCXT bitget fetch_my_trades might need symbol, or we can use fetch_orders
        # For Bitget Swap, we may need to use fetchMyTrades with specific symbols, or implicit
        # let's try implicit
        trades = exchange.fetch_my_trades(params={'productType': 'USDT-FUTURES', 'limit': 50})
        for t in trades[-10:]:
            print(f"[{t['datetime']}] {t['symbol']} | {t['side']} | amt:{t['amount']} | price:{t['price']} | cost:{t['cost']} | fee:{t.get('fee', {}).get('cost', 0)}")
    except Exception as e:
        print("Couldn't fetch trades without symbol:", e)
        # fallback: load symbols from symbols_v62.txt
        sym_file = BASE_DIR / "symbols_v62.txt"
        if sym_file.exists():
            symbols = [line.strip() for line in sym_file.read_text().splitlines() if line.strip()]
            for sym in symbols[:5]: # just check first 5
                ccxt_sym = sym.replace("BITGET:", "").replace(".P", "").replace("PERP", "USDT") + ":USDT" if "USDT" not in sym else sym.replace("BITGET:", "")+":USDT"
                try:
                    trades = exchange.fetch_my_trades(symbol=ccxt_sym, limit=5, params={'productType': 'USDT-FUTURES'})
                    for t in trades:
                        print(f"[{t['datetime']}] {t['symbol']} | {t['side']} | amt:{t['amount']} | price:{t['price']} | cost:{t['cost']} | fee:{t.get('fee', {}).get('cost', 0)}")
                except: pass

except Exception as e:
    import traceback
    traceback.print_exc()

