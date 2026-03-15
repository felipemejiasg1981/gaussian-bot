import os
import ccxt
from pathlib import Path

BASE_DIR = Path("/Users/felipe/Desktop/Codigo/Gaussian/Gaussian v6.2")

def load_env():
    for line in (BASE_DIR / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

load_env()
try:
    exchange = ccxt.bitget({'apiKey': os.environ.get("BITGET_API_KEY"), 'secret': os.environ.get("BITGET_API_SECRET"), 'password': os.environ.get("BITGET_PASSWORD") or os.environ.get("BG_PASS"), 'options': {'defaultType': 'swap'}, 'enableRateLimit': True})
    poss = exchange.fetch_positions()
    upnl = 0
    for p in poss:
        contracts = abs(float(p.get('contracts', 0) or 0))
        if contracts > 0:
            upnl += float(p.get('unrealizedPnl', 0) or 0)
            print(f"{p['symbol']} | {p['side']} | UPNL: {p.get('unrealizedPnl')}")
            
    print(f"Total UPNL: {upnl}")
except Exception as e:
    import traceback
    traceback.print_exc()
