import os
import sqlite3
import ccxt
from pathlib import Path
from datetime import datetime

BASE_DIR = Path("/Users/felipe/Desktop/Codigo/Gaussian/Gaussian v6.2")
DB_PATH = BASE_DIR / "trade_analytics_v62.db"

def load_env():
    for line in (BASE_DIR / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

def sync_bitget():
    load_env()
    exchange = ccxt.bitget({
        'apiKey': os.environ.get("BITGET_API_KEY"),
        'secret': os.environ.get("BITGET_API_SECRET"),
        'password': os.environ.get("BITGET_PASSWORD") or os.environ.get("BG_PASS"),
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True
    })
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("Sincronizando posiciones abiertas desde Bitget...")
    poss = exchange.fetch_positions()
    
    inserted_open = 0
    updated_open = 0
    
    symbols_to_check = set()
    
    for p in poss:
        contracts = abs(float(p.get('contracts', 0) or 0))
        if contracts > 0:
            symbol = p['symbol'].replace('/', '').replace(':USDT', '')
            if 'USDT' not in symbol:
                symbol = p['symbol'].split(':')[0].replace('/', '')
                
            side = p['side']  # 'long' or 'short'
            if side == 'long': side = 'buy'
            if side == 'short': side = 'sell'
            
            entry_price = float(p.get('entryPrice', 0))
            leverage = int(p.get('leverage', 10))
            
            # Record symbol for further history check
            symbols_to_check.add(p['symbol'])
            
            # Check if this open position already exists in the DB
            c.execute("SELECT id, contracts FROM trades WHERE symbol = ? AND side = ? AND status = 'open'", (symbol, side))
            existing = c.fetchone()
            
            if not existing:
                # Insert it
                print(f" [+] Añadiendo nueva posición ABIERTA: {symbol} {side.upper()} @ {entry_price}")
                c.execute('''INSERT INTO trades 
                    (trade_id, symbol, side, entry_price, leverage, contracts, opened_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'open')''', 
                    (f"SYNC-BITGET-{int(datetime.now().timestamp())}", symbol, side, entry_price, leverage, contracts, datetime.now().strftime('%Y-%m-%d %H:%M')))
                inserted_open += 1
            else:
                # Posi exist, maybe update contracts if different
                if abs(float(existing['contracts'] or 0) - contracts) > 0.1:
                    print(f" [~] Actualizando contratos para {symbol} {side.upper()}: {existing['contracts']} -> {contracts}")
                    c.execute("UPDATE trades SET contracts = ?, entry_price = ? WHERE id = ?", (contracts, entry_price, existing['id']))
                    updated_open += 1
    
    conn.commit()
    print(f" -> {inserted_open} posiciones insertadas, {updated_open} actualizadas.\n")
    
    # Also fetch history for recently seen symbols in user screenshot just to be safe:
    # We saw: RDNT, IN, PHB, RED, KERNEL, UAI, DOLO, DOOD, AZTEC, LIGHT, PROVE, BERA, KMNO, RECALL, SAFE
    extra_symbols = ['RDNT/USDT:USDT', 'IN/USDT:USDT', 'PHB/USDT:USDT', 'RED/USDT:USDT', 'KERNEL/USDT:USDT', 
                     'UAI/USDT:USDT', 'DOLO/USDT:USDT', 'DOOD/USDT:USDT', 'AZTEC/USDT:USDT', 'LIGHT/USDT:USDT', 
                     'PROVE/USDT:USDT', 'BERA/USDT:USDT', 'KMNO/USDT:USDT', 'RECALL/USDT:USDT', 'SAFE/USDT:USDT', '1INCH/USDT:USDT', '4/USDT:USDT']
    for sym in extra_symbols:
        symbols_to_check.add(sym)
        
    print("Buscando trades cerrados recientes en Bitget (últimos 3 días)...")
    import time
    since = int((datetime.now().timestamp() - 3*24*3600) * 1000)
    
    inserted_closed = 0
    for sym in symbols_to_check:
        try:
            trades = exchange.fetch_my_trades(symbol=sym, since=since, limit=100)
            if not trades: continue
            
            # Agrupar las "órdenes" en un trade. Un poco complejo reconstruir el estado completo pero
            # por lo menos si vemos un 'realizedPnl' > 0 (o != 0) podemos asumir que fue un cierre.
            # ccxt fetch_my_trades might report PNL on closing legs manually.
            
            # Simple heuristic: si vemos una orden de cierre (reduceOnly / close), podemos buscar un correlato pero
            # por ahora para simplificar: si el trade en ccxt tiene PNL o reduceOnly:
            
            # Bitget usually reports PnL in the closing trades
            for t in trades:
                # CCXT parsing of bitget fees/pnl can be tricky. "info" has raw stuff
                info = t.get('info', {})
                # tradeScope could be 'close_long', 'close_short'
                t_scope = info.get('tradeScope', '')
                is_close = 'close' in t_scope.lower()
                # or orderType
                # In futures, real close trades have realized pnl
                # We won't attempt full complex reconstruction here to avoid polluting the DB,
                # but we will rely mostly on open positions for what the user is seeing right now.
                pass
                
        except Exception as e:
            # print(f"Error fetching {sym}: {e}")
            pass
            
    print("Sincronización terminada.")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    sync_bitget()
