import os
import time
import ccxt
import pandas as pd
import numpy as np
import statsmodels.api as sm
from pathlib import Path
import datetime
import csv

BASE_DIR = Path(__file__).resolve().parent

# Configuración del Bot
TRADE_AMOUNT_USDT = 6.0  # $6 por pata (pierna) para cumplir mínimos de Bitget
Z_SCORE_ENTRY = 2.0      # Umbral para entrar al mercado
Z_SCORE_EXIT = 0.5       # Umbral para salir (acercándose a 0)
TIMEFRAME = '15m'
KLINES_LIMIT = 500
LEVERAGE = 10

# Los mejores pares encontrados en la Fase 1
PORTFOLIO = [
    ('ETH/USDT:USDT', 'INJ/USDT:USDT'),
    ('NEAR/USDT:USDT', 'SUI/USDT:USDT'),
    ('SOL/USDT:USDT', 'LINK/USDT:USDT'),
    ('BTC/USDT:USDT', 'XRP/USDT:USDT'),
    ('SOL/USDT:USDT', 'ADA/USDT:USDT')
]

# Variables de estado y persistencia
PAIRS_STATE_FILE = BASE_DIR / "pairs_state.json"
active_positions = {f"{p1}-{p2}": False for p1, p2 in PORTFOLIO}
position_details = {f"{p1}-{p2}": {} for p1, p2 in PORTFOLIO}

def save_pairs_state():
    try:
        data = {
            "active_positions": active_positions,
            "position_details": position_details
        }
        with open(PAIRS_STATE_FILE, 'w') as f:
            import json
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f" [!] Error guardando estado de pares: {e}")

def load_pairs_state():
    global active_positions, position_details
    try:
        if PAIRS_STATE_FILE.exists():
            with open(PAIRS_STATE_FILE, 'r') as f:
                import json
                data = json.load(f)
                active_positions = data.get("active_positions", active_positions)
                position_details = data.get("position_details", position_details)
                print(f" ✅ Estado de pares cargado desde {PAIRS_STATE_FILE}")
    except Exception as e:
        print(f" [!] Error cargando estado de pares: {e}")

load_pairs_state()

def load_env():
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

def get_exchange():
    load_env()
    return ccxt.bitget({
        'apiKey': os.environ.get("BITGET_API_KEY"),
        'secret': os.environ.get("BITGET_API_SECRET"),
        'password': os.environ.get("BITGET_PASSWORD") or os.environ.get("BG_PASS"),
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True
    })

def calculate_zscore(S1, S2):
    # OLS Regression for hedge ratio (beta)
    S1 = sm.add_constant(S1)
    results = sm.OLS(S2, S1).fit()
    S1 = S1['close1'] # Remove constant for further math
    b = results.params['close1']
    
    spread = S2 - b * S1
    mean_spread = spread.mean()
    std_spread = spread.std()
    
    if std_spread == 0:
        return 0, b
        
    zscore = (spread.iloc[-1] - mean_spread) / std_spread
    return zscore, b

def set_leverage_for_symbol(exchange, symbol, lev):
    try:
        exchange.set_leverage(lev, symbol)
    except Exception as e:
        # Ignore if leverage is already set or API doesn't support direct modification in testnet
        pass

def place_order(exchange, symbol, side, usdt_amount=None, qty=None):
    try:
        # Get market rules to calculate correct contracts
        exchange.load_markets()
        market = exchange.market(symbol)
        
        # Get current price
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        
        # In bitget inverse/linear contracts, amount is often in 'contracts', we calculate based on contractSize
        contract_size = float(market['info'].get('sizeMultiplier', 1)) 
        if contract_size == 0 or contract_size is None:
            contract_size = 1.0
            
        if qty is None and usdt_amount is not None:
            amount_crypto = usdt_amount / current_price
            contracts = amount_crypto / contract_size
            
            # Apply market limits and precision
            min_amount = market['limits']['amount']['min'] or 0
            if contracts < min_amount:
                print(f" [!] {symbol}: El monto ${usdt_amount} ({contracts} cont) es menor al mínimo permitido ({min_amount}).")
                contracts = min_amount
        else:
            contracts = qty
            
        contracts = exchange.amount_to_precision(symbol, contracts)
        
        print(f" [+] Ejecutando orden a MKT en {symbol}: {side.upper()} {contracts} contratos.")
        # execute
        order = exchange.create_market_order(symbol, side, contracts)
        return order, float(contracts), float(current_price), float(contract_size)
        
    except Exception as e:
        print(f" [X] Error ejecutando orden {side} en {symbol}: {e}")
        return None, 0, 0, 1

import openpyxl

def log_trade(pair_name, p1_sym, p2_sym, pnl_usd, duration_mins):
    excel_file = BASE_DIR / "Pairs_History.xlsx"
    file_exists = excel_file.exists()
    
    if not file_exists:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Historial Pairs"
        ws.append(['Fecha/Hora Cierre', 'Par', 'Symbol 1', 'Symbol 2', 'PnL Total (USDT)', 'Duracion (Minutos)'])
    else:
        wb = openpyxl.load_workbook(excel_file)
        ws = wb.active
        
    ws.append([
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        pair_name,
        p1_sym,
        p2_sym,
        round(pnl_usd, 4),
        round(duration_mins, 1)
    ])
    
    # Auto ancho de columnas
    for col in ws.columns:
        max_length = 0
        column_letter = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        ws.column_dimensions[column_letter].width = max_length + 2
        
    wb.save(excel_file)
    print(f" 💾 Historial guardado en Pairs_History.xlsx -> PnL: ${pnl_usd:.4f}")

def run_bot():
    exchange = get_exchange()
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Iniciando Pairs Trading Bot (Mean Reversion)...")
    print(f"Configuración: ${TRADE_AMOUNT_USDT} por pierna | Z-Entry: {Z_SCORE_ENTRY} | Z-Exit: {Z_SCORE_EXIT}\n")
    
    # Set leverage
    for p1, p2 in PORTFOLIO:
        set_leverage_for_symbol(exchange, p1, LEVERAGE)
        set_leverage_for_symbol(exchange, p2, LEVERAGE)

    iteration = 1
    while True:
        try:
            print(f"\n--- Iteración #{iteration} [{datetime.datetime.now().strftime('%H:%M:%S')}] ---")
            
            for p1, p2 in PORTFOLIO:
                pair_name = f"{p1}-{p2}"
                
                # Fetch Klines
                try:
                    ohlcv1 = exchange.fetch_ohlcv(p1, timeframe=TIMEFRAME, limit=KLINES_LIMIT)
                    ohlcv2 = exchange.fetch_ohlcv(p2, timeframe=TIMEFRAME, limit=KLINES_LIMIT)
                except Exception as e:
                    print(f"Error de red bajando datos de {pair_name}: {e}")
                    continue
                    
                df1 = pd.DataFrame(ohlcv1, columns=['t', 'o', 'h', 'l', 'close1', 'v'])
                df2 = pd.DataFrame(ohlcv2, columns=['t', 'o', 'h', 'l', 'close2', 'v'])
                
                # Ensure they align
                min_len = min(len(df1), len(df2))
                df1 = df1.iloc[-min_len:]
                df2 = df2.iloc[-min_len:]
                
                zscore, beta = calculate_zscore(df1['close1'], df2['close2'])
                is_active = active_positions[pair_name]
                
                print(f"📊 {pair_name} | Z-Score: {zscore:.2f} | En posición: {'Sí' if is_active else 'No'}")
                
                # LÓGICA DE SALIDA (EXIT)
                if is_active:
                    # Si cruzó hacia el 0 o está muy cerca
                    if abs(zscore) < Z_SCORE_EXIT:
                        print(f" 💰 [SALIDA] {pair_name}: El Z-Score volvió a la zona de confort. Cerrando posiciones...")
                        st = position_details[pair_name]
                        
                        side_p1 = 'buy' if st['p1_side'] == 'sell' else 'sell'
                        side_p2 = 'buy' if st['p2_side'] == 'sell' else 'sell'
                        
                        o1, _, p1_exit_price, _ = place_order(exchange, p1, side_p1, qty=st['p1_contracts'])
                        o2, _, p2_exit_price, _ = place_order(exchange, p2, side_p2, qty=st['p2_contracts'])
                        
                        # Calculo de PnL en base al lado y precios de entrada y salida
                        # PnL Long = (Exit - Entry) * Contracts * Multiplier
                        # PnL Short = (Entry - Exit) * Contracts * Multiplier
                        p1_pnl = (p1_exit_price - st['p1_entry']) * st['p1_contracts'] * st['p1_mult'] if st['p1_side'] == 'buy' else (st['p1_entry'] - p1_exit_price) * st['p1_contracts'] * st['p1_mult']
                        p2_pnl = (p2_exit_price - st['p2_entry']) * st['p2_contracts'] * st['p2_mult'] if st['p2_side'] == 'buy' else (st['p2_entry'] - p2_exit_price) * st['p2_contracts'] * st['p2_mult']
                        
                        total_pnl = p1_pnl + p2_pnl
                        duration_mins = (time.time() - st['time_entry']) / 60.0
                        
                        log_trade(pair_name, p1, p2, total_pnl, duration_mins)
                        
                        active_positions[pair_name] = False
                        position_details[pair_name] = {}
                        save_pairs_state()
                        
                # LÓGICA DE ENTRADA (ENTRY)
                elif not is_active:
                    if zscore > Z_SCORE_ENTRY:
                        print(f" 🚀 [ENTRADA] {pair_name}: Z-Score excesivamente ALTO (+{zscore:.2f}).")
                        print(f"     => Acortando {p2} (SELL) y Poniendo largo a {p1} (BUY)")
                        o1, c1, p1_precio, p1_mult = place_order(exchange, p1, 'buy', usdt_amount=TRADE_AMOUNT_USDT)
                        o2, c2, p2_precio, p2_mult = place_order(exchange, p2, 'sell', usdt_amount=TRADE_AMOUNT_USDT)
                        
                        if o1 and o2:
                            active_positions[pair_name] = True
                            position_details[pair_name] = {
                                'p1_side': 'buy', 'p2_side': 'sell',
                                'p1_contracts': c1, 'p2_contracts': c2,
                                'p1_entry': p1_precio, 'p2_entry': p2_precio,
                                'p1_mult': p1_mult, 'p2_mult': p2_mult,
                                'time_entry': time.time()
                            }
                            save_pairs_state()
                            
                    elif zscore < -Z_SCORE_ENTRY:
                        print(f" 🚀 [ENTRADA] {pair_name}: Z-Score excesivamente BAJO ({zscore:.2f}).")
                        print(f"     => Poniendo largo a {p2} (BUY) y Acortando a {p1} (SELL)")
                        o1, c1, p1_precio, p1_mult = place_order(exchange, p1, 'sell', usdt_amount=TRADE_AMOUNT_USDT)
                        o2, c2, p2_precio, p2_mult = place_order(exchange, p2, 'buy', usdt_amount=TRADE_AMOUNT_USDT)
                        
                        if o1 and o2:
                            active_positions[pair_name] = True
                            position_details[pair_name] = {
                                'p1_side': 'sell', 'p2_side': 'buy',
                                'p1_contracts': c1, 'p2_contracts': c2,
                                'p1_entry': p1_precio, 'p2_entry': p2_precio,
                                'p1_mult': p1_mult, 'p2_mult': p2_mult,
                                'time_entry': time.time()
                            }
                            save_pairs_state()
                            
            iteration += 1
            # Wait 1 minute before checking again
            print("\nEsperando 60 segundos para próxima lectura...")
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("\nBot apagado por el usuario.")
            break
        except Exception as e:
            print(f"Error inesperado en loop principal: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_bot()
