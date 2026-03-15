import ccxt
import pandas as pd
import numpy as np
import statsmodels.tsa.stattools as ts
import time
import requests
from itertools import combinations
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_top_symbols(limit=30):
    print("Obteniendo los mercados más líquidos de Bitget...")
    exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})
    exchange.load_markets()
    
    symbols_vol = []
    for symbol in exchange.markets:
        market = exchange.markets[symbol]
        if market['active'] and market['quote'] == 'USDT' and market['linear']:
            symbols_vol.append(symbol)
            
    # We will just fetch a set of known top coins to make it faster for this test
    top_coins = [
        'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 'XRP/USDT:USDT', 'ADA/USDT:USDT', 'AVAX/USDT:USDT',
        'LINK/USDT:USDT', 'LTC/USDT:USDT', 'BCH/USDT:USDT', 'MATI/USDT:USDT', 'DOT/USDT:USDT', 'NEAR/USDT:USDT',
        'APT/USDT:USDT', 'SUI/USDT:USDT', 'ARB/USDT:USDT', 'OP/USDT:USDT', 'INJ/USDT:USDT', 'RNDR/USDT:USDT',
        'TIA/USDT:USDT', 'SEI/USDT:USDT', 'MATIC/USDT:USDT', 'DOGE/USDT:USDT', 'SHIB/USDT:USDT', 'PEPE/USDT:USDT'
    ]
    
    available = [s for s in top_coins if s in exchange.markets]
    return available

def fetch_data(symbols, timeframe='1h', limit=500):
    exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})
    df_dict = {}
    print(f"Descargando datos históricos (velas de {timeframe}) para {len(symbols)} monedas...")
    
    for sym in symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df_dict[sym] = df['close']
            time.sleep(0.1)  # rate limit
        except Exception as e:
            # print(f"Error con {sym}: {e}")
            pass
            
    data = pd.DataFrame(df_dict)
    data = data.dropna() # keep only times where we have data for all
    return data

def find_cointegrated_pairs(data):
    print("\nAnalizando todos los pares posibles (Buscando el 'elástico')...")
    n = data.shape[1]
    keys = data.columns
    pairs = []
    
    # Check every possible combination
    count = 0
    total = (n * (n - 1)) / 2
    
    for i in range(n):
        for j in range(i+1, n):
            asset_1 = keys[i]
            asset_2 = keys[j]
            
            S1 = data[asset_1]
            S2 = data[asset_2]
            
            # Cointegration test
            score, pvalue, _ = ts.coint(S1, S2)
            
            # Correlation
            correlation = S1.corr(S2)
            
            if pvalue < 0.05 and correlation > 0.7:  # Highly cointegrated and somewhat correlated positively
                pairs.append({
                    'Pair 1': asset_1,
                    'Pair 2': asset_2,
                    'p-value': pvalue,
                    'Correlation': correlation
                })
            
            count += 1
            if count % 50 == 0:
                print(f"Progreso: {count}/{int(total)} combinaciones analizadas...")
                
    df_res = pd.DataFrame(pairs)
    if not df_res.empty:
        df_res = df_res.sort_values(by='p-value', ascending=True)
    return df_res

if __name__ == '__main__':
    symbols = get_top_symbols()
    data = fetch_data(symbols, timeframe='15m', limit=1000)
    
    if data.empty:
        print("No se pudieron descargar los datos.")
    else:
        results = find_cointegrated_pairs(data)
        
        print("\n🏆 TOP PARES COINTEGRADOS ENCONTRADOS 🏆")
        print("="*60)
        print("Nota: Un p-value menor a 0.05 significa que el par es estadísticamente viable.")
        print("="*60)
        
        if results.empty:
            print("No se encontraron pares fuertemente cointegrados en este set de datos.")
        else:
            print(results.head(10).to_string(index=False))
            print("\nGuardando resultados en 'pares_recomendados.txt'...")
            results.head(10).to_csv('pares_recomendados.txt', sep='\t', index=False)
