# Bitget -> TradingView Watchlists

## Resumen

- Total `USDT-FUTURES` activos en Bitget: `539`
- Sin `RWA`: `499`
- Lista cripto usable para TradingView (heurística): `496`

## Archivos

- Lista recomendada para TradingView con prefijo exchange:
  - `tradingview_bitget_crypto_watchlist_prefixed.txt`
- Lista simple sin prefijo exchange:
  - `tradingview_bitget_crypto_watchlist_plain.txt`
- Universo completo USDT-FUTURES:
  - `bitget_usdt_futures_all_symbols.txt`
- Universo sin RWA:
  - `bitget_usdt_futures_non_rwa_symbols.txt`

## Partes

Si TradingView se pone incómodo con una lista muy larga, usa estas partes:

- `tradingview_bitget_crypto_watchlist_part_1.txt`
- `tradingview_bitget_crypto_watchlist_part_2.txt`
- `tradingview_bitget_crypto_watchlist_part_3.txt`
- `tradingview_bitget_crypto_watchlist_part_4.txt`
- `tradingview_bitget_crypto_watchlist_part_5.txt`

## Recomendación

Para importar en TradingView usa primero:

- `tradingview_bitget_crypto_watchlist_prefixed.txt`

Si TradingView no reconoce el formato con exchange, prueba:

- `tradingview_bitget_crypto_watchlist_plain.txt`

## Nota

La lista `crypto` usa una heurística:

- excluye contratos `RWA`
- excluye algunos tickers de estilo tradfi muy obvios como `SPY`, `TSM`, `AVGO`, `WMT`, `COST`, `XOM`, `OXY`, `COPPER`

Puede quedar algún contrato temático raro mezclado, porque Bitget no separa perfecto todo el universo en el payload.
