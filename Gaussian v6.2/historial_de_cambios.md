# Historial de Cambios - Gaussian v6.2

En este archivo registraremos todas las modificaciones realizadas a la estrategia de TradingView (Pine Script) y a la configuración del servidor Python.

## Formato de Registro
- **Fecha**: [AAAA-MM-DD]
- **Componente**: [TradingView / Bot Python / Config]
- **Cambio**: [Descripción del cambio]
- **Razón**: [Por qué se hizo el cambio]

---

## Registros

### 2026-03-13 13:00
- **Símbolo**: General (Pine Script v6)
- **Descripción del Fallo**: Errores de compilación masivos (>18). Incluía tipos "position" inválidos, redefinición de variables MTF (`m5Bull`, etc.) y multi-declaraciones ilegales en v6.
- **Posible Causa**: Degradación de código durante la sincronización de versiones y cambios restrictivos en el compilador @version=6 de TradingView.
- **Solución/Estado**: Resuelto. Se realizó una limpieza total de variables duplicadas, se corrigió el tipado a string y se separaron todas las declaraciones. Estrategia e Indicador sincronizados al 100%.

### 2026-03-13 (Mejoras de Fiabilidad y Compatibilidad)
- **Componente**: Bot Python (`bot_v62.py`)
- **Cambio**: 
    1. Implementada limpieza de prefijos `BITGET:` y `BINANCE:` en símbolos.
    2. Añadido soporte para variable de entorno `BG_PASS`.
    3. Implementado endpoint `/logs` para monitoreo remoto.
    4. Añadido reintento de órdenes (3 intentos) y pausa de seguridad en flips.
- **Razón**: Resolver fallos de ejecución detectados en Railway y mejorar la robustez ante errores del exchange o latencia.
29: 
30: ### 2026-03-14 (Ajuste de Límites y Sync Robusta)
31: - **Componente**: Bot Python (`bot_v62.py`)
32: - **Cambio**: 
33:     1. Aumento de límites: `MAX_TOTAL_TRADES` subido a 15 y `LEVERAGE` a 12x.
34:     2. Implementada actualización automática de `Registro_de_Trades.xlsx` en cada evento (apertura, cierre, sl_update) y mediante un hilo periódico (cada 10 min).
35:     3. Rediseño completo de `sync_positions_from_exchange` para reconciliación bidireccional (cierra huérfanos y agrega faltantes).
36: - **Razón**: Solicitud del usuario para permitir más operaciones y corregir inconsistencias en el Excel.

### 2026-03-14 22:11
- **Componente**: Bot Python (`bot_v62.py`)
- **Cambio**: Aumento de `MAX_TOTAL_TRADES` de 15 a 20.
- **Razón**: Solicitud del usuario para ampliar la capacidad de operaciones simultáneas.
