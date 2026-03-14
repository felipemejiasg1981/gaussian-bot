# Historial de Fallos - Gaussian v6.2

En este archivo registraremos todos los errores, bugs y comportamientos inesperados detectados durante la operación de la estrategia y el bot.

## Formato de Registro
- **Fecha**: [AAAA-MM-DD HH:MM]
- **Símbolo**: [BTCUSDT, etc.]
- **Descripción del Fallo**: [Qué sucedió]
- **Posible Causa**: [Análisis inicial]
- **Solución/Estado**: [Pendiente / Investigando / Resuelto]

---

### 2026-03-13 18:30
- **Símbolo**: BERA (y otros de alta volatilidad)
- **Descripción del Fallo**: El bot abría más posiciones de las permitidas o posiciones duplicadas simultáneas a pesar de las protecciones de memoria (`trades_abiertos`).
- **Posible Causa**: "Condición de Carrera" (Race Condition). TradingView disparaba dos webhooks idénticos en menos de 0.1s. Ambos webhooks leían que no había trade en memoria al mismo tiempo, y ambos ordenaban a Bitget abrir.
- **Solución/Estado**: Resuelto. Se implementó un `threading.Lock()` en el servidor Flask para procesar webhooks de uno en uno en fila. Además se añadió un chequeo `State-less` usando `ex.fetch_positions()` antes de cada `OPEN`, garantizando que Bitget real no tiene una orden para el símbolo antes de lanzarla.

### 2026-03-13 20:50
- **Símbolo**: Todos
- **Descripción del Fallo**: Error intermitente en el log de TradingView: "Error en la entrega del webhook: request took too long and timed out". Además los TP (cierres parciales) aparentaban éxito en memoria pero Bitget nunca ejecutaba la venta parcial (Silent fail). Las alertas de `re-entry` y `update_sl` (Trailing / Break-Even) no reaccionaban.
- **Posible Causa**: 
  1. *Timeout:* El bot ejecutó webhooks sincrónicamente. Cuando las llamadas a la API de Bitget tomaban más de 3 segundos (Límite máximo que permite TradingView), se forzaba un Time-Out cancelando la conexión de su lado.
  2. *Cierres Silenciosos:* Si fallaba el inicio en la DB remota o Railway se reiniciaba, el caché temporal lo borraba, forzando fallos de chequeos. 
  3. *Endpoints Faltantes:* Los tipos de alerta `update_sl` y `reentry` no tenían bloque condicional `if` programado en `bot_v62.py`.
- **Solución/Estado**: Resuelto.
  - **Bot Resiliente:** Refactor completo del Endpoint principal (`/webhook`). Ahora responde HTTP 200 INMEDIATO y procesa el tradeo como Tarea de Fondo (Background Thread `process_webhook_logic`).
  - **Stateless:** `cerrar_posicion`, `cerrar_parcial` extraen la talla y la presencia directamente revisando el book de exchange. (Con soporte a un predefinido `pct=30.0` si TradingView envía payload trunco).
  - **Nuevas Lógicas Añadidas y Verificadas:** `actualizar_sl` reprogramado correctamente en el código; maneja triggers SL; lógica limpia para escalar DCA en `reentry`.## Registros

### 2026-03-13 15:10
- **Símbolo**: General (Todos)
- **Descripción del Fallo**: Órdenes rechazadas con el mensaje "Confianza insuficiente (Esperaba 100 para 5 estrellas)". 
- **Posible Causa**: El servidor Railway estaba ejecutando código antiguo (v5/v6.1) porque la carpeta `Gaussian v6.2` y los nuevos archivos de arranque (`main.py`) no se habían subido a GitHub (estaban como untracked).
- **Solución/Estado**: Resuelto. Se sincronizó el repositorio local con GitHub (`git add .`, `commit`, `push`). Railway ya está desplegando la versión 6.2 real sin el filtro de estrellas.

### 2026-03-13 15:30
- **Símbolo**: SAFEUSDT.P (y otros con prefijo)
- **Descripción del Fallo**: El bot recibía `BITGET:SAFEUSDT.P` pero el exchange rechazaba el símbolo por incluir el prefijo del exchange.
- **Posible Causa**: Falta de normalización en la función `par_ccxt` para el prefijo "BITGET:".
- **Solución/Estado**: Resuelto. Se actualizó la lógica para limpiar cualquier texto antes de los dos puntos (`:`).

---
