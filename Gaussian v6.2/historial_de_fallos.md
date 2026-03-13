# Historial de Fallos - Gaussian v6.2

En este archivo registraremos todos los errores, bugs y comportamientos inesperados detectados durante la operación de la estrategia y el bot.

## Formato de Registro
- **Fecha**: [AAAA-MM-DD HH:MM]
- **Símbolo**: [BTCUSDT, etc.]
- **Descripción del Fallo**: [Qué sucedió]
- **Posible Causa**: [Análisis inicial]
- **Solución/Estado**: [Pendiente / Investigando / Resuelto]

---

## Registros

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
