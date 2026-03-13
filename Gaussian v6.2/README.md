# Gaussian v6.2 - Guía de Sincronización

Esta carpeta contiene la versión 6.2 de la estrategia Gaussian Trend IA Pro.

## Archivos en esta carpeta
- `Gaussian Trend IA Pro v6.2 Extreme Strategy.pine`: Estrategia activa.
- `bot_v62.py`: Servidor de trading Python (aislado).
- `.env.template`: Plantilla para credenciales de Bitget.
- `historial_de_cambios.md`: Registro de modificaciones.
- `historial_de_fallos.md`: Registro de errores operativos.

## Aislamiento Total (v6.2)
Esta versión está diseñada para funcionar de forma independiente:
1. **Puerto**: El bot por defecto usa el puerto `5002`.
2. **Archivos de Estado**: Usa `trades_v62.json` y `webhook_events_v62.json` locales.
3. **Configuración**: Lee el archivo `.env` dentro de esta misma carpeta.

## Cómo Iniciar el Bot
1. Instala las dependencias si no las tienes: `pip install flask ccxt`
2. Copia `.env.template` a `.env` y pon tus API Keys de Bitget.
3. Ejecuta: `python3 bot_v62.py`

## 🚀 Automatización con Master Monitor (Recomendado)
Este es el método profesional para gestionar 60+ monedas con solo 2 alertas.

1. **Añadir Scripts**: Asegúrate de haber cargado `Gaussian_v62_Monitor_P1.pine` y `Gaussian_v62_Monitor_P2.pine` en tu gráfico.
2. **Configurar Alerta**:
   - Pulsa el icono de Alerta (reloj) en TradingView.
   - **Condición**: Selecciona `Gaussian v6.2 Master Monitor - Part 1`.
   - **Trigger**: Selecciona `Any alert() function call`.
   - **Webhook URL**: Pega tu URL de Railway (ej. `https://tu-proyecto.up.railway.app/webhook`).
3. **Repetir**: Haz lo mismo para la **Parte 2**.

## 🔌 Conexión (Webhook)
El servidor Python activo (`bot_v62.py`) escucha en el endpoint `/webhook`.
En Railway, asegúrate de que el dominio público esté configurado y que apunte al servicio donde corre el bot.

## 📊 Estado del Bot
Puedes entrar a `https://tu-proyecto.up.railway.app/status` para ver:
- Trades activos en Bitget.
- Historial de eventos de webhooks recibidos.
- Modo de operación (Real o Dry Run).

