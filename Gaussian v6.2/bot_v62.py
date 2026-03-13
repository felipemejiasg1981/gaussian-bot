import os
import time
import json
import threading
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify
import ccxt

app = Flask(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN AISLADA — Gaussian v6.2 Bot                 ║
# ╚══════════════════════════════════════════════════════════════╝

# Carpeta base del bot (v6.2)
BASE_DIR = Path(__file__).resolve().parent
TRADES_FILE = BASE_DIR / "trades_v62.json"
EVENTS_FILE = BASE_DIR / "webhook_events_v62.json"
ERROR_LOG = BASE_DIR / "historial_de_fallos.md"

# 🧪 MODO DE OPERACIÓN
DRY_RUN = os.environ.get("DRY_RUN", "False").lower() == "true"

# 💰 Capital por trade (en USDT)
MONTO_POR_TRADE = 8.0
LEVERAGE = 10

# 🔒 Límites de trades
MAX_TOTAL_TRADES = 10
MAX_TRADES_POR_PAR = 1
trade_lock = threading.Lock()

# ════════════════════════════════════════════════════════════════
# CARGA DE VARIABLES
# ════════════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════════════
# CONEXIÓN A BITGET
# ════════════════════════════════════════════════════════════════
exchange = None

def get_exchange():
    global exchange
    if exchange: return exchange
    if DRY_RUN: return None

    key = os.environ.get("BITGET_API_KEY")
    secret = os.environ.get("BITGET_API_SECRET")
    password = os.environ.get("BITGET_PASSWORD") or os.environ.get("BG_PASS")

    if not all([key, secret, password]):
        log("⚠️ Faltan API Keys (API_KEY, SECRET o PASSWORD/BG_PASS) en el entorno")
        return None

    try:
        exchange = ccxt.bitget({
            'apiKey': key,
            'secret': secret,
            'password': password,
            'options': {'defaultType': 'swap'},
            'enableRateLimit': True,
        })
        exchange.load_markets()
        # Asegurar modo unilateral
        try:
            exchange.set_position_mode(False, params={'productType': 'USDT-FUTURES'})
            log("🔄 Modo de posición asegurado: UNILATERAL")
        except: pass
        log("✅ Conexión con Bitget (v6.2) establecida")
        return exchange
    except Exception as e:
        log(f"❌ Error de conexión: {e}")
        return None

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{ts}] [v6.2] {msg}"
    print(formatted_msg)
    
    # Si es un error crítico, persistir en historial_de_fallos.md
    if "❌" in msg or "⚠️" in msg:
        try:
            with open(ERROR_LOG, "a") as f:
                # Si el archivo está vacío, añadir cabecera (puedo omitir esto si asumo que ya existe)
                f.write(f"\n- **Fecha**: {ts}\n- **Descripción**: {msg}\n---\n")
        except: pass

# ════════════════════════════════════════════════════════════════
# PERSISTENCIA LOCAL (AISLADA)
# ════════════════════════════════════════════════════════════════
trades_abiertos = {}
webhook_eventos = []

def save_state():
    try:
        # Mantener solo los últimos 200 eventos
        global webhook_eventos
        if len(webhook_eventos) > 200:
            webhook_eventos = webhook_eventos[-200:]
            
        with open(TRADES_FILE, 'w') as f:
            json.dump(trades_abiertos, f, indent=4)
        with open(EVENTS_FILE, 'w') as f:
            json.dump(webhook_eventos, f, indent=4)
    except Exception as e:
        log(f"⚠️ Error guardando estado: {e}")

def load_state():
    global trades_abiertos, webhook_eventos
    if TRADES_FILE.exists():
        try:
            trades_abiertos.update(json.loads(TRADES_FILE.read_text()))
            log(f"📂 {len(trades_abiertos)} trades cargados (v6.2)")
        except: pass
    if EVENTS_FILE.exists():
        try:
            webhook_eventos.extend(json.loads(EVENTS_FILE.read_text()))
        except: pass

load_state()

# ════════════════════════════════════════════════════════════════
# LÓGICA DE TRADING (BITGET)
# ════════════════════════════════════════════════════════════════
def par_ccxt(symbol):
    # Limpiar prefijos de exchange (ej: BITGET:SAFEUSDT.P -> SAFEUSDT.P)
    s = symbol.upper()
    if ":" in s:
        s = s.split(":")[-1]
    
    s = s.replace("BINANCE:", "").replace(".P", "").replace("PERP", "").replace("-", "").replace("/", "")
    if not s.endswith("USDT"): s += "USDT"
    base = s.replace("USDT", "")
    return f"{base}/USDT:USDT"

def record_event(symbol, action, status, reason, **extra):
    event = {"ts": datetime.now().isoformat(), "symbol": symbol, "action": action, "status": status, "reason": reason}
    event.update(extra)
    webhook_eventos.append(event)
    save_state()

def abrir_posicion(symbol, side, price, sl):
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] OPEN {side.upper()} {symbol} @ {price}")
        return {"id": f"DRY-{int(time.time())}", "amount": 1.0}

    ex = get_exchange()
    if not ex: return None

    # Normalizar símbolo para Bitget
    sym_ccxt = par_ccxt(symbol)

    # 1. EVITAR DUPLICADOS LOCALES
    if symbol in trades_abiertos or any(s in symbol for s in trades_abiertos):
        log(f"⚠️ Ya existe una posición abierta para {symbol}. Ignorando duplicado.")
        return None

    # 1.5. EVITAR DUPLICADOS EN BITGET (Hard Check en vivo)
    try:
        positions = ex.fetch_positions([sym_ccxt])
        for p in positions:
            if abs(float(p.get('contracts', 0))) > 0:
                log(f"⚠️ DUPLICADO EN EXCHANGE: Ya existe posición para {symbol} en Bitget. Abortando.")
                if symbol not in trades_abiertos:
                    trades_abiertos[symbol] = {
                        "id": f"SYNC-{int(time.time())}", 
                        "side": p.get('side', 'unknown'), 
                        "amount": abs(float(p.get('contracts', 0)))
                    }
                    save_state()
                return None
    except Exception as e:
        log(f"⚠️ Error al verificar posiciones en exchange: {e}")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 2. Sincronizar Apalancamiento (Causa probable de margenes altos si está en 1x/3x en Bitget)
            try:
                ex.set_leverage(LEVERAGE, sym_ccxt)
                log(f"⚙️ Apalancamiento ajustado a {LEVERAGE}x para {symbol}")
            except Exception as le:
                log(f"⚠️ No se pudo ajustar apalancamiento (puede que ya esté en {LEVERAGE}x): {le}")

            # 3. Calcular cantidad exacta considerando el apalancamiento del bot
            qty = (MONTO_POR_TRADE * LEVERAGE) / float(price)
            qty = float(ex.amount_to_precision(sym_ccxt, qty))

            # Abrir orden
            order = ex.create_order(
                symbol=sym_ccxt,
                type='market',
                side=side,
                amount=qty,
                params={'marginMode': 'cross', 'oneWayMode': True}
            )
            log(f"✅ ORDEN EJECUTADA: {side.upper()} {qty} {symbol}")

            # Colocar SL Real
            if sl and sl != 'N/A':
                close_side = 'sell' if side == 'buy' else 'buy'
                ex.create_order(
                    symbol=sym_ccxt,
                    type='market',
                    side=close_side,
                    amount=qty,
                    params={
                        'stop': True,
                        'triggerPrice': float(sl),
                        'triggerType': 'mark_price',
                        'reduceOnly': True
                    }
                )
                log(f"🛡️ SL COLOCADO: {sl}")
            
            return {"id": order['id'], "amount": qty}
        except Exception as e:
            if attempt < max_retries - 1:
                log(f"⚠️ Intento {attempt+1} fallido para {symbol}: {e}. Reintentando...")
                time.sleep(1.5)
            else:
                log(f"❌ Error final abriendo posición en {symbol} tras {max_retries} intentos: {e}")
                return None

def cerrar_parcial(symbol, side_presente, pct):
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] PARTIAL CLOSE {pct}% {symbol}")
        return True

    ex = get_exchange()
    if not ex: return False

    try:
        # Obtener posición actual para cerrar el porcentaje exacto
        positions = ex.fetch_positions([sym_ccxt])
        for p in positions:
            amt_total = abs(float(p.get('contracts', 0)))
            if amt_total > 0:
                amt_cerrar = amt_total * (float(pct) / 100.0)
                amt_cerrar = float(ex.amount_to_precision(sym_ccxt, amt_cerrar))
                if amt_cerrar > 0:
                    side_cerrar = 'sell' if side_presente == 'buy' else 'buy'
                    ex.create_order(sym_ccxt, 'market', side_cerrar, amt_cerrar, params={'reduceOnly': True})
                    log(f"🧩 CIERRE PARCIAL: {symbol} {pct}% ({amt_cerrar} contratos)")
        return True
    except Exception as e:
        log(f"❌ Error cierre parcial: {e}")
        return False

def cerrar_posicion(symbol, side_presente):
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] CLOSE {symbol}")
        return True

    ex = get_exchange()
    if not ex: return False

    try:
        # Cerrar todo y cancelar triggers
        ex.cancel_all_orders(sym_ccxt, params={'stop': True})
        
        # Obtener posición actual para cerrar exacto
        positions = ex.fetch_positions([sym_ccxt])
        for p in positions:
            amt = abs(float(p.get('contracts', 0)))
            if amt > 0:
                side_cerrar = 'sell' if side_presente == 'buy' else 'buy'
                ex.create_order(sym_ccxt, 'market', side_cerrar, amt, params={'reduceOnly': True})
                log(f"🏁 POSICIÓN CERRADA: {symbol} ({amt} contratos)")
        return True
    except Exception as e:
        log(f"❌ Error cerrando posición: {e}")
        return False

# ════════════════════════════════════════════════════════════════
# ENDPOINTS
# ════════════════════════════════════════════════════════════════
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "error", "reason": "No JSON"}), 400

    action = data.get('action', '').lower()
    symbol = data.get('symbol', 'N/A')
    side   = data.get('side', '').lower()
    price  = data.get('price', 0)
    sl     = data.get('sl', 'N/A')
    tid    = data.get('trade_id', 'N/A')
    pct    = data.get('close_pct', 0)

    log(f"🚨 WEBHOOK RECIBIDO: {action.upper()} | {symbol} | Trade #{tid}")
    # Guardar evento de recepción inmediata para depuración
    record_event(symbol, action, "received", "Webhook reached bot", trade_id=tid)

    if action == 'open':
        with trade_lock:
            if len(trades_abiertos) >= MAX_TOTAL_TRADES:
                log(f"⚠️ Máximo de trades alcanzado ({MAX_TOTAL_TRADES})")
                return jsonify({"status": "rejected", "reason": "Max trades reached"}), 200
            
            # Pequeña pausa si acabamos de recibir un close para evitar conflicto de órdenes en Bitget
            if symbol in webhook_eventos and webhook_eventos[-1]['action'] in ['close', 'exit']:
                 log(f"⏳ Pausa de seguridad para {symbol} por cambio de tendencia...")
                 time.sleep(1.0)
                 
            # Si el símbolo ya está, no intentamos abrirlo nuevamente (Double check bajo el candado)
            if symbol in trades_abiertos:
                log(f"⚠️ Posición {symbol} ya registrada en memoria bajo bloqueo. Ignorando.")
                return jsonify({"status": "rejected", "reason": "Already open"}), 200

            res = abrir_posicion(symbol, side, price, sl)
            if res:
                trades_abiertos[symbol] = {
                    "id": tid, "side": side, "entry": price, "sl": sl, 
                    "amount": res['amount'], "ts": datetime.now().isoformat()
                }
                record_event(symbol, action, "opened", "Success", trade_id=tid)
                return jsonify({"status": "success"}), 200
        
    elif action == 'partial_close':
        if symbol in trades_abiertos:
            if cerrar_parcial(symbol, trades_abiertos[symbol]['side'], pct):
                record_event(symbol, action, "partial", f"Closed {pct}%", trade_id=tid)
                return jsonify({"status": "success"}), 200

    elif action in ['close', 'exit']:
        if symbol in trades_abiertos:
            if cerrar_posicion(symbol, trades_abiertos[symbol]['side']):
                trades_abiertos.pop(symbol)
                record_event(symbol, action, "closed", "Success", trade_id=tid)
                return jsonify({"status": "success"}), 200

    return jsonify({"status": "ignored"}), 200

@app.route('/status')
def status():
    # Asegurar que solo devolvemos los últimos 20 para el status
    eventos_recientes = webhook_eventos
    if len(eventos_recientes) > 20:
        eventos_recientes = eventos_recientes[-20:]
        
    return jsonify({
        "bot": "Gaussian v6.2 Premium (Sync)",
        "mode": "DRY_RUN" if DRY_RUN else "REAL",
        "active_trades": trades_abiertos,
        "recent_events": eventos_recientes
    })

@app.route('/logs')
def view_logs():
    try:
        if ERROR_LOG.exists():
            return f"<pre>{ERROR_LOG.read_text()}</pre>"
        return "No hay logs de errores aún."
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5002))
    log(f"🚀 Servidor v6.2 iniciado en puerto {port}")
    app.run(host="0.0.0.0", port=port)
