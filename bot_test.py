from flask import Flask, request, jsonify
from datetime import datetime
import ccxt
import os
import time

app = Flask(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN DEL BOT — Gaussian Trend v5 Pro Bot         ║
# ╚══════════════════════════════════════════════════════════════╝

# 🧪 MODO DE OPERACIÓN
# True  = Simula localmente (no toca el exchange, ideal para probar)
# False = Opera en Bitget REAL con dinero real
DRY_RUN = False

# 🪙 Whitelist — Pares donde el bot puede operar
PARES_PERMITIDOS = [
    "ARCUSDT", "AXSUSDT", "FLOCKUSDT", "JELLYUSDT", "RIVERUSDT", "DEEPUSDT", "BEATUSDT", "BERAUSDT", 
    "LINEAUSDT", "MUBARAKUSDT", "NOMUSDT", "ORDIUSDT", "PIPPINUSDT", "ROSEUSDT", "SOLUSDT", "SQDUSDT", "UNIUSDT",
    "NEIROUSDT", "ASTERUSDT", "AWEUSDT", "1000BONKUSDT", "BIOUSDT", "BREVUSDT", "CAKEUSDT", "COTIUSDT", "CYBERUSDT"
]

# 💰 Capital por trade (en USDT)
MONTO_POR_TRADE = 5.0

# ⚡ Apalancamiento
LEVERAGE = 5

# 🔒 Límites de trades
MAX_TRADES_POR_PAR = 1
MAX_TOTAL_TRADES   = 5  # Máximo 5 criptos en operación a la vez

# ════════════════════════════════════════════════════════════════
# CONEXIÓN A BITGET (Lógica Segura para Railway)
# ════════════════════════════════════════════════════════════════
exchange = None

def get_exchange():
    global exchange
    if exchange:
        return exchange
    
    if DRY_RUN:
        return None
        
    # Obtener llaves solo cuando se necesiten (Runtime, no Build)
    key    = os.environ.get("BITGET_API_KEY", "")
    secret = os.environ.get("BITGET_API_SECRET", "")
    password = os.environ.get("BG_PASS", "")

    # Verificación de llaves antes de conectar
    if not key or not secret or not password:
        log("⚠️ Advertencia: API Keys/Passphrase (BG_PASS) no encontradas.")
        return None

    try:
        exchange = ccxt.bitget({
            'apiKey': key,
            'secret': secret,
            'password': password,
            'options': {
                'defaultType': 'swap'
            },
            'enableRateLimit': True,
        })
        exchange.load_markets()
        
        # Forzar modo Unilateral (One-way) para evitar Error 40774
        try:
            # En CCXT Bitget: False = Unilateral, True = Hedged
            exchange.set_position_mode(False, params={'productType': 'USDT-FUTURES'})
            log("   🔄 Modo de posición asegurado: UNILATERAL")
        except Exception as e:
            log(f"   ℹ️  Nota sobre modo de posición: {e}")
            
        log("✅ Conexión con Bitget establecida correctamente")
        return exchange
    except Exception as e:
        log(f"❌ Error crítico de conexión a Bitget: {e}")
        return None

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ════════════════════════════════════════════════════════════════
# Estado interno y Persistencia
# ════════════════════════════════════════════════════════════════
import json
TRADES_FILE = "trades.json"
trades_abiertos = {}
historial = []

def save_trades():
    """Guarda los trades activos en un archivo JSON"""
    try:
        with open(TRADES_FILE, 'w') as f:
            json.dump(trades_abiertos, f, indent=4)
    except Exception as e:
        log(f"⚠️ Error guardando trades: {e}")

def load_trades():
    """Carga los trades activos desde el archivo JSON"""
    global trades_abiertos
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, 'r') as f:
                data = json.load(f)
                trades_abiertos.clear()
                trades_abiertos.update(data)
            log(f"📂 {len(trades_abiertos)} trades cargados desde {TRADES_FILE}")
        except Exception as e:
            log(f"⚠️ Error cargando trades: {e}")

# Cargar trades al iniciar el módulo
load_trades()


def par_limpio(symbol):
    """Normaliza el símbolo: BINANCE:RIVERUSDT.P → RIVERUSDT"""
    if not symbol: return ""
    s = symbol.upper()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace("/", "").replace("-", "").replace(".P", "").replace("PERP", "")
    if not s.endswith("USDT") and s != "":
        s += "USDT"
    return s


def par_ccxt(symbol):
    """Convierte RIVERUSDT → RIVER/USDT:USDT (formato ccxt swap)"""
    base = symbol.replace("USDT", "")
    return f"{base}/USDT:USDT"


def product_type_for_market(market):
    """Mapea el settle coin al productType que Bitget espera."""
    settle = (market.get('settleId') or market.get('settle') or 'USDT').upper()
    mapping = {
        'USDT': 'USDT-FUTURES',
        'USDC': 'USDC-FUTURES',
        'SUSDT': 'SUSDT-FUTURES',
        'SUSDC': 'SUSDC-FUTURES',
    }
    return mapping.get(settle, market.get('info', {}).get('productType', 'USDT-FUTURES'))


def get_swap_market(symbol_ccxt):
    """Valida que el símbolo exista en Bitget Futures USDT-M."""
    ex = get_exchange()
    if not ex:
        raise RuntimeError("Exchange no disponible")

    market = ex.market(symbol_ccxt)
    if not market.get('swap'):
        raise ValueError(f"{symbol_ccxt} no es un mercado swap en Bitget")

    settle = (market.get('settleId') or market.get('settle') or '').upper()
    if settle != 'USDT':
        raise ValueError(f"{symbol_ccxt} no liquida en USDT (settle={settle or 'N/A'})")

    return market


def build_bitget_params(symbol_ccxt, reduce_only=False):
    """Genera params consistentes para Bitget futures en modo one-way."""
    market = get_swap_market(symbol_ccxt)
    params = {
        'marginCoin': market.get('settleId') or 'USDT',
        'productType': product_type_for_market(market),
        'marginMode': 'cross',
        'oneWayMode': True,
    }
    if reduce_only:
        params['reduceOnly'] = True
    return params


def infer_trade_side_from_position(position):
    """Convierte el side normalizado de CCXT a buy/sell del bot."""
    side = (position.get('side') or '').lower()
    if side in ('long', 'buy'):
        return 'buy'
    if side in ('short', 'sell'):
        return 'sell'
    return None


def fetch_open_position(symbol_ccxt):
    """Devuelve la posición abierta real en Bitget para el símbolo."""
    ex = get_exchange()
    if not ex:
        return None

    market = get_swap_market(symbol_ccxt)
    params = {
        'productType': product_type_for_market(market),
        'marginCoin': market.get('settleId') or 'USDT',
    }
    positions = ex.fetch_positions([symbol_ccxt], params=params)
    for pos in positions:
        contracts = abs(float(pos.get('contracts') or 0))
        if pos.get('symbol') == symbol_ccxt and contracts > 0:
            return pos
    return None


def sync_trade_from_exchange(symbol, symbol_ccxt, trade_id='N/A', fallback_price='N/A'):
    """Reconstruye el estado local desde la posición real del exchange."""
    try:
        position = fetch_open_position(symbol_ccxt)
        if not position:
            return None

        side = infer_trade_side_from_position(position)
        if not side:
            return None

        entry_price = position.get('entryPrice') or fallback_price
        trades_abiertos[symbol] = {
            "trade_id": trade_id,
            "side": side,
            "entry": entry_price,
            "sl": trades_abiertos.get(symbol, {}).get('sl', 'N/A'),
            "posicion_pct": trades_abiertos.get(symbol, {}).get('posicion_pct', 100),
            "order_id": trades_abiertos.get(symbol, {}).get('order_id'),
            "synced_from_exchange": True,
        }
        save_trades()
        log(f"   🔄 Estado reconstruido desde Bitget para {symbol}: {side.upper()} | entry={entry_price}")
        return trades_abiertos[symbol]
    except Exception as e:
        log(f"   ⚠️ No se pudo reconstruir {symbol} desde Bitget: {e}")
        return None


def wait_for_position_confirmation(symbol_ccxt, retries=3, delay=0.8):
    """Confirma que la orden realmente terminó en una posición abierta."""
    if DRY_RUN:
        return {'status': 'simulated'}

    for attempt in range(retries):
        position = fetch_open_position(symbol_ccxt)
        if position:
            return position
        if attempt < retries - 1:
            time.sleep(delay)
    return None


def set_leverage(symbol_ccxt):
    """Configura el apalancamiento para el par"""
    if DRY_RUN:
        log(f"   ⚡ [DRY RUN] Leverage {LEVERAGE}x para {symbol_ccxt}")
        return
    try:
        ex = get_exchange()
        if ex:
            market = get_swap_market(symbol_ccxt)
            params = {
                'marginCoin': market.get('settleId') or 'USDT',
                'productType': product_type_for_market(market),
            }
            ex.set_leverage(LEVERAGE, symbol_ccxt, params=params)
            log(f"   ⚡ Leverage {LEVERAGE}x configurado para {symbol_ccxt}")
    except Exception as e:
        log(f"   ⚠️  Error configurando leverage: {e}")


def calcular_cantidad(symbol_ccxt, precio):
    """Calcula la cantidad de monedas a comprar con $MONTO_POR_TRADE"""
    try:
        precio_f = float(precio)
        cantidad = (MONTO_POR_TRADE * LEVERAGE) / precio_f
        if not DRY_RUN:
            ex = get_exchange()
            if ex:
                cantidad = float(ex.amount_to_precision(symbol_ccxt, cantidad))
        log(f"   📊 Cantidad calculada: {cantidad} para {symbol_ccxt} @ {precio}")
        return round(cantidad, 6)
    except Exception as e:
        log(f"   ⚠️ Error calculando cantidad para {symbol_ccxt}: {e}")
        return 0


def abrir_orden(symbol_ccxt, side, precio):
    """Abre una orden de mercado"""
    cantidad = calcular_cantidad(symbol_ccxt, precio)
    if cantidad <= 0:
        log(f"   ❌ Cantidad inválida: {cantidad}")
        return None

    set_leverage(symbol_ccxt)

    if DRY_RUN:
        fake_id = f"DRY-{datetime.now().strftime('%H%M%S')}"
        log(f"   ✅ [DRY RUN] ORDEN: {side.upper()} {cantidad} {symbol_ccxt} @ {precio}")
        log(f"   📋 Order ID: {fake_id}")
        return {'id': fake_id, 'status': 'simulated'}

    try:
        ex = get_exchange()
        if not ex:
            log("   ❌ Abortando orden: Exchange no disponible (API Keys?)")
            return None

        get_swap_market(symbol_ccxt)
        params = build_bitget_params(symbol_ccxt)
        log(f"   📡 Enviando orden market a Bitget: {side} {cantidad} {symbol_ccxt}")
        order = ex.create_order(
            symbol=symbol_ccxt,
            type='market',
            side=side,
            amount=cantidad,
            price=float(precio),
            params=params,
        )
        log(f"   ✅ ORDEN EJECUTADA: {side.upper()} {cantidad} {symbol_ccxt}")
        log(f"   📋 Order ID: {order['id']}")
        return order
    except Exception as e:
        log(f"   ❌ ERROR CRÍTICO al abrir orden en Bitget: {e}")
        return None


def cerrar_parcial(symbol_ccxt, side_original, porcentaje, precio):
    """Cierra un % de la posición (side inverso)"""
    if DRY_RUN:
        cantidad_total = calcular_cantidad(symbol_ccxt, precio)
        cantidad_cerrar = round(cantidad_total * (porcentaje / 100.0), 6)
        close_side = 'sell' if side_original == 'buy' else 'buy'
        log(f"   ✅ [DRY RUN] PARCIAL: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}%)")
        return {'id': f"DRY-P-{datetime.now().strftime('%H%M%S')}", 'status': 'simulated'}

    try:
        ex = get_exchange()
        if not ex:
            log("   ❌ Abortando parcial: Exchange no disponible")
            return None

        position = fetch_open_position(symbol_ccxt)
        if not position:
            log(f"   ⚠️  No hay posición real abierta en Bitget para {symbol_ccxt}")
            return None

        side_real = infer_trade_side_from_position(position)
        close_side = 'sell' if side_real == 'buy' else 'buy'
        cantidad_total = abs(float(position.get('contracts') or 0))
        cantidad_cerrar = cantidad_total * (porcentaje / 100.0)
        cantidad_cerrar = float(ex.amount_to_precision(symbol_ccxt, cantidad_cerrar))

        if cantidad_cerrar <= 0:
            log(f"   ⚠️  Cantidad a cerrar muy pequeña: {cantidad_cerrar}")
            return None

        params = build_bitget_params(symbol_ccxt, reduce_only=True)
        order = ex.create_order(
            symbol=symbol_ccxt,
            type='market',
            side=close_side,
            amount=cantidad_cerrar,
            price=float(precio),
            params=params,
        )
        log(f"   ✅ PARCIAL CERRADO: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}%)")
        return order
    except Exception as e:
        log(f"   ❌ ERROR cerrando parcial: {e}")
        return None


def cerrar_todo(symbol_ccxt, side_original):
    """Cierra toda la posición abierta"""
    close_side = 'sell' if side_original == 'buy' else 'buy'

    if DRY_RUN:
        log(f"   ✅ [DRY RUN] POSICIÓN CERRADA: {close_side.upper()} toda la posición de {symbol_ccxt}")
        return {'id': f"DRY-C-{datetime.now().strftime('%H%M%S')}", 'status': 'simulated'}

    try:
        ex = get_exchange()
        if not ex:
            log("   ❌ Abortando cierre: Exchange no disponible")
            return None

        position = fetch_open_position(symbol_ccxt)
        if position:
            side_real = infer_trade_side_from_position(position)
            close_side = 'sell' if side_real == 'buy' else 'buy'
            amt = abs(float(position.get('contracts') or 0))
            ticker = ex.fetch_ticker(symbol_ccxt)
            curr_price = float(ticker['last'])
            params = build_bitget_params(symbol_ccxt, reduce_only=True)
            order = ex.create_order(
                symbol=symbol_ccxt,
                type='market',
                side=close_side,
                amount=amt,
                price=curr_price,
                params=params,
            )
            log(f"   ✅ POSICIÓN CERRADA: {symbol_ccxt} | {amt} contratos")
            return order
        log(f"   ⚠️  No se encontró posición abierta en {symbol_ccxt}")
        return None
    except Exception as e:
        log(f"   ❌ ERROR cerrando posición: {e}")
        return None


# ════════════════════════════════════════════════════════════════
# ENDPOINTS
# ════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    pares = ", ".join(PARES_PERMITIDOS)
    activos = len(trades_abiertos)
    modo = "🧪 DRY RUN" if DRY_RUN else "🔴 REAL"
    return (
        f"✅ Bot Gaussian v5 Pro en línea — {modo}<br>"
        f"💰 ${MONTO_POR_TRADE} x{LEVERAGE} por trade<br>"
        f"🪙 Pares: {pares}<br>"
        f"📊 Trades activos: {activos}/{len(PARES_PERMITIDOS)}<br>"
        f"⏳ Esperando webhooks en /webhook..."
    )


@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🤖 Bot de Gaussian Trend activo. Esperando señales POST desde TradingView."
    
    data = request.get_json(force=True, silent=True)
    print(f"\n{'='*50}")
    log(f"🚨 WEBHOOK RECIBIDO")
    print(f"   Payload: {data}")
    
    if not data:
        raw_msg = request.data.decode('utf-8', errors='ignore')[:100]
        log(f"⚠️ Webhook IGNORADO (no es JSON): {raw_msg}...")
        print(f"{'='*50}")
        return jsonify({"status": "ignored", "message": "Payload is not JSON"}), 200
    
    action   = data.get('action', '').lower()
    symbol   = par_limpio(data.get('symbol', ''))
    trade_id = data.get('trade_id', 'N/A')
    side     = data.get('side', '').lower()
    price    = data.get('price', 'N/A')
    sym_ccxt = par_ccxt(symbol)

    log(f"📋 PROCESANDO: {action.upper()} | {symbol} (Orig: {data.get('symbol')}) | Trade #{trade_id}")
    print(f"{'='*50}")

    # ─── FILTRO: Par permitido (solo USDT) ───
    if not symbol.endswith("USDT"):
        log(f"❌ RECHAZADO — {symbol} no es un par USDT válido")
        return jsonify({"status": "rejected", "reason": "Solo pares USDT"}), 200

    # ═══════════════════════════════════════════════════
    # OPEN — Abrir nuevo trade
    # ═══════════════════════════════════════════════════
    if action == 'open':
        # 1. Límite de trades totales (Max 5)
        if len(trades_abiertos) >= MAX_TOTAL_TRADES:
            log(f"❌ RECHAZADO — Límite de {MAX_TOTAL_TRADES} trades simultáneos alcanzado. (Activos: {len(trades_abiertos)})")
            return jsonify({"status": "rejected", "reason": "Max total trades reached"}), 200

        # 2. Límite por par (Max 1)
        if symbol in trades_abiertos:
            log(f"❌ RECHAZADO — Ya hay trade en {symbol} (#{trades_abiertos[symbol]['trade_id']})")
            return jsonify({"status": "rejected", "reason": "Max 1 trade por par"}), 200

        sl   = data.get('sl', 'N/A')
        conf = int(data.get('conf', 0))

        # 3. Filtro de Confianza (5 estrellas = 100 puntos)
        if conf < 100:
            log(f"❌ RECHAZADO — Confianza insuficiente: {conf}/100. Necesitas 5 estrellas (100 puntos) para abrir.")
            return jsonify({"status": "rejected", "reason": "Low confidence (need 5 stars)"}), 200

        log(f"📤 ABRIENDO {side.upper()} {symbol} @ {price}")
        log(f"   💵 ${MONTO_POR_TRADE} x{LEVERAGE} = ${MONTO_POR_TRADE * LEVERAGE} exposición")

        # ── EJECUTAR EN BINANCE ──
        order = abrir_orden(sym_ccxt, side, price)

        position_confirmation = wait_for_position_confirmation(sym_ccxt)

        if order and position_confirmation and (order.get('id') or order.get('status') == 'simulated' or not DRY_RUN):
            trades_abiertos[symbol] = {
                "trade_id": trade_id,
                "side": side,
                "entry": price,
                "sl": sl,
                "posicion_pct": 100,
                "order_id": order['id'] if order else None,
                "symbol_ccxt": sym_ccxt,
            }
            save_trades() # Persistir cambio
        else:
            log(f"❌ ABORTADO — Bitget no confirmó posición abierta para {symbol}")
            return jsonify({"status": "error", "reason": "Exchange position not confirmed"}), 200

    # ═══════════════════════════════════════════════════
    # PARTIAL_CLOSE — Cierre parcial (TP hit)
    # ═══════════════════════════════════════════════════
    elif action == 'partial_close':
        reason    = data.get('reason', 'N/A')
        close_pct = int(data.get('close_pct', 0))
        new_sl    = data.get('new_sl', 'N/A')

        if symbol not in trades_abiertos:
            synced_trade = sync_trade_from_exchange(symbol, sym_ccxt, trade_id=trade_id, fallback_price=price)
            if not synced_trade:
                log(f"⚠️  partial_close para {symbol} pero no hay trade abierto ni posición en Bitget")
                return jsonify({"status": "error", "reason": "No trade abierto"}), 200

        side_orig = trades_abiertos[symbol]['side']
        log(f"🎯 {reason} HIT — Cerrando {close_pct}% de {symbol}")

        # ── EJECUTAR PARCIAL EN BINANCE ──
        partial_order = cerrar_parcial(sym_ccxt, side_orig, close_pct, price)
        if not partial_order:
            log(f"❌ ABORTADO — El parcial no se pudo ejecutar en Bitget")
            return jsonify({"status": "error", "reason": "Partial close failed"}), 200

        trades_abiertos[symbol]['sl'] = new_sl
        trades_abiertos[symbol]['posicion_pct'] = max(0, trades_abiertos[symbol]['posicion_pct'] - close_pct)
        save_trades() # Persistir cambio

        log(f"   🛑 SL movido a: {new_sl}")
        log(f"   📊 Posición restante: {trades_abiertos[symbol]['posicion_pct']}%")

    # ═══════════════════════════════════════════════════
    # UPDATE_SL — Trailing SL dinámico
    # ═══════════════════════════════════════════════════
    elif action == 'update_sl':
        new_sl = data.get('new_sl', 'N/A')
        if symbol in trades_abiertos:
            old_sl = trades_abiertos[symbol].get('sl', '?')
            trades_abiertos[symbol]['sl'] = new_sl
            save_trades() # Persistir cambio
            log(f"🔄 SL: {old_sl} → {new_sl} ({symbol})")
        else:
            synced_trade = sync_trade_from_exchange(symbol, sym_ccxt, trade_id=trade_id, fallback_price=price)
            if synced_trade:
                old_sl = synced_trade.get('sl', '?')
                trades_abiertos[symbol]['sl'] = new_sl
                save_trades()
                log(f"🔄 SL: {old_sl} → {new_sl} ({symbol}, reconstruido desde exchange)")
            else:
                log(f"⚠️  update_sl para {symbol} pero no hay trade abierto")

    # ═══════════════════════════════════════════════════
    # ADD — Entry 2 / Re-entry
    # ═══════════════════════════════════════════════════
    elif action == 'add':
        reason = data.get('reason', 'N/A')
        log(f"➕ {reason.upper()} — {side.upper()} {symbol} @ {price}")

        # ── EJECUTAR ADD EN BINANCE ──
        abrir_orden(sym_ccxt, side, price)

    # ═══════════════════════════════════════════════════
    # CLOSE — Trade cerrado (SL hit)
    # ═══════════════════════════════════════════════════
    elif action == 'close':
        pnl    = data.get('pnl', 'N/A')
        reason = data.get('reason', 'SL')

        log(f"🔴 CERRANDO {symbol} — {reason} | PnL: {pnl}")

        if symbol not in trades_abiertos:
            sync_trade_from_exchange(symbol, sym_ccxt, trade_id=trade_id, fallback_price=price)

        if symbol in trades_abiertos:
            side_orig = trades_abiertos[symbol]['side']

            # ── CERRAR TODO EN BINANCE ──
            cerrar_todo(sym_ccxt, side_orig)

            trade_info = trades_abiertos.pop(symbol)
            save_trades() # Persistir cambio
            historial.append({**trade_info, "pnl": pnl, "closed_at": datetime.now().isoformat()})
            log(f"   🟢 {symbol} libre para nuevos trades")
        else:
            log(f"⚠️  close para {symbol} pero no hay trade registrado")

    else:
        log(f"⚠️  Acción desconocida: {action}")

    # Estado global
    print(f"\n📋 Trades activos: {len(trades_abiertos)}/{len(PARES_PERMITIDOS)}")
    for par, info in trades_abiertos.items():
        print(f"   • {par}: {info['side'].upper()} desde {info['entry']} | SL: {info['sl']} | Pos: {info['posicion_pct']}%")
    print()

    return jsonify({"status": "success", "message": f"{action} procesado"}), 200


@app.route('/status')
def status():
    return jsonify({
        "modo": "SIMULADO" if DRY_RUN else "REAL",
        "activos": trades_abiertos,
        "historial": historial[-10:],
        "config": {
            "pares": PARES_PERMITIDOS,
            "monto": MONTO_POR_TRADE,
            "leverage": LEVERAGE,
        }
    })


@app.route('/balance')
def balance():
    """Ver el balance de la cuenta"""
    if DRY_RUN:
        return jsonify({"modo": "DRY RUN", "total": "simulado", "libre": "simulado"})
    try:
        ex = get_exchange()
        if not ex: return jsonify({"error": "Exchange no disponible"}), 500
        bal = ex.fetch_balance()
        usdt = bal.get('USDT', {})
        return jsonify({
            "total": usdt.get('total', 0),
            "libre": usdt.get('free', 0),
            "en_uso": usdt.get('used', 0),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/clear_trades')
def clear_trades():
    """Limpia todos los trades activos (solo para debug)"""
    global trades_abiertos
    count = len(trades_abiertos)
    trades_abiertos.clear()
    save_trades()
    log(f"🗑️ Trades limpiados manualmente via endpoint ({count} eliminados)")
    return jsonify({"status": "success", "message": f"{count} trades eliminados", "current": trades_abiertos})


if not DRY_RUN:
    log("🚀 Iniciando bot en modo PRODUCCIÓN (Bitget)")
    # Intento de conexión inicial para validar llaves en el log
    get_exchange()

if __name__ == '__main__':
    modo = "🧪 DRY RUN (simulación)" if DRY_RUN else "🔴 PRODUCCIÓN REAL"

    print()
    print("╔══════════════════════════════════════════════════╗")
    print(f"║   🤖 Gaussian Trend IA Pro v6 EXTREME Bot       ║")
    print(f"║   {modo:<46} ║")
    print("╠══════════════════════════════════════════════════╣")
    print(f"║   💰 Monto    : ${MONTO_POR_TRADE} por trade              ║")
    print(f"║   ⚡ Leverage : x{LEVERAGE}                              ║")
    print(f"║   🪙 Pares    : {', '.join(PARES_PERMITIDOS):<22}     ║")
    print(f"║   🔒 Máx/par  : {MAX_TRADES_POR_PAR} trade                        ║")
    print("╠══════════════════════════════════════════════════╣")
    print("║   📡 Webhook   : http://127.0.0.1:5001/webhook  ║")
    print("║   📊 Estado    : http://127.0.0.1:5001/status   ║")
    print("╚══════════════════════════════════════════════════╝")

    port = int(os.environ.get("PORT", 5001))
    print(f"📡 Webhook en espera: http://0.0.0.0:{port}/webhook")
    app.run(host="0.0.0.0", port=port, debug=(not os.environ.get("RAILWAY_ENVIRONMENT")))
