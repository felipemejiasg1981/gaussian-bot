from flask import Flask, request, jsonify
from datetime import datetime
import ccxt
import os

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
                'defaultType': 'swap',
                'createMarketBuyOrderRequiresPrice': False
            },
            'enableRateLimit': True,
        })
        exchange.load_markets()
        log("✅ Conexión con Bitget establecida correctamente")
        return exchange
    except Exception as e:
        log(f"❌ Error crítico de conexión a Bitget: {e}")
        return None

# ════════════════════════════════════════════════════════════════
# Estado interno
# ════════════════════════════════════════════════════════════════
trades_abiertos = {}
historial = []


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


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
    """Convierte RIVERUSDT → RIVER/USDT (formato ccxt)"""
    base = symbol.replace("USDT", "")
    return f"{base}/USDT"


def set_leverage(symbol_ccxt):
    """Configura el apalancamiento para el par"""
    if DRY_RUN:
        log(f"   ⚡ [DRY RUN] Leverage {LEVERAGE}x para {symbol_ccxt}")
        return
    try:
        ex = get_exchange()
        if ex:
            # Bitget requiere marginCoin para set_leverage
            ex.set_leverage(LEVERAGE, symbol_ccxt, params={'marginCoin': 'USDT'})
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
        return round(cantidad, 6)
    except Exception as e:
        log(f"   ⚠️  Error calculando cantidad: {e}")
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
            log("   ❌ Abortando orden: Exchange no disponible")
            return None
            
        order = ex.create_order(
            symbol=symbol_ccxt,
            type='market',
            side=side,
            amount=cantidad,
            price=float(precio)
        )
        log(f"   ✅ ORDEN EJECUTADA: {side.upper()} {cantidad} {symbol_ccxt}")
        log(f"   📋 Order ID: {order['id']}")
        return order
    except Exception as e:
        log(f"   ❌ ERROR abriendo orden: {e}")
        return None


def cerrar_parcial(symbol_ccxt, side_original, porcentaje, precio):
    """Cierra un % de la posición (side inverso)"""
    close_side = 'sell' if side_original == 'buy' else 'buy'
    cantidad_total = calcular_cantidad(symbol_ccxt, precio)
    cantidad_cerrar = round(cantidad_total * (porcentaje / 100.0), 6)

    if cantidad_cerrar <= 0:
        log(f"   ⚠️  Cantidad a cerrar muy pequeña: {cantidad_cerrar}")
        return None

    if DRY_RUN:
        log(f"   ✅ [DRY RUN] PARCIAL: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}%)")
        return {'id': f"DRY-P-{datetime.now().strftime('%H%M%S')}", 'status': 'simulated'}

    try:
        ex = get_exchange()
        if not DRY_RUN:
            if ex:
                cantidad_cerrar = float(ex.amount_to_precision(symbol_ccxt, cantidad_cerrar))
                order = ex.create_order(
                    symbol=symbol_ccxt,
                    type='market',
                    side=close_side,
                    amount=cantidad_cerrar,
                    price=float(precio),
                    params={'reduceOnly': True}
                )
                log(f"   ✅ PARCIAL CERRADO: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}%)")
                return order
            else:
                log("   ❌ Abortando parcial: Exchange no disponible")
                return None
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
            
        positions = ex.fetch_positions([symbol_ccxt])
        for pos in positions:
            amt = abs(float(pos.get('contracts', 0)))
            if amt > 0:
                ticker = ex.fetch_ticker(symbol_ccxt)
                curr_price = float(ticker['last'])
                order = ex.create_order(
                    symbol=symbol_ccxt,
                    type='market',
                    side=close_side,
                    amount=amt,
                    price=curr_price,
                    params={'reduceOnly': True}
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
    print(f"{'='*50}")

    if not data:
        raw_msg = request.data.decode('utf-8', errors='ignore')[:100]
        log(f"⚠️ Webhook IGNORADO (no es JSON): {raw_msg}...")
        return jsonify({"status": "ignored", "message": "Payload is not JSON"}), 200

    action   = data.get('action', '').lower()
    symbol   = par_limpio(data.get('symbol', ''))
    trade_id = data.get('trade_id', 'N/A')
    side     = data.get('side', '').lower()
    price    = data.get('price', 'N/A')
    sym_ccxt = par_ccxt(symbol)

    log(f"📋 PROCESANDO: {action.upper()} | {symbol} (Orig: {data.get('symbol')}) | Trade #{trade_id}")

    # ─── FILTRO: Par permitido (solo USDT) ───
    if not symbol.endswith("USDT"):
        log(f"❌ RECHAZADO — {symbol} no es un par USDT válido")
        return jsonify({"status": "rejected", "reason": "Solo pares USDT"}), 200

    # ═══════════════════════════════════════════════════
    # OPEN — Abrir nuevo trade
    # ═══════════════════════════════════════════════════
    if action == 'open':
        # 1. Límite de trades totales (Max 3)
        if len(trades_abiertos) >= MAX_TOTAL_TRADES:
            log(f"❌ RECHAZADO — Límite de {MAX_TOTAL_TRADES} trades alcanzado.")
            return jsonify({"status": "rejected", "reason": "Max total trades reached"}), 200

        # 2. Límite por par (Max 1)
        if symbol in trades_abiertos:
            log(f"❌ RECHAZADO — Ya hay trade en {symbol} (#{trades_abiertos[symbol]['trade_id']})")
            return jsonify({"status": "rejected", "reason": "Max 1 trade por par"}), 200

        sl   = data.get('sl', 'N/A')
        conf = int(data.get('conf', 0))

        # 3. Filtro de Confianza (5 estrellas = 100 puntos)
        if conf < 100:
            log(f"❌ RECHAZADO — Confianza insuficiente: {conf}/100 (Esperaba 100 para 5 estrellas)")
            return jsonify({"status": "rejected", "reason": "Low confidence (need 5 stars)"}), 200

        log(f"📤 ABRIENDO {side.upper()} {symbol} @ {price}")
        log(f"   💵 ${MONTO_POR_TRADE} x{LEVERAGE} = ${MONTO_POR_TRADE * LEVERAGE} exposición")

        # ── EJECUTAR EN BINANCE ──
        order = abrir_orden(sym_ccxt, side, price)

        trades_abiertos[symbol] = {
            "trade_id": trade_id,
            "side": side,
            "entry": price,
            "sl": sl,
            "posicion_pct": 100,
            "order_id": order['id'] if order else None,
        }

    # ═══════════════════════════════════════════════════
    # PARTIAL_CLOSE — Cierre parcial (TP hit)
    # ═══════════════════════════════════════════════════
    elif action == 'partial_close':
        reason    = data.get('reason', 'N/A')
        close_pct = int(data.get('close_pct', 0))
        new_sl    = data.get('new_sl', 'N/A')

        if symbol not in trades_abiertos:
            log(f"⚠️  partial_close para {symbol} pero no hay trade abierto")
            return jsonify({"status": "error", "reason": "No trade abierto"}), 200

        side_orig = trades_abiertos[symbol]['side']
        log(f"🎯 {reason} HIT — Cerrando {close_pct}% de {symbol}")

        # ── EJECUTAR PARCIAL EN BINANCE ──
        cerrar_parcial(sym_ccxt, side_orig, close_pct, price)

        trades_abiertos[symbol]['sl'] = new_sl
        trades_abiertos[symbol]['posicion_pct'] -= close_pct

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
            log(f"🔄 SL: {old_sl} → {new_sl} ({symbol})")
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

        if symbol in trades_abiertos:
            side_orig = trades_abiertos[symbol]['side']

            # ── CERRAR TODO EN BINANCE ──
            cerrar_todo(sym_ccxt, side_orig)

            trade_info = trades_abiertos.pop(symbol)
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

