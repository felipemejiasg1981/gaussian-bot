from flask import Flask, request, jsonify
from datetime import datetime
import ccxt
import os

app = Flask(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN DEL BOT — Gaussian Trend v5 Pro Bot         ║
# ╚══════════════════════════════════════════════════════════════╝

# 🔑 API KEYS — Se leen de variables de entorno (seguro para la nube)
# En Railway: se configuran en el dashboard → Variables
# En local: se usan los valores de abajo como fallback
API_KEY    = os.environ.get("BINANCE_API_KEY", "PCNKVFyJacgU0FLulSw6LcHpU1KSbtLaSJBRg5ihLLbsi7tfWcLsDiVyMWTtjMN4")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "uOxQhv9babmSZO80MG4dfIVB2iiUXLE02e1aBjTK8cf8AjmSTcoCkZWC4j4GKBrJ")
# 🧪 MODO DE OPERACIÓN
# True  = Simula localmente (no toca el exchange, ideal para probar)
# False = Opera en Binance REAL con dinero real
DRY_RUN = False

# 🪙 Pares permitidos (solo estos se operan)
PARES_PERMITIDOS = ["BTCUSDT", "RIVERUSDT", "PIPPINUSDT"]

# 💰 Capital por trade (en USDT)
MONTO_POR_TRADE = 10.0

# ⚡ Apalancamiento
LEVERAGE = 5

# 🔒 Máximo 1 trade abierto por cripto
MAX_TRADES_POR_PAR = 1

# ════════════════════════════════════════════════════════════════
# CONEXIÓN A BINANCE (solo si DRY_RUN = False)
# ════════════════════════════════════════════════════════════════
exchange = None
if not DRY_RUN:
    exchange = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {
            'defaultType': 'future',
        },
        'enableRateLimit': True,
    })

# ════════════════════════════════════════════════════════════════
# Estado interno
# ════════════════════════════════════════════════════════════════
trades_abiertos = {}
historial = []


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def par_limpio(symbol):
    """Normaliza el símbolo: RIVER/USDT → RIVERUSDT"""
    s = symbol.upper().replace("/", "").replace("-", "").replace(".P", "")
    if not s.endswith("USDT"):
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
        exchange.set_leverage(LEVERAGE, symbol_ccxt)
        log(f"   ⚡ Leverage {LEVERAGE}x configurado para {symbol_ccxt}")
    except Exception as e:
        log(f"   ⚠️  Error configurando leverage: {e}")


def calcular_cantidad(symbol_ccxt, precio):
    """Calcula la cantidad de monedas a comprar con $MONTO_POR_TRADE"""
    try:
        precio_f = float(precio)
        cantidad = (MONTO_POR_TRADE * LEVERAGE) / precio_f
        if not DRY_RUN:
            cantidad = float(exchange.amount_to_precision(symbol_ccxt, cantidad))
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
        order = exchange.create_market_order(
            symbol=symbol_ccxt,
            side=side,
            amount=cantidad,
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
        if not DRY_RUN:
            cantidad_cerrar = float(exchange.amount_to_precision(symbol_ccxt, cantidad_cerrar))
        order = exchange.create_market_order(
            symbol=symbol_ccxt,
            side=close_side,
            amount=cantidad_cerrar,
            params={'reduceOnly': True}
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
        positions = exchange.fetch_positions([symbol_ccxt])
        for pos in positions:
            amt = abs(float(pos.get('contracts', 0)))
            if amt > 0:
                order = exchange.create_market_order(
                    symbol=symbol_ccxt,
                    side=close_side,
                    amount=amt,
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


@app.route('/webhook', methods=['POST'])
def webhook_receiver():
    data = request.get_json(force=True, silent=True)

    if not data:
        log("⚠️  Webhook vacío recibido")
        return jsonify({"status": "error", "message": "No JSON"}), 400

    action   = data.get('action', '').lower()
    symbol   = par_limpio(data.get('symbol', ''))
    trade_id = data.get('trade_id', 'N/A')
    side     = data.get('side', '').lower()
    price    = data.get('price', 'N/A')
    sym_ccxt = par_ccxt(symbol)

    print(f"\n{'='*50}")
    log(f"🚨 WEBHOOK: {action.upper()} | {symbol} | Trade #{trade_id}")
    print(f"{'='*50}")

    # ─── FILTRO: Par permitido ───
    if symbol not in PARES_PERMITIDOS:
        log(f"❌ RECHAZADO — {symbol} no está en la lista {PARES_PERMITIDOS}")
        return jsonify({"status": "rejected", "reason": f"Par {symbol} no permitido"}), 200

    # ═══════════════════════════════════════════════════
    # OPEN — Abrir nuevo trade
    # ═══════════════════════════════════════════════════
    if action == 'open':
        if symbol in trades_abiertos:
            log(f"❌ RECHAZADO — Ya hay trade en {symbol} (#{trades_abiertos[symbol]['trade_id']})")
            return jsonify({"status": "rejected", "reason": "Max 1 trade por par"}), 200

        sl   = data.get('sl', 'N/A')
        conf = data.get('conf', 'N/A')

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
        "modo": "TESTNET" if exchange.sandbox else "REAL",
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
        bal = exchange.fetch_balance()
        usdt = bal.get('USDT', {})
        return jsonify({
            "total": usdt.get('total', 0),
            "libre": usdt.get('free', 0),
            "en_uso": usdt.get('used', 0),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    modo = "🧪 DRY RUN (simulación)" if DRY_RUN else "🔴 PRODUCCIÓN REAL"

    print()
    print("╔══════════════════════════════════════════════════╗")
    print(f"║   🤖 Gaussian Trend v5 Pro Bot                   ║")
    print(f"║   {modo:<46} ║")
    print("╠══════════════════════════════════════════════════╣")
    print(f"║   💰 Monto    : ${MONTO_POR_TRADE} por trade              ║")
    print(f"║   ⚡ Leverage : x{LEVERAGE}                              ║")
    print(f"║   🪙 Pares    : {', '.join(PARES_PERMITIDOS):<22}     ║")
    print(f"║   🔒 Máx/par  : {MAX_TRADES_POR_PAR} trade                        ║")
    print("╠══════════════════════════════════════════════════╣")
    print("║   📡 Webhook   : http://127.0.0.1:5000/webhook  ║")
    print("║   📊 Estado    : http://127.0.0.1:5000/status   ║")
    print("╚══════════════════════════════════════════════════╝")

    if DRY_RUN:
        print(f"\n✅ Modo DRY RUN activo — todas las órdenes se simulan localmente")
        print(f"   Para operar con dinero real, cambia DRY_RUN = False en bot_test.py")
    else:
        try:
            bal = exchange.fetch_balance()
            usdt_total = bal.get('USDT', {}).get('total', 0)
            print(f"\n✅ Conectado a Binance")
            print(f"💰 Balance USDT: {usdt_total}")
        except Exception as e:
            print(f"\n❌ Error conectando a Binance: {e}")
            print("   Verifica tus API Keys en las líneas 14-15 del archivo")

    print()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=(not os.environ.get("RAILWAY_ENVIRONMENT")))

