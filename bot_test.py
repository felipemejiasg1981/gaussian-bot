from flask import Flask, request, jsonify
from datetime import datetime
import ccxt
import os
import time
import threading
from pathlib import Path

app = Flask(__name__)


def load_local_env():
    """Carga variables desde .env comunes del workspace si existen."""
    env_candidates = [
        Path(__file__).with_name(".env"),
        Path(__file__).resolve().parent.parent / "gaussian-bot" / ".env",
    ]

    for env_path in env_candidates:
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
        except Exception as e:
            print(f"⚠️ Error cargando {env_path}: {e}")


load_local_env()

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN DEL BOT — Gaussian Trend v5 Pro Bot         ║
# ╚══════════════════════════════════════════════════════════════╝

# 🧪 MODO DE OPERACIÓN
# True  = Simula localmente (no toca el exchange, ideal para probar)
# False = Opera en Bitget REAL con dinero real
DRY_RUN = False

# 🪙 Universo dinámico — Cualquier swap USDT listado por Bitget
USDT_ONLY = True

# 💰 Capital por trade (en USDT)
MONTO_POR_TRADE = 8.0

# ⚡ Apalancamiento
LEVERAGE = 10

# 🔒 Límites de trades
MAX_TRADES_POR_PAR = 1
MAX_TOTAL_TRADES   = 10  # Máximo 10 criptos en operación a la vez

# 🎯 Umbral mínimo de confianza para abrir
# 0 = el bot no vuelve a filtrar señales que ya aprobó TradingView
MIN_CONF_OPEN = 0

# 🛟 SL de emergencia para recuperación tras reinicios
EMERGENCY_SL_PCT = 3.0

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
EVENTS_FILE = "webhook_events.json"
trades_abiertos = {}
historial = []
webhook_eventos = []


def record_webhook_event(symbol, action, status, reason, trade_id='N/A', **extra):
    """Guarda un historial corto de lo que realmente hizo el bot con cada webhook."""
    event = {
        "ts": datetime.now().isoformat(),
        "symbol": symbol,
        "action": action,
        "status": status,
        "reason": reason,
        "trade_id": trade_id,
    }
    if extra:
        event.update(extra)
    webhook_eventos.append(event)
    if len(webhook_eventos) > 200:
        del webhook_eventos[:-200]
    try:
        with open(EVENTS_FILE, 'w') as f:
            json.dump(webhook_eventos, f, indent=4)
    except Exception as e:
        log(f"⚠️ Error guardando eventos webhook: {e}")


def trade_id_matches(symbol, incoming_trade_id):
    """Evita que un close/update viejo cierre o modifique un trade nuevo del mismo símbolo."""
    if symbol not in trades_abiertos:
        return True
    current_trade_id = str(trades_abiertos[symbol].get('trade_id', 'N/A'))
    incoming_trade_id = str(incoming_trade_id or 'N/A')
    if incoming_trade_id == 'N/A' or current_trade_id == 'N/A':
        return True
    return incoming_trade_id == current_trade_id

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


def load_webhook_events():
    """Carga el historial reciente de eventos del bot."""
    global webhook_eventos
    if os.path.exists(EVENTS_FILE):
        try:
            with open(EVENTS_FILE, 'r') as f:
                data = json.load(f)
                webhook_eventos.clear()
                if isinstance(data, list):
                    webhook_eventos.extend(data[-200:])
        except Exception as e:
            log(f"⚠️ Error cargando eventos webhook: {e}")

# Cargar trades al iniciar el módulo
load_trades()
load_webhook_events()


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


def local_symbol_from_ccxt(symbol_ccxt):
    """Convierte BTC/USDT:USDT → BTCUSDT."""
    if not symbol_ccxt:
        return ""
    return symbol_ccxt.upper().replace("/", "").replace(":USDT", "")


def get_supported_usdt_pairs():
    """Lista dinámica de swaps USDT disponibles en Bitget."""
    ex = get_exchange()
    if not ex or not getattr(ex, 'markets', None):
        return []

    pairs = []
    for market in ex.markets.values():
        settle = (market.get('settleId') or market.get('settle') or '').upper()
        if market.get('swap') and settle == 'USDT' and market.get('active', True):
            symbol = market.get('symbol')
            if symbol:
                pairs.append(symbol)
    return sorted(pairs)


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


def fetch_trigger_orders(symbol_ccxt):
    """Devuelve las órdenes trigger activas del símbolo."""
    ex = get_exchange()
    if not ex:
        return []

    market = get_swap_market(symbol_ccxt)
    params = {
        'productType': product_type_for_market(market),
        'marginCoin': market.get('settleId') or 'USDT',
        'stop': True,
    }
    return ex.fetch_open_orders(symbol_ccxt, params=params)


def extract_trigger_price(order):
    """Extrae el trigger price desde formatos CCXT/Bitget."""
    info = order.get('info', {}) if isinstance(order.get('info'), dict) else {}
    raw = (
        order.get('triggerPrice')
        or order.get('stopPrice')
        or info.get('triggerPrice')
        or info.get('stopPrice')
        or info.get('planPrice')
    )
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def extract_order_amount(order):
    """Extrae cantidad de una orden trigger."""
    info = order.get('info', {}) if isinstance(order.get('info'), dict) else {}
    raw = order.get('amount') or order.get('remaining') or info.get('size') or info.get('qty')
    try:
        return abs(float(raw))
    except (TypeError, ValueError):
        return None


def find_matching_sl_trigger(symbol_ccxt, side, sl_price, quantity):
    """Busca un trigger SL que ya proteja la posición actual."""
    close_side = 'sell' if side == 'buy' else 'buy'
    trigger_orders = fetch_trigger_orders(symbol_ccxt)
    sl_price = float(sl_price)
    price_tol = max(abs(sl_price) * 0.001, 1e-8)
    qty_tol = max(abs(quantity) * 0.05, 1e-8)

    for order in trigger_orders:
        order_side = (order.get('side') or '').lower()
        trigger_price = extract_trigger_price(order)
        order_amount = extract_order_amount(order)

        if order_side != close_side or trigger_price is None:
            continue
        if abs(trigger_price - sl_price) > price_tol:
            continue
        if order_amount is not None and abs(order_amount - quantity) > qty_tol:
            continue
        return order

    return None


def find_existing_sl_trigger(symbol_ccxt, side, quantity):
    """Busca el mejor candidato a SL existente para una posición sin estado local."""
    close_side = 'sell' if side == 'buy' else 'buy'
    trigger_orders = fetch_trigger_orders(symbol_ccxt)
    qty_tol = max(abs(quantity) * 0.05, 1e-8)
    candidates = []

    for order in trigger_orders:
        order_side = (order.get('side') or '').lower()
        trigger_price = extract_trigger_price(order)
        order_amount = extract_order_amount(order)

        if order_side != close_side or trigger_price is None:
            continue
        if order_amount is not None and abs(order_amount - quantity) > qty_tol:
            continue
        candidates.append(order)

    if len(candidates) == 1:
        return candidates[0]
    return None


def ensure_sl_protection(symbol, trade, symbol_ccxt, side, sl_price, quantity):
    """Verifica que exista un trigger SL correcto; lo recrea si falta."""
    try:
        existing_sl = find_matching_sl_trigger(symbol_ccxt, side, sl_price, quantity)
    except Exception as e:
        log(f"   ⚠️ No se pudo verificar SL de {symbol}: {e}")
        existing_sl = None

    if existing_sl:
        trade['sl_order_id'] = existing_sl.get('id')
        return True

    log(f"   🛡️ SL faltante o desalineado en {symbol}; recreando trigger @ {sl_price}")
    sl_order = colocar_sl_exchange(symbol_ccxt, side, quantity, sl_price)
    if sl_order:
        trade['sl_order_id'] = sl_order.get('id')
        return True

    return False


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
        contracts = abs(float(position.get('contracts') or 0))
        trades_abiertos[symbol] = {
            "trade_id": trade_id,
            "side": side,
            "entry": entry_price,
            "sl": trades_abiertos.get(symbol, {}).get('sl', 'N/A'),
            "emergency_sl": trades_abiertos.get(symbol, {}).get(
                'emergency_sl',
                trades_abiertos.get(symbol, {}).get('sl', 'N/A')
            ),
            "trend_line": trades_abiertos.get(symbol, {}).get('trend_line', 'N/A'),
            "posicion_pct": trades_abiertos.get(symbol, {}).get('posicion_pct', 100),
            "order_id": trades_abiertos.get(symbol, {}).get('order_id'),
            "symbol_ccxt": symbol_ccxt,
            "cantidad_original": trades_abiertos.get(symbol, {}).get('cantidad_original', contracts),
            "sl_order_id": trades_abiertos.get(symbol, {}).get('sl_order_id'),
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


def normalize_price(symbol_ccxt, price):
    """Ajusta el precio a la precisión del exchange si está disponible."""
    try:
        ex = get_exchange()
        if ex:
            return float(ex.price_to_precision(symbol_ccxt, price))
    except Exception:
        pass
    return float(price)


def compute_emergency_sl(symbol_ccxt, side, entry_price, pct=EMERGENCY_SL_PCT):
    """Calcula un SL de emergencia basado en % desde la entrada."""
    try:
        entry = float(entry_price)
    except (TypeError, ValueError):
        return None

    if side == 'buy':
        sl_price = entry * (1.0 - pct / 100.0)
    elif side == 'sell':
        sl_price = entry * (1.0 + pct / 100.0)
    else:
        return None

    return normalize_price(symbol_ccxt, sl_price)


def preferred_recovery_sl(trade, symbol_ccxt, side, entry_price):
    """Prioriza SLs persistidos desde Pine antes de caer al fallback porcentual."""
    candidates = [
        trade.get('emergency_sl'),
        trade.get('sl'),
        trade.get('trend_line'),
    ]

    for candidate in candidates:
        if candidate in (None, '', 'N/A'):
            continue
        try:
            return normalize_price(symbol_ccxt, float(candidate))
        except (TypeError, ValueError):
            continue

    return compute_emergency_sl(symbol_ccxt, side, entry_price)


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
        return {'id': fake_id, 'status': 'simulated', 'amount': cantidad}

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
        order['amount'] = cantidad  # Guardar cantidad calculada
        log(f"   ✅ ORDEN EJECUTADA: {side.upper()} {cantidad} {symbol_ccxt}")
        log(f"   📋 Order ID: {order['id']}")
        return order
    except Exception as e:
        log(f"   ❌ ERROR CRÍTICO al abrir orden en Bitget: {e}")
        return None


def colocar_sl_exchange(symbol_ccxt, side, cantidad, sl_price):
    """Coloca una orden Stop Loss REAL en Bitget como trigger order"""
    close_side = 'sell' if side == 'buy' else 'buy'

    if DRY_RUN:
        fake_id = f"DRY-SL-{datetime.now().strftime('%H%M%S')}"
        log(f"   🛡️ [DRY RUN] SL COLOCADO en exchange: {close_side.upper()} {cantidad} @ trigger={sl_price}")
        return {'id': fake_id, 'status': 'simulated'}

    try:
        ex = get_exchange()
        if not ex:
            log("   ❌ No se pudo colocar SL: Exchange no disponible")
            return None

        params = build_bitget_params(symbol_ccxt, reduce_only=True)
        params['triggerPrice'] = float(sl_price)
        params['triggerType'] = 'mark_price'

        order = ex.create_order(
            symbol=symbol_ccxt,
            type='market',
            side=close_side,
            amount=float(cantidad),
            params=params,
        )
        log(f"   🛡️ SL REAL colocado en Bitget: {close_side.upper()} {cantidad} @ trigger={sl_price}")
        log(f"   📋 SL Order ID: {order['id']}")
        return order
    except Exception as e:
        log(f"   ⚠️ ERROR colocando SL en Bitget: {e}")
        return None


def cancelar_ordenes_trigger(symbol_ccxt):
    """Cancela todas las órdenes trigger (SL/TP) abiertas para un símbolo"""
    if DRY_RUN:
        log(f"   🗑️ [DRY RUN] Órdenes trigger canceladas para {symbol_ccxt}")
        return True

    try:
        ex = get_exchange()
        if not ex:
            return False

        open_orders = ex.fetch_open_orders(symbol_ccxt, params={
            'productType': 'USDT-FUTURES',
            'stop': True,  # Fetch trigger/stop orders
        })
        for order in open_orders:
            try:
                ex.cancel_order(order['id'], symbol_ccxt, params={
                    'productType': 'USDT-FUTURES',
                    'stop': True,
                })
                log(f"   🗑️ Orden trigger cancelada: {order['id']}")
            except Exception as e:
                log(f"   ⚠️ Error cancelando orden {order['id']}: {e}")
        return True
    except Exception as e:
        log(f"   ⚠️ Error obteniendo órdenes trigger: {e}")
        return False


def reconcile_startup_trades():
    """Reconcilia trades locales con posiciones y SLs reales al arrancar."""
    if DRY_RUN:
        return

    ex = get_exchange()
    if not ex:
        log("⚠️ Reconciliación omitida: exchange no disponible")
        return

    if not trades_abiertos:
        log("ℹ️ Reconciliación: no hay trades persistidos para revisar")
        return

    log(f"🔄 Reconciliando {len(trades_abiertos)} trades al arrancar...")
    dirty = False

    for symbol in list(trades_abiertos.keys()):
        trade = trades_abiertos.get(symbol, {})
        symbol_ccxt = trade.get('symbol_ccxt') or par_ccxt(symbol)

        try:
            position = fetch_open_position(symbol_ccxt)
        except Exception as e:
            log(f"   ⚠️ No se pudo revisar {symbol}: {e}")
            continue

        if not position:
            log(f"   🧹 Trade local sin posición real en exchange: {symbol} — eliminado")
            trades_abiertos.pop(symbol, None)
            dirty = True
            continue

        side = infer_trade_side_from_position(position)
        if not side:
            log(f"   ⚠️ No se pudo inferir side para {symbol}; se conserva como estaba")
            side = trade.get('side', 'buy')

        contracts = abs(float(position.get('contracts') or 0))
        entry_price = position.get('entryPrice') or trade.get('entry', 'N/A')

        trade['side'] = side
        trade['entry'] = entry_price
        trade['symbol_ccxt'] = symbol_ccxt
        trade['cantidad_original'] = max(float(trade.get('cantidad_original', 0) or 0), contracts)
        trade['posicion_pct'] = trade.get('posicion_pct', 100) or 100
        trade['synced_from_exchange'] = True

        sl_value = trade.get('sl', 'N/A')
        if sl_value == 'N/A':
            recovery_sl = preferred_recovery_sl(trade, symbol_ccxt, side, entry_price)
            if recovery_sl is not None:
                trade['sl'] = recovery_sl
                trade['emergency_sl'] = recovery_sl
                if ensure_sl_protection(symbol, trade, symbol_ccxt, side, recovery_sl, contracts):
                    log(f"   🛟 {symbol} no tenía SL persistido; se restauró/cargó SL @ {recovery_sl}")
                else:
                    log(f"   ❌ {symbol} sin SL persistido; falló la creación del SL recuperado")
            else:
                log(f"   ⚠️ {symbol} sigue abierto pero no tiene SL persistido; requiere revisión manual")
            trades_abiertos[symbol] = trade
            dirty = True
            continue

        try:
            sl_price = float(sl_value)
        except (TypeError, ValueError):
            recovery_sl = preferred_recovery_sl(trade, symbol_ccxt, side, entry_price)
            if recovery_sl is not None:
                trade['sl'] = recovery_sl
                trade['emergency_sl'] = recovery_sl
                if ensure_sl_protection(symbol, trade, symbol_ccxt, side, recovery_sl, contracts):
                    log(f"   🛟 {symbol} tenía SL inválido ({sl_value}); se restauró/cargó SL @ {recovery_sl}")
                else:
                    log(f"   ❌ {symbol} tenía SL inválido ({sl_value}); falló la creación del SL recuperado")
            else:
                log(f"   ⚠️ {symbol} tiene SL inválido ({sl_value}); requiere revisión manual")
            trades_abiertos[symbol] = trade
            dirty = True
            continue

        if ensure_sl_protection(symbol, trade, symbol_ccxt, side, sl_price, contracts):
            log(f"   ✅ SL verificado para {symbol}: trigger @ {sl_price}")
        else:
            log(f"   ❌ No se pudo restaurar SL para {symbol}; queda solo con monitor de seguridad")

        trade['emergency_sl'] = trade.get('emergency_sl', sl_price)

        trades_abiertos[symbol] = trade
        dirty = True

    if dirty:
        save_trades()


def reconcile_orphan_positions():
    """Detecta posiciones reales en Bitget que no existen en trades.json."""
    if DRY_RUN:
        return

    ex = get_exchange()
    if not ex:
        log("⚠️ Revisión de posiciones huérfanas omitida: exchange no disponible")
        return

    try:
        positions = ex.fetch_positions()
    except Exception as e:
        log(f"⚠️ No se pudieron obtener posiciones abiertas del exchange: {e}")
        return

    dirty = False
    for position in positions:
        contracts = abs(float(position.get('contracts') or 0))
        if contracts <= 0:
            continue

        symbol_ccxt = position.get('symbol', '')
        market = position.get('info', {}) if isinstance(position.get('info'), dict) else {}
        settle = (position.get('settle') or position.get('settleId') or market.get('marginCoin') or '').upper()
        if not symbol_ccxt or settle != 'USDT':
            continue

        local_symbol = local_symbol_from_ccxt(symbol_ccxt)
        if not local_symbol or local_symbol in trades_abiertos:
            continue

        side = infer_trade_side_from_position(position)
        if not side:
            log(f"   ⚠️ Posición huérfana detectada pero sin side interpretable: {symbol_ccxt}")
            continue

        entry_price = position.get('entryPrice') or position.get('average') or 'N/A'
        existing_sl = None
        try:
            existing_sl = find_existing_sl_trigger(symbol_ccxt, side, contracts)
        except Exception as e:
            log(f"   ⚠️ No se pudo inspeccionar trigger SL de huérfana {symbol_ccxt}: {e}")

        sl_price = extract_trigger_price(existing_sl) if existing_sl else 'N/A'
        sl_order_id = existing_sl.get('id') if existing_sl else None

        if sl_price == 'N/A':
            emergency_sl = compute_emergency_sl(symbol_ccxt, side, entry_price)
            sl_price = emergency_sl if emergency_sl is not None else 'N/A'

        trades_abiertos[local_symbol] = {
            "trade_id": f"RECOVERED-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "side": side,
            "entry": entry_price,
            "sl": sl_price,
            "emergency_sl": sl_price,
            "trend_line": 'N/A',
            "posicion_pct": 100,
            "order_id": None,
            "symbol_ccxt": symbol_ccxt,
            "cantidad_original": contracts,
            "sl_order_id": sl_order_id,
            "synced_from_exchange": True,
            "orphan_recovered": True,
        }
        dirty = True

        if existing_sl:
            log(f"   ✅ Posición huérfana recuperada: {local_symbol} | SL detectado @ {sl_price}")
        else:
            if sl_price != 'N/A' and ensure_sl_protection(local_symbol, trades_abiertos[local_symbol], symbol_ccxt, side, sl_price, contracts):
                log(f"   🛟 Posición huérfana recuperada con SL de emergencia: {local_symbol} | SL @ {sl_price}")
            else:
                log(f"   ⚠️ Posición huérfana recuperada sin SL detectado: {local_symbol} | requiere revisión manual")

    if dirty:
        save_trades()


def cerrar_parcial(symbol_ccxt, side_original, porcentaje, precio, symbol=None):
    """Cierra un % de la posición ORIGINAL (no de lo que queda)"""
    if DRY_RUN:
        # En DRY_RUN, usar cantidad_original guardada si existe
        cantidad_original = 0
        if symbol and symbol in trades_abiertos:
            cantidad_original = float(trades_abiertos[symbol].get('cantidad_original', 0))
        if cantidad_original <= 0:
            cantidad_original = calcular_cantidad(symbol_ccxt, precio)
        cantidad_cerrar = round(cantidad_original * (porcentaje / 100.0), 6)
        close_side = 'sell' if side_original == 'buy' else 'buy'
        log(f"   ✅ [DRY RUN] PARCIAL: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}% del original={cantidad_original})")
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

        # FIX Bug 2: Usar cantidad ORIGINAL, no la posición actual
        cantidad_original = 0
        if symbol and symbol in trades_abiertos:
            cantidad_original = float(trades_abiertos[symbol].get('cantidad_original', 0))
        if cantidad_original <= 0:
            # Fallback: usar posición actual (comportamiento anterior)
            cantidad_original = abs(float(position.get('contracts') or 0))
            log(f"   ⚠️ No se encontró cantidad_original, usando posición actual: {cantidad_original}")

        cantidad_cerrar = cantidad_original * (porcentaje / 100.0)
        cantidad_cerrar = float(ex.amount_to_precision(symbol_ccxt, cantidad_cerrar))

        # Asegurar que no cerramos más de lo que queda
        cantidad_actual = abs(float(position.get('contracts') or 0))
        if cantidad_cerrar > cantidad_actual:
            log(f"   ⚠️ Ajustando cierre: {cantidad_cerrar} > posición actual {cantidad_actual}")
            cantidad_cerrar = cantidad_actual

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
        log(f"   ✅ PARCIAL CERRADO: {close_side.upper()} {cantidad_cerrar} {symbol_ccxt} ({porcentaje}% del original={cantidad_original})")
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
    supported_pairs = get_supported_usdt_pairs()
    activos = len(trades_abiertos)
    modo = "🧪 DRY RUN" if DRY_RUN else "🔴 REAL"
    pares_msg = "Swaps USDT de Bitget (dinámico)" if not supported_pairs else f"{len(supported_pairs)} pares USDT dinámicos"
    return (
        f"✅ Bot Gaussian v5 Pro en línea — {modo}<br>"
        f"💰 ${MONTO_POR_TRADE} x{LEVERAGE} por trade<br>"
        f"🪙 Pares: {pares_msg}<br>"
        f"📊 Trades activos: {activos}/{MAX_TOTAL_TRADES}<br>"
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
        record_webhook_event("", "unknown", "ignored", "Payload is not JSON")
        print(f"{'='*50}")
        return jsonify({"status": "ignored", "message": "Payload is not JSON"}), 200
    
    action   = data.get('action', '').lower()
    symbol   = par_limpio(data.get('symbol', ''))
    trade_id = data.get('trade_id', 'N/A')
    side     = data.get('side', '').lower()
    price    = data.get('price', 'N/A')
    exchange = data.get('exchange', 'N/A')
    timeframe = data.get('timeframe', 'N/A')
    market = data.get('market', 'N/A')
    emergency_sl = data.get('emergency_sl', 'N/A')
    trend_line = data.get('trend_line', 'N/A')
    sym_ccxt = par_ccxt(symbol)

    log(f"📋 PROCESANDO: {action.upper()} | {symbol} (Orig: {data.get('symbol')}) | {exchange} {timeframe} | {market} | Trade #{trade_id}")
    print(f"{'='*50}")

    # ─── FILTRO: Solo swaps USDT válidos de Bitget ───
    if USDT_ONLY and not symbol.endswith("USDT"):
        log(f"❌ RECHAZADO — {symbol} no es un par USDT válido")
        record_webhook_event(symbol, action, "rejected", "Solo pares USDT", trade_id=trade_id)
        return jsonify({"status": "rejected", "reason": "Solo pares USDT"}), 200

    try:
        get_swap_market(sym_ccxt)
    except Exception as e:
        log(f"❌ RECHAZADO — {symbol} no está disponible en Bitget USDT swaps: {e}")
        record_webhook_event(symbol, action, "rejected", "Par no disponible en Bitget USDT swaps", trade_id=trade_id)
        return jsonify({"status": "rejected", "reason": "Par no disponible en Bitget USDT swaps"}), 200

    # ═══════════════════════════════════════════════════
    # OPEN — Abrir nuevo trade
    # ═══════════════════════════════════════════════════
    if action == 'open':
        # 1. Límite de trades totales (Max 5)
        if len(trades_abiertos) >= MAX_TOTAL_TRADES:
            log(f"❌ RECHAZADO — Límite de {MAX_TOTAL_TRADES} trades simultáneos alcanzado. (Activos: {len(trades_abiertos)})")
            record_webhook_event(symbol, action, "rejected", "Max total trades reached", trade_id=trade_id)
            return jsonify({"status": "rejected", "reason": "Max total trades reached"}), 200

        # 2. Límite por par (Max 1)
        if MAX_TRADES_POR_PAR <= 1 and symbol in trades_abiertos:
            log(f"❌ RECHAZADO — Ya hay trade en {symbol} (#{trades_abiertos[symbol]['trade_id']})")
            record_webhook_event(symbol, action, "rejected", f"Max {MAX_TRADES_POR_PAR} trade por par", trade_id=trade_id)
            return jsonify({"status": "rejected", "reason": f"Max {MAX_TRADES_POR_PAR} trade por par"}), 200

        sl   = data.get('sl', 'N/A')
        try:
            conf = int(float(data.get('conf', 0)))
        except Exception:
            conf = 0

        # 3. Filtro opcional de confianza
        if conf < MIN_CONF_OPEN:
            log(f"❌ RECHAZADO — Confianza insuficiente: {conf}/10. Necesitas >= {MIN_CONF_OPEN} para abrir.")
            record_webhook_event(symbol, action, "rejected", f"Low confidence ({conf}/10, need >= {MIN_CONF_OPEN})", trade_id=trade_id, conf=conf)
            return jsonify({"status": "rejected", "reason": f"Low confidence ({conf}/10, need >= {MIN_CONF_OPEN})"}), 200

        log(f"📤 ABRIENDO {side.upper()} {symbol} @ {price} | Confianza: {conf}/10")
        log(f"   💵 ${MONTO_POR_TRADE} x{LEVERAGE} = ${MONTO_POR_TRADE * LEVERAGE} exposición")

        # ── EJECUTAR EN BITGET ──
        order = abrir_orden(sym_ccxt, side, price)

        position_confirmation = wait_for_position_confirmation(sym_ccxt)

        if order and position_confirmation and (order.get('id') or order.get('status') == 'simulated' or not DRY_RUN):
            # Obtener cantidad real de la posición confirmada
            cantidad_original = 0
            if not DRY_RUN and isinstance(position_confirmation, dict):
                cantidad_original = abs(float(position_confirmation.get('contracts') or 0))
            if cantidad_original <= 0:
                cantidad_original = order.get('amount', 0)

            trades_abiertos[symbol] = {
                "trade_id": trade_id,
                "side": side,
                "entry": price,
                "sl": sl,
                "emergency_sl": emergency_sl if emergency_sl != 'N/A' else sl,
                "trend_line": trend_line,
                "posicion_pct": 100,
                "order_id": order['id'] if order else None,
                "symbol_ccxt": sym_ccxt,
                "cantidad_original": cantidad_original,
                "sl_order_id": None,
            }

            # 🛡️ FIX Bug 1+5: Colocar SL REAL en Bitget
            if sl != 'N/A' and cantidad_original > 0:
                sl_order = colocar_sl_exchange(sym_ccxt, side, cantidad_original, sl)
                if sl_order:
                    trades_abiertos[symbol]['sl_order_id'] = sl_order.get('id')

            save_trades() # Persistir cambio
            record_webhook_event(symbol, action, "opened", "Trade abierto", trade_id=trade_id, side=side, conf=conf, order_id=order.get('id') if order else None)
        else:
            log(f"❌ ABORTADO — Bitget no confirmó posición abierta para {symbol}")
            record_webhook_event(symbol, action, "error", "Exchange position not confirmed", trade_id=trade_id, side=side, conf=conf)
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
                record_webhook_event(symbol, action, "ignored", "No trade abierto", trade_id=trade_id)
                return jsonify({"status": "error", "reason": "No trade abierto"}), 200

        if not trade_id_matches(symbol, trade_id):
            current_trade_id = trades_abiertos[symbol].get('trade_id', 'N/A')
            log(f"⚠️  partial_close IGNORADO — trade_id viejo {trade_id} no coincide con activo {current_trade_id} en {symbol}")
            record_webhook_event(symbol, action, "ignored", f"Stale trade_id ({trade_id} != {current_trade_id})", trade_id=trade_id)
            return jsonify({"status": "ignored", "reason": "Stale trade_id"}), 200

        side_orig = trades_abiertos[symbol]['side']
        log(f"🎯 {reason} HIT — Cerrando {close_pct}% de {symbol}")

        # ── EJECUTAR PARCIAL EN BITGET (FIX Bug 2: usa % del original) ──
        partial_order = cerrar_parcial(sym_ccxt, side_orig, close_pct, price, symbol=symbol)
        if not partial_order:
            log(f"❌ ABORTADO — El parcial no se pudo ejecutar en Bitget")
            record_webhook_event(symbol, action, "error", "Partial close failed", trade_id=trade_id, reason_tp=reason)
            return jsonify({"status": "error", "reason": "Partial close failed"}), 200

        trades_abiertos[symbol]['sl'] = new_sl
        if emergency_sl != 'N/A':
            trades_abiertos[symbol]['emergency_sl'] = emergency_sl
        if trend_line != 'N/A':
            trades_abiertos[symbol]['trend_line'] = trend_line
        trades_abiertos[symbol]['posicion_pct'] = max(0, trades_abiertos[symbol]['posicion_pct'] - close_pct)

        # Actualizar SL en exchange si cambió
        if new_sl != 'N/A':
            cancelar_ordenes_trigger(sym_ccxt)
            position = fetch_open_position(sym_ccxt)
            if position:
                cant_restante = abs(float(position.get('contracts') or 0))
                if cant_restante > 0:
                    sl_order = colocar_sl_exchange(sym_ccxt, side_orig, cant_restante, new_sl)
                    if sl_order:
                        trades_abiertos[symbol]['sl_order_id'] = sl_order.get('id')

        save_trades() # Persistir cambio

        log(f"   🛑 SL movido a: {new_sl} (actualizado en Bitget)")
        log(f"   📊 Posición restante: {trades_abiertos[symbol]['posicion_pct']}%")
        record_webhook_event(symbol, action, "partial_closed", reason, trade_id=trade_id, close_pct=close_pct, new_sl=new_sl)

    # ═══════════════════════════════════════════════════
    # UPDATE_SL — Trailing SL dinámico
    # ═══════════════════════════════════════════════════
    elif action == 'update_sl':
        new_sl = data.get('new_sl', 'N/A')
        if symbol in trades_abiertos:
            if not trade_id_matches(symbol, trade_id):
                current_trade_id = trades_abiertos[symbol].get('trade_id', 'N/A')
                log(f"⚠️  update_sl IGNORADO — trade_id viejo {trade_id} no coincide con activo {current_trade_id} en {symbol}")
                record_webhook_event(symbol, action, "ignored", f"Stale trade_id ({trade_id} != {current_trade_id})", trade_id=trade_id)
                return jsonify({"status": "ignored", "reason": "Stale trade_id"}), 200

            old_sl = trades_abiertos[symbol].get('sl', '?')
            trades_abiertos[symbol]['sl'] = new_sl
            if emergency_sl != 'N/A':
                trades_abiertos[symbol]['emergency_sl'] = emergency_sl
            if trend_line != 'N/A':
                trades_abiertos[symbol]['trend_line'] = trend_line

            # FIX Bug 4: Actualizar SL REAL en Bitget (no solo texto)
            if new_sl != 'N/A':
                cancelar_ordenes_trigger(sym_ccxt)
                position = fetch_open_position(sym_ccxt)
                if position:
                    side_orig = trades_abiertos[symbol]['side']
                    cant_restante = abs(float(position.get('contracts') or 0))
                    if cant_restante > 0:
                        sl_order = colocar_sl_exchange(sym_ccxt, side_orig, cant_restante, new_sl)
                        if sl_order:
                            trades_abiertos[symbol]['sl_order_id'] = sl_order.get('id')

            save_trades() # Persistir cambio
            log(f"🔄 SL: {old_sl} → {new_sl} ({symbol}) — Actualizado en Bitget")
            record_webhook_event(symbol, action, "updated", f"SL {old_sl} -> {new_sl}", trade_id=trade_id)
        else:
            synced_trade = sync_trade_from_exchange(symbol, sym_ccxt, trade_id=trade_id, fallback_price=price)
            if synced_trade:
                old_sl = synced_trade.get('sl', '?')
                trades_abiertos[symbol]['sl'] = new_sl
                if emergency_sl != 'N/A':
                    trades_abiertos[symbol]['emergency_sl'] = emergency_sl
                if trend_line != 'N/A':
                    trades_abiertos[symbol]['trend_line'] = trend_line
                save_trades()
                log(f"🔄 SL: {old_sl} → {new_sl} ({symbol}, reconstruido desde exchange)")
                record_webhook_event(symbol, action, "updated", f"SL {old_sl} -> {new_sl} (reconstruido)", trade_id=trade_id)
            else:
                log(f"⚠️  update_sl para {symbol} pero no hay trade abierto")
                record_webhook_event(symbol, action, "ignored", "No trade abierto", trade_id=trade_id)

    # ═══════════════════════════════════════════════════
    # ADD — Entry 2 / Re-entry
    # ═══════════════════════════════════════════════════
    elif action == 'add':
        reason = data.get('reason', 'N/A')
        log(f"➕ {reason.upper()} — {side.upper()} {symbol} @ {price}")

        # ── EJECUTAR ADD EN BINANCE ──
        abrir_orden(sym_ccxt, side, price)
        record_webhook_event(symbol, action, "processed", reason, trade_id=trade_id, side=side)

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
            if not trade_id_matches(symbol, trade_id):
                current_trade_id = trades_abiertos[symbol].get('trade_id', 'N/A')
                log(f"⚠️  close IGNORADO — trade_id viejo {trade_id} no coincide con activo {current_trade_id} en {symbol}")
                record_webhook_event(symbol, action, "ignored", f"Stale trade_id ({trade_id} != {current_trade_id})", trade_id=trade_id, close_reason=reason)
                return jsonify({"status": "ignored", "reason": "Stale trade_id"}), 200

            side_orig = trades_abiertos[symbol]['side']

            # ── CERRAR TODO EN BITGET ──
            cancelar_ordenes_trigger(sym_ccxt)  # Cancelar SL trigger
            cerrar_todo(sym_ccxt, side_orig)

            trade_info = trades_abiertos.pop(symbol)
            save_trades() # Persistir cambio
            historial.append({**trade_info, "pnl": pnl, "closed_at": datetime.now().isoformat()})
            log(f"   🟢 {symbol} libre para nuevos trades")
            record_webhook_event(symbol, action, "closed", reason, trade_id=trade_id, pnl=pnl)
        else:
            log(f"⚠️  close para {symbol} pero no hay trade registrado")
            record_webhook_event(symbol, action, "ignored", "No trade registrado", trade_id=trade_id, close_reason=reason)

    else:
        log(f"⚠️  Acción desconocida: {action}")
        record_webhook_event(symbol, action, "ignored", "Acción desconocida", trade_id=trade_id)

    # Estado global
    print(f"\n📋 Trades activos: {len(trades_abiertos)}/{MAX_TOTAL_TRADES}")
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
        "eventos_webhook": webhook_eventos[-30:],
        "config": {
            "mercado": "Bitget USDT swaps",
            "pares_disponibles": len(get_supported_usdt_pairs()),
            "monto": MONTO_POR_TRADE,
            "leverage": LEVERAGE,
            "min_conf_open": MIN_CONF_OPEN,
            "max_trades_por_par": MAX_TRADES_POR_PAR,
            "max_total_trades": MAX_TOTAL_TRADES,
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


# ════════════════════════════════════════════════════════════════
# 🛡️ MONITOR DE SL — Red de seguridad (Thread en background)
# ════════════════════════════════════════════════════════════════
SL_MONITOR_INTERVAL = 30  # segundos

def monitor_sl_thread():
    """Cada 30s verifica si alguna posición superó su SL y la cierra"""
    while True:
        try:
            time.sleep(SL_MONITOR_INTERVAL)
            if DRY_RUN or not trades_abiertos:
                continue

            ex = get_exchange()
            if not ex:
                continue

            # Copiar keys para evitar RuntimeError por cambio durante iteración
            symbols_to_check = list(trades_abiertos.keys())
            for symbol in symbols_to_check:
                if symbol not in trades_abiertos:
                    continue
                trade = trades_abiertos[symbol]
                sl_str = trade.get('sl', 'N/A')

                try:
                    sym_ccxt = trade.get('symbol_ccxt') or par_ccxt(symbol)
                    side = trade.get('side', 'buy')
                    entry_price = trade.get('entry', 'N/A')
                    if sl_str == 'N/A':
                        recovery_sl = preferred_recovery_sl(trade, sym_ccxt, side, entry_price)
                        if recovery_sl is None:
                            log(f"   ⚠️ Monitor SL: {symbol} sigue sin SL recuperable")
                            continue
                        trade['sl'] = recovery_sl
                        trade['emergency_sl'] = recovery_sl
                        sl_str = recovery_sl
                        save_trades()

                    sl_price = float(sl_str)
                    ticker = ex.fetch_ticker(sym_ccxt)
                    current_price = float(ticker['last'])
                    position = fetch_open_position(sym_ccxt)

                    if not position:
                        log(f"   ℹ️ Monitor SL: {symbol} ya no tiene posición abierta en exchange, limpiando estado local")
                        trade_info = trades_abiertos.pop(symbol, {})
                        save_trades()
                        historial.append({**trade_info, "pnl": "SYNC_NO_POSITION", "closed_at": datetime.now().isoformat()})
                        continue

                    contracts = abs(float(position.get('contracts') or 0))
                    if contracts > 0:
                        trade['cantidad_original'] = max(float(trade.get('cantidad_original', 0) or 0), contracts)
                        if ensure_sl_protection(symbol, trade, sym_ccxt, side, sl_price, contracts):
                            save_trades()
                        else:
                            log(f"   ⚠️ Monitor SL: no se pudo garantizar trigger SL para {symbol}")

                    # Verificar si el precio superó el SL
                    sl_hit = False
                    if side == 'buy' and current_price <= sl_price:
                        sl_hit = True
                    elif side == 'sell' and current_price >= sl_price:
                        sl_hit = True

                    if sl_hit:
                        log(f"🚨 MONITOR SL: {symbol} precio={current_price} cruzó SL={sl_price} — CERRANDO")
                        cancelar_ordenes_trigger(sym_ccxt)
                        cerrar_todo(sym_ccxt, side)
                        trade_info = trades_abiertos.pop(symbol, {})
                        save_trades()
                        historial.append({**trade_info, "pnl": "SL_MONITOR", "closed_at": datetime.now().isoformat()})
                        log(f"   ✅ MONITOR SL: {symbol} cerrado exitosamente")
                except Exception as e:
                    log(f"   ⚠️ Monitor SL error para {symbol}: {e}")
        except Exception as e:
            log(f"⚠️ Monitor SL error general: {e}")


if not DRY_RUN:
    log("🚀 Iniciando bot en modo PRODUCCIÓN (Bitget)")
    # Intento de conexión inicial para validar llaves en el log
    get_exchange()
    reconcile_startup_trades()
    reconcile_orphan_positions()
    # Iniciar monitor de SL en background
    sl_thread = threading.Thread(target=monitor_sl_thread, daemon=True)
    sl_thread.start()
    log("🛡️ Monitor de SL iniciado (cada 30s)")

if __name__ == '__main__':
    modo = "🧪 DRY RUN (simulación)" if DRY_RUN else "🔴 PRODUCCIÓN REAL"

    print()
    print("╔══════════════════════════════════════════════════╗")
    print(f"║   🤖 Gaussian Trend IA Pro v6 EXTREME Bot       ║")
    print(f"║   {modo:<46} ║")
    print("╠══════════════════════════════════════════════════╣")
    print(f"║   💰 Monto    : ${MONTO_POR_TRADE} por trade              ║")
    print(f"║   ⚡ Leverage : x{LEVERAGE}                              ║")
    print("║   🪙 Pares    : Bitget USDT swaps (dinámico)     ║")
    print(f"║   🔒 Máx/par  : {MAX_TRADES_POR_PAR} trade                        ║")
    print("╠══════════════════════════════════════════════════╣")
    print("║   📡 Webhook   : http://127.0.0.1:5001/webhook  ║")
    print("║   📊 Estado    : http://127.0.0.1:5001/status   ║")
    print("║   🛡️  SL Monitor: Cada 30s (red de seguridad)   ║")
    print("╚══════════════════════════════════════════════════╝")

    port = int(os.environ.get("PORT", 5001))
    print(f"📡 Webhook en espera: http://0.0.0.0:{port}/webhook")
    app.run(host="0.0.0.0", port=port, debug=(not os.environ.get("RAILWAY_ENVIRONMENT")))
