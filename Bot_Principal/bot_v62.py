import os
import time
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify
import ccxt

# Importar el nuevo manager de Excel
from excel_manager import export_trades_to_excel

app = Flask(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN AISLADA — Gaussian v6.2 Bot                 ║
# ╚══════════════════════════════════════════════════════════════╝

# Carpeta base del bot (v6.2)
BASE_DIR = Path(__file__).resolve().parent
TRADES_FILE = BASE_DIR / "trades_v62.json"
EVENTS_FILE = BASE_DIR / "webhook_events_v62.json"
ERROR_LOG = BASE_DIR / "historial_de_fallos.md"
ANALYTICS_DB = BASE_DIR / "trade_analytics_v62.db"

# 🧪 MODO DE OPERACIÓN
DRY_RUN = os.environ.get("DRY_RUN", "False").lower() == "true"

# 💰 Capital por trade (en USDT)
MONTO_POR_TRADE = 10.0
LEVERAGE = 12

# 🔒 Límites de trades
MAX_TOTAL_TRADES = 20
MAX_TRADES_POR_PAR = 1
trade_lock = threading.Lock()

def normalizar_symbol(raw):
    """Normaliza símbolo: BITGET:SAFEUSDT.P o BASE/USDT:USDT → BASEUSDT"""
    s = str(raw).upper().strip()
    if ":" in s:
        parts = s.split(":")
        # Si tiene un '/' en la primera parte, es CCXT (BASE/USDT:USDT)
        if "/" in parts[0]:
            s = parts[0]
        else:
            # Si no, asumimos formato TRADINGVIEW (EXCHANGE:SYMBOL)
            s = parts[-1]
    
    s = s.replace(".P", "").replace("PERP", "").replace("-", "").replace("/", "").replace("BITGET", "")
    return s

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

# ════════════════════════════════════════════════════════════════
# CARGA DE CONFIGURACIÓN
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
# VARIABLES GLOBALES
# ════════════════════════════════════════════════════════════════

def sync_positions_from_exchange():
    """Sincronización robusa: reconcilia DB local con Bitget (Cierra huérfanos, agrega faltantes)."""
    ex = get_exchange()
    if not ex:
        log("⚠️ No se pudo sincronizar con exchange (sin conexión)")
        return
    try:
        # 1. Obtener posiciones reales de Bitget
        positions = ex.fetch_positions()
        real_trades_dict = {}
        for p in positions:
            contracts = abs(float(p.get('contracts', 0) or 0))
            if contracts > 0:
                raw_sym = p.get('symbol', '')
                norm_sym = normalizar_symbol(raw_sym)
                
                # ⚙️ FORZAR APALANCAMIENTO CONFIGURADO (FIX 10x vs 12x)
                try:
                    current_lev = int(float(p.get('leverage', 0)))
                    if current_lev != LEVERAGE:
                        ex.set_leverage(LEVERAGE, raw_sym)
                        log(f"⚙️ SYNC: Ajustando apalancamiento de {current_lev}x a {LEVERAGE}x para {norm_sym}")
                except Exception as lev_err:
                    log(f"⚠️ SYNC: No se pudo forzar apalancamiento en {norm_sym}: {lev_err}")

                real_trades_dict[norm_sym] = {
                    "symbol": norm_sym,
                    "side": p.get('side', 'unknown'),
                    "amount": contracts,
                    "entry": float(p.get('entryPrice', 0) or 0),
                    "ts": datetime.now().isoformat()
                }

        # 2. Reconciliar Base de Datos
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        
        # A. Detectar huérfanos: están 'open' en DB pero NO en Bitget
        c.execute("SELECT id, symbol FROM trades WHERE status = 'open'")
        db_open_trades = c.fetchall()
        orphans_closed = 0
        for db_id, db_sym in db_open_trades:
            if db_sym not in real_trades_dict:
                # Marcar como cerrado (razón: SYNC_MISSING)
                log(f"🧹 SYNC: Cerrando huérfano en DB: {db_sym} (No encontrado en Bitget)")
                c.execute('''UPDATE trades SET status = 'closed', close_reason = 'SYNC_MISSING', 
                            closed_at = ? WHERE id = ?''', (datetime.now().isoformat(), db_id))
                orphans_closed += 1
        
        # B. Agregar faltantes: están en Bitget pero NO 'open' en DB
        new_synced = 0
        for norm_sym, tdata in real_trades_dict.items():
            c.execute("SELECT COUNT(*) FROM trades WHERE symbol = ? AND status = 'open'", (norm_sym,))
            if c.fetchone()[0] == 0:
                log(f"➕ SYNC: Agregando posición faltante a DB: {norm_sym} ({tdata['side']})")
                c.execute('''INSERT INTO trades 
                    (trade_id, symbol, side, entry_price, leverage, amount_usdt, contracts, opened_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')''', (
                    f"SYNC-{int(time.time())}", norm_sym, tdata['side'], tdata['entry'], 
                    LEVERAGE, MONTO_POR_TRADE, tdata['amount'], tdata['ts']
                ))
                new_synced += 1
        
        conn.commit()
        conn.close()

        # 3. Sincronizar Memoria trades_abiertos
        trades_abiertos.clear()
        for sym, data in real_trades_dict.items():
            trades_abiertos[sym] = {
                "id": f"SYNC-{int(time.time())}", 
                "side": data['side'], 
                "entry": data['entry'],
                "amount": data['amount'], 
                "ts": data['ts']
            }
        save_state()

        log(f"🔄 SYNC: {len(real_trades_dict)} posiciones reales | {orphans_closed} huérfanos cerrados | {new_synced} nuevos registrados")
        
        # 4. Forzar actualización del Excel
        threading.Thread(target=export_trades_to_excel).start()

    except Exception as e:
        log(f"❌ Error de sincronización robusa: {e}")

# ════════════════════════════════════════════════════════════════
# BASE DE DATOS DE ANALYTICS (SQLite)
# ════════════════════════════════════════════════════════════════
def init_analytics_db():
    """Crea las tablas de analytics si no existen."""
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id        TEXT,
            symbol          TEXT NOT NULL,
            side            TEXT,
            entry_price     REAL,
            exit_price      REAL,
            sl_original     REAL,
            sl_final        REAL,
            tp1             REAL,
            tp2             REAL,
            tp3             REAL,
            hit_tp1         INTEGER DEFAULT 0,
            hit_tp2         INTEGER DEFAULT 0,
            hit_tp3         INTEGER DEFAULT 0,
            hit_sl          INTEGER DEFAULT 0,
            hit_be          INTEGER DEFAULT 0,
            close_reason    TEXT,
            confidence      REAL,
            trend_line      TEXT,
            timeframe       TEXT,
            exchange        TEXT,
            leverage        INTEGER DEFAULT 10,
            amount_usdt     REAL DEFAULT 10.0,
            contracts       REAL,
            pnl_usdt        REAL,
            pnl_pct         REAL,
            duration_min    REAL,
            opened_at       TEXT,
            closed_at       TEXT,
            status          TEXT DEFAULT 'open'
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS trade_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id    TEXT,
            symbol      TEXT NOT NULL,
            action      TEXT NOT NULL,
            reason      TEXT,
            status      TEXT,
            price       REAL,
            sl          REAL,
            new_sl      REAL,
            close_pct   REAL,
            confidence  REAL,
            raw_payload TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        )''')
        conn.commit()
        conn.close()
        log("📊 Analytics DB inicializada correctamente")
    except Exception as e:
        log(f"❌ Error inicializando Analytics DB: {e}")

def _safe_float(val, default=None):
    """Convierte un valor a float de forma segura."""
    if val is None or val == 'N/A' or val == '':
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def db_log_event(data, symbol, action, status, reason=None):
    """Registra CADA webhook en trade_events con el payload completo."""
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        c.execute('''INSERT INTO trade_events 
            (trade_id, symbol, action, reason, status, price, sl, new_sl, close_pct, confidence, raw_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            str(data.get('trade_id', '')),
            symbol,
            action,
            reason or data.get('reason', ''),
            status,
            _safe_float(data.get('price')),
            _safe_float(data.get('sl', data.get('emergency_sl'))),
            _safe_float(data.get('new_sl')),
            _safe_float(data.get('close_pct')),
            _safe_float(data.get('conf')),
            json.dumps(data, default=str)
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log(f"⚠️ Error registrando evento en DB: {e}")

def db_open_trade(data, symbol, contracts):
    """Registra un trade abierto en la tabla trades."""
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        c.execute('''INSERT INTO trades 
            (trade_id, symbol, side, entry_price, sl_original, sl_final,
             tp1, tp2, tp3, confidence, trend_line, timeframe, exchange,
             leverage, amount_usdt, contracts, opened_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')''', (
            str(data.get('trade_id', '')),
            symbol,
            data.get('side', '').lower(),
            _safe_float(data.get('price')),
            _safe_float(data.get('sl', data.get('emergency_sl'))),
            _safe_float(data.get('sl', data.get('emergency_sl'))),
            _safe_float(data.get('tp1')),
            _safe_float(data.get('tp2')),
            _safe_float(data.get('tp3')),
            _safe_float(data.get('conf')),
            data.get('trend_line', ''),
            data.get('timeframe', ''),
            data.get('exchange', ''),
            LEVERAGE,
            MONTO_POR_TRADE,
            contracts,
            datetime.now().isoformat(),
        ))
        conn.commit()
        conn.close()
        log(f"📊 Trade {symbol} registrado en Analytics DB")
        
        # 🟢 Actualizar Excel de forma asíncrona para no bloquear
        threading.Thread(target=export_trades_to_excel).start()
        
    except Exception as e:
        log(f"⚠️ Error registrando trade en DB: {e}")

def db_update_partial(data, symbol):
    """Marca TP hits en el trade cuando se recibe un partial_close."""
    reason = data.get('reason', '').upper()
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        updates = []
        if 'TP1' in reason:
            updates.append("hit_tp1 = 1")
        if 'TP2' in reason:
            updates.append("hit_tp2 = 1")
        if 'TP3' in reason:
            updates.append("hit_tp3 = 1")
        if updates:
            sql = f"UPDATE trades SET {', '.join(updates)} WHERE symbol = ? AND status = 'open'"
            c.execute(sql, (symbol,))
            conn.commit()
            log(f"📊 Trade {symbol} actualizado: {reason}")
        conn.close()
        
        # 🟢 Actualizar Excel de forma asíncrona
        threading.Thread(target=export_trades_to_excel).start()
    except Exception as e:
        log(f"⚠️ Error actualizando TP en DB: {e}")

def db_update_sl(data, symbol):
    """Actualiza el SL final y detecta si es Break-Even."""
    new_sl = _safe_float(data.get('new_sl'))
    if new_sl is None:
        return
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        # Obtener entry_price para detectar BE
        c.execute("SELECT entry_price, side FROM trades WHERE symbol = ? AND status = 'open'", (symbol,))
        row = c.fetchone()
        hit_be = 0
        if row:
            entry_price, side = row
            if entry_price:
                # BE = SL movido al precio de entrada (con tolerancia de 0.5%)
                if abs(new_sl - entry_price) / entry_price < 0.005:
                    hit_be = 1
                    log(f"📊 BREAK-EVEN detectado para {symbol}: SL={new_sl} ≈ Entry={entry_price}")
        c.execute("UPDATE trades SET sl_final = ?, hit_be = MAX(hit_be, ?) WHERE symbol = ? AND status = 'open'",
                  (new_sl, hit_be, symbol))
        conn.commit()
        conn.close()
        
        # 🟢 Actualizar Excel de forma asíncrona
        threading.Thread(target=export_trades_to_excel).start()
    except Exception as e:
        log(f"⚠️ Error actualizando SL en DB: {e}")

def db_close_trade(data, symbol):
    """Cierra el trade: calcula PnL, duración, razón de cierre."""
    exit_price = _safe_float(data.get('price'))
    close_reason = data.get('reason', 'TREND_CHANGE').upper()
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        c = conn.cursor()
        c.execute("SELECT entry_price, side, opened_at, contracts FROM trades WHERE symbol = ? AND status = 'open'",
                  (symbol,))
        row = c.fetchone()
        pnl_usdt = None
        pnl_pct = None
        duration_min = None
        if row:
            entry_price, side, opened_at, contracts = row
            # Calcular PnL
            if entry_price and exit_price and contracts:
                if side == 'buy':
                    pnl_usdt = (exit_price - entry_price) * contracts
                else:
                    pnl_usdt = (entry_price - exit_price) * contracts
                pnl_pct = ((exit_price - entry_price) / entry_price * 100) if side == 'buy' else ((entry_price - exit_price) / entry_price * 100)
            # Calcular duración
            if opened_at:
                try:
                    opened_dt = datetime.fromisoformat(opened_at)
                    duration_min = (datetime.now() - opened_dt).total_seconds() / 60.0
                except: pass
        
        # Detectar si cerró por SL
        hit_sl = 1 if 'SL' in close_reason or 'STOP' in close_reason else 0
        
        c.execute('''UPDATE trades SET 
            exit_price = ?, close_reason = ?, pnl_usdt = ?, pnl_pct = ?,
            duration_min = ?, closed_at = ?, hit_sl = MAX(hit_sl, ?), status = 'closed'
            WHERE symbol = ? AND status = 'open' ''',
            (exit_price, close_reason, pnl_usdt, pnl_pct,
             duration_min, datetime.now().isoformat(), hit_sl, symbol))
        conn.commit()
        conn.close()
        if pnl_usdt is not None:
            emoji = "💰" if pnl_usdt >= 0 else "📉"
            log(f"{emoji} Analytics: {symbol} cerrado | PnL: ${pnl_usdt:.4f} ({pnl_pct:.2f}%) | Razón: {close_reason} | Duración: {duration_min:.1f}min")
            
        # 🔴 Actualizar Excel de forma asíncrona al cerrar un trade
        threading.Thread(target=export_trades_to_excel).start()
        
    except Exception as e:
        log(f"⚠️ Error cerrando trade en DB: {e}")

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
    sym_norm = normalizar_symbol(symbol)
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] OPEN {side.upper()} {sym_norm} @ {price}")
        return {"id": f"DRY-{int(time.time())}", "amount": 1.0}

    ex = get_exchange()
    if not ex: return None

    # 1. EVITAR DUPLICADOS LOCALES (usando clave normalizada)
    if sym_norm in trades_abiertos:
        log(f"⚠️ Ya existe una posición abierta para {sym_norm}. Ignorando duplicado.")
        return None

    # 2. VERIFICAR LÍMITE DE TRADES POR PAR
    count_par = sum(1 for k in trades_abiertos if normalizar_symbol(k) == sym_norm)
    if count_par >= MAX_TRADES_POR_PAR:
        log(f"⚠️ Máximo de trades por par alcanzado para {sym_norm} ({MAX_TRADES_POR_PAR})")
        return None

    # 3. EVITAR DUPLICADOS EN BITGET (Hard Check en vivo)
    try:
        positions = ex.fetch_positions([sym_ccxt])
        for p in positions:
            if abs(float(p.get('contracts', 0))) > 0:
                log(f"⚠️ DUPLICADO EN EXCHANGE: Ya existe posición para {sym_norm} en Bitget. Abortando.")
                if sym_norm not in trades_abiertos:
                    trades_abiertos[sym_norm] = {
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

            # Abrir orden principal
            order = ex.create_order(
                symbol=sym_ccxt,
                type='market',
                side=side,
                amount=qty,
                params={'marginMode': 'cross', 'oneWayMode': True}
            )
            log(f"✅ ORDEN EJECUTADA: {side.upper()} {qty} {symbol}")
            
            # Una vez la orden principal está ejecutada, intentamos el SL en un bloque SEPARADO
            # para no duplicar la orden de market si el SL falla.
            if sl and sl != 'N/A':
                try:
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
                except Exception as sl_error:
                    log(f"⚠️ Error colocando SL nativo para {symbol}: {sl_error}. La posición quedó abierta sin SL de Exchange.")
            
            return {"id": order['id'], "amount": qty}

        except Exception as e:
            if attempt < max_retries - 1:
                log(f"⚠️ Intento {attempt+1} fallido completando OPEN para {symbol}: {e}. Reintentando...")
                time.sleep(1.5)
            else:
                log(f"❌ Error final abriendo posición MARKET en {symbol} tras {max_retries} intentos: {e}")
                return None
def cerrar_parcial(symbol, pct):
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] PARTIAL CLOSE {pct}% {symbol}")
        return True

    ex = get_exchange()
    if not ex: return False

    try:
        # Obtener posición actual directamente de Bitget (STATELESS)
        positions = ex.fetch_positions([sym_ccxt])
        executed = False
        for p in positions:
            amt_total = abs(float(p.get('contracts', 0)))
            if amt_total > 0:
                side_presente = 'buy' if p.get('side', 'long') == 'long' else 'sell'
                
                amt_cerrar = amt_total * (float(pct) / 100.0)
                amt_cerrar = float(ex.amount_to_precision(sym_ccxt, amt_cerrar))
                
                if amt_cerrar > 0:
                    side_cerrar = 'sell' if side_presente == 'buy' else 'buy'
                    ex.create_order(sym_ccxt, 'market', side_cerrar, amt_cerrar, params={'reduceOnly': True})
                    log(f"🧩 CIERRE PARCIAL: {symbol} {pct}% ({amt_cerrar} contratos limitados)")
                    executed = True
                else:
                    log(f"⚠️ Cierre parcial descartado para {symbol}: cantidad calculada muy pequeña.")
        
        # Si no se encontró ninguna posición para cerrar, loggear
        if not executed:
            log(f"⚠️ No hay posición abierta en Bitget para cierre parcial de {symbol}")
            
        return executed
    except Exception as e:
        log(f"❌ Error cierre parcial para {symbol}: {e}")
        return False

def cerrar_posicion(symbol):
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
                side_presente = 'buy' if p.get('side', 'long') == 'long' else 'sell'
                side_cerrar = 'sell' if side_presente == 'buy' else 'buy'
                ex.create_order(sym_ccxt, 'market', side_cerrar, amt, params={'reduceOnly': True})
                log(f"🏁 POSICIÓN CERRADA: {symbol} ({amt} contratos)")
        return True
    except Exception as e:
        log(f"❌ Error cerrando posición: {e}")
        return False

def actualizar_sl(symbol, nuevo_sl):
    sym_ccxt = par_ccxt(symbol)
    if DRY_RUN:
        log(f"🧪 [DRY RUN] UPDATE SL {symbol} @ {nuevo_sl}")
        return True
    
    ex = get_exchange()
    if not ex: return False
    
    try:
        # Cancelar triggers de SL/TP anteriores
        ex.cancel_all_orders(sym_ccxt, params={'stop': True})
        
        # Buscar posición actual para colocar el SL sobre la cantidad actual real
        positions = ex.fetch_positions([sym_ccxt])
        executed = False
        for p in positions:
            amt = abs(float(p.get('contracts', 0)))
            if amt > 0:
                side_presente = 'buy' if p.get('side', 'long') == 'long' else 'sell'
                close_side = 'sell' if side_presente == 'buy' else 'buy'
                
                ex.create_order(
                    symbol=sym_ccxt,
                    type='market',
                    side=close_side,
                    amount=amt,
                    params={
                        'stop': True,
                        'triggerPrice': float(nuevo_sl),
                        'triggerType': 'mark_price',
                        'reduceOnly': True
                    }
                )
                log(f"🛡️ SL ACTUALIZADO: {symbol} -> {nuevo_sl}")
                executed = True
        return executed
    except Exception as e:
        log(f"❌ Error actualizando SL para {symbol}: {e}")
        return False

# ════════════════════════════════════════════════════════════════
# ENDPOINTS Y PROCESAMIENTO EN SEGUNDO PLANO
# ════════════════════════════════════════════════════════════════
def process_webhook_logic(data):
    action = data.get('action', '').lower()
    raw_symbol = data.get('symbol', 'N/A')
    symbol = normalizar_symbol(raw_symbol)  # ← Siempre normalizado
    side   = data.get('side', '').lower()
    price  = data.get('price', 0)
    sl     = data.get('sl', data.get('emergency_sl', 'N/A'))
    nuevo_sl = data.get('new_sl', 'N/A')
    tid    = data.get('trade_id', 'N/A')
    pct    = data.get('close_pct') or 30.0

    log(f"🚨 WEBHOOK RECIBIDO: {action.upper()} | {symbol} (raw: {raw_symbol}) | Trade #{tid}")
    record_event(symbol, action, "received", "Webhook reached bot", trade_id=tid)
    
    # 📊 Registrar CADA evento en Analytics DB (con payload completo)
    db_log_event(data, symbol, action, "received")

    if action == 'open':
        with trade_lock:
            if len(trades_abiertos) >= MAX_TOTAL_TRADES:
                log(f"⚠️ Máximo de trades alcanzado ({len(trades_abiertos)}/{MAX_TOTAL_TRADES}). Rechazando {symbol}.")
                record_event(symbol, action, "rejected", "Max trades reached", trade_id=tid)
                db_log_event(data, symbol, action, "rejected", "Max trades reached")
                return
            
            # Verificar si ya hay operación para este par (1 por cripto)
            if symbol in trades_abiertos:
                log(f"⚠️ Posición {symbol} ya registrada. Ignorando (regla: 1 por cripto).")
                record_event(symbol, action, "rejected", "Already open", trade_id=tid)
                db_log_event(data, symbol, action, "rejected", "Already open")
                return

            # Pausa de seguridad si el último evento para ESTE símbolo fue un close
            recent_closes = [e for e in webhook_eventos[-5:] if e.get('symbol') == symbol and e.get('action') in ['close', 'exit']]
            if recent_closes:
                log(f"⏳ Pausa de seguridad para {symbol} por cambio de tendencia...")
                time.sleep(1.5)

            res = abrir_posicion(raw_symbol, side, price, sl)
            if res:
                trades_abiertos[symbol] = {
                    "id": tid, "side": side, "entry": price, "sl": sl, 
                    "amount": res['amount'], "ts": datetime.now().isoformat()
                }
                record_event(symbol, action, "opened", "Success", trade_id=tid)
                db_log_event(data, symbol, action, "opened", "Success")
                db_open_trade(data, symbol, res['amount'])
                log(f"✅ Trades abiertos: {len(trades_abiertos)}/{MAX_TOTAL_TRADES}")
            else:
                log(f"⚠️ No se pudo abrir posición para {symbol}")
                record_event(symbol, action, "failed", "abrir_posicion returned None", trade_id=tid)
                db_log_event(data, symbol, action, "failed", "abrir_posicion returned None")
        
    elif action == 'partial_close':
        with trade_lock:
            if pct <= 0: pct = 30.0
            if cerrar_parcial(raw_symbol, pct):
                if symbol in trades_abiertos:
                    amt_f = float(trades_abiertos[symbol]['amount'])
                    trades_abiertos[symbol]['amount'] = amt_f - (amt_f * (float(pct)/100.0))
                    save_state()
                record_event(symbol, action, "partial", f"Closed {pct}%", trade_id=tid)
                db_log_event(data, symbol, action, "partial", data.get('reason', f'Closed {pct}%'))
                db_update_partial(data, symbol)
            else:
                log(f"⚠️ Cierre parcial ignorado para {symbol}: sin posición o tamaño muy pequeño")

    elif action in ['close', 'exit']:
        with trade_lock:
            if cerrar_posicion(raw_symbol):
                if symbol in trades_abiertos:
                    trades_abiertos.pop(symbol)
                    save_state()
                record_event(symbol, action, "closed", "Success", trade_id=tid)
                db_log_event(data, symbol, action, "closed", data.get('reason', 'CLOSED'))
                db_close_trade(data, symbol)
                log(f"✅ Trades abiertos: {len(trades_abiertos)}/{MAX_TOTAL_TRADES}")
            else:
                # Limpiar de memoria incluso si falló en exchange
                if symbol in trades_abiertos:
                    trades_abiertos.pop(symbol)
                    save_state()
                db_log_event(data, symbol, action, "failed", "Close failed on exchange")
                db_close_trade(data, symbol)
                log(f"⚠️ Close fallido en exchange para {symbol}, limpiado de memoria")

    elif action == 'update_sl':
        if nuevo_sl and nuevo_sl != 'N/A':
            if actualizar_sl(raw_symbol, nuevo_sl):
                if symbol in trades_abiertos:
                    trades_abiertos[symbol]['sl'] = nuevo_sl
                    save_state()
                record_event(symbol, action, "sl_updated", f"New SL: {nuevo_sl}", trade_id=tid)
                db_log_event(data, symbol, action, "sl_updated", f"New SL: {nuevo_sl}")
                db_update_sl(data, symbol)
    
    elif action == 'reentry':
        # 🔒 DESHABILITADO: Regla de 1 operación por cripto a $10/10x
        log(f"⚠️ Reentry DESHABILITADO para {symbol}. Regla: 1 operación por cripto.")
        record_event(symbol, action, "rejected", "Reentry disabled by rules", trade_id=tid)
        db_log_event(data, symbol, action, "rejected", "Reentry disabled by rules")
    
    else:
        log(f"⚠️ Acción desconocida: '{action}' para {symbol}")
        db_log_event(data, symbol, action, "unknown", f"Unknown action: {action}")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True, silent=True)
    log(f"📥 PETICIÓN WEBHOOK ENTRANTE: {data if data else 'Sin Data'}")
    if not data: return jsonify({"status": "error", "reason": "No JSON"}), 400
    
    # 1. Start execution in background (Threading) to prevent TradingView 3.0s Timeout
    threading.Thread(target=process_webhook_logic, args=(data,)).start()
    
    # 2. Inmediately return 200 OK
    return jsonify({"status": "received", "message": "processing in background"}), 200

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

@app.route('/analytics')
def analytics():
    """Endpoint para consultar el historial de trades desde la DB."""
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Parámetros de filtro opcionales
        status_filter = request.args.get('status', 'all')  # open, closed, all
        limit = int(request.args.get('limit', 50))
        
        if status_filter == 'all':
            c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        else:
            c.execute("SELECT * FROM trades WHERE status = ? ORDER BY id DESC LIMIT ?", (status_filter, limit))
        
        trades = [dict(row) for row in c.fetchall()]
        
        # Estadísticas rápidas
        c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN pnl_usdt <= 0 THEN 1 ELSE 0 END) as losses, SUM(pnl_usdt) as total_pnl, AVG(duration_min) as avg_duration FROM trades WHERE status = 'closed'")
        stats_row = c.fetchone()
        stats = dict(stats_row) if stats_row else {}
        
        # TP hit rates
        c.execute("SELECT SUM(hit_tp1) as tp1_hits, SUM(hit_tp2) as tp2_hits, SUM(hit_tp3) as tp3_hits, SUM(hit_sl) as sl_hits, SUM(hit_be) as be_hits, COUNT(*) as total FROM trades WHERE status = 'closed'")
        tp_row = c.fetchone()
        tp_stats = dict(tp_row) if tp_row else {}
        
        conn.close()
        return jsonify({
            "trades": trades,
            "stats": stats,
            "tp_stats": tp_stats
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/events')
def analytics_events():
    """Endpoint para consultar eventos granulares."""
    try:
        conn = sqlite3.connect(str(ANALYTICS_DB))
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        symbol = request.args.get('symbol', None)
        limit = int(request.args.get('limit', 100))
        
        if symbol:
            c.execute("SELECT * FROM trade_events WHERE symbol = ? ORDER BY id DESC LIMIT ?", (symbol, limit))
        else:
            c.execute("SELECT * FROM trade_events ORDER BY id DESC LIMIT ?", (limit,))
        
        events = [dict(row) for row in c.fetchall()]
        conn.close()
        return jsonify({"events": events})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def background_excel_update():
    """Hilo de respaldo para actualizar el Excel cada 10 minutos."""
    while True:
        try:
            export_trades_to_excel()
        except: pass
        time.sleep(600)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5002))
    log(f"🚀 Servidor v6.2 iniciando en puerto {port}")
    log(f"📊 Config: ${MONTO_POR_TRADE} x {LEVERAGE}x | Max: {MAX_TOTAL_TRADES} trades | {MAX_TRADES_POR_PAR} por par")
    # Inicializar Analytics DB
    init_analytics_db()
    # Sincronizar estado real con Bitget antes de aceptar webhooks
    sync_positions_from_exchange()
    
    # Iniciar actualizador periódico de Excel en segundo plano
    threading.Thread(target=background_excel_update, daemon=True).start()
    
    log(f"🚀 Servidor v6.2 LISTO — {len(trades_abiertos)} posiciones reales sincronizadas")
    app.run(host="0.0.0.0", port=port)
