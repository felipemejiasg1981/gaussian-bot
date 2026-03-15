#!/usr/bin/env python3
import argparse
import importlib
import json
import math
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


BASE_URL = "https://api.bitget.com"
CONTRACTS_URL = f"{BASE_URL}/api/v2/mix/market/contracts"
CANDLES_URL = f"{BASE_URL}/api/v2/mix/market/candles"
HEADERS = {"User-Agent": "codex-bitget-gaussian-scanner/1.0"}
SSL_CONTEXT = ssl._create_unverified_context()
REQUEST_TIMEOUT = 20
RETRIES = 3
PRODUCT_TYPE = "USDT-FUTURES"


@dataclass
class ScannerConfig:
    strategy: str
    profile: str
    granularity: str
    lookback_days: int
    min_trades: int
    max_workers: int
    top: int
    max_symbols: int | None
    len_cfg: int
    mode_cfg: str
    dist_cfg: float
    use_adaptive_sigma: bool
    sigma_base: float
    sigma_min: float
    sigma_max: float
    use_adx_filter: bool
    adx_len: int
    adx_threshold: float
    use_dmi_confirm: bool
    use_smc_filter: bool
    structure_lookback: int
    swing_strength: int
    smc_break_atr_mult: float
    smc_fresh_bars: int
    use_volume_absorption: bool
    absorption_threshold: float
    use_multi_osc: bool
    use_rsi: bool
    use_stoch: bool
    use_cci: bool
    rsi_len: int
    stoch_len: int
    cci_len: int
    rsi_oversold: float
    rsi_overbought: float
    use_kill_zones: bool
    kz_london_open: bool
    kz_ny_open: bool
    kz_london_close: bool
    kz_asia_session: bool
    use_chop: bool
    chop_len: int
    chop_threshold: float
    use_squeeze: bool
    sqz_bb_len: int
    sqz_bb_mult: float
    sqz_kc_len: int
    sqz_kc_mult: float
    sqz_lookback: int
    use_wae: bool
    wae_sens: int
    wae_fast: int
    wae_slow: int
    wae_bb_len: int
    wae_bb_mult: float
    wae_dead_zone: float
    use_fisher: bool
    fisher_len: int
    fisher_extreme: float
    use_frost: bool
    min_frost_conf: int
    frost_mode: str
    use_htf_alignment: bool
    require_dual_htf: bool
    min_context_score: int
    use_trend_slope: bool
    min_trend_slope_atr: float
    use_band_width: bool
    min_band_width_atr: float
    sl_mode: str
    target_mode: str
    use_liquidity_filter: bool
    require_sweep_reject: bool
    sweep_lookback: int
    pivot_len: int
    piv_hold_bars: int
    prox_atr: float
    break_strength_min: float
    snap_pct: float
    use_divergence: bool
    div_lookback: int
    div_pivot_strength: int
    min_score_ratio: float
    use_reentry: bool
    reentry_bars: int
    atr_len: int
    max_sl_pct: float
    sl_buf: float
    tp1_r: float
    tp2_r: float
    tp3_r: float
    tp4_r: float
    pct_tp1: float
    pct_tp2: float
    pct_tp3: float
    pct_runner: float
    fee_pct: float


def _base_defaults() -> dict:
    return {
        "profile": "Base TV",
        "granularity": "4H",
        "lookback_days": 365,
        "min_trades": 5,
        "max_workers": 8,
        "top": 20,
        "max_symbols": None,
        "len_cfg": 35,
        "mode_cfg": "AVG",
        "dist_cfg": 2.0,
        "use_adaptive_sigma": True,
        "sigma_base": 3.0,
        "sigma_min": 5.0,
        "sigma_max": 18.0,
        "use_adx_filter": False,
        "adx_len": 14,
        "adx_threshold": 25.0,
        "use_dmi_confirm": False,
        "use_smc_filter": False,
        "structure_lookback": 50,
        "swing_strength": 5,
        "smc_break_atr_mult": 0.10,
        "smc_fresh_bars": 12,
        "use_volume_absorption": True,
        "absorption_threshold": 1.5,
        "use_multi_osc": True,
        "use_rsi": False,
        "use_stoch": False,
        "use_cci": False,
        "rsi_len": 14,
        "stoch_len": 14,
        "cci_len": 20,
        "rsi_oversold": 35.0,
        "rsi_overbought": 65.0,
        "use_kill_zones": True,
        "kz_london_open": True,
        "kz_ny_open": True,
        "kz_london_close": False,
        "kz_asia_session": False,
        "use_chop": True,
        "chop_len": 20,
        "chop_threshold": 50.0,
        "use_squeeze": False,
        "sqz_bb_len": 20,
        "sqz_bb_mult": 2.0,
        "sqz_kc_len": 20,
        "sqz_kc_mult": 1.5,
        "sqz_lookback": 8,
        "use_wae": True,
        "wae_sens": 150,
        "wae_fast": 20,
        "wae_slow": 40,
        "wae_bb_len": 20,
        "wae_bb_mult": 2.0,
        "wae_dead_zone": 3.7,
        "use_fisher": False,
        "fisher_len": 10,
        "fisher_extreme": 1.5,
        "use_frost": True,
        "min_frost_conf": 2,
        "frost_mode": "Intraday",
        "use_htf_alignment": True,
        "require_dual_htf": True,
        "min_context_score": 2,
        "use_trend_slope": True,
        "min_trend_slope_atr": 0.12,
        "use_band_width": True,
        "min_band_width_atr": 1.10,
        "sl_mode": "Hibrido",
        "target_mode": "Hibrido",
        "use_liquidity_filter": True,
        "require_sweep_reject": True,
        "sweep_lookback": 30,
        "pivot_len": 3,
        "piv_hold_bars": 200,
        "prox_atr": 1.0,
        "break_strength_min": 0.55,
        "snap_pct": 18.0,
        "use_divergence": True,
        "div_lookback": 30,
        "div_pivot_strength": 5,
        "min_score_ratio": 0.35,
        "use_reentry": True,
        "reentry_bars": 6,
        "atr_len": 14,
        "max_sl_pct": 0.06,
        "sl_buf": 0.002,
        "tp1_r": 1.0,
        "tp2_r": 2.0,
        "tp3_r": 3.0,
        "tp4_r": 4.236,
        "pct_tp1": 0.30,
        "pct_tp2": 0.30,
        "pct_tp3": 0.20,
        "pct_runner": 0.20,
        "fee_pct": 0.0015,
    }


def _profile_defaults(profile_key: str) -> dict:
    defaults = _base_defaults()
    if profile_key == "manual":
        defaults["profile"] = "Manual"
    elif profile_key == "memecoins":
        defaults.update(
            {
                "profile": "Memecoins",
                "len_cfg": 28,
                "dist_cfg": 2.2,
                "sigma_base": 2.5,
                "sigma_min": 4.5,
                "sigma_max": 16.0,
                "use_kill_zones": False,
                "require_dual_htf": False,
                "min_context_score": 1,
                "min_trend_slope_atr": 0.08,
                "min_band_width_atr": 0.90,
                "prox_atr": 1.4,
                "break_strength_min": 0.45,
                "snap_pct": 24.0,
                "chop_threshold": 52.0,
                "wae_sens": 130,
                "wae_fast": 16,
                "wae_slow": 32,
                "wae_dead_zone": 3.2,
                "min_frost_conf": 1,
                "frost_mode": "Scalping",
                "min_score_ratio": 0.25,
                "pct_tp1": 0.20,
                "pct_tp2": 0.25,
                "pct_tp3": 0.25,
                "pct_runner": 0.30,
            }
        )
    elif profile_key == "majors":
        defaults.update(
            {
                "profile": "Majors",
                "len_cfg": 42,
                "dist_cfg": 1.8,
                "sigma_base": 4.0,
                "sigma_min": 6.0,
                "sigma_max": 20.0,
                "require_dual_htf": True,
                "min_context_score": 2,
                "min_trend_slope_atr": 0.18,
                "min_band_width_atr": 1.25,
                "prox_atr": 0.8,
                "break_strength_min": 0.65,
                "snap_pct": 14.0,
                "chop_threshold": 48.0,
                "wae_sens": 170,
                "wae_fast": 24,
                "wae_slow": 48,
                "wae_dead_zone": 4.0,
                "min_frost_conf": 3,
                "frost_mode": "Swing",
                "min_score_ratio": 0.45,
                "pct_tp1": 0.30,
                "pct_tp2": 0.30,
                "pct_tp3": 0.20,
                "pct_runner": 0.20,
            }
        )
    return defaults


def _normalize_override_key(key: str) -> str:
    mapping = {
        "len": "len_cfg",
        "mode": "mode_cfg",
        "distance": "dist_cfg",
        "useAdaptiveSigma": "use_adaptive_sigma",
        "sigmaBase": "sigma_base",
        "sigmaMin": "sigma_min",
        "sigmaMax": "sigma_max",
        "useADXFilter": "use_adx_filter",
        "adxLen": "adx_len",
        "adxThreshold": "adx_threshold",
        "useDMIConfirm": "use_dmi_confirm",
        "useSMCFilter": "use_smc_filter",
        "structureLookback": "structure_lookback",
        "swingStrength": "swing_strength",
        "smcBreakAtrMult": "smc_break_atr_mult",
        "smcFreshBars": "smc_fresh_bars",
        "useVolumeAbsorption": "use_volume_absorption",
        "absorptionThreshold": "absorption_threshold",
        "useMultiOsc": "use_multi_osc",
        "useRSI": "use_rsi",
        "useStoch": "use_stoch",
        "useCCI": "use_cci",
        "rsiLen": "rsi_len",
        "stochLen": "stoch_len",
        "cciLen": "cci_len",
        "rsiOversold": "rsi_oversold",
        "rsiOverbought": "rsi_overbought",
        "useKillZones": "use_kill_zones",
        "kzLondonOpen": "kz_london_open",
        "kzNYOpen": "kz_ny_open",
        "kzLondonClose": "kz_london_close",
        "kzAsiaSession": "kz_asia_session",
        "useHTFAlignment": "use_htf_alignment",
        "requireDualHTF": "require_dual_htf",
        "useTrendSlopeFilter": "use_trend_slope",
        "minTrendSlopeATR": "min_trend_slope_atr",
        "useBandWidthFilter": "use_band_width",
        "minBandWidthATR": "min_band_width_atr",
        "slMode": "sl_mode",
        "targetMode": "target_mode",
        "useLiquidityFilter": "use_liquidity_filter",
        "requireSweepReject": "require_sweep_reject",
        "sweepLookback": "sweep_lookback",
        "pivotLen": "pivot_len",
        "pivHoldBars": "piv_hold_bars",
        "proxATR": "prox_atr",
        "breakStrengthMin": "break_strength_min",
        "snapPct": "snap_pct",
        "useDivergence": "use_divergence",
        "divLookback": "div_lookback",
        "divPivotStrength": "div_pivot_strength",
        "useChopFilter": "use_chop",
        "chopLen": "chop_len",
        "chopThreshold": "chop_threshold",
        "useSqueeze": "use_squeeze",
        "sqzBBLen": "sqz_bb_len",
        "sqzBBMult": "sqz_bb_mult",
        "sqzKCLen": "sqz_kc_len",
        "sqzKCMult": "sqz_kc_mult",
        "sqzLookback": "sqz_lookback",
        "useWAE": "use_wae",
        "waeSens": "wae_sens",
        "waeFastLen": "wae_fast",
        "waeSlowLen": "wae_slow",
        "waeBBLen": "wae_bb_len",
        "waeBBMult": "wae_bb_mult",
        "waeDeadZone": "wae_dead_zone",
        "useFisher": "use_fisher",
        "fisherLen": "fisher_len",
        "fisherExtreme": "fisher_extreme",
        "useFrost": "use_frost",
        "minFrostConf": "min_frost_conf",
        "frostMode": "frost_mode",
        "minScoreRatio": "min_score_ratio",
        "useReEntry": "use_reentry",
        "reEntryBars": "reentry_bars",
        "atrLen": "atr_len",
        "maxSLPct": "max_sl_pct",
        "slBuf": "sl_buf",
        "tp1R": "tp1_r",
        "tp2R": "tp2_r",
        "tp3R": "tp3_r",
        "tp4R": "tp4_r",
        "pctTp1": "pct_tp1",
        "pctTp2": "pct_tp2",
        "pctTp3": "pct_tp3",
        "pctRunner": "pct_runner",
        "feePct": "fee_pct",
    }
    return mapping.get(key, key)

def build_config(args: argparse.Namespace, overrides: dict) -> ScannerConfig:
    data = _profile_defaults(args.profile.lower())

    # Apply command line basic args
    data["granularity"] = args.granularity
    data["lookback_days"] = args.lookback_days
    data["min_trades"] = args.min_trades
    data["max_workers"] = args.max_workers
    data["top"] = args.top
    data["max_symbols"] = args.max_symbols
    data["strategy"] = args.strategy

    # Apply JSON/File overrides
    for k, v in overrides.items():
        norm_key = _normalize_override_key(k)
        data[norm_key] = v

    # Type casting to match ScannerConfig fields
    typed_data: dict[str, Any] = {}
    config_fields = {f.name: f.type for f in fields(ScannerConfig)}

    for key, val in data.items():
        if key not in config_fields:
            continue
        target_type = config_fields[key]

        try:
            if target_type == bool:
                if isinstance(val, str):
                    typed_data[key] = val.lower() in ("true", "1", "yes")
                else:
                    typed_data[key] = bool(val)
            elif target_type == int:
                typed_data[key] = int(val)
            elif target_type == float:
                typed_data[key] = float(val)
            elif key == "max_symbols":
                typed_data[key] = int(val) if val is not None else None
            else:
                typed_data[key] = val
        except (ValueError, TypeError):
            typed_data[key] = val

    return ScannerConfig(**typed_data)


def adaptive_htf_keys(granularity: str, tp_num: int) -> tuple[str, str]:
    if granularity in ("5m", "15m"):
        return (f"h1h", f"h1l") if tp_num <= 2 else (f"h4h", f"h4l")
    if granularity in ("30m", "45m", "1H"):
        return (f"h4h", f"h4l") if tp_num <= 2 else (f"pdh", f"pdl")
    return (f"pdh", f"pdl") if tp_num <= 2 else (f"pwh", f"pwl")


def get_json(url: str) -> dict:
    last_error = None
    for attempt in range(RETRIES):
        try:
            request = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT, context=SSL_CONTEXT) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"request failed for {url}: {last_error}")


def fetch_contracts(max_symbols: int | None) -> list[dict]:
    query = urllib.parse.urlencode({"productType": PRODUCT_TYPE})
    payload = get_json(f"{CONTRACTS_URL}?{query}")
    if payload.get("code") != "00000":
        raise RuntimeError(f"contracts endpoint error: {payload}")
    contracts = [
        item
        for item in payload.get("data", [])
        if item.get("symbolStatus") == "normal"
        and item.get("symbolType") == "perpetual"
        and item.get("quoteCoin") == "USDT"
        and item.get("isRwa") != "YES"
        and item.get("baseCoin") not in {"USDC", "USDT", "PAXG", "XAUT", "XAU"}
    ]
    contracts.sort(key=lambda row: row["symbol"])
    return contracts[:max_symbols] if max_symbols else contracts


def granularity_to_ms(granularity: str) -> int:
    mapping = {
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "45m": 45 * 60 * 1000,
        "1H": 60 * 60 * 1000,
        "4H": 4 * 60 * 60 * 1000,
        "1D": 24 * 60 * 60 * 1000,
    }
    if granularity not in mapping:
        raise ValueError(f"unsupported granularity: {granularity}")
    return mapping[granularity]


def bars_for_lookback(granularity: str, lookback_days: int) -> int:
    gran_ms = granularity_to_ms(granularity)
    day_ms = 24 * 60 * 60 * 1000
    return int(math.ceil((lookback_days * day_ms) / gran_ms))


def bitget_granularity(granularity: str) -> str:
    if granularity == "45m":
        return "15m"
    return granularity


def resample_candles(frame: pd.DataFrame, granularity: str) -> pd.DataFrame:
    if granularity != "45m":
        return frame
    resampled = frame.resample("45min", origin="epoch").agg(
        {
            "timestamp": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    resampled = resampled.dropna(subset=["open", "high", "low", "close"])
    return resampled


def fetch_candles(symbol: str, granularity: str, lookback_days: int) -> pd.DataFrame:
    target_bars = bars_for_lookback(granularity, lookback_days)
    api_granularity = bitget_granularity(granularity)
    api_target_bars = bars_for_lookback(api_granularity, lookback_days) + (20 if granularity == "45m" else 0)
    gran_ms = granularity_to_ms(api_granularity)
    end_time = int(time.time() * 1000)
    all_rows: list[list[str]] = []
    seen = set()

    while len(all_rows) < api_target_bars:
        remaining = api_target_bars - len(all_rows)
        limit = min(1000, max(200, remaining))
        query = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "granularity": api_granularity,
                "limit": limit,
                "endTime": end_time,
            }
        )
        payload = get_json(f"{CANDLES_URL}?{query}")
        if payload.get("code") != "00000":
            raise RuntimeError(f"candles endpoint error for {symbol}: {payload}")
        rows = payload.get("data", [])
        if not rows:
            break

        rows = sorted(rows, key=lambda row: int(row[0]))
        added = 0
        for row in rows:
            ts = int(row[0])
            if ts not in seen:
                seen.add(ts)
                all_rows.append(row)
                added += 1
        if added == 0 or len(rows) < limit:
            break
        end_time = int(rows[0][0]) - gran_ms

    all_rows = sorted(all_rows, key=lambda row: int(row[0]))[-api_target_bars:]
    frame = pd.DataFrame(
        {
            "timestamp": [int(row[0]) for row in all_rows],
            "open": [float(row[1]) for row in all_rows],
            "high": [float(row[2]) for row in all_rows],
            "low": [float(row[3]) for row in all_rows],
            "close": [float(row[4]) for row in all_rows],
            "volume": [float(row[5]) for row in all_rows],
        }
    )
    frame["dt"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame = frame.set_index("dt")
    frame = resample_candles(frame, granularity)
    frame = frame.tail(target_bars)
    return frame


# La lógica de prepare_indicators ha sido movida a las estrategias individuales.


def close_trade(trades: list[float], equity_curve: list[float], equity: float, peak: float, trade_return: float) -> tuple[float, float]:
    trades.append(trade_return)
    equity *= 1.0 + trade_return
    peak = max(peak, equity)
    equity_curve.append(equity)
    return equity, peak


def run_backtest(data: pd.DataFrame, config: ScannerConfig) -> dict:
    position: dict[str, Any] | None = None
    trades: list[float] = []
    equity = 1.0
    peak = 1.0
    equity_curve = [equity]

    def htf_level(row: pd.Series, is_bull: bool, tp_num: int) -> float:
        high_key, low_key = adaptive_htf_keys(config.granularity, tp_num)
        key = high_key if is_bull else low_key
        value = row.get(key, np.nan)
        return float(value) if pd.notna(value) else np.nan

    def snap_target(entry: float, r_target: float, is_bull: bool, htf_target: float, struct_target: float) -> float:
        if config.target_mode == "R Multiples":
            return r_target
        snap = r_target
        tolerance = abs(r_target - entry) * (config.snap_pct / 100.0)
        valid_htf = np.isfinite(htf_target) and ((htf_target > entry) if is_bull else (htf_target < entry)) and abs(htf_target - r_target) < tolerance
        valid_struct = np.isfinite(struct_target) and ((struct_target > entry) if is_bull else (struct_target < entry)) and abs(struct_target - r_target) < tolerance
        if config.target_mode == "Liquidez/HTF":
            if valid_htf:
                snap = htf_target
            elif valid_struct:
                snap = struct_target
        else:
            if valid_struct:
                snap = struct_target
            elif valid_htf:
                snap = htf_target
        return snap

    def open_trade(side: str, row: pd.Series) -> dict | None:
        entry_price = float(row["close"])
        trend_line = float(row["trend_line"]) if pd.notna(row["trend_line"]) else np.nan
        near_low = float(row["near_low"]) if pd.notna(row["near_low"]) else np.nan
        near_high = float(row["near_high"]) if pd.notna(row["near_high"]) else np.nan
        if not np.isfinite(entry_price) or not np.isfinite(trend_line):
            return None
        if side == "long":
            trend_sl = max(trend_line * (1.0 - config.sl_buf), entry_price * (1.0 - config.max_sl_pct))
            swing_sl = trend_sl
            if np.isfinite(near_low) and (entry_price - near_low) <= entry_price * config.max_sl_pct:
                swing_sl = near_low * (1.0 - config.sl_buf)
            if config.sl_mode == "Trend Line":
                stop = trend_sl
            elif config.sl_mode == "Estructura (Swing)":
                stop = swing_sl
            else:
                stop = max(trend_sl, swing_sl)
            risk = entry_price - stop
            if risk <= 0:
                return None
            struct_target = near_high if np.isfinite(near_high) and near_high > entry_price else np.nan
            return {
                "side": side,
                "entry": entry_price,
                "stop": stop,
                "be": entry_price * (1.0 + config.fee_pct),
                "targets": [
                    snap_target(entry_price, entry_price + risk * config.tp1_r, True, htf_level(row, True, 1), struct_target),
                    snap_target(entry_price, entry_price + risk * config.tp2_r, True, htf_level(row, True, 2), struct_target),
                    snap_target(entry_price, entry_price + risk * config.tp3_r, True, htf_level(row, True, 3), struct_target),
                    snap_target(entry_price, entry_price + risk * config.tp4_r, True, htf_level(row, True, 4), struct_target),
                ],
                "target_pcts": [config.pct_tp1, config.pct_tp2, config.pct_tp3, config.pct_runner],
                "remaining": 1.0,
                "realized": 0.0,
                "be_armed": False,
            }
        trend_sl = min(trend_line * (1.0 + config.sl_buf), entry_price * (1.0 + config.max_sl_pct))
        swing_sl = trend_sl
        if np.isfinite(near_high) and (near_high - entry_price) <= entry_price * config.max_sl_pct:
            swing_sl = near_high * (1.0 + config.sl_buf)
        if config.sl_mode == "Trend Line":
            stop = trend_sl
        elif config.sl_mode == "Estructura (Swing)":
            stop = swing_sl
        else:
            stop = min(trend_sl, swing_sl)
        risk = stop - entry_price
        if risk <= 0:
            return None
        struct_target = near_low if np.isfinite(near_low) and near_low < entry_price else np.nan
        return {
            "side": side,
            "entry": entry_price,
            "stop": stop,
            "be": entry_price * (1.0 - config.fee_pct),
            "targets": [
                snap_target(entry_price, entry_price - risk * config.tp1_r, False, htf_level(row, False, 1), struct_target),
                snap_target(entry_price, entry_price - risk * config.tp2_r, False, htf_level(row, False, 2), struct_target),
                snap_target(entry_price, entry_price - risk * config.tp3_r, False, htf_level(row, False, 3), struct_target),
                snap_target(entry_price, entry_price - risk * config.tp4_r, False, htf_level(row, False, 4), struct_target),
            ],
            "target_pcts": [config.pct_tp1, config.pct_tp2, config.pct_tp3, config.pct_runner],
            "remaining": 1.0,
            "realized": 0.0,
            "be_armed": False,
        }

    for idx, row in data.iterrows():
        close_price = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])
        trend_line = float(row["trend_line"]) if np.isfinite(row["trend_line"]) else np.nan

        if position:
            if position["side"] == "long" and np.isfinite(trend_line):
                position["stop"] = max(position["stop"], trend_line * (1.0 - config.sl_buf))
            if position["side"] == "short" and np.isfinite(trend_line):
                position["stop"] = min(position["stop"], trend_line * (1.0 + config.sl_buf))

            stop_hit = False
            if position["side"] == "long":
                if low <= position["stop"]:
                    position["realized"] += position["remaining"] * ((position["stop"] / position["entry"]) - 1.0)
                    position["remaining"] = 0.0
                    stop_hit = True
                else:
                    for target_idx, target in enumerate(position["targets"]):
                        target_pct = position["target_pcts"][target_idx]
                        if target_pct > 0 and high >= target:
                            take_pct = min(position["remaining"], target_pct)
                            if take_pct > 0:
                                position["realized"] += take_pct * ((target / position["entry"]) - 1.0)
                                position["remaining"] -= take_pct
                                position["target_pcts"][target_idx] = 0.0
                                if target_idx == 0 and not position["be_armed"]:
                                    position["stop"] = max(position["stop"], position["be"])
                                    position["be_armed"] = True
            else:
                if high >= position["stop"]:
                    position["realized"] += position["remaining"] * ((position["entry"] / position["stop"]) - 1.0)
                    position["remaining"] = 0.0
                    stop_hit = True
                else:
                    for target_idx, target in enumerate(position["targets"]):
                        target_pct = position["target_pcts"][target_idx]
                        if target_pct > 0 and low <= target:
                            take_pct = min(position["remaining"], target_pct)
                            if take_pct > 0:
                                position["realized"] += take_pct * ((position["entry"] / target) - 1.0)
                                position["remaining"] -= take_pct
                                position["target_pcts"][target_idx] = 0.0
                                if target_idx == 0 and not position["be_armed"]:
                                    position["stop"] = min(position["stop"], position["be"])
                                    position["be_armed"] = True

            reverse = (position["side"] == "long" and bool(row["short_signal"])) or (
                position["side"] == "short" and bool(row["long_signal"])
            )
            if position and position["remaining"] > 0 and reverse:
                if position["side"] == "long":
                    position["realized"] += position["remaining"] * ((close_price / position["entry"]) - 1.0)
                else:
                    position["realized"] += position["remaining"] * ((position["entry"] / close_price) - 1.0)
                position["remaining"] = 0.0

            if position and position["remaining"] <= 1e-9:
                equity, peak = close_trade(trades, equity_curve, equity, peak, position["realized"])
                position = None

        if not position:
            if bool(row["long_signal"]):
                position = open_trade("long", row)
            elif bool(row["short_signal"]):
                position = open_trade("short", row)

    if position and position["remaining"] > 0:
        last_close = float(data["close"].iat[-1])
        if position["side"] == "long":
            position["realized"] += position["remaining"] * ((last_close / position["entry"]) - 1.0)
        else:
            position["realized"] += position["remaining"] * ((position["entry"] / last_close) - 1.0)
        equity, peak = close_trade(trades, equity_curve, equity, peak, position["realized"])

    gross_profit = sum(x for x in trades if x > 0)
    gross_loss = abs(sum(x for x in trades if x < 0))
    wins = [x for x in trades if x > 0]
    losses = [x for x in trades if x < 0]
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    win_rate = (len(wins) / len(trades)) if trades else 0.0

    max_drawdown = 0.0
    max_peak = equity_curve[0]
    for value in equity_curve:
        max_peak = max(max_peak, value)
        max_drawdown = min(max_drawdown, (value / max_peak) - 1.0)

    net_return = equity - 1.0
    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate * 100.0, 4),
        "profit_factor": "inf" if math.isinf(profit_factor) else round(profit_factor, 4),
        "gross_profit_pct": round(gross_profit * 100.0, 4),
        "gross_loss_pct": round(-gross_loss * 100.0, 4),
        "avg_trade_pct": round((sum(trades) / len(trades)) * 100.0, 4) if trades else 0.0,
        "avg_win_pct": round((sum(wins) / len(wins)) * 100.0, 4) if wins else 0.0,
        "avg_loss_pct": round((sum(losses) / len(losses)) * 100.0, 4) if losses else 0.0,
        "expectancy_pct": round(((win_rate * (sum(wins) / len(wins) if wins else 0.0)) + ((1 - win_rate) * (sum(losses) / len(losses) if losses else 0.0))) * 100.0, 4) if trades else 0.0,
        "max_drawdown_pct": round(max_drawdown * 100.0, 4),
        "net_return_pct": round(net_return * 100.0, 4),
    }


def quality_score(win_rate_pct: float, profit_factor: float | str, max_drawdown_pct: float, trades: int) -> float:
    pf = 5.0 if profit_factor == "inf" else float(profit_factor)
    dd = max(5.0, abs(max_drawdown_pct))
    trade_boost = min(trades, 20) / 20.0
    return round((pf * (win_rate_pct / 100.0) * trade_boost * 100.0) / dd, 4)


def scan_symbol(contract: dict, config: ScannerConfig, strategy_mod) -> dict | None:
    frame = fetch_candles(contract["symbol"], config.granularity, config.lookback_days)
    if frame.empty or len(frame) < max(config.len_cfg + 50, 250):
        return None
    enriched = strategy_mod.prepare_indicators(frame, config).dropna(subset=["trend_line", "atr"])
    if enriched.empty:
        return None
    stats = run_backtest(enriched, config)
    if stats["trades"] < config.min_trades:
        return None
    last = enriched.iloc[-1]
    score = quality_score(stats["win_rate_pct"], stats["profit_factor"], stats["max_drawdown_pct"], stats["trades"])
    return {
        "symbol": contract["symbol"],
        "baseCoin": contract["baseCoin"],
        "granularity": config.granularity,
        "profile": config.profile,
        "lookback_days": config.lookback_days,
        "bars": int(len(enriched)),
        "trades": stats["trades"],
        "wins": stats["wins"],
        "losses": stats["losses"],
        "win_rate_pct": stats["win_rate_pct"],
        "profit_factor": stats["profit_factor"],
        "gross_profit_pct": stats["gross_profit_pct"],
        "gross_loss_pct": stats["gross_loss_pct"],
        "avg_trade_pct": stats["avg_trade_pct"],
        "avg_win_pct": stats["avg_win_pct"],
        "avg_loss_pct": stats["avg_loss_pct"],
        "expectancy_pct": stats["expectancy_pct"],
        "max_drawdown_pct": stats["max_drawdown_pct"],
        "net_return_pct": stats["net_return_pct"],
        "quality_score": score,
        "last_close": round(float(last["close"]), 8),
        "trend_state": "LONG" if int(last["trend_state"]) == 1 else "SHORT",
        "last_signal": "LONG" if bool(last["long_signal"]) else "SHORT" if bool(last["short_signal"]) else "NONE",
        "last_bar_utc": enriched.index[-1].isoformat(),
    }


def sort_key(value: float | str) -> float:
    return math.inf if value == "inf" else float(value)


def config_groups(config: ScannerConfig) -> dict:
    flat = asdict(config)
    return {
        "flat": flat,
        "scanner_controls": {
            "profile": config.profile,
            "granularity": config.granularity,
            "lookback_days": config.lookback_days,
            "min_trades": config.min_trades,
            "max_workers": config.max_workers,
            "top": config.top,
            "max_symbols": config.max_symbols,
        },
        "gaussian_kernel": {
            "len_cfg": config.len_cfg,
            "mode_cfg": config.mode_cfg,
            "dist_cfg": config.dist_cfg,
        },
        "extreme_engine": {
            "use_adaptive_sigma": config.use_adaptive_sigma,
            "sigma_base": config.sigma_base,
            "sigma_min": config.sigma_min,
            "sigma_max": config.sigma_max,
        },
        "adx_trend_strength": {
            "use_adx_filter": config.use_adx_filter,
            "adx_len": config.adx_len,
            "adx_threshold": config.adx_threshold,
            "use_dmi_confirm": config.use_dmi_confirm,
        },
        "market_structure": {
            "use_smc_filter": config.use_smc_filter,
            "structure_lookback": config.structure_lookback,
            "swing_strength": config.swing_strength,
            "smc_break_atr_mult": config.smc_break_atr_mult,
            "smc_fresh_bars": config.smc_fresh_bars,
        },
        "volume_delta": {
            "use_volume_absorption": config.use_volume_absorption,
            "absorption_threshold": config.absorption_threshold,
        },
        "oscillator_confluence": {
            "use_multi_osc": config.use_multi_osc,
            "use_rsi": config.use_rsi,
            "use_stoch": config.use_stoch,
            "use_cci": config.use_cci,
            "rsi_len": config.rsi_len,
            "stoch_len": config.stoch_len,
            "cci_len": config.cci_len,
            "rsi_oversold": config.rsi_oversold,
            "rsi_overbought": config.rsi_overbought,
        },
        "kill_zones": {
            "use_kill_zones": config.use_kill_zones,
            "kz_london_open": config.kz_london_open,
            "kz_ny_open": config.kz_ny_open,
            "kz_london_close": config.kz_london_close,
            "kz_asia_session": config.kz_asia_session,
        },
        "regime_filters": {
            "use_htf_alignment": config.use_htf_alignment,
            "require_dual_htf": config.require_dual_htf,
            "use_trend_slope": config.use_trend_slope,
            "min_trend_slope_atr": config.min_trend_slope_atr,
            "use_band_width": config.use_band_width,
            "min_band_width_atr": config.min_band_width_atr,
        },
        "liquidity_targets": {
            "sl_mode": config.sl_mode,
            "target_mode": config.target_mode,
            "use_liquidity_filter": config.use_liquidity_filter,
            "require_sweep_reject": config.require_sweep_reject,
            "sweep_lookback": config.sweep_lookback,
            "pivot_len": config.pivot_len,
            "piv_hold_bars": config.piv_hold_bars,
            "prox_atr": config.prox_atr,
            "break_strength_min": config.break_strength_min,
            "snap_pct": config.snap_pct,
        },
        "divergence_engine": {
            "use_divergence": config.use_divergence,
            "div_lookback": config.div_lookback,
            "div_pivot_strength": config.div_pivot_strength,
        },
        "choppiness_index": {
            "use_chop": config.use_chop,
            "chop_len": config.chop_len,
            "chop_threshold": config.chop_threshold,
        },
        "squeeze_momentum": {
            "use_squeeze": config.use_squeeze,
            "sqz_bb_len": config.sqz_bb_len,
            "sqz_bb_mult": config.sqz_bb_mult,
            "sqz_kc_len": config.sqz_kc_len,
            "sqz_kc_mult": config.sqz_kc_mult,
            "sqz_lookback": config.sqz_lookback,
        },
        "wae_explosion": {
            "use_wae": config.use_wae,
            "wae_sens": config.wae_sens,
            "wae_fast": config.wae_fast,
            "wae_slow": config.wae_slow,
            "wae_bb_len": config.wae_bb_len,
            "wae_bb_mult": config.wae_bb_mult,
            "wae_dead_zone": config.wae_dead_zone,
        },
        "fisher_transform": {
            "use_fisher": config.use_fisher,
            "fisher_len": config.fisher_len,
            "fisher_extreme": config.fisher_extreme,
        },
        "frost_engine": {
            "use_frost": config.use_frost,
            "min_frost_conf": config.min_frost_conf,
            "frost_mode": config.frost_mode,
        },
        "score_engine": {
            "min_score_ratio": config.min_score_ratio,
        },
        "reentry_system": {
            "use_reentry": config.use_reentry,
            "reentry_bars": config.reentry_bars,
        },
        "targets_stop_loss": {
            "atr_len": config.atr_len,
            "max_sl_pct": config.max_sl_pct,
            "sl_buf": config.sl_buf,
            "tp1_r": config.tp1_r,
            "tp2_r": config.tp2_r,
            "tp3_r": config.tp3_r,
            "tp4_r": config.tp4_r,
            "pct_tp1": config.pct_tp1,
            "pct_tp2": config.pct_tp2,
            "pct_tp3": config.pct_tp3,
            "pct_runner": config.pct_runner,
        },
        "break_even": {
            "fee_pct": config.fee_pct,
        },
    }


def write_outputs(rows: list[dict], config: ScannerConfig, errors: list[dict]) -> tuple[Path, Path, Path]:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    base = Path("/Users/felipe/Desktop/Scaneo")
    csv_path = base / f"bitget_gaussian_scan_{config.granularity}_{timestamp}.csv"
    summary_path = base / f"bitget_gaussian_scan_{config.granularity}_{timestamp}.json"
    errors_path = base / f"bitget_gaussian_scan_{config.granularity}_{timestamp}_errors.json"

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame.to_csv(csv_path, index=False)
    else:
        frame = pd.DataFrame(
            columns=[
                "symbol",
                "baseCoin",
                "granularity",
                "profile",
                "lookback_days",
                "bars",
                "trades",
                "wins",
                "losses",
                "win_rate_pct",
                "profit_factor",
                "gross_profit_pct",
                "gross_loss_pct",
                "avg_trade_pct",
                "avg_win_pct",
                "avg_loss_pct",
                "expectancy_pct",
                "max_drawdown_pct",
                "net_return_pct",
                "quality_score",
                "last_close",
                "trend_state",
                "last_signal",
                "last_bar_utc",
            ]
        )
        frame.to_csv(csv_path, index=False)

    def top_rows(field: str, reverse: bool = True) -> list[dict]:
        ordered = sorted(rows, key=lambda row: sort_key(row[field]), reverse=reverse)
        return ordered[: config.top]

    grouped_config = config_groups(config)
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": {
            "contracts": f"{CONTRACTS_URL}?productType={PRODUCT_TYPE}",
            "candles": CANDLES_URL,
        },
        "scanner": {
            "strategy": config.strategy,
            "profile": config.profile,
            "granularity": config.granularity,
            "lookback_days": config.lookback_days,
            "min_trades": config.min_trades,
            "supported_timeframes": ["5m", "15m", "30m", "45m", "1H", "4H"],
            "universe": "Bitget USDT perpetual futures, crypto only (RWA/stable/metals filtered out)",
            "strategy_scope": "Python approximation of the Gaussian Trend IA Pro v6.2 core: dynamic Gaussian trend, score engine, SMC, oscillators, WAE, Frost, HTF, CHOP, liquidity context, and trailing stop with partial targets",
            "metrics_origin": "Backtest por símbolo del scanner, pensado para emular las métricas de estrategia que TradingView reporta por activo: trades, winrate, PF, drawdown, retorno, expectancy, wins y losses.",
            "pine_effective_config": grouped_config,
            "pine_not_ported_exactly": [
                "Mode MEDIAN/TRIMMED exacto del gaussian ensemble",
                "SMC BOS/CHoCH y pivots exactos barra a barra como Pine",
                "target snap por estructura/HTF idéntico a TradingView",
                "kill zones y ejecucion intrabar con bar magnifier",
                "re-entry 50/50 completo",
                "ejecución intrabar idéntica a bar magnifier",
            ],
        },
        "counts": {
            "symbols_ranked": len(rows),
            "errors": len(errors),
        },
        "top": {
            "quality_score": top_rows("quality_score"),
            "win_rate": top_rows("win_rate_pct"),
            "profit_factor": top_rows("profit_factor"),
            "max_drawdown_best": top_rows("max_drawdown_pct"),
            "net_return": top_rows("net_return_pct"),
        },
    }

    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    errors_path.write_text(json.dumps(errors, indent=2, ensure_ascii=False), encoding="utf-8")
    return csv_path, summary_path, errors_path


def load_strategy(strategy_name: str):
    try:
        # Assuming strategies are in a 'strategies' directory
        module = importlib.import_module(f"strategies.{strategy_name}")
        return module
    except ImportError as e:
        raise ValueError(f"Could not load strategy '{strategy_name}': {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bitget Gaussian Scanner")
    parser.add_argument("--profile", type=str, default="base", help="Profile (base, memecoins, majors)")
    parser.add_argument("--granularity", type=str, default="4H", help="Granularity")
    parser.add_argument("--lookback-days", type=int, default=365, help="Lookback days")
    parser.add_argument("--min-trades", type=int, default=5, help="Min trades")
    parser.add_argument("--max-workers", type=int, default=8, help="Max workers")
    parser.add_argument("--top", type=int, default=20, help="Top N results")
    parser.add_argument("--max-symbols", type=int, default=None, help="Max symbols to scan")
    parser.add_argument("--config-json", default=None)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--strategy", type=str, default="gaussian_v6_2", help="Strategy to use (file in strategies/)")
    args = parser.parse_args()
    overrides: dict = {}
    if args.config_file:
        overrides.update(json.loads(Path(args.config_file).read_text(encoding="utf-8")))
    if args.config_json:
        overrides.update(json.loads(args.config_json))
    config = build_config(args, overrides)
    strategy_mod = load_strategy(args.strategy)

    print(f"Iniciando escaneo con estrategia: {args.strategy}")
    contracts = fetch_contracts(config.max_symbols)
    rows: list[dict] = []
    errors: list[dict] = []

    started = time.time()
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {executor.submit(scan_symbol, contract, config, strategy_mod): contract["symbol"] for contract in contracts}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result:
                    rows.append(result)
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": symbol, "error": str(exc)})

    rows.sort(key=lambda row: row["quality_score"], reverse=True)
    csv_path, summary_path, errors_path = write_outputs(rows, config, errors)

    print(
        json.dumps(
            {
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(float(time.time() - started), 2),
                "contracts_considered": len(contracts),
                "symbols_ranked": len(rows),
                "errors": len(errors),
                "top_quality": rows[: config.top],
                "csv": str(csv_path),
                "summary": str(summary_path),
                "errors_file": str(errors_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
