import numpy as np
import pandas as pd
import math

def rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1.0 / length, adjust=False).mean()

def gaussian_avg(close: np.ndarray, sigma_arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(close.size, np.nan, dtype=float)
    x = np.arange(length, dtype=float)
    center = length / 2.0
    for idx in range(length - 1, close.size):
        sigma = max(0.5, float(sigma_arr[idx]))
        weights = np.exp(-0.5 * np.square((x - center) / sigma)) / math.sqrt(sigma * 2.0 * math.pi)
        window = close[idx - length + 1 : idx + 1]
        total = weights.sum()
        out[idx] = float(np.dot(window, weights) / total) if total > 0 else close[idx]
    return out

def frost_filter(close: np.ndarray, smooth_range: np.ndarray) -> np.ndarray:
    out = np.full(close.size, np.nan, dtype=float)
    if close.size == 0:
        return out
    out[0] = close[0]
    for idx in range(1, close.size):
        prev = out[idx - 1]
        x = close[idx]
        r = smooth_range[idx]
        if x > prev:
            out[idx] = prev if x - r < prev else x - r
        else:
            out[idx] = prev if x + r > prev else x + r
    return out

def counter_direction(values: np.ndarray, positive: bool) -> np.ndarray:
    out = np.zeros(values.size, dtype=float)
    for idx in range(1, values.size):
        if positive:
            if values[idx] > values[idx - 1]:
                out[idx] = out[idx - 1] + 1
            elif values[idx] < values[idx - 1]:
                out[idx] = 0
            else:
                out[idx] = out[idx - 1]
        else:
            if values[idx] < values[idx - 1]:
                out[idx] = out[idx - 1] + 1
            elif values[idx] > values[idx - 1]:
                out[idx] = 0
            else:
                out[idx] = out[idx - 1]
    return out

def htf_alignment(index: pd.DatetimeIndex, close: pd.Series, base_granularity: str) -> tuple[pd.Series, pd.Series]:
    # Hardcoded mapping from original script
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
        return mapping.get(granularity, 0)

    base_ms = granularity_to_ms(base_granularity)
    if base_ms <= granularity_to_ms("15m"):
        primary_tf, secondary_tf = "1H", "4H"
    elif base_ms <= granularity_to_ms("1H"):
        primary_tf, secondary_tf = "4H", "1D"
    else:
        primary_tf, secondary_tf = "1D", "1W"

    freq_map = {"1H": "1h", "4H": "4h", "1D": "1D", "1W": "1W"}

    def compute(tf: str) -> pd.Series:
        resampled = close.resample(freq_map[tf]).last().dropna()
        bull = (resampled > resampled.ewm(span=200, adjust=False).mean()).astype(bool)
        aligned = bull.reindex(index, method="ffill")
        return pd.Series(np.where(aligned.isna(), False, aligned.astype(bool)), index=index, dtype=bool)

    return compute(primary_tf), compute(secondary_tf)

def previous_levels(high: pd.Series, low: pd.Series, rule: str, index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    tf_high = high.resample(rule).max().shift(1)
    tf_low = low.resample(rule).min().shift(1)
    return tf_high.reindex(index, method="ffill"), tf_low.reindex(index, method="ffill")

def compute_rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100.0 - (100.0 / (1.0 + rs))).fillna(50.0)

def compute_stoch(close: pd.Series, high: pd.Series, low: pd.Series, length: int) -> pd.Series:
    lowest = low.rolling(length).min()
    highest = high.rolling(length).max()
    return np.divide((close - lowest) * 100.0, (highest - lowest), out=np.full(close.shape[0], 50.0, dtype=float), where=(highest - lowest).to_numpy() > 0)

def compute_cci(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    typical = (high + low + close) / 3.0
    sma = typical.rolling(length).mean()
    mad = typical.rolling(length).apply(lambda values: np.mean(np.abs(values - np.mean(values))), raw=True)
    return ((typical - sma) / (0.015 * mad.replace(0, np.nan))).fillna(0.0)

def confirmed_pivots(high_values: np.ndarray, low_values: np.ndarray, left: int, right: int) -> tuple[np.ndarray, np.ndarray]:
    size = high_values.size
    piv_high = np.full(size, np.nan, dtype=float)
    piv_low = np.full(size, np.nan, dtype=float)
    if size < (left + right + 1):
        return piv_high, piv_low
    for idx in range(left + right, size):
        pivot_idx = idx - right
        high_window = high_values[pivot_idx - left : idx + 1]
        low_window = low_values[pivot_idx - left : idx + 1]
        pivot_high = high_values[pivot_idx]
        pivot_low = low_values[pivot_idx]
        if np.isfinite(pivot_high) and pivot_high >= np.nanmax(high_window):
            piv_high[idx] = pivot_high
        if np.isfinite(pivot_low) and pivot_low <= np.nanmin(low_window):
            piv_low[idx] = pivot_low
    return piv_high, piv_low

def prepare_indicators(frame: pd.DataFrame, config):
    data = frame.copy()
    prev_close = data["close"].shift(1)
    true_range = pd.concat([data["high"] - data["low"], (data["high"] - prev_close).abs(), (data["low"] - prev_close).abs()], axis=1).max(axis=1)
    atr = rma(true_range, config.atr_len)
    atr_fast = rma(true_range, 14)
    atr_100 = atr_fast.rolling(100).mean()
    atr_norm = (atr_fast / atr_100.replace(0, np.nan)).clip(lower=0.5).fillna(1.0)
    if config.use_adaptive_sigma:
        dynamic_sigma = (config.sigma_base / atr_norm).clip(lower=config.sigma_min, upper=config.sigma_max)
    else:
        dynamic_sigma = pd.Series(config.sigma_base, index=data.index, dtype=float)
    gaussian = gaussian_avg(data["close"].to_numpy(), dynamic_sigma.to_numpy(), config.len_cfg)
    vol = (data["high"] - data["low"]).rolling(100).mean()
    band_low = gaussian - vol.to_numpy() * config.dist_cfg
    band_high = gaussian + vol.to_numpy() * config.dist_cfg
    trend_state = np.full(data.shape[0], -1, dtype=int)
    for idx in range(1, data.shape[0]):
        prev_state = trend_state[idx - 1]
        crossed_up = data["close"].iat[idx] > band_high[idx] and data["close"].iat[idx - 1] <= band_high[idx - 1]
        crossed_down = data["close"].iat[idx] < band_low[idx] and data["close"].iat[idx - 1] >= band_low[idx - 1]
        trend_state[idx] = 1 if crossed_up else -1 if crossed_down else prev_state
    trend_line = np.where(trend_state == 1, band_low, band_high)
    trend_changed = np.diff(np.r_[-1, trend_state])
    chop_atr_sum = true_range.rolling(config.chop_len).sum()
    chop_hl = data["high"].rolling(config.chop_len).max() - data["low"].rolling(config.chop_len).min()
    chop_index = np.where(chop_hl > 0, 100.0 * np.log10((chop_atr_sum / chop_hl).replace(0, np.nan)) / np.log10(config.chop_len), 50.0)
    wae_macd1 = data["close"].ewm(span=config.wae_fast, adjust=False).mean() - data["close"].ewm(span=config.wae_slow, adjust=False).mean()
    wae_macd2 = wae_macd1.shift(1)
    wae_trend = (wae_macd1 - wae_macd2) * config.wae_sens
    wae_basis = data["close"].rolling(config.wae_bb_len).mean()
    wae_dev = data["close"].rolling(config.wae_bb_len).std() * config.wae_bb_mult
    wae_explosion = (wae_basis + wae_dev) - (wae_basis - wae_dev)
    wae_dead_line = rma(true_range, 100) * config.wae_dead_zone
    abs_diff = data["close"].diff().abs().fillna(0.0)
    frost_smrng1 = abs_diff.ewm(span=27, adjust=False).mean().ewm(span=53, adjust=False).mean() * 1.5
    frost_mult = 1.3 if config.frost_mode == "Scalping" else 3.5 if config.frost_mode == "Intraday" else 4.2
    frost_smrng2 = abs_diff.ewm(span=55, adjust=False).mean().ewm(span=109, adjust=False).mean() * frost_mult
    frost_smrng = (frost_smrng1 + frost_smrng2) / 2.0
    frost_filt = frost_filter(data["close"].to_numpy(), frost_smrng.to_numpy())
    frost_up = counter_direction(frost_filt, True)
    frost_dn = counter_direction(frost_filt, False)
    dm_plus = (data["high"].diff()).clip(lower=0.0)
    dm_minus = (-data["low"].diff()).clip(lower=0.0)
    dm_minus = dm_minus.where(dm_minus > dm_plus, 0.0)
    dm_plus = dm_plus.where(dm_plus > dm_minus, 0.0)
    smooth_tr = rma(true_range, config.adx_len)
    smooth_dm_plus = rma(dm_plus, config.adx_len)
    smooth_dm_minus = rma(dm_minus, config.adx_len)
    di_plus = np.where(smooth_tr > 0, (smooth_dm_plus / smooth_tr) * 100.0, 0.0)
    di_minus = np.where(smooth_tr > 0, (smooth_dm_minus / smooth_tr) * 100.0, 0.0)
    dx = np.divide(np.abs(di_plus - di_minus) * 100.0, di_plus + di_minus, out=np.zeros_like(di_plus, dtype=float), where=(di_plus + di_minus) > 0)
    adx = rma(pd.Series(dx, index=data.index), config.adx_len)
    rsi_val = compute_rsi(data["close"], config.rsi_len)
    stoch_k = pd.Series(compute_stoch(data["close"], data["high"], data["low"], config.stoch_len), index=data.index)
    cci_val = compute_cci(data["high"], data["low"], data["close"], config.cci_len)
    rsi_long_ok = (not config.use_rsi) | (rsi_val < config.rsi_oversold) | ((rsi_val > 40.0) & (rsi_val < 60.0))
    rsi_short_ok = (not config.use_rsi) | (rsi_val > config.rsi_overbought) | ((rsi_val > 40.0) & (rsi_val < 60.0))
    stoch_long_ok = (not config.use_stoch) | (stoch_k < 30.0) | ((stoch_k > 20.0) & (stoch_k < 80.0))
    stoch_short_ok = (not config.use_stoch) | (stoch_k > 70.0) | ((stoch_k > 20.0) & (stoch_k < 80.0))
    cci_long_ok = (not config.use_cci) | (cci_val < -100.0) | ((cci_val > -50.0) & (cci_val < 50.0))
    cci_short_ok = (not config.use_cci) | (cci_val > 100.0) | ((cci_val > -50.0) & (cci_val < 50.0))
    osc_active_count = (1 if config.use_rsi else 0) + (1 if config.use_stoch else 0) + (1 if config.use_cci else 0)
    osc_required = max(1, int(math.ceil(osc_active_count * 0.66)))
    osc_confluence_long = rsi_long_ok.astype(int) + stoch_long_ok.astype(int) + cci_long_ok.astype(int)
    osc_confluence_short = rsi_short_ok.astype(int) + stoch_short_ok.astype(int) + cci_short_ok.astype(int)
    multi_osc_enabled = config.use_multi_osc and osc_active_count > 0
    utc_hour = pd.Index(data.index).hour
    in_london_open = (utc_hour >= 7) & (utc_hour < 10)
    in_ny_open = (utc_hour >= 12) & (utc_hour < 15)
    in_london_close = (utc_hour >= 15) & (utc_hour < 17)
    in_asia_session = (utc_hour >= 0) & (utc_hour < 3)
    in_kill_zone = (not config.use_kill_zones) | (config.kz_london_open and in_london_open) | (config.kz_ny_open and in_ny_open) | (config.kz_london_close and in_london_close) | (config.kz_asia_session and in_asia_session)
    sqz_bb_basis = data["close"].rolling(config.sqz_bb_len).mean()
    sqz_bb_dev = data["close"].rolling(config.sqz_bb_len).std() * config.sqz_bb_mult
    sqz_upper_bb, sqz_lower_bb = sqz_bb_basis + sqz_bb_dev, sqz_bb_basis - sqz_bb_dev
    sqz_kc_basis = data["close"].rolling(config.sqz_kc_len).mean()
    sqz_kc_range = atr_fast.rolling(config.sqz_kc_len).mean() * config.sqz_kc_mult
    sqz_upper_kc, sqz_lower_kc = sqz_kc_basis + sqz_kc_range, sqz_kc_basis - sqz_kc_range
    sqz_on = (sqz_lower_bb > sqz_lower_kc) & (sqz_upper_bb < sqz_upper_kc)
    sqz_off = ~sqz_on
    sqz_fire = sqz_on.shift(1, fill_value=False) & sqz_off
    sqz_fired_recent = sqz_fire.rolling(config.sqz_lookback).max().fillna(0).astype(bool)
    sqz_avg_hl = (data["high"].rolling(config.sqz_bb_len).max() + data["low"].rolling(config.sqz_bb_len).min()) / 2.0
    sqz_avg_close = data["close"].rolling(config.sqz_bb_len).mean()
    sqz_mom = (data["close"] - ((sqz_avg_hl + sqz_avg_close) / 2.0)).rolling(config.sqz_bb_len).mean()
    fisher_range = data["high"].rolling(config.fisher_len).max() - data["low"].rolling(config.fisher_len).min()
    fisher_raw = np.where(fisher_range > 0, 2.0 * ((data["close"] - data["low"].rolling(config.fisher_len).min()) / fisher_range - 0.5), 0.0)
    fisher_clamped = np.clip(fisher_raw, -0.999, 0.999)
    fisher_value = np.zeros(data.shape[0], dtype=float)
    for idx in range(1, data.shape[0]):
        fisher_value[idx] = 0.5 * math.log((1.0 + fisher_clamped[idx]) / (1.0 - fisher_clamped[idx])) + 0.5 * fisher_value[idx - 1]
    body_range = (data["close"] - data["open"]).abs()
    upper_wick = data["high"] - data[["close", "open"]].max(axis=1)
    lower_wick = data[["close", "open"]].min(axis=1) - data["low"]
    vol_sma20 = data["volume"].rolling(20).mean()
    vol_ratio = np.where(vol_sma20 > 0, data["volume"] / vol_sma20, 1.0)
    bullish_absorption = config.use_volume_absorption & (lower_wick > body_range * 1.5) & (data["close"] > data["open"]) & (vol_ratio > config.absorption_threshold) & (data["low"] == data["low"].rolling(10).min())
    bearish_absorption = config.use_volume_absorption & (upper_wick > body_range * 1.5) & (data["close"] < data["open"]) & (vol_ratio > config.absorption_threshold) & (data["high"] == data["high"].rolling(10).max())
    smc_piv_high, smc_piv_low = confirmed_pivots(data["high"].to_numpy(), data["low"].to_numpy(), config.swing_strength, config.swing_strength)
    piv_high, piv_low = confirmed_pivots(data["high"].to_numpy(), data["low"].to_numpy(), config.pivot_len, config.pivot_len)
    rsi_piv_high, rsi_piv_low = confirmed_pivots(rsi_val.to_numpy(), rsi_val.to_numpy(), config.div_pivot_strength, config.div_pivot_strength)
    price_div_high, price_div_low = confirmed_pivots(data["high"].to_numpy(), data["low"].to_numpy(), config.div_pivot_strength, config.div_pivot_strength)
    smc_long_ok, smc_short_ok = np.full(data.shape[0], not config.use_smc_filter, dtype=bool), np.full(data.shape[0], not config.use_smc_filter, dtype=bool)
    near_high, near_low = np.full(data.shape[0], np.nan, dtype=float), np.full(data.shape[0], np.nan, dtype=float)
    bear_sweep_recent, bull_sweep_recent = np.zeros(data.shape[0], dtype=bool), np.zeros(data.shape[0], dtype=bool)
    bullish_divergence, bearish_divergence = np.zeros(data.shape[0], dtype=bool), np.zeros(data.shape[0], dtype=bool)
    last_swing_high, last_swing_low, structure_bias = np.nan, np.nan, 0
    last_bull_structure_bar, last_bear_structure_bar = -10_000, -10_000
    stored_piv_highs, stored_piv_lows = [], []
    prev_rsi_high, prev_rsi_low, prev_price_high, prev_price_low = np.nan, np.nan, np.nan, np.nan
    for idx in range(data.shape[0]):
        if np.isfinite(smc_piv_high[idx]): last_swing_high = smc_piv_high[idx]
        if np.isfinite(smc_piv_low[idx]): last_swing_low = smc_piv_low[idx]
        bullish_break_level = last_swing_high + atr.iat[idx] * config.smc_break_atr_mult if np.isfinite(last_swing_high) else np.nan
        bearish_break_level = last_swing_low - atr.iat[idx] * config.smc_break_atr_mult if np.isfinite(last_swing_low) else np.nan
        bullish_bos = np.isfinite(bullish_break_level) and data["close"].iat[idx] > bullish_break_level and (idx == 0 or data["close"].iat[idx-1] <= bullish_break_level)
        bearish_bos = np.isfinite(bearish_break_level) and data["close"].iat[idx] < bearish_break_level and (idx == 0 or data["close"].iat[idx-1] >= bearish_break_level)
        if bullish_bos: structure_bias, last_bull_structure_bar = 1, idx
        if bearish_bos: structure_bias, last_bear_structure_bar = -1, idx
        smc_long_ok[idx] = (not config.use_smc_filter) or (structure_bias == 1 and (idx - last_bull_structure_bar) <= config.smc_fresh_bars)
        smc_short_ok[idx] = (not config.use_smc_filter) or (structure_bias == -1 and (idx - last_bear_structure_bar) <= config.smc_fresh_bars)
        if np.isfinite(piv_high[idx]): stored_piv_highs.append((idx, float(piv_high[idx])))
        if np.isfinite(piv_low[idx]): stored_piv_lows.append((idx, float(piv_low[idx])))
        stored_piv_highs = [(bar, value) for bar, value in stored_piv_highs if idx - bar <= config.piv_hold_bars]
        stored_piv_lows = [(bar, value) for bar, value in stored_piv_lows if idx - bar <= config.piv_hold_bars]
        above = [v for _, v in stored_piv_highs if v >= data["close"].iat[idx]]
        below = [v for _, v in stored_piv_lows if v <= data["close"].iat[idx]]
        if above: near_high[idx] = min(above)
        if below: near_low[idx] = max(below)
        if config.require_sweep_reject:
            start = max(0, idx - config.sweep_lookback + 1)
            if np.isfinite(near_high[idx]): bear_sweep_recent[idx] = any((data["high"].iat[p] > near_high[idx]) and (data["close"].iat[p] < near_high[idx]) for p in range(start, idx + 1))
            if np.isfinite(near_low[idx]): bull_sweep_recent[idx] = any((data["low"].iat[p] < near_low[idx]) and (data["close"].iat[p] > near_low[idx]) for p in range(start, idx + 1))
        if np.isfinite(rsi_piv_high[idx]) and np.isfinite(price_div_high[idx]):
            if np.isfinite(prev_rsi_high): bearish_divergence[idx] = price_div_high[idx] > prev_price_high and rsi_piv_high[idx] < prev_rsi_high
            prev_rsi_high, prev_price_high = rsi_piv_high[idx], price_div_high[idx]
        if np.isfinite(rsi_piv_low[idx]) and np.isfinite(price_div_low[idx]):
            if np.isfinite(prev_rsi_low): bullish_divergence[idx] = price_div_low[idx] < prev_price_low and rsi_piv_low[idx] > prev_rsi_low
            prev_rsi_low, prev_price_low = rsi_piv_low[idx], price_div_low[idx]
    primary_bull, secondary_bull = htf_alignment(data.index, data["close"], config.granularity)
    h1h, h1l = previous_levels(data["high"], data["low"], "1h", data.index)
    h4h, h4l = previous_levels(data["high"], data["low"], "4h", data.index)
    pdh, pdl = previous_levels(data["high"], data["low"], "1D", data.index)
    pwh, pwl = previous_levels(data["high"], data["low"], "1W", data.index)
    trend_slope_atr = (pd.Series(gaussian, index=data.index).diff().abs() / atr).fillna(0.0)
    band_width_atr = pd.Series((band_high - band_low), index=data.index).div(atr).fillna(0.0)
    frost_conf_long = (frost_up > 0).astype(int) + ((data["close"] > frost_filt + atr * 0.5).astype(int)) + ((frost_up >= 3).astype(int)) + ((frost_smrng1 > frost_smrng2).astype(int)) + ((adx >= 20).astype(int))
    frost_conf_short = (frost_dn > 0).astype(int) + ((data["close"] < frost_filt - atr * 0.5).astype(int)) + ((frost_dn >= 3).astype(int)) + ((frost_smrng1 > frost_smrng2).astype(int)) + ((adx >= 20).astype(int))
    data["atr"], data["atr_fast"], data["gaussian"], data["trend_line"] = atr, atr_fast, gaussian, trend_line
    data["trend_state"], data["trend_changed_long"], data["trend_changed_short"] = trend_state, trend_changed > 0, trend_changed < 0
    data["chop_ok"] = True if not config.use_chop else (pd.Series(chop_index, index=data.index) < config.chop_threshold)
    data["wae_bull_ok"] = True if not config.use_wae else ((wae_trend > 0) & (wae_trend > wae_explosion) & (wae_trend > wae_dead_line))
    data["wae_bear_ok"] = True if not config.use_wae else ((wae_trend < 0) & (wae_trend.abs() > wae_explosion) & (wae_trend.abs() > wae_dead_line))
    data["frost_long_ok"] = True if not config.use_frost else ((frost_up > 0) & (frost_conf_long >= config.min_frost_conf))
    data["frost_short_ok"] = True if not config.use_frost else ((frost_dn > 0) & (frost_conf_short >= config.min_frost_conf))
    data["htf_long_ok"] = True if not config.use_htf_alignment else (primary_bull & (secondary_bull if config.require_dual_htf else True))
    data["htf_short_ok"] = True if not config.use_htf_alignment else ((~primary_bull.astype(bool)) & ((~secondary_bull.astype(bool)) if config.require_dual_htf else True))
    data["slope_long_ok"], data["slope_short_ok"] = (not config.use_trend_slope) or (trend_slope_atr >= config.min_trend_slope_atr), (not config.use_trend_slope) or (trend_slope_atr >= config.min_trend_slope_atr)
    data["band_width_ok"] = True if not config.use_band_width else (band_width_atr >= config.min_band_width_atr)
    data["rsi"], data["stoch_k"], data["cci"], data["adx"], data["di_plus"], data["di_minus"] = rsi_val, stoch_k, cci_val, adx, di_plus, di_minus
    data["is_adx_strong"], data["dmi_long_ok"], data["dmi_short_ok"], data["multi_osc_enabled"] = adx >= config.adx_threshold, di_plus > di_minus, di_minus > di_plus, multi_osc_enabled
    data["is_multi_osc_long"], data["is_multi_osc_short"], data["in_kill_zone"], data["sqz_fired_recent"] = osc_confluence_long >= osc_required, osc_confluence_short >= osc_required, in_kill_zone, sqz_fired_recent
    data["sqz_mom_long"], data["sqz_mom_short"], data["fisher_value"] = sqz_mom > 0, sqz_mom < 0, fisher_value
    data["fisher_not_extreme_long"], data["fisher_not_extreme_short"] = fisher_value < config.fisher_extreme, fisher_value > -config.fisher_extreme
    data["bullish_absorption"], data["bearish_absorption"], data["smc_long_ok"], data["smc_short_ok"] = bullish_absorption, bearish_absorption, smc_long_ok, smc_short_ok
    data["near_high"], data["near_low"], data["bear_sweep_recent"], data["bull_sweep_recent"] = near_high, near_low, bear_sweep_recent, bull_sweep_recent
    
    # Convert to Series for easier logical operations
    s_near_high = pd.Series(near_high, index=data.index)
    s_near_low = pd.Series(near_low, index=data.index)
    
    dist_to_high_atr = (s_near_high - data["close"]) / atr
    dist_to_low_atr = (data["close"] - s_near_low) / atr
    
    strength = np.clip(((data["volume"] / vol_sma20) - 1.0) * 0.6 + ((data["close"] - data["open"]).abs() / atr) * 0.4, 0, 1)
    
    data["block_long_by_liq"] = config.use_liquidity_filter & s_near_high.notna() & dist_to_high_atr.between(0, config.prox_atr) & ((strength < config.break_strength_min) | bear_sweep_recent)
    data["block_short_by_liq"] = config.use_liquidity_filter & s_near_low.notna() & dist_to_low_atr.between(0, config.prox_atr) & ((strength < config.break_strength_min) | bull_sweep_recent)
    data["bullish_divergence"], data["bearish_divergence"] = config.use_divergence & bullish_divergence, config.use_divergence & bearish_divergence
    data["h1h"], data["h1l"], data["h4h"], data["h4l"], data["pdh"], data["pdl"], data["pwh"], data["pwl"] = h1h, h1l, h4h, h4l, pdh, pdl, pwh, pwl
    sqz_mom_long = sqz_mom > 0
    sqz_mom_short = sqz_mom < 0
    
    min_score_long = int(math.ceil(((2 if multi_osc_enabled else 0) + (2 if config.use_smc_filter else 0)) * config.min_score_ratio))
    score_long = ((2 if multi_osc_enabled else 0) * data["is_multi_osc_long"].astype(int)) + ((2 if config.use_smc_filter else 0) * data["smc_long_ok"].astype(int)) + ((1 if config.use_adx_filter else 0)*data["is_adx_strong"].astype(int)) + ((1 if config.use_dmi_confirm else 0)*data["dmi_long_ok"].astype(int)) + ((1 if config.use_kill_zones else 0)*data["in_kill_zone"].astype(int)) + ((1 if config.use_squeeze else 0)*(sqz_fired_recent & sqz_mom_long).astype(int)) + ((1 if config.use_fisher else 0)*data["fisher_not_extreme_long"].astype(int)) + (data["bullish_absorption"].astype(int)*2) + (data["bullish_divergence"].astype(int))
    score_short = ((2 if multi_osc_enabled else 0) * data["is_multi_osc_short"].astype(int)) + ((2 if config.use_smc_filter else 0) * data["smc_short_ok"].astype(int)) + ((1 if config.use_adx_filter else 0)*data["is_adx_strong"].astype(int)) + ((1 if config.use_dmi_confirm else 0)*data["dmi_short_ok"].astype(int)) + ((1 if config.use_kill_zones else 0)*data["in_kill_zone"].astype(int)) + ((1 if config.use_squeeze else 0)*(sqz_fired_recent & sqz_mom_short).astype(int)) + ((1 if config.use_fisher else 0)*data["fisher_not_extreme_short"].astype(int)) + (data["bearish_absorption"].astype(int)*2) + (data["bearish_divergence"].astype(int))
    data["long_signal"] = data["trend_changed_long"] & data["chop_ok"] & data["wae_bull_ok"] & data["frost_long_ok"] & data["htf_long_ok"] & data["slope_long_ok"] & data["band_width_ok"] & (~data["block_long_by_liq"]) & (score_long >= min_score_long)
    data["short_signal"] = data["trend_changed_short"] & data["chop_ok"] & data["wae_bear_ok"] & data["frost_short_ok"] & data["htf_short_ok"] & data["slope_short_ok"] & data["band_width_ok"] & (~data["block_short_by_liq"]) & (score_short >= min_score_long)
    return data.replace([np.inf, -np.inf], np.nan)
