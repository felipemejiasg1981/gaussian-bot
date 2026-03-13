"""
Backtester for Gaussian Trend IA Pro v6.1 EXTREME [Strategy].

This is not a generic Pine interpreter. It is a purpose-built Python port of
the core trading logic of the v6.1 strategy so we can compare:
  - win rate
  - net profit
  - profit factor
  - max drawdown
  - expectancy
  - partial TP behavior, BE and trailing SL

It is intentionally separate from the older simplified backtester.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def sma(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    if length <= 0 or len(arr) < length:
        return out
    csum = np.cumsum(np.insert(arr.astype(float), 0, 0.0))
    out[length - 1 :] = (csum[length:] - csum[:-length]) / length
    return out


def ema(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    if length <= 0 or len(arr) < length:
        return out
    alpha = 2.0 / (length + 1.0)
    out[length - 1] = np.nanmean(arr[:length])
    for i in range(length, len(arr)):
        out[i] = alpha * arr[i] + (1.0 - alpha) * out[i - 1]
    return out


def rma(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    if length <= 0 or len(arr) < length:
        return out
    out[length - 1] = np.nanmean(arr[:length])
    alpha = 1.0 / length
    for i in range(length, len(arr)):
        out[i] = alpha * arr[i] + (1.0 - alpha) * out[i - 1]
    return out


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    tr = np.full(len(close), np.nan, dtype=float)
    if len(close) == 0:
        return tr
    tr[0] = high[0] - low[0]
    for i in range(1, len(close)):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
    return rma(tr, length)


def rolling_lowest(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(length - 1, len(arr)):
        out[i] = np.nanmin(arr[i - length + 1 : i + 1])
    return out


def rolling_highest(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(length - 1, len(arr)):
        out[i] = np.nanmax(arr[i - length + 1 : i + 1])
    return out


def rsi(close: np.ndarray, length: int) -> np.ndarray:
    delta = np.diff(close, prepend=np.nan)
    gains = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)
    avg_gain = rma(gains, length)
    avg_loss = rma(losses, length)
    rs = np.divide(avg_gain, avg_loss, out=np.zeros_like(avg_gain), where=avg_loss != 0)
    out = 100.0 - (100.0 / (1.0 + rs))
    out[np.isnan(avg_gain) | np.isnan(avg_loss)] = np.nan
    return out


def stochastic_k(close: np.ndarray, high: np.ndarray, low: np.ndarray, length: int) -> np.ndarray:
    hh = rolling_highest(high, length)
    ll = rolling_lowest(low, length)
    den = hh - ll
    out = np.divide(
        close - ll,
        den,
        out=np.full(len(close), np.nan, dtype=float),
        where=den != 0,
    )
    return out * 100.0


def cci(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    tp = (high + low + close) / 3.0
    ma = sma(tp, length)
    out = np.full(len(tp), np.nan, dtype=float)
    for i in range(length - 1, len(tp)):
        window = tp[i - length + 1 : i + 1]
        md = np.mean(np.abs(window - ma[i]))
        if md != 0:
            out[i] = (tp[i] - ma[i]) / (0.015 * md)
    return out


def rolling_std(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(length - 1, len(arr)):
        out[i] = float(np.nanstd(arr[i - length + 1 : i + 1], ddof=0))
    return out


def linreg(arr: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    if length <= 1:
        return arr.astype(float).copy()
    x = np.arange(length, dtype=float)
    x_mean = np.mean(x)
    x_var = np.sum((x - x_mean) ** 2)
    for i in range(length - 1, len(arr)):
        y = arr[i - length + 1 : i + 1]
        if np.isnan(y).any():
            continue
        y_mean = np.mean(y)
        cov = np.sum((x - x_mean) * (y - y_mean))
        slope = cov / x_var if x_var != 0 else 0.0
        intercept = y_mean - slope * x_mean
        out[i] = intercept + slope * (length - 1)
    return out


def smoothrng(arr: np.ndarray, length: int, mult: float) -> np.ndarray:
    wper = length * 2 - 1
    delta = np.full(len(arr), np.nan, dtype=float)
    delta[1:] = np.abs(arr[1:] - arr[:-1])
    avrng = ema(delta, length)
    smoothed = ema(avrng, wper)
    return smoothed * mult


def rngfilt(arr: np.ndarray, rng: np.ndarray) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    if len(arr) == 0:
        return out
    out[0] = arr[0]
    for i in range(1, len(arr)):
        prev = out[i - 1]
        x = arr[i]
        r = 0.0 if np.isnan(rng[i]) else rng[i]
        if x > prev:
            out[i] = prev if x - r < prev else x - r
        else:
            out[i] = prev if x + r > prev else x + r
    return out


def pivot_high(arr: np.ndarray, left: int, right: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(left, len(arr) - right):
        val = arr[i]
        if np.all(val > arr[i - left : i]) and np.all(val >= arr[i + 1 : i + right + 1]):
            out[i] = val
    return out


def pivot_low(arr: np.ndarray, left: int, right: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(left, len(arr) - right):
        val = arr[i]
        if np.all(val < arr[i - left : i]) and np.all(val <= arr[i + 1 : i + right + 1]):
            out[i] = val
    return out


def crossover(a_now: float, a_prev: float, b_now: float, b_prev: float) -> bool:
    return a_prev <= b_prev and a_now > b_now


def crossunder(a_now: float, a_prev: float, b_now: float, b_prev: float) -> bool:
    return a_prev >= b_prev and a_now < b_now


def gaussian_weight(i: int, length: int, sigma: float) -> float:
    x = (i - length / 2.0) / sigma
    return np.exp(-0.5 * (x**2)) / np.sqrt(sigma * 2.0 * np.pi)


def gaussian_filter_dynamic(src: np.ndarray, base_length: int, step: int, sigma: float) -> np.ndarray:
    n = base_length + step
    out = np.full(len(src), np.nan, dtype=float)
    weights = np.array([gaussian_weight(i, n, sigma) for i in range(n)], dtype=float)
    weights_sum = weights.sum()
    if weights_sum == 0:
        return src.copy()
    weights = weights / weights_sum
    for i in range(n - 1, len(src)):
        window = src[i - n + 1 : i + 1][::-1]
        out[i] = float(np.sum(window * weights))
    return out


def gaussian_last_value(
    src: np.ndarray,
    end_idx: int,
    base_length: int,
    step: int,
    sigma: float,
    weight_cache: Dict[Tuple[int, float], np.ndarray],
) -> float:
    n = base_length + step
    if end_idx < n - 1:
        return np.nan

    sigma_key = round(float(sigma), 4)
    cache_key = (n, sigma_key)
    weights = weight_cache.get(cache_key)
    if weights is None:
        weights = np.array([gaussian_weight(i, n, sigma_key) for i in range(n)], dtype=float)
        weights_sum = weights.sum()
        if weights_sum == 0:
            return np.nan
        weights = weights / weights_sum
        weight_cache[cache_key] = weights

    window = src[end_idx - n + 1 : end_idx + 1][::-1]
    return float(np.sum(window * weights))


def map_htf(index: pd.DatetimeIndex, close: pd.Series, timeframe: str) -> pd.Series:
    rule = {"5": "5min", "15": "15min", "60": "1h", "240": "4h", "D": "1D", "W": "1W"}[timeframe]
    resampled = close.resample(rule).last().dropna()
    ema200 = resampled.ewm(span=200, adjust=False).mean()
    signal = (resampled > ema200).reindex(index, method="ffill")
    return signal.fillna(False)


def map_htf_rev(index: pd.DatetimeIndex, close: pd.Series, timeframe: str) -> pd.Series:
    rule = {"5": "5min", "15": "15min", "60": "1h", "240": "4h", "D": "1D", "W": "1W"}[timeframe]
    resampled = close.resample(rule).last().dropna()
    ema50 = resampled.ewm(span=50, adjust=False).mean()
    ema200 = resampled.ewm(span=200, adjust=False).mean()
    cross = ((ema50 > ema200) & (ema50.shift(1) <= ema200.shift(1))) | (
        (ema50 < ema200) & (ema50.shift(1) >= ema200.shift(1))
    )
    return cross.reindex(index, method="ffill").fillna(False)


@dataclass
class Trade:
    entry_time: str
    exit_time: str
    side: str
    entry: float
    exit: float
    pnl_pct: float
    bars: int
    tp1_hit: bool
    tp2_hit: bool
    tp3_hit: bool
    conf: int


class GaussianV61Backtest:
    def __init__(
        self,
        length: int = 20,
        mode: str = "AVG",
        distance: float = 1.0,
        use_adaptive_sigma: bool = True,
        sigma_base: float = 10.0,
        sigma_min: float = 5.0,
        sigma_max: float = 18.0,
        use_adx_filter: bool = True,
        adx_len: int = 14,
        adx_threshold: float = 25.0,
        use_dmi_confirm: bool = True,
        use_smc_filter: bool = True,
        swing_strength: int = 5,
        strict_structure_bias: bool = True,
        use_volume_absorption: bool = True,
        absorption_threshold: float = 1.5,
        use_multi_osc: bool = True,
        use_rsi: bool = True,
        use_stoch: bool = True,
        use_cci: bool = True,
        rsi_len: int = 14,
        stoch_len: int = 14,
        cci_len: int = 20,
        rsi_oversold: float = 35.0,
        rsi_overbought: float = 65.0,
        use_kill_zones: bool = True,
        kz_london_open: bool = True,
        kz_ny_open: bool = True,
        kz_london_close: bool = True,
        kz_asia_session: bool = False,
        use_chop_filter: bool = True,
        chop_len: int = 20,
        chop_threshold: float = 50.0,
        use_squeeze: bool = True,
        sqz_bb_len: int = 20,
        sqz_bb_mult: float = 2.0,
        sqz_kc_len: int = 20,
        sqz_kc_mult: float = 1.5,
        sqz_lookback: int = 8,
        use_wae: bool = True,
        wae_sens: int = 150,
        wae_fast_len: int = 20,
        wae_slow_len: int = 40,
        wae_bb_len: int = 20,
        wae_bb_mult: float = 2.0,
        wae_dead_zone: float = 3.7,
        use_fisher: bool = True,
        fisher_len: int = 10,
        fisher_extreme: float = 1.5,
        use_frost: bool = True,
        min_frost_conf: int = 2,
        frost_mode: str = "Intraday",
        use_reentry: bool = False,
        reentry_bars: int = 6,
        use_htf_alignment: bool = True,
        require_dual_htf: bool = True,
        use_trend_slope_filter: bool = True,
        min_trend_slope_atr: float = 0.12,
        use_band_width_filter: bool = True,
        min_band_width_atr: float = 1.10,
        use_divergence: bool = True,
        div_pivot_strength: int = 5,
        atr_len: int = 14,
        max_sl_pct: float = 0.06,
        sl_buf: float = 0.002,
        tp1_r: float = 1.0,
        tp2_r: float = 2.0,
        tp3_r: float = 3.0,
        tp4_r: float = 4.236,
        pct_tp1: float = 30.0,
        pct_tp2: float = 30.0,
        pct_tp3: float = 20.0,
        fee_pct: float = 0.0015,
        initial_equity: float = 10000.0,
    ) -> None:
        self.length = length
        self.mode = mode
        self.distance = distance
        self.use_adaptive_sigma = use_adaptive_sigma
        self.sigma_base = sigma_base
        self.sigma_min = sigma_min
        self.sigma_max = sigma_max
        self.use_adx_filter = use_adx_filter
        self.adx_len = adx_len
        self.adx_threshold = adx_threshold
        self.use_dmi_confirm = use_dmi_confirm
        self.use_smc_filter = use_smc_filter
        self.swing_strength = swing_strength
        self.strict_structure_bias = strict_structure_bias
        self.use_volume_absorption = use_volume_absorption
        self.absorption_threshold = absorption_threshold
        self.use_multi_osc = use_multi_osc
        self.use_rsi = use_rsi
        self.use_stoch = use_stoch
        self.use_cci = use_cci
        self.rsi_len = rsi_len
        self.stoch_len = stoch_len
        self.cci_len = cci_len
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.use_kill_zones = use_kill_zones
        self.kz_london_open = kz_london_open
        self.kz_ny_open = kz_ny_open
        self.kz_london_close = kz_london_close
        self.kz_asia_session = kz_asia_session
        self.use_chop_filter = use_chop_filter
        self.chop_len = chop_len
        self.chop_threshold = chop_threshold
        self.use_squeeze = use_squeeze
        self.sqz_bb_len = sqz_bb_len
        self.sqz_bb_mult = sqz_bb_mult
        self.sqz_kc_len = sqz_kc_len
        self.sqz_kc_mult = sqz_kc_mult
        self.sqz_lookback = sqz_lookback
        self.use_wae = use_wae
        self.wae_sens = wae_sens
        self.wae_fast_len = wae_fast_len
        self.wae_slow_len = wae_slow_len
        self.wae_bb_len = wae_bb_len
        self.wae_bb_mult = wae_bb_mult
        self.wae_dead_zone = wae_dead_zone
        self.use_fisher = use_fisher
        self.fisher_len = fisher_len
        self.fisher_extreme = fisher_extreme
        self.use_frost = use_frost
        self.min_frost_conf = min_frost_conf
        self.frost_mode = frost_mode
        self.use_reentry = use_reentry
        self.reentry_bars = reentry_bars
        self.use_htf_alignment = use_htf_alignment
        self.require_dual_htf = require_dual_htf
        self.use_trend_slope_filter = use_trend_slope_filter
        self.min_trend_slope_atr = min_trend_slope_atr
        self.use_band_width_filter = use_band_width_filter
        self.min_band_width_atr = min_band_width_atr
        self.use_divergence = use_divergence
        self.div_pivot_strength = div_pivot_strength
        self.atr_len = atr_len
        self.max_sl_pct = max_sl_pct
        self.sl_buf = sl_buf
        self.tp1_r = tp1_r
        self.tp2_r = tp2_r
        self.tp3_r = tp3_r
        self.tp4_r = tp4_r
        self.pct_tp1 = pct_tp1 / 100.0
        self.pct_tp2 = pct_tp2 / 100.0
        self.pct_tp3 = pct_tp3 / 100.0
        self.pct_runner = max(0.0, 1.0 - self.pct_tp1 - self.pct_tp2 - self.pct_tp3)
        self.fee_pct = fee_pct
        self.initial_equity = initial_equity

    def _calc_adx(self, high: np.ndarray, low: np.ndarray, close: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        tr = np.full(len(close), np.nan, dtype=float)
        dm_plus = np.zeros(len(close), dtype=float)
        dm_minus = np.zeros(len(close), dtype=float)
        for i in range(1, len(close)):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
            up_move = max(high[i] - high[i - 1], 0.0)
            down_move = max(low[i - 1] - low[i], 0.0)
            if up_move > down_move:
                dm_plus[i] = up_move
            elif down_move > up_move:
                dm_minus[i] = down_move
        smooth_tr = rma(tr, self.adx_len)
        smooth_plus = rma(dm_plus, self.adx_len)
        smooth_minus = rma(dm_minus, self.adx_len)
        di_plus = np.divide(smooth_plus, smooth_tr, out=np.zeros_like(smooth_plus), where=smooth_tr != 0) * 100.0
        di_minus = np.divide(smooth_minus, smooth_tr, out=np.zeros_like(smooth_minus), where=smooth_tr != 0) * 100.0
        den = di_plus + di_minus
        dx = np.divide(np.abs(di_plus - di_minus), den, out=np.zeros_like(di_plus), where=den != 0) * 100.0
        adx = rma(dx, self.adx_len)
        return adx, di_plus, di_minus

    def snapshot(self, df: pd.DataFrame) -> Dict:
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex")

        o = df["open"].astype(float).to_numpy()
        h = df["high"].astype(float).to_numpy()
        l = df["low"].astype(float).to_numpy()
        c = df["close"].astype(float).to_numpy()
        v = df["volume"].astype(float).to_numpy()
        idx = df.index
        n = len(df)
        if n < max(self.length + 25, 250):
            return {"ready": False, "reason": "insufficient_candles", "candles": n}

        atr_sl = atr(h, l, c, self.atr_len)
        atr14 = atr(h, l, c, 14)
        atr14_sma100 = sma(atr14, 100)
        atr_norm = np.divide(atr14, atr14_sma100, out=np.full(n, np.nan), where=~np.isnan(atr14_sma100) & (atr14_sma100 != 0))
        dynamic_sigma = np.full(n, self.sigma_base, dtype=float)
        if self.use_adaptive_sigma:
            for i in range(n):
                if np.isnan(atr_norm[i]):
                    continue
                dynamic_sigma[i] = max(self.sigma_min, min(self.sigma_max, self.sigma_base * (1.0 / max(0.5, atr_norm[i]))))

        gaussians = []
        weight_cache: Dict[Tuple[int, float], np.ndarray] = {}
        for step in range(21):
            series = np.full(n, np.nan, dtype=float)
            for i in range(n):
                sigma = dynamic_sigma[i] if not np.isnan(dynamic_sigma[i]) else self.sigma_base
                series[i] = gaussian_last_value(c, i, self.length, step, sigma, weight_cache)
            gaussians.append(series)

        avg = np.full(n, np.nan, dtype=float)
        band_low = np.full(n, np.nan, dtype=float)
        band_high = np.full(n, np.nan, dtype=float)
        trend = np.zeros(n, dtype=bool)
        trend_line = np.full(n, np.nan, dtype=float)
        avg_range_100 = sma(h - l, 100)

        for i in range(n):
            vals = np.array([g[i] for g in gaussians if not np.isnan(g[i])], dtype=float)
            if len(vals) != len(gaussians) or np.isnan(avg_range_100[i]):
                continue
            if self.mode == "MEDIAN":
                avg[i] = float(np.median(vals))
            else:
                avg[i] = float(np.mean(vals))
            band_low[i] = avg[i] - avg_range_100[i] * self.distance
            band_high[i] = avg[i] + avg_range_100[i] * self.distance
            if i == 0:
                trend[i] = False
            else:
                bull_cross = crossover(c[i], c[i - 1], band_high[i], band_high[i - 1]) if not np.isnan(band_high[i - 1]) else False
                bear_cross = crossunder(c[i], c[i - 1], band_low[i], band_low[i - 1]) if not np.isnan(band_low[i - 1]) else False
                if bull_cross:
                    trend[i] = True
                elif bear_cross:
                    trend[i] = False
                else:
                    trend[i] = trend[i - 1]
            trend_line[i] = band_low[i] if trend[i] else band_high[i]

        adx_val, di_plus, di_minus = self._calc_adx(h, l, c)
        rsi_val = rsi(c, self.rsi_len)
        stoch_val = stochastic_k(c, h, l, self.stoch_len)
        cci_val = cci(h, l, c, self.cci_len)

        swing_hi = pivot_high(h, self.swing_strength, self.swing_strength)
        swing_lo = pivot_low(l, self.swing_strength, self.swing_strength)
        structure_bias = np.zeros(n, dtype=int)
        last_swing_high = np.nan
        prev_swing_high = np.nan
        last_swing_low = np.nan
        prev_swing_low = np.nan
        bullish_div = np.zeros(n, dtype=bool)
        bearish_div = np.zeros(n, dtype=bool)

        rsi_ph = pivot_high(rsi_val, self.div_pivot_strength, self.div_pivot_strength)
        rsi_pl = pivot_low(rsi_val, self.div_pivot_strength, self.div_pivot_strength)
        price_ph = pivot_high(h, self.div_pivot_strength, self.div_pivot_strength)
        price_pl = pivot_low(l, self.div_pivot_strength, self.div_pivot_strength)
        last_rsi_high = np.nan
        last_rsi_low = np.nan
        last_price_high = np.nan
        last_price_low = np.nan

        for i in range(n):
            if not np.isnan(swing_hi[i]):
                prev_swing_high = last_swing_high
                last_swing_high = swing_hi[i]
            if not np.isnan(swing_lo[i]):
                prev_swing_low = last_swing_low
                last_swing_low = swing_lo[i]

            bull_break = not np.isnan(prev_swing_high) and c[i] > prev_swing_high
            bear_break = not np.isnan(prev_swing_low) and c[i] < prev_swing_low
            if bull_break:
                structure_bias[i] = 1
            elif bear_break:
                structure_bias[i] = -1
            elif i > 0:
                structure_bias[i] = structure_bias[i - 1]

            if not np.isnan(rsi_ph[i]) and not np.isnan(price_ph[i]):
                if not np.isnan(last_rsi_high) and not np.isnan(last_price_high):
                    bearish_div[i] = price_ph[i] > last_price_high and rsi_ph[i] < last_rsi_high
                last_rsi_high = rsi_ph[i]
                last_price_high = price_ph[i]
            if not np.isnan(rsi_pl[i]) and not np.isnan(price_pl[i]):
                if not np.isnan(last_rsi_low) and not np.isnan(last_price_low):
                    bullish_div[i] = price_pl[i] < last_price_low and rsi_pl[i] > last_rsi_low
                last_rsi_low = rsi_pl[i]
                last_price_low = price_pl[i]

        m5_bull = map_htf(idx, df["close"], "5").to_numpy(dtype=bool)
        m15_bull = map_htf(idx, df["close"], "15").to_numpy(dtype=bool)
        h1_bull = map_htf(idx, df["close"], "60").to_numpy(dtype=bool)
        h4_bull = map_htf(idx, df["close"], "240").to_numpy(dtype=bool)
        d_bull = map_htf(idx, df["close"], "D").to_numpy(dtype=bool)
        w_bull = map_htf(idx, df["close"], "W").to_numpy(dtype=bool)
        _ = m5_bull, m15_bull

        tf_sec = int((idx[1] - idx[0]).total_seconds()) if len(idx) > 1 else 0
        hour_utc = pd.Series(idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")).dt.hour.to_numpy()
        in_london_open = (hour_utc >= 7) & (hour_utc < 10)
        in_ny_open = (hour_utc >= 12) & (hour_utc < 15)
        in_london_close = (hour_utc >= 15) & (hour_utc < 17)
        in_asia = (hour_utc >= 0) & (hour_utc < 3)
        in_kill_zone = (
            (~np.array([self.use_kill_zones] * n))
            | (self.kz_london_open & in_london_open)
            | (self.kz_ny_open & in_ny_open)
            | (self.kz_london_close & in_london_close)
            | (self.kz_asia_session & in_asia)
        )

        tr1 = atr(h, l, c, 1)
        chop_atr_sum = np.full(n, np.nan, dtype=float)
        for j in range(self.chop_len - 1, n):
            chop_atr_sum[j] = np.nansum(tr1[j - self.chop_len + 1 : j + 1])
        chop_hl = rolling_highest(h, self.chop_len) - rolling_lowest(l, self.chop_len)
        chop_index = np.where(
            chop_hl > 0,
            100.0 * np.log10(np.divide(chop_atr_sum, chop_hl, out=np.full(n, np.nan), where=chop_hl > 0)) / np.log10(self.chop_len),
            50.0,
        )
        chop_ok = (~np.array([self.use_chop_filter] * n)) | (chop_index < self.chop_threshold)

        sqz_bb_basis = sma(c, self.sqz_bb_len)
        sqz_bb_dev = rolling_std(c, self.sqz_bb_len) * self.sqz_bb_mult
        sqz_upper_bb = sqz_bb_basis + sqz_bb_dev
        sqz_lower_bb = sqz_bb_basis - sqz_bb_dev
        sqz_kc_basis = sma(c, self.sqz_kc_len)
        sqz_kc_range = sma(atr(h, l, c, 10), self.sqz_kc_len) * self.sqz_kc_mult
        sqz_upper_kc = sqz_kc_basis + sqz_kc_range
        sqz_lower_kc = sqz_kc_basis - sqz_kc_range
        sqz_on = (sqz_lower_bb > sqz_lower_kc) & (sqz_upper_bb < sqz_upper_kc)
        sqz_off = ~sqz_on
        squeeze_ok_arr = np.full(n, not self.use_squeeze, dtype=bool)
        for j in range(n):
            fired = False
            for k in range(1, self.sqz_lookback + 1):
                if j - k < 0 or j - k + 1 < 0:
                    continue
                if sqz_on[j - k] and sqz_off[j - k + 1]:
                    fired = True
                    break
            squeeze_ok_arr[j] = (not self.use_squeeze) or fired

        wae_macd1 = ema(c, self.wae_fast_len) - ema(c, self.wae_slow_len)
        wae_macd2 = np.roll(wae_macd1, 1)
        wae_macd2[0] = np.nan
        wae_trend = (wae_macd1 - wae_macd2) * self.wae_sens
        wae_bb_basis2 = sma(c, self.wae_bb_len)
        wae_bb_dev2 = rolling_std(c, self.wae_bb_len) * self.wae_bb_mult
        wae_explosion = (wae_bb_basis2 + wae_bb_dev2) - (wae_bb_basis2 - wae_bb_dev2)
        wae_dead_line = atr(h, l, c, 100) * self.wae_dead_zone
        wae_bull_ok = (wae_trend > 0) & (wae_trend > wae_explosion) & (wae_trend > wae_dead_line)
        wae_bear_ok = (wae_trend < 0) & (np.abs(wae_trend) > wae_explosion) & (np.abs(wae_trend) > wae_dead_line)

        fisher_range = rolling_highest(h, self.fisher_len) - rolling_lowest(l, self.fisher_len)
        fisher_raw = np.divide(c - rolling_lowest(l, self.fisher_len), fisher_range, out=np.zeros(n), where=fisher_range > 0)
        fisher_raw = 2.0 * (fisher_raw - 0.5)
        fisher_clamped = np.clip(fisher_raw, -0.999, 0.999)
        fisher_value = np.full(n, np.nan, dtype=float)
        fisher_prev = np.full(n, np.nan, dtype=float)
        prev_val = 0.0
        for j in range(n):
            fisher_prev[j] = prev_val
            if np.isnan(fisher_clamped[j]):
                fisher_value[j] = prev_val
            else:
                fisher_value[j] = 0.5 * np.log((1.0 + fisher_clamped[j]) / (1.0 - fisher_clamped[j])) + 0.5 * (0.0 if j == 0 or np.isnan(fisher_value[j - 1]) else fisher_value[j - 1])
            prev_val = fisher_value[j]
        fisher_not_extreme_long = fisher_value < self.fisher_extreme
        fisher_not_extreme_short = fisher_value > -self.fisher_extreme

        frost_sens = 1.3 if self.frost_mode == "Scalping" else 3.5 if self.frost_mode == "Intraday" else 4.2
        frost_smrng1 = smoothrng(c, 27, 1.5)
        frost_smrng2 = smoothrng(c, 55, frost_sens)
        frost_smrng = (frost_smrng1 + frost_smrng2) / 2.0
        frost_filt = rngfilt(c, frost_smrng)
        frost_up = np.zeros(n, dtype=float)
        frost_dn = np.zeros(n, dtype=float)
        for j in range(1, n):
            prev_filt = frost_filt[j - 1] if not np.isnan(frost_filt[j - 1]) else frost_filt[j]
            curr_filt = frost_filt[j]
            if curr_filt > prev_filt:
                frost_up[j] = frost_up[j - 1] + 1
                frost_dn[j] = 0
            elif curr_filt < prev_filt:
                frost_up[j] = 0
                frost_dn[j] = frost_dn[j - 1] + 1
            else:
                frost_up[j] = frost_up[j - 1]
                frost_dn[j] = frost_dn[j - 1]

        frost_conf_long = np.zeros(n, dtype=int)
        frost_conf_short = np.zeros(n, dtype=int)
        for j in range(n):
            if frost_up[j] > 0:
                frost_conf_long[j] += 1
                if not np.isnan(atr_sl[j]) and c[j] > frost_filt[j] + atr_sl[j] * 0.5:
                    frost_conf_long[j] += 1
                if frost_up[j] >= 3:
                    frost_conf_long[j] += 1
                if frost_smrng1[j] > frost_smrng2[j]:
                    frost_conf_long[j] += 1
                if not np.isnan(adx_val[j]) and adx_val[j] >= 20:
                    frost_conf_long[j] += 1
            if frost_dn[j] > 0:
                frost_conf_short[j] += 1
                if not np.isnan(atr_sl[j]) and c[j] < frost_filt[j] - atr_sl[j] * 0.5:
                    frost_conf_short[j] += 1
                if frost_dn[j] >= 3:
                    frost_conf_short[j] += 1
                if frost_smrng1[j] > frost_smrng2[j]:
                    frost_conf_short[j] += 1
                if not np.isnan(adx_val[j]) and adx_val[j] >= 20:
                    frost_conf_short[j] += 1

        filter_frost_l_arr = (~np.array([self.use_frost] * n)) | ((frost_up > 0) & (frost_conf_long >= self.min_frost_conf))
        filter_frost_s_arr = (~np.array([self.use_frost] * n)) | ((frost_dn > 0) & (frost_conf_short >= self.min_frost_conf))

        i = n - 1
        if i <= 0 or np.isnan(avg[i]) or np.isnan(trend_line[i]) or np.isnan(atr_sl[i]):
            return {"ready": False, "reason": "invalid_last_bar", "candles": n}

        is_adx_strong = adx_val[i] >= self.adx_threshold if not np.isnan(adx_val[i]) else False
        filter_adx = (not self.use_adx_filter) or is_adx_strong
        filter_dmi_l = (not self.use_dmi_confirm) or (di_plus[i] > di_minus[i])
        filter_dmi_s = (not self.use_dmi_confirm) or (di_minus[i] > di_plus[i])

        if self.strict_structure_bias:
            is_smc_bull = structure_bias[i] == 1
            is_smc_bear = structure_bias[i] == -1
        else:
            is_smc_bull = structure_bias[i] >= 0
            is_smc_bear = structure_bias[i] <= 0
        filter_smc_l = (not self.use_smc_filter) or is_smc_bull
        filter_smc_s = (not self.use_smc_filter) or is_smc_bear

        rsi_long_ok = (not self.use_rsi) or ((rsi_val[i] < self.rsi_oversold) or (40 < rsi_val[i] < 60)) if not np.isnan(rsi_val[i]) or (not self.use_rsi) else False
        rsi_short_ok = (not self.use_rsi) or ((rsi_val[i] > self.rsi_overbought) or (40 < rsi_val[i] < 60)) if not np.isnan(rsi_val[i]) or (not self.use_rsi) else False
        stoch_long_ok = (not self.use_stoch) or ((stoch_val[i] < 30) or (20 < stoch_val[i] < 80)) if not np.isnan(stoch_val[i]) or (not self.use_stoch) else False
        stoch_short_ok = (not self.use_stoch) or ((stoch_val[i] > 70) or (20 < stoch_val[i] < 80)) if not np.isnan(stoch_val[i]) or (not self.use_stoch) else False
        cci_long_ok = (not self.use_cci) or ((cci_val[i] < -100) or (-50 < cci_val[i] < 50)) if not np.isnan(cci_val[i]) or (not self.use_cci) else False
        cci_short_ok = (not self.use_cci) or ((cci_val[i] > 100) or (-50 < cci_val[i] < 50)) if not np.isnan(cci_val[i]) or (not self.use_cci) else False
        osc_long = int(rsi_long_ok) + int(stoch_long_ok) + int(cci_long_ok)
        osc_short = int(rsi_short_ok) + int(stoch_short_ok) + int(cci_short_ok)
        osc_active_count = int(self.use_rsi) + int(self.use_stoch) + int(self.use_cci)
        osc_required = max(1, int(np.ceil(osc_active_count * 0.66)))
        filter_osc_l = (not self.use_multi_osc) or (osc_long >= osc_required)
        filter_osc_s = (not self.use_multi_osc) or (osc_short >= osc_required)

        body = abs(c[i] - o[i])
        upper_wick = h[i] - max(c[i], o[i])
        lower_wick = min(c[i], o[i]) - l[i]
        vol_sma20 = np.nanmean(v[max(0, i - 19) : i + 1])
        vol_ratio = v[i] / vol_sma20 if vol_sma20 and not np.isnan(vol_sma20) else 1.0
        bullish_abs = (
            self.use_volume_absorption
            and lower_wick > body * 1.5
            and c[i] > o[i]
            and vol_ratio > self.absorption_threshold
            and l[i] <= np.nanmin(l[max(0, i - 9) : i + 1])
        )
        bearish_abs = (
            self.use_volume_absorption
            and upper_wick > body * 1.5
            and c[i] < o[i]
            and vol_ratio > self.absorption_threshold
            and h[i] >= np.nanmax(h[max(0, i - 9) : i + 1])
        )

        trend_int_now = 1 if trend[i] else -1
        trend_int_prev = 1 if trend[i - 1] else -1
        trend_changed_long = (trend_int_now - trend_int_prev) > 0
        trend_changed_short = (trend_int_now - trend_int_prev) < 0

        if tf_sec <= 900:
            htf_primary_bull, htf_secondary_bull = h1_bull[i], h4_bull[i]
        elif tf_sec <= 3600:
            htf_primary_bull, htf_secondary_bull = h4_bull[i], d_bull[i]
        else:
            htf_primary_bull, htf_secondary_bull = d_bull[i], w_bull[i]
        htf_primary_bear = not htf_primary_bull
        htf_secondary_bear = not htf_secondary_bull

        trend_slope_atr = abs(trend_line[i] - trend_line[i - 1]) / atr_sl[i] if atr_sl[i] and not np.isnan(trend_line[i - 1]) else 0.0
        band_width_atr = (band_high[i] - band_low[i]) / atr_sl[i] if atr_sl[i] and not np.isnan(band_high[i]) and not np.isnan(band_low[i]) else 0.0
        slope_long_ok = (not self.use_trend_slope_filter) or (trend_line[i] > trend_line[i - 1] and trend_slope_atr >= self.min_trend_slope_atr)
        slope_short_ok = (not self.use_trend_slope_filter) or (trend_line[i] < trend_line[i - 1] and trend_slope_atr >= self.min_trend_slope_atr)
        band_width_ok = (not self.use_band_width_filter) or (band_width_atr >= self.min_band_width_atr)
        filter_htf_l = (not self.use_htf_alignment) or (htf_primary_bull and ((not self.require_dual_htf) or htf_secondary_bull))
        filter_htf_s = (not self.use_htf_alignment) or (htf_primary_bear and ((not self.require_dual_htf) or htf_secondary_bear))

        long_setup = trend_changed_long and filter_adx and filter_dmi_l and filter_smc_l and filter_osc_l and in_kill_zone[i]
        short_setup = trend_changed_short and filter_adx and filter_dmi_s and filter_smc_s and filter_osc_s and in_kill_zone[i]
        long_signal = long_setup and filter_htf_l and slope_long_ok and band_width_ok and chop_ok[i] and squeeze_ok_arr[i] and ((not self.use_wae) or wae_bull_ok[i]) and ((not self.use_fisher) or fisher_not_extreme_long[i]) and filter_frost_l_arr[i]
        short_signal = short_setup and filter_htf_s and slope_short_ok and band_width_ok and chop_ok[i] and squeeze_ok_arr[i] and ((not self.use_wae) or wae_bear_ok[i]) and ((not self.use_fisher) or fisher_not_extreme_short[i]) and filter_frost_s_arr[i]

        return {
            "ready": True,
            "timestamp": str(idx[i]),
            "close": float(c[i]),
            "trend": "bull" if trend[i] else "bear",
            "trend_line": float(trend_line[i]),
            "band_low": float(band_low[i]),
            "band_high": float(band_high[i]),
            "adx": float(adx_val[i]) if not np.isnan(adx_val[i]) else np.nan,
            "di_plus": float(di_plus[i]) if not np.isnan(di_plus[i]) else np.nan,
            "di_minus": float(di_minus[i]) if not np.isnan(di_minus[i]) else np.nan,
            "rsi": float(rsi_val[i]) if not np.isnan(rsi_val[i]) else np.nan,
            "stoch": float(stoch_val[i]) if not np.isnan(stoch_val[i]) else np.nan,
            "cci": float(cci_val[i]) if not np.isnan(cci_val[i]) else np.nan,
            "atr": float(atr_sl[i]) if not np.isnan(atr_sl[i]) else np.nan,
            "band_width_atr": float(band_width_atr),
            "trend_slope_atr": float(trend_slope_atr),
            "volume_ratio": float(vol_ratio),
            "chop_index": float(chop_index[i]) if not np.isnan(chop_index[i]) else np.nan,
            "wae_trend": float(wae_trend[i]) if not np.isnan(wae_trend[i]) else np.nan,
            "wae_explosion": float(wae_explosion[i]) if not np.isnan(wae_explosion[i]) else np.nan,
            "frost_conf_long": int(frost_conf_long[i]),
            "frost_conf_short": int(frost_conf_short[i]),
            "bullish_div": bool(bullish_div[i]),
            "bearish_div": bool(bearish_div[i]),
            "bullish_absorption": bool(bullish_abs),
            "bearish_absorption": bool(bearish_abs),
            "long_setup": bool(long_setup),
            "short_setup": bool(short_setup),
            "long_signal": bool(long_signal),
            "short_signal": bool(short_signal),
            "distance_to_trend_pct": float(((c[i] - trend_line[i]) / c[i]) * 100.0) if c[i] else np.nan,
        }

    def run(self, df: pd.DataFrame) -> Dict:
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex")

        o = df["open"].astype(float).to_numpy()
        h = df["high"].astype(float).to_numpy()
        l = df["low"].astype(float).to_numpy()
        c = df["close"].astype(float).to_numpy()
        v = df["volume"].astype(float).to_numpy()
        idx = df.index
        n = len(df)

        atr_sl = atr(h, l, c, self.atr_len)
        atr14 = atr(h, l, c, 14)
        atr14_sma100 = sma(atr14, 100)
        atr_norm = np.divide(atr14, atr14_sma100, out=np.full(n, np.nan), where=~np.isnan(atr14_sma100) & (atr14_sma100 != 0))
        dynamic_sigma = np.full(n, self.sigma_base, dtype=float)
        if self.use_adaptive_sigma:
            for i in range(n):
                if np.isnan(atr_norm[i]):
                    continue
                dynamic_sigma[i] = max(self.sigma_min, min(self.sigma_max, self.sigma_base * (1.0 / max(0.5, atr_norm[i]))))

        gaussians = []
        weight_cache: Dict[Tuple[int, float], np.ndarray] = {}
        for step in range(21):
            series = np.full(n, np.nan, dtype=float)
            for i in range(n):
                sigma = dynamic_sigma[i] if not np.isnan(dynamic_sigma[i]) else self.sigma_base
                series[i] = gaussian_last_value(c, i, self.length, step, sigma, weight_cache)
            gaussians.append(series)

        avg = np.full(n, np.nan, dtype=float)
        score = np.full(n, np.nan, dtype=float)
        band_low = np.full(n, np.nan, dtype=float)
        band_high = np.full(n, np.nan, dtype=float)
        trend = np.zeros(n, dtype=bool)
        trend_line = np.full(n, np.nan, dtype=float)
        avg_range_100 = sma(h - l, 100)

        for i in range(n):
            vals = np.array([g[i] for g in gaussians if not np.isnan(g[i])], dtype=float)
            if len(vals) != len(gaussians) or np.isnan(avg_range_100[i]):
                continue
            first = vals[0]
            score[i] = float(np.sum(vals > first) * 0.05)
            if self.mode == "MEDIAN":
                avg[i] = float(np.median(vals))
            else:
                avg[i] = float(np.mean(vals))
            band_low[i] = avg[i] - avg_range_100[i] * self.distance
            band_high[i] = avg[i] + avg_range_100[i] * self.distance
            if i == 0:
                trend[i] = False
            else:
                bull_cross = crossover(c[i], c[i - 1], band_high[i], band_high[i - 1]) if not np.isnan(band_high[i - 1]) else False
                bear_cross = crossunder(c[i], c[i - 1], band_low[i], band_low[i - 1]) if not np.isnan(band_low[i - 1]) else False
                if bull_cross:
                    trend[i] = True
                elif bear_cross:
                    trend[i] = False
                else:
                    trend[i] = trend[i - 1]
            trend_line[i] = band_low[i] if trend[i] else band_high[i]

        adx_val, di_plus, di_minus = self._calc_adx(h, l, c)
        rsi_val = rsi(c, self.rsi_len)
        stoch_val = stochastic_k(c, h, l, self.stoch_len)
        cci_val = cci(h, l, c, self.cci_len)

        swing_hi = pivot_high(h, self.swing_strength, self.swing_strength)
        swing_lo = pivot_low(l, self.swing_strength, self.swing_strength)
        structure_bias = np.zeros(n, dtype=int)
        last_swing_high = np.nan
        prev_swing_high = np.nan
        last_swing_low = np.nan
        prev_swing_low = np.nan
        bullish_div = np.zeros(n, dtype=bool)
        bearish_div = np.zeros(n, dtype=bool)

        rsi_ph = pivot_high(rsi_val, self.div_pivot_strength, self.div_pivot_strength)
        rsi_pl = pivot_low(rsi_val, self.div_pivot_strength, self.div_pivot_strength)
        price_ph = pivot_high(h, self.div_pivot_strength, self.div_pivot_strength)
        price_pl = pivot_low(l, self.div_pivot_strength, self.div_pivot_strength)
        last_rsi_high = np.nan
        last_rsi_low = np.nan
        last_price_high = np.nan
        last_price_low = np.nan

        for i in range(n):
            if not np.isnan(swing_hi[i]):
                prev_swing_high = last_swing_high
                last_swing_high = swing_hi[i]
            if not np.isnan(swing_lo[i]):
                prev_swing_low = last_swing_low
                last_swing_low = swing_lo[i]

            bull_break = not np.isnan(prev_swing_high) and c[i] > prev_swing_high
            bear_break = not np.isnan(prev_swing_low) and c[i] < prev_swing_low
            if bull_break:
                structure_bias[i] = 1
            elif bear_break:
                structure_bias[i] = -1
            elif i > 0:
                structure_bias[i] = structure_bias[i - 1]

            if not np.isnan(rsi_ph[i]) and not np.isnan(price_ph[i]):
                if not np.isnan(last_rsi_high) and not np.isnan(last_price_high):
                    bearish_div[i] = price_ph[i] > last_price_high and rsi_ph[i] < last_rsi_high
                last_rsi_high = rsi_ph[i]
                last_price_high = price_ph[i]
            if not np.isnan(rsi_pl[i]) and not np.isnan(price_pl[i]):
                if not np.isnan(last_rsi_low) and not np.isnan(last_price_low):
                    bullish_div[i] = price_pl[i] < last_price_low and rsi_pl[i] > last_rsi_low
                last_rsi_low = rsi_pl[i]
                last_price_low = price_pl[i]

        m5_bull = map_htf(idx, df["close"], "5").to_numpy(dtype=bool)
        m15_bull = map_htf(idx, df["close"], "15").to_numpy(dtype=bool)
        h1_bull = map_htf(idx, df["close"], "60").to_numpy(dtype=bool)
        h4_bull = map_htf(idx, df["close"], "240").to_numpy(dtype=bool)
        d_bull = map_htf(idx, df["close"], "D").to_numpy(dtype=bool)
        w_bull = map_htf(idx, df["close"], "W").to_numpy(dtype=bool)
        _ = m5_bull, m15_bull  # retained for possible diagnostics

        tf_sec = int((idx[1] - idx[0]).total_seconds()) if len(idx) > 1 else 0
        hour_utc = pd.Series(idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")).dt.hour.to_numpy()
        in_london_open = (hour_utc >= 7) & (hour_utc < 10)
        in_ny_open = (hour_utc >= 12) & (hour_utc < 15)
        in_london_close = (hour_utc >= 15) & (hour_utc < 17)
        in_asia = (hour_utc >= 0) & (hour_utc < 3)
        in_kill_zone = (
            (~np.array([self.use_kill_zones] * n))
            | (self.kz_london_open & in_london_open)
            | (self.kz_ny_open & in_ny_open)
            | (self.kz_london_close & in_london_close)
            | (self.kz_asia_session & in_asia)
        )

        tr1 = atr(h, l, c, 1)
        chop_atr_sum = np.full(n, np.nan, dtype=float)
        for j in range(self.chop_len - 1, n):
            chop_atr_sum[j] = np.nansum(tr1[j - self.chop_len + 1 : j + 1])
        chop_hl = rolling_highest(h, self.chop_len) - rolling_lowest(l, self.chop_len)
        chop_index = np.where(
            chop_hl > 0,
            100.0 * np.log10(np.divide(chop_atr_sum, chop_hl, out=np.full(n, np.nan), where=chop_hl > 0)) / np.log10(self.chop_len),
            50.0,
        )
        chop_ok = (~np.array([self.use_chop_filter] * n)) | (chop_index < self.chop_threshold)

        sqz_bb_basis = sma(c, self.sqz_bb_len)
        sqz_bb_dev = rolling_std(c, self.sqz_bb_len) * self.sqz_bb_mult
        sqz_upper_bb = sqz_bb_basis + sqz_bb_dev
        sqz_lower_bb = sqz_bb_basis - sqz_bb_dev
        sqz_kc_basis = sma(c, self.sqz_kc_len)
        sqz_kc_range = sma(atr(h, l, c, 10), self.sqz_kc_len) * self.sqz_kc_mult
        sqz_upper_kc = sqz_kc_basis + sqz_kc_range
        sqz_lower_kc = sqz_kc_basis - sqz_kc_range
        sqz_on = (sqz_lower_bb > sqz_lower_kc) & (sqz_upper_bb < sqz_upper_kc)
        sqz_off = ~sqz_on
        squeeze_ok_arr = np.full(n, not self.use_squeeze, dtype=bool)
        for j in range(n):
            fired = False
            for k in range(1, self.sqz_lookback + 1):
                if j - k < 0 or j - k + 1 < 0:
                    continue
                if sqz_on[j - k] and sqz_off[j - k + 1]:
                    fired = True
                    break
            squeeze_ok_arr[j] = (not self.use_squeeze) or fired

        wae_macd1 = ema(c, self.wae_fast_len) - ema(c, self.wae_slow_len)
        wae_macd2 = np.roll(wae_macd1, 1)
        wae_macd2[0] = np.nan
        wae_trend = (wae_macd1 - wae_macd2) * self.wae_sens
        wae_bb_basis2 = sma(c, self.wae_bb_len)
        wae_bb_dev2 = rolling_std(c, self.wae_bb_len) * self.wae_bb_mult
        wae_explosion = (wae_bb_basis2 + wae_bb_dev2) - (wae_bb_basis2 - wae_bb_dev2)
        wae_dead_line = atr(h, l, c, 100) * self.wae_dead_zone
        wae_bull_ok = (wae_trend > 0) & (wae_trend > wae_explosion) & (wae_trend > wae_dead_line)
        wae_bear_ok = (wae_trend < 0) & (np.abs(wae_trend) > wae_explosion) & (np.abs(wae_trend) > wae_dead_line)

        fisher_range = rolling_highest(h, self.fisher_len) - rolling_lowest(l, self.fisher_len)
        fisher_raw = np.divide(c - rolling_lowest(l, self.fisher_len), fisher_range, out=np.zeros(n), where=fisher_range > 0)
        fisher_raw = 2.0 * (fisher_raw - 0.5)
        fisher_clamped = np.clip(fisher_raw, -0.999, 0.999)
        fisher_value = np.full(n, np.nan, dtype=float)
        prev_val = 0.0
        for j in range(n):
            if np.isnan(fisher_clamped[j]):
                fisher_value[j] = prev_val
            else:
                fisher_value[j] = 0.5 * np.log((1.0 + fisher_clamped[j]) / (1.0 - fisher_clamped[j])) + 0.5 * (0.0 if j == 0 or np.isnan(fisher_value[j - 1]) else fisher_value[j - 1])
            prev_val = fisher_value[j]
        fisher_not_extreme_long = fisher_value < self.fisher_extreme
        fisher_not_extreme_short = fisher_value > -self.fisher_extreme

        frost_sens = 1.3 if self.frost_mode == "Scalping" else 3.5 if self.frost_mode == "Intraday" else 4.2
        frost_smrng1 = smoothrng(c, 27, 1.5)
        frost_smrng2 = smoothrng(c, 55, frost_sens)
        frost_smrng = (frost_smrng1 + frost_smrng2) / 2.0
        frost_filt = rngfilt(c, frost_smrng)
        frost_up = np.zeros(n, dtype=float)
        frost_dn = np.zeros(n, dtype=float)
        for j in range(1, n):
            prev_filt = frost_filt[j - 1] if not np.isnan(frost_filt[j - 1]) else frost_filt[j]
            curr_filt = frost_filt[j]
            if curr_filt > prev_filt:
                frost_up[j] = frost_up[j - 1] + 1
                frost_dn[j] = 0
            elif curr_filt < prev_filt:
                frost_up[j] = 0
                frost_dn[j] = frost_dn[j - 1] + 1
            else:
                frost_up[j] = frost_up[j - 1]
                frost_dn[j] = frost_dn[j - 1]

        frost_conf_long = np.zeros(n, dtype=int)
        frost_conf_short = np.zeros(n, dtype=int)
        for j in range(n):
            if frost_up[j] > 0:
                frost_conf_long[j] += 1
                if not np.isnan(atr_sl[j]) and c[j] > frost_filt[j] + atr_sl[j] * 0.5:
                    frost_conf_long[j] += 1
                if frost_up[j] >= 3:
                    frost_conf_long[j] += 1
                if frost_smrng1[j] > frost_smrng2[j]:
                    frost_conf_long[j] += 1
                if not np.isnan(adx_val[j]) and adx_val[j] >= 20:
                    frost_conf_long[j] += 1
            if frost_dn[j] > 0:
                frost_conf_short[j] += 1
                if not np.isnan(atr_sl[j]) and c[j] < frost_filt[j] - atr_sl[j] * 0.5:
                    frost_conf_short[j] += 1
                if frost_dn[j] >= 3:
                    frost_conf_short[j] += 1
                if frost_smrng1[j] > frost_smrng2[j]:
                    frost_conf_short[j] += 1
                if not np.isnan(adx_val[j]) and adx_val[j] >= 20:
                    frost_conf_short[j] += 1

        filter_frost_l_arr = (~np.array([self.use_frost] * n)) | ((frost_up > 0) & (frost_conf_long >= self.min_frost_conf))
        filter_frost_s_arr = (~np.array([self.use_frost] * n)) | ((frost_dn > 0) & (frost_conf_short >= self.min_frost_conf))

        trades: List[Trade] = []
        equity_curve = [self.initial_equity]
        equity = self.initial_equity

        in_trade = False
        side = 0
        entry = sl = be = tp1 = tp2 = tp3 = tp4 = np.nan
        tp1_hit = tp2_hit = tp3_hit = False
        pos_auto_be = False
        trade_conf = 0
        entry_bar = 0
        entry_time = None
        rem_tp1, rem_tp2, rem_tp3, rem_runner = self.pct_tp1, self.pct_tp2, self.pct_tp3, self.pct_runner

        start_bar = max(self.length + 25, 250)
        for i in range(start_bar, n):
            if np.isnan(avg[i]) or np.isnan(trend_line[i]) or np.isnan(atr_sl[i]):
                equity_curve.append(equity)
                continue

            is_adx_strong = adx_val[i] >= self.adx_threshold if not np.isnan(adx_val[i]) else False
            filter_adx = (not self.use_adx_filter) or is_adx_strong
            filter_dmi_l = (not self.use_dmi_confirm) or (di_plus[i] > di_minus[i])
            filter_dmi_s = (not self.use_dmi_confirm) or (di_minus[i] > di_plus[i])

            if self.strict_structure_bias:
                is_smc_bull = structure_bias[i] == 1
                is_smc_bear = structure_bias[i] == -1
            else:
                is_smc_bull = structure_bias[i] >= 0
                is_smc_bear = structure_bias[i] <= 0
            filter_smc_l = (not self.use_smc_filter) or is_smc_bull
            filter_smc_s = (not self.use_smc_filter) or is_smc_bear

            rsi_long_ok = (not self.use_rsi) or ((rsi_val[i] < self.rsi_oversold) or (40 < rsi_val[i] < 60)) if not np.isnan(rsi_val[i]) or (not self.use_rsi) else False
            rsi_short_ok = (not self.use_rsi) or ((rsi_val[i] > self.rsi_overbought) or (40 < rsi_val[i] < 60)) if not np.isnan(rsi_val[i]) or (not self.use_rsi) else False
            stoch_long_ok = (not self.use_stoch) or ((stoch_val[i] < 30) or (20 < stoch_val[i] < 80)) if not np.isnan(stoch_val[i]) or (not self.use_stoch) else False
            stoch_short_ok = (not self.use_stoch) or ((stoch_val[i] > 70) or (20 < stoch_val[i] < 80)) if not np.isnan(stoch_val[i]) or (not self.use_stoch) else False
            cci_long_ok = (not self.use_cci) or ((cci_val[i] < -100) or (-50 < cci_val[i] < 50)) if not np.isnan(cci_val[i]) or (not self.use_cci) else False
            cci_short_ok = (not self.use_cci) or ((cci_val[i] > 100) or (-50 < cci_val[i] < 50)) if not np.isnan(cci_val[i]) or (not self.use_cci) else False
            osc_long = int(rsi_long_ok) + int(stoch_long_ok) + int(cci_long_ok)
            osc_short = int(rsi_short_ok) + int(stoch_short_ok) + int(cci_short_ok)
            osc_active_count = int(self.use_rsi) + int(self.use_stoch) + int(self.use_cci)
            osc_required = max(1, int(np.ceil(osc_active_count * 0.66)))
            filter_osc_l = (not self.use_multi_osc) or (osc_long >= osc_required)
            filter_osc_s = (not self.use_multi_osc) or (osc_short >= osc_required)

            body = abs(c[i] - o[i])
            upper_wick = h[i] - max(c[i], o[i])
            lower_wick = min(c[i], o[i]) - l[i]
            vol_sma20 = np.nanmean(v[max(0, i - 19) : i + 1])
            vol_ratio = v[i] / vol_sma20 if vol_sma20 and not np.isnan(vol_sma20) else 1.0
            bullish_abs = (
                self.use_volume_absorption
                and lower_wick > body * 1.5
                and c[i] > o[i]
                and vol_ratio > self.absorption_threshold
                and l[i] <= np.nanmin(l[max(0, i - 9) : i + 1])
            )
            bearish_abs = (
                self.use_volume_absorption
                and upper_wick > body * 1.5
                and c[i] < o[i]
                and vol_ratio > self.absorption_threshold
                and h[i] >= np.nanmax(h[max(0, i - 9) : i + 1])
            )

            trend_int_now = 1 if trend[i] else -1
            trend_int_prev = 1 if trend[i - 1] else -1
            trend_changed_long = (trend_int_now - trend_int_prev) > 0
            trend_changed_short = (trend_int_now - trend_int_prev) < 0

            if tf_sec <= 900:
                htf_primary_bull, htf_secondary_bull = h1_bull[i], h4_bull[i]
            elif tf_sec <= 3600:
                htf_primary_bull, htf_secondary_bull = h4_bull[i], d_bull[i]
            else:
                htf_primary_bull, htf_secondary_bull = d_bull[i], w_bull[i]
            htf_primary_bear = not htf_primary_bull
            htf_secondary_bear = not htf_secondary_bull

            trend_slope_atr = abs(trend_line[i] - trend_line[i - 1]) / atr_sl[i] if i > 0 and atr_sl[i] and not np.isnan(trend_line[i - 1]) else 0.0
            band_width_atr = (band_high[i] - band_low[i]) / atr_sl[i] if atr_sl[i] and not np.isnan(band_high[i]) and not np.isnan(band_low[i]) else 0.0
            slope_long_ok = (not self.use_trend_slope_filter) or (trend_line[i] > trend_line[i - 1] and trend_slope_atr >= self.min_trend_slope_atr)
            slope_short_ok = (not self.use_trend_slope_filter) or (trend_line[i] < trend_line[i - 1] and trend_slope_atr >= self.min_trend_slope_atr)
            band_width_ok = (not self.use_band_width_filter) or (band_width_atr >= self.min_band_width_atr)
            filter_htf_l = (not self.use_htf_alignment) or (htf_primary_bull and ((not self.require_dual_htf) or htf_secondary_bull))
            filter_htf_s = (not self.use_htf_alignment) or (htf_primary_bear and ((not self.require_dual_htf) or htf_secondary_bear))

            long_setup = trend_changed_long and filter_adx and filter_dmi_l and filter_smc_l and filter_osc_l and in_kill_zone[i]
            short_setup = trend_changed_short and filter_adx and filter_dmi_s and filter_smc_s and filter_osc_s and in_kill_zone[i]

            conf_long = 0
            if long_setup:
                conf_long += 2 if is_adx_strong else 0
                conf_long += 2 if is_smc_bull else 0
                conf_long += 2 if osc_long >= 2 else 0
                conf_long += 1 if in_kill_zone[i] else 0
                conf_long += 2 if bullish_abs else 0
                conf_long += 1 if (self.use_divergence and bullish_div[i]) else 0

            conf_short = 0
            if short_setup:
                conf_short += 2 if is_adx_strong else 0
                conf_short += 2 if is_smc_bear else 0
                conf_short += 2 if osc_short >= 2 else 0
                conf_short += 1 if in_kill_zone[i] else 0
                conf_short += 2 if bearish_abs else 0
                conf_short += 1 if (self.use_divergence and bearish_div[i]) else 0

            long_signal = (
                long_setup
                and filter_htf_l
                and slope_long_ok
                and band_width_ok
                and chop_ok[i]
                and squeeze_ok_arr[i]
                and ((not self.use_wae) or wae_bull_ok[i])
                and ((not self.use_fisher) or fisher_not_extreme_long[i])
                and filter_frost_l_arr[i]
            )
            short_signal = (
                short_setup
                and filter_htf_s
                and slope_short_ok
                and band_width_ok
                and chop_ok[i]
                and squeeze_ok_arr[i]
                and ((not self.use_wae) or wae_bear_ok[i])
                and ((not self.use_fisher) or fisher_not_extreme_short[i])
                and filter_frost_s_arr[i]
            )

            if in_trade:
                if side == 1 and not np.isnan(trend_line[i]):
                    sl = max(sl, trend_line[i] * (1.0 - self.sl_buf))
                elif side == -1 and not np.isnan(trend_line[i]):
                    sl = min(sl, trend_line[i] * (1.0 + self.sl_buf))

                if side == 1 and (not pos_auto_be) and h[i] >= tp1:
                    sl = max(sl, be)
                    pos_auto_be = True
                elif side == -1 and (not pos_auto_be) and l[i] <= tp1:
                    sl = min(sl, be)
                    pos_auto_be = True

                realized = 0.0
                remaining = rem_tp1 + rem_tp2 + rem_tp3 + rem_runner

                def leg_pnl(exit_price: float) -> float:
                    if side == 1:
                        return (exit_price - entry) / entry
                    return (entry - exit_price) / entry

                if side == 1:
                    if (not tp1_hit) and h[i] >= tp1:
                        realized += leg_pnl(tp1) * rem_tp1
                        remaining -= rem_tp1
                        rem_tp1 = 0.0
                        tp1_hit = True
                    if tp1_hit and (not tp2_hit) and h[i] >= tp2:
                        realized += leg_pnl(tp2) * rem_tp2
                        remaining -= rem_tp2
                        rem_tp2 = 0.0
                        tp2_hit = True
                    if tp2_hit and (not tp3_hit) and h[i] >= tp3:
                        realized += leg_pnl(tp3) * rem_tp3
                        remaining -= rem_tp3
                        rem_tp3 = 0.0
                        tp3_hit = True
                    if h[i] >= tp4:
                        exit_price = tp4
                        realized += leg_pnl(exit_price) * remaining
                        realized -= self.fee_pct
                        equity *= 1.0 + realized
                        trades.append(
                            Trade(str(entry_time), str(idx[i]), "LONG", entry, exit_price, round(realized * 100, 2), i - entry_bar, tp1_hit, tp2_hit, tp3_hit, trade_conf)
                        )
                        in_trade = False
                    elif l[i] <= sl and in_trade:
                        exit_price = sl
                        realized += leg_pnl(exit_price) * remaining
                        realized -= self.fee_pct
                        equity *= 1.0 + realized
                        trades.append(
                            Trade(str(entry_time), str(idx[i]), "LONG", entry, exit_price, round(realized * 100, 2), i - entry_bar, tp1_hit, tp2_hit, tp3_hit, trade_conf)
                        )
                        in_trade = False
                else:
                    if (not tp1_hit) and l[i] <= tp1:
                        realized += leg_pnl(tp1) * rem_tp1
                        remaining -= rem_tp1
                        rem_tp1 = 0.0
                        tp1_hit = True
                    if tp1_hit and (not tp2_hit) and l[i] <= tp2:
                        realized += leg_pnl(tp2) * rem_tp2
                        remaining -= rem_tp2
                        rem_tp2 = 0.0
                        tp2_hit = True
                    if tp2_hit and (not tp3_hit) and l[i] <= tp3:
                        realized += leg_pnl(tp3) * rem_tp3
                        remaining -= rem_tp3
                        rem_tp3 = 0.0
                        tp3_hit = True
                    if l[i] <= tp4:
                        exit_price = tp4
                        realized += leg_pnl(exit_price) * remaining
                        realized -= self.fee_pct
                        equity *= 1.0 + realized
                        trades.append(
                            Trade(str(entry_time), str(idx[i]), "SHORT", entry, exit_price, round(realized * 100, 2), i - entry_bar, tp1_hit, tp2_hit, tp3_hit, trade_conf)
                        )
                        in_trade = False
                    elif h[i] >= sl and in_trade:
                        exit_price = sl
                        realized += leg_pnl(exit_price) * remaining
                        realized -= self.fee_pct
                        equity *= 1.0 + realized
                        trades.append(
                            Trade(str(entry_time), str(idx[i]), "SHORT", entry, exit_price, round(realized * 100, 2), i - entry_bar, tp1_hit, tp2_hit, tp3_hit, trade_conf)
                        )
                        in_trade = False

            if (not in_trade) and long_signal:
                entry = c[i]
                base_sl = max(trend_line[i] * (1.0 - self.sl_buf), entry * (1.0 - self.max_sl_pct))
                risk = max(entry - base_sl, 1e-9)
                sl = base_sl
                be = entry * (1.0 + self.fee_pct)
                tp1 = entry + risk * self.tp1_r
                tp2 = entry + risk * self.tp2_r
                tp3 = entry + risk * self.tp3_r
                tp4 = entry + risk * self.tp4_r
                tp1_hit = tp2_hit = tp3_hit = False
                pos_auto_be = False
                rem_tp1, rem_tp2, rem_tp3, rem_runner = self.pct_tp1, self.pct_tp2, self.pct_tp3, self.pct_runner
                trade_conf = conf_long
                side = 1
                in_trade = True
                entry_bar = i
                entry_time = idx[i]
                equity *= 1.0 - self.fee_pct

            elif (not in_trade) and short_signal:
                entry = c[i]
                base_sl = min(trend_line[i] * (1.0 + self.sl_buf), entry * (1.0 + self.max_sl_pct))
                risk = max(base_sl - entry, 1e-9)
                sl = base_sl
                be = entry * (1.0 - self.fee_pct)
                tp1 = entry - risk * self.tp1_r
                tp2 = entry - risk * self.tp2_r
                tp3 = entry - risk * self.tp3_r
                tp4 = entry - risk * self.tp4_r
                tp1_hit = tp2_hit = tp3_hit = False
                pos_auto_be = False
                rem_tp1, rem_tp2, rem_tp3, rem_runner = self.pct_tp1, self.pct_tp2, self.pct_tp3, self.pct_runner
                trade_conf = conf_short
                side = -1
                in_trade = True
                entry_bar = i
                entry_time = idx[i]
                equity *= 1.0 - self.fee_pct

            equity_curve.append(equity)

        if in_trade:
            exit_price = c[-1]
            pnl = ((exit_price - entry) / entry) if side == 1 else ((entry - exit_price) / entry)
            equity *= 1.0 + pnl * (rem_tp1 + rem_tp2 + rem_tp3 + rem_runner) - self.fee_pct
            trades.append(
                Trade(str(entry_time), str(idx[-1]), "LONG" if side == 1 else "SHORT", entry, exit_price, round(pnl * 100, 2), n - 1 - entry_bar, tp1_hit, tp2_hit, tp3_hit, trade_conf)
            )

        if not trades:
            return {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "net_profit_pct": 0.0,
                "profit_factor": 0.0,
                "max_drawdown_pct": 0.0,
                "expectancy_pct": 0.0,
                "avg_trade_pct": 0.0,
                "best_trade_pct": 0.0,
                "worst_trade_pct": 0.0,
                "final_equity": round(equity, 2),
                "trades": [],
                "equity_curve": equity_curve,
            }

        pnl_list = [t.pnl_pct for t in trades]
        wins = [x for x in pnl_list if x > 0]
        losses = [x for x in pnl_list if x <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        peak = equity_curve[0]
        max_dd = 0.0
        for val in equity_curve:
            peak = max(peak, val)
            if peak > 0:
                max_dd = max(max_dd, (peak - val) / peak * 100.0)

        return {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(trades) * 100.0, 2),
            "net_profit_pct": round((equity / self.initial_equity - 1.0) * 100.0, 2),
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else 999.0,
            "max_drawdown_pct": round(max_dd, 2),
            "expectancy_pct": round(float(np.mean(pnl_list)), 3),
            "avg_trade_pct": round(float(np.mean(pnl_list)), 3),
            "best_trade_pct": round(max(pnl_list), 2),
            "worst_trade_pct": round(min(pnl_list), 2),
            "final_equity": round(equity, 2),
            "tp1_hits": sum(1 for t in trades if t.tp1_hit),
            "tp2_hits": sum(1 for t in trades if t.tp2_hit),
            "tp3_hits": sum(1 for t in trades if t.tp3_hit),
            "trades": [asdict(t) for t in trades],
            "equity_curve": equity_curve,
        }
