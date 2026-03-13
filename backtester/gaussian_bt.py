"""
Gaussian Trend Backtester — Core Engine
Ports the Gaussian kernel filter + trading logic from PineScript to Python.
"""
import numpy as np
import pandas as pd

# ─── GAUSSIAN KERNEL FILTER ───────────────────────────────────
def gaussian_kernel(length: int, sigma: float = None) -> np.ndarray:
    """Generate Gaussian kernel weights."""
    if sigma is None:
        sigma = length / 4.0
    x = np.arange(length)
    w = np.exp(-0.5 * ((x - 0) / sigma) ** 2)
    return w / w.sum()

def gaussian_filter(series: np.ndarray, length: int, mode: str = "AVG") -> np.ndarray:
    """Apply Gaussian-weighted filter to a series."""
    n = len(series)
    weights = gaussian_kernel(length)
    result = np.full(n, np.nan)
    for i in range(length - 1, n):
        window = series[i - length + 1: i + 1][::-1]
        if mode == "AVG":
            result[i] = np.sum(window * weights)
        elif mode == "MEDIAN":
            sorted_idx = np.argsort(window)
            cum_w = np.cumsum(weights[sorted_idx])
            mid = cum_w[-1] / 2.0
            idx = np.searchsorted(cum_w, mid)
            result[i] = window[sorted_idx[idx]]
        else:  # MODE
            nbins = max(10, length // 2)
            hist, edges = np.histogram(window, bins=nbins, weights=weights)
            peak = np.argmax(hist)
            result[i] = (edges[peak] + edges[peak + 1]) / 2.0
    return result

# ─── TECHNICAL INDICATORS ─────────────────────────────────────
def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Average True Range."""
    n = len(close)
    tr = np.zeros(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
    return sma(tr, period)

def sma(series: np.ndarray, period: int) -> np.ndarray:
    """Simple Moving Average."""
    result = np.full(len(series), np.nan)
    for i in range(period - 1, len(series)):
        result[i] = np.mean(series[i - period + 1: i + 1])
    return result

def ema(series: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average."""
    result = np.full(len(series), np.nan)
    k = 2.0 / (period + 1)
    result[period - 1] = np.mean(series[:period])
    for i in range(period, len(series)):
        result[i] = series[i] * k + result[i-1] * (1 - k)
    return result

def stdev(series: np.ndarray, period: int) -> np.ndarray:
    """Rolling standard deviation."""
    result = np.full(len(series), np.nan)
    for i in range(period - 1, len(series)):
        result[i] = np.std(series[i - period + 1: i + 1], ddof=0)
    return result

# ─── BACKTEST ENGINE ──────────────────────────────────────────
class GaussianBacktest:
    def __init__(self, length=30, mode="AVG", distance=1.5, atr_len=14,
                 sl_buf_pct=0.002, max_sl_pct=0.06, tp_mult=(1.5, 3.0, 4.5),
                 tp_pcts=(0.30, 0.30, 0.20, 0.20), fee_pct=0.00075):
        self.length = length
        self.mode = mode
        self.distance = distance
        self.atr_len = atr_len
        self.sl_buf_pct = sl_buf_pct
        self.max_sl_pct = max_sl_pct
        self.tp_mult = tp_mult
        self.tp_pcts = tp_pcts  # tp1, tp2, tp3, runner
        self.fee_pct = fee_pct  # per side

    def run(self, df: pd.DataFrame) -> dict:
        """
        Run backtest on OHLCV DataFrame.
        Returns dict with stats + trade list + equity curve.
        """
        o = df['open'].values.astype(float)
        h = df['high'].values.astype(float)
        l = df['low'].values.astype(float)
        c = df['close'].values.astype(float)
        n = len(c)

        # Gaussian filter
        avg = gaussian_filter(c, self.length, self.mode)
        sd = stdev(c, self.length)
        atr_vals = atr(h, l, c, self.atr_len)

        # Bands
        band_hi = np.full(n, np.nan)
        band_lo = np.full(n, np.nan)
        for i in range(n):
            if not np.isnan(avg[i]) and not np.isnan(sd[i]):
                spread = sd[i] * self.distance
                band_hi[i] = avg[i] + spread
                band_lo[i] = avg[i] - spread

        # Score & trend
        score = np.full(n, np.nan)
        trend = np.zeros(n, dtype=bool)  # True = bull
        trend_line = np.full(n, np.nan)

        for i in range(1, n):
            if np.isnan(avg[i]) or np.isnan(band_hi[i]):
                continue
            rng = band_hi[i] - band_lo[i]
            if rng > 0:
                score[i] = (c[i] - band_lo[i]) / rng
            else:
                score[i] = 0.5

            if score[i] > 0.5:
                trend[i] = True
            elif score[i] < 0.5:
                trend[i] = False
            else:
                trend[i] = trend[i-1]

            trend_line[i] = band_lo[i] if trend[i] else band_hi[i]

        # Detect flips
        just_flipped_bull = np.zeros(n, dtype=bool)
        just_flipped_bear = np.zeros(n, dtype=bool)
        for i in range(1, n):
            if trend[i] and not trend[i-1]:
                just_flipped_bull[i] = True
            elif not trend[i] and trend[i-1]:
                just_flipped_bear[i] = True

        # ── SIMULATE TRADES ──
        trades = []
        equity = [10000.0]
        capital = 10000.0

        in_trade = False
        entry_price = 0.0
        sl_price = 0.0
        trail_sl = 0.0
        is_bull = True
        tp1_price = tp2_price = tp3_price = 0.0
        tp1_hit = tp2_hit = tp3_hit = False
        be_price = 0.0
        entry_bar = 0
        entered_this_trend = False
        prev_trend = False

        for i in range(self.length + 1, n):
            if np.isnan(avg[i]) or np.isnan(atr_vals[i]):
                equity.append(capital)
                continue

            # Reset entered_this_trend on trend flip
            if trend[i] != prev_trend:
                entered_this_trend = False
            prev_trend = trend[i]

            # ── CHECK EXIT ──
            if in_trade:
                # Trailing SL update
                if is_bull:
                    new_sl = trend_line[i] * (1 - self.sl_buf_pct) if not np.isnan(trend_line[i]) else trail_sl
                    trail_sl = max(trail_sl, new_sl)
                else:
                    new_sl = trend_line[i] * (1 + self.sl_buf_pct) if not np.isnan(trend_line[i]) else trail_sl
                    trail_sl = min(trail_sl, new_sl)

                # TP detection
                if not tp1_hit:
                    if (is_bull and h[i] >= tp1_price) or (not is_bull and l[i] <= tp1_price):
                        tp1_hit = True
                        # Move SL to BE
                        if is_bull:
                            trail_sl = max(trail_sl, be_price)
                        else:
                            trail_sl = min(trail_sl, be_price)

                if tp1_hit and not tp2_hit:
                    if (is_bull and h[i] >= tp2_price) or (not is_bull and l[i] <= tp2_price):
                        tp2_hit = True

                if tp2_hit and not tp3_hit:
                    if (is_bull and h[i] >= tp3_price) or (not is_bull and l[i] <= tp3_price):
                        tp3_hit = True

                # SL hit check
                sl_hit = False
                if is_bull and l[i] <= trail_sl:
                    sl_hit = True
                    exit_price = min(c[i], trail_sl)
                elif not is_bull and h[i] >= trail_sl:
                    sl_hit = True
                    exit_price = max(c[i], trail_sl)

                if sl_hit:
                    # Calculate PnL with partial TPs
                    pnl = self._calc_pnl(is_bull, entry_price, exit_price,
                                         tp1_hit, tp2_hit, tp3_hit,
                                         tp1_price, tp2_price, tp3_price)
                    capital *= (1 + pnl)
                    trades.append({
                        'entry_bar': entry_bar, 'exit_bar': i,
                        'entry_date': str(df.index[entry_bar]) if hasattr(df.index, '__getitem__') else entry_bar,
                        'exit_date': str(df.index[i]) if hasattr(df.index, '__getitem__') else i,
                        'side': 'LONG' if is_bull else 'SHORT',
                        'entry': entry_price, 'exit': exit_price,
                        'pnl_pct': round(pnl * 100, 2),
                        'tp1_hit': tp1_hit, 'tp2_hit': tp2_hit, 'tp3_hit': tp3_hit,
                        'bars': i - entry_bar
                    })
                    in_trade = False

            # ── CHECK ENTRY ──
            if not in_trade and not entered_this_trend:
                signal_long = just_flipped_bull[i]
                signal_short = just_flipped_bear[i]

                if signal_long or signal_short:
                    is_bull = signal_long
                    entry_price = c[i]
                    risk = atr_vals[i]

                    if is_bull:
                        sl_raw = band_lo[i] * (1 - self.sl_buf_pct) if not np.isnan(band_lo[i]) else entry_price - risk
                        sl_price = max(sl_raw, entry_price * (1 - self.max_sl_pct))
                        tp1_price = entry_price + risk * self.tp_mult[0]
                        tp2_price = entry_price + risk * self.tp_mult[1]
                        tp3_price = entry_price + risk * self.tp_mult[2]
                    else:
                        sl_raw = band_hi[i] * (1 + self.sl_buf_pct) if not np.isnan(band_hi[i]) else entry_price + risk
                        sl_price = min(sl_raw, entry_price * (1 + self.max_sl_pct))
                        tp1_price = entry_price - risk * self.tp_mult[0]
                        tp2_price = entry_price - risk * self.tp_mult[1]
                        tp3_price = entry_price - risk * self.tp_mult[2]

                    trail_sl = sl_price
                    be_price = entry_price * (1 + self.fee_pct * 2) if is_bull else entry_price * (1 - self.fee_pct * 2)
                    tp1_hit = tp2_hit = tp3_hit = False
                    entry_bar = i
                    in_trade = True
                    entered_this_trend = True

            equity.append(capital)

        # ── STATS ──
        if len(trades) == 0:
            return self._empty_result(equity, df)

        wins = [t for t in trades if t['pnl_pct'] > 0]
        losses = [t for t in trades if t['pnl_pct'] <= 0]
        pnls = [t['pnl_pct'] for t in trades]
        tp1_count = sum(1 for t in trades if t['tp1_hit'])
        tp2_count = sum(1 for t in trades if t['tp2_hit'])
        tp3_count = sum(1 for t in trades if t['tp3_hit'])

        # Max drawdown
        peak = equity[0]
        max_dd = 0
        for e in equity:
            peak = max(peak, e)
            dd = (peak - e) / peak * 100 if peak > 0 else 0
            max_dd = max(max_dd, dd)

        return {
            'total_trades': len(trades),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(len(wins) / len(trades) * 100, 1) if trades else 0,
            'total_pnl': round((capital / 10000 - 1) * 100, 2),
            'avg_pnl': round(np.mean(pnls), 2),
            'best_trade': round(max(pnls), 2),
            'worst_trade': round(min(pnls), 2),
            'avg_win': round(np.mean([t['pnl_pct'] for t in wins]), 2) if wins else 0,
            'avg_loss': round(np.mean([t['pnl_pct'] for t in losses]), 2) if losses else 0,
            'tp1_hits': tp1_count,
            'tp2_hits': tp2_count,
            'tp3_hits': tp3_count,
            'max_drawdown': round(max_dd, 2),
            'profit_factor': round(abs(sum(t['pnl_pct'] for t in wins)) / abs(sum(t['pnl_pct'] for t in losses)), 2) if losses and sum(t['pnl_pct'] for t in losses) != 0 else 999,
            'final_equity': round(capital, 2),
            'trades': trades,
            'equity': equity[::max(1, len(equity)//500)]  # downsample for chart
        }

    def _calc_pnl(self, is_bull, entry, exit_price, tp1, tp2, tp3, tp1p, tp2p, tp3p):
        """Calculate PnL with partial TP exits."""
        total_pnl = 0.0
        remaining = 1.0

        if tp1:
            partial = self.tp_pcts[0]
            pnl = (tp1p - entry) / entry if is_bull else (entry - tp1p) / entry
            total_pnl += pnl * partial
            remaining -= partial

        if tp2:
            partial = self.tp_pcts[1]
            pnl = (tp2p - entry) / entry if is_bull else (entry - tp2p) / entry
            total_pnl += pnl * partial
            remaining -= partial

        if tp3:
            partial = self.tp_pcts[2]
            pnl = (tp3p - entry) / entry if is_bull else (entry - tp3p) / entry
            total_pnl += pnl * partial
            remaining -= partial

        # Runner / remaining at exit
        pnl_exit = (exit_price - entry) / entry if is_bull else (entry - exit_price) / entry
        total_pnl += pnl_exit * remaining

        # Fees (both sides)
        total_pnl -= self.fee_pct * 2

        return total_pnl

    def _empty_result(self, equity, df):
        return {
            'total_trades': 0, 'wins': 0, 'losses': 0, 'win_rate': 0,
            'total_pnl': 0, 'avg_pnl': 0, 'best_trade': 0, 'worst_trade': 0,
            'avg_win': 0, 'avg_loss': 0, 'tp1_hits': 0, 'tp2_hits': 0, 'tp3_hits': 0,
            'max_drawdown': 0, 'profit_factor': 0, 'final_equity': 10000,
            'trades': [], 'equity': equity[::max(1, len(equity)//500)]
        }
