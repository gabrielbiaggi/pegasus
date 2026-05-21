from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import ta

from logger import logger

try:
    from scipy.integrate import trapezoid as integrate_trapezoid
except ImportError:  # pragma: no cover - numpy keeps the bot usable without scipy installed.
    integrate_trapezoid = np.trapezoid if hasattr(np, "trapezoid") else np.trapz

try:
    from hmmlearn import hmm as _hmm_lib
    _HMM_AVAILABLE = True
except ImportError:  # pragma: no cover
    _HMM_AVAILABLE = False
    _hmm_lib = None  # type: ignore[assignment]

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    _SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb
    _XGB_AVAILABLE = True
except ImportError:  # pragma: no cover
    _XGB_AVAILABLE = False
    xgb = None  # type: ignore[assignment]


@dataclass(frozen=True)
class AccumulatorStrategyConfig:
    min_score: int = 17
    bb_window: int = 20
    bb_std_dev: float = 2.0
    max_bb_width_percent: float = 0.08
    atr_window: int = 20
    max_tick_atr_percent: float = 0.015
    recent_window: int = 5
    max_recent_move_percent: float = 0.05
    squeeze_weight: int = 4
    atr_weight: int = 4
    stability_weight: int = 2
    hawkes_alpha: float = 1.0
    hawkes_beta: float = 0.85
    hawkes_jump_atr_multiplier: float = 1.5
    max_hawkes_intensity: float = 0.2
    imbalance_window: int = 10
    max_abs_tick_imbalance: int = 2
    hurst_window: int = 30
    max_hurst_exponent: float = 0.45
    derivative_window: int = 20
    max_velocity_zscore: float = 2.0
    max_acceleration_zscore: float = 2.0
    integral_window: int = 20
    max_pmi_distance_percent: float = 0.005
    markov_window: int = 50
    max_markov_continuation_prob: float = 0.45
    shannon_entropy_window: int = 30
    min_shannon_entropy: float = 0.80
    kalman_q: float = 1e-5
    kalman_r: float = 1e-2
    max_kalman_residual_zscore: float = 2.0
    # Calm ACCU voting threshold (out of 27 max: 10 primary + 10 quant + 7 advanced)
    calm_min_score: int = 15
    # HMM regime filter
    hmm_window: int = 200
    hmm_n_states: int = 2
    hmm_high_variance_blocks: bool = True
    # Ensemble scoring (XGBoost P(LOSS) gate)
    use_ensemble: bool = False
    ensemble_min_prob: float = 0.294
    # RF directional indicator windows
    ols_window: int = 20
    momentum_window: int = 5
    autocorr_window: int = 30
    skewness_window: int = 20
    fft_window: int = 64

    @property
    def minimum_ticks(self) -> int:
        return max(
            self.bb_window + 2,
            self.atr_window + 2,
            self.recent_window + 2,
            self.imbalance_window + 2,
            self.hurst_window + 2,
            self.derivative_window + 2,
            self.integral_window + 2,
            self.markov_window + 2,
            self.shannon_entropy_window + 2,
            self.fft_window + 2,
        )


def calculate_tick_indicators(ticks: list[dict], config: AccumulatorStrategyConfig | None = None) -> pd.DataFrame:
    config = config or AccumulatorStrategyConfig()
    df = pd.DataFrame(ticks)
    if df.empty:
        return df

    required = {"epoch", "quote"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Ticks sem campos obrigatorios: {sorted(missing)}")

    df = df.drop_duplicates(subset=["epoch"], keep="last").sort_values("epoch").reset_index(drop=True)
    df["epoch"] = df["epoch"].astype(int)
    df["close"] = pd.to_numeric(df["quote"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)

    if len(df) < config.minimum_ticks:
        return df

    bb = ta.volatility.BollingerBands(df["close"], window=config.bb_window, window_dev=config.bb_std_dev)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()
    df["bb_width_percent"] = (df["bb_upper"] - df["bb_lower"]) / df["close"] * 100

    df["abs_tick_move_percent"] = df["close"].pct_change().abs() * 100
    df["tick_atr_percent"] = df["abs_tick_move_percent"].rolling(config.atr_window).mean()
    df["recent_move_percent"] = df["close"].pct_change(config.recent_window).abs() * 100
    df["tick_imbalance"] = _calculate_tick_imbalance(df["close"], config.imbalance_window)
    df["hawkes_intensity"] = _calculate_hawkes_intensity(df, config)
    df["hurst_exponent"] = (
        df["close"].rolling(config.hurst_window).apply(_hurst_exponent_from_prices, raw=True)
    )
    df["price_velocity"] = _finite_velocity(df["close"])
    df["price_acceleration"] = _finite_acceleration(df["close"])
    df["velocity_zscore"] = _rolling_abs_zscore(df["price_velocity"], config.derivative_window)
    df["acceleration_zscore"] = _rolling_abs_zscore(df["price_acceleration"], config.derivative_window)
    df["integral_mean_price"] = (
        df["close"].rolling(config.integral_window).apply(_integral_mean_price, raw=True)
    )
    df["pmi_distance_percent"] = (
        (df["close"] - df["integral_mean_price"]).abs() / df["close"] * 100
    )
    markov = _markov_transition_probabilities(df["close"], config.markov_window)
    df["markov_p_up_given_up"] = markov["markov_p_up_given_up"]
    df["markov_p_down_given_down"] = markov["markov_p_down_given_down"]
    df["shannon_entropy"] = _shannon_entropy(df["close"], config.shannon_entropy_window)
    kalman = _kalman_filter_metrics(df["close"], config.kalman_q, config.kalman_r)
    df["kalman_estimate"] = kalman["kalman_estimate"]
    df["kalman_covariance"] = kalman["kalman_covariance"]
    df["kalman_residual"] = kalman["kalman_residual"]
    df["kalman_residual_zscore"] = _rolling_abs_zscore(df["kalman_residual"], config.derivative_window)

    # --- RF directional indicators (signed, direction-predictive) ---
    df["ols_slope"] = df["close"].rolling(config.ols_window).apply(_ols_slope, raw=True)
    df["price_momentum"] = df["close"].pct_change(config.momentum_window) * 100
    _ema_fast = ta.trend.EMAIndicator(df["close"], window=5).ema_indicator()
    _ema_slow = ta.trend.EMAIndicator(df["close"], window=20).ema_indicator()
    df["ema_diff"] = _ema_fast - _ema_slow
    df["run_length"] = _run_length(df["close"])
    _markov2 = _second_order_markov(df["close"], config.markov_window)
    df["markov2_puu"] = _markov2["puu"]
    df["markov2_pdd"] = _markov2["pdd"]
    df["return_autocorr_lag1"] = _rolling_return_autocorr(df["close"], lag=1, window=config.autocorr_window)
    df["return_skewness"] = df["close"].diff().rolling(config.skewness_window).skew()
    df["fft_dominant_period"] = df["close"].rolling(config.fft_window).apply(_fft_dominant_period, raw=True)

    # --- Advanced calculus & statistical indicators (universal) ---
    df["price_jerk"] = _finite_jerk(df["close"])
    df["jerk_zscore"] = _rolling_abs_zscore(df["price_jerk"], config.derivative_window)
    df["price_curvature"] = _price_curvature(df["close"])
    df["curvature_zscore"] = _rolling_abs_zscore(df["price_curvature"], config.derivative_window)
    df["integral_momentum_div"] = _integral_momentum_divergence(
        df["close"], _ema_slow, config.integral_window,
    )
    df["derivative_energy"] = _derivative_energy(df["price_velocity"], config.derivative_window)
    df["trend_exhaustion"] = _trend_exhaustion(df["close"], config.integral_window)
    df["return_zscore"] = _rolling_return_zscore(df["close"], config.derivative_window)
    df["lyapunov_exponent"] = df["close"].rolling(
        config.hurst_window
    ).apply(_rolling_lyapunov, raw=True)

    # --- Advanced mathematical intelligence filters ---
    df["bayesian_prob_up"] = _bayesian_prob_up(df["close"], config.shannon_entropy_window)
    df["renyi_entropy"] = _renyi_entropy(df["close"], config.shannon_entropy_window, alpha=0.5)
    df["fisher_information"] = _fisher_information(df["close"], config.derivative_window)
    df["wavelet_energy_ratio"] = _wavelet_energy_ratio(df["close"], window=32)
    df["cusum_score"] = _cusum_score(df["close"], config.shannon_entropy_window)
    df["tail_dependence"] = _copula_tail_dependence(df["close"], config.markov_window)
    df["mi_flow"] = _mutual_information_flow(df["close"], config.shannon_entropy_window, lag=1)

    return df


def generate_accumulator_signal(
    df: pd.DataFrame,
    config: AccumulatorStrategyConfig | None = None,
    ensemble_scorer: EnsembleScorer | None = None,
) -> tuple[Optional[str], int, Optional[float]]:
    config = config or AccumulatorStrategyConfig()
    if len(df) < config.minimum_ticks:
        return None, 0, None

    last = df.iloc[-1]
    score = score_accumulator_row(last, config)

    if score == 0 and last[["bb_width_percent", "tick_atr_percent", "recent_move_percent"]].isna().any():
        return None, 0, None

    # --- HMM regime gate: block trades during high-variance regime ---
    if config.hmm_high_variance_blocks and _HMM_AVAILABLE:
        if hmm_regime_is_high_variance(df["close"], n_states=config.hmm_n_states, window=config.hmm_window):
            logger.info("HMM: regime de alta variancia detectado. Trade bloqueado.")
            return None, 0, None

    # Log quant filter reasons for diagnostics (no longer a hard gate — score handles it)
    _, reason = accumulator_quant_filters_pass(last, config)

    logger.debug(
        (
            "ACCU score=%s | BBWidth%%=%.4f | TickATR%%=%.4f | RecentMove%%=%.4f | "
            "H=%.4f | imbalance=%s | hawkes=%.4f | vel_z=%.4f | accel_z=%.4f | "
            "pmi_dist%%=%.5f | markovUU=%.4f | markovDD=%.4f | entropy=%.4f | kalman_z=%.4f"
        ),
        score,
        last["bb_width_percent"],
        last["tick_atr_percent"],
        last["recent_move_percent"],
        last.get("hurst_exponent", float("nan")),
        last.get("tick_imbalance", float("nan")),
        last.get("hawkes_intensity", float("nan")),
        last.get("velocity_zscore", float("nan")),
        last.get("acceleration_zscore", float("nan")),
        last.get("pmi_distance_percent", float("nan")),
        last.get("markov_p_up_given_up", float("nan")),
        last.get("markov_p_down_given_down", float("nan")),
        last.get("shannon_entropy", float("nan")),
        last.get("kalman_residual_zscore", float("nan")),
    )

    if score < config.min_score:
        return None, 0, None

    if reason != "ok":
        logger.info("Score %d >= %d com quant parcial: %s", score, config.min_score, reason)

    # --- Ensemble gate (optional): replace boolean AND with probabilistic score ---
    p_loss: Optional[float] = None
    if config.use_ensemble and ensemble_scorer is not None:
        p_loss = ensemble_scorer.predict_loss_probability(last)
        logger.info("EnsembleScorer P(LOSS)=%.4f limiar=%.4f", p_loss, config.ensemble_min_prob)
        if p_loss >= config.ensemble_min_prob:
            logger.warning("⛔ Sinal cancelado pela IA! P(LOSS)=%.4f >= %.4f", p_loss, config.ensemble_min_prob)
            return None, 0, None
    return "ACCU", score, p_loss


def score_accumulator_row(row: pd.Series, config: AccumulatorStrategyConfig | None = None) -> int:
    config = config or AccumulatorStrategyConfig()
    required = ["bb_width_percent", "tick_atr_percent", "recent_move_percent"]
    if row[required].isna().any():
        return 0

    score = 0
    # Primary 3 indicators (max 10 pts: squeeze=4, atr=4, stability=2)
    if row["bb_width_percent"] <= config.max_bb_width_percent:
        score += config.squeeze_weight
    if row["tick_atr_percent"] <= config.max_tick_atr_percent:
        score += config.atr_weight
    if row["recent_move_percent"] <= config.max_recent_move_percent:
        score += config.stability_weight
    # Quant indicators: 1 pt each (max 10 pts) — todos os 10 contribuem ao score
    h = row.get("hurst_exponent")
    if not pd.isna(h) and float(h) < config.max_hurst_exponent:
        score += 1
    ti = row.get("tick_imbalance")
    if not pd.isna(ti) and abs(int(ti)) < config.max_abs_tick_imbalance:
        score += 1
    hi = row.get("hawkes_intensity")
    if not pd.isna(hi) and float(hi) <= config.max_hawkes_intensity:
        score += 1
    vz = row.get("velocity_zscore")
    if not pd.isna(vz) and abs(float(vz)) <= config.max_velocity_zscore:
        score += 1
    az = row.get("acceleration_zscore")
    if not pd.isna(az) and abs(float(az)) <= config.max_acceleration_zscore:
        score += 1
    pd_ = row.get("pmi_distance_percent")
    if not pd.isna(pd_) and float(pd_) <= config.max_pmi_distance_percent:
        score += 1
    muu = row.get("markov_p_up_given_up")
    if not pd.isna(muu) and float(muu) < config.max_markov_continuation_prob:
        score += 1
    mdd = row.get("markov_p_down_given_down")
    if not pd.isna(mdd) and float(mdd) < config.max_markov_continuation_prob:
        score += 1
    se = row.get("shannon_entropy")
    if not pd.isna(se) and float(se) >= config.min_shannon_entropy:
        score += 1
    kz = row.get("kalman_residual_zscore")
    if not pd.isna(kz) and abs(float(kz)) <= config.max_kalman_residual_zscore:
        score += 1
    return score


def accumulator_quant_filters_pass(
    row: pd.Series,
    config: AccumulatorStrategyConfig | None = None,
) -> tuple[bool, str]:
    config = config or AccumulatorStrategyConfig()
    checks = {
        "hurst_exponent": row.get("hurst_exponent"),
        "tick_imbalance": row.get("tick_imbalance"),
        "hawkes_intensity": row.get("hawkes_intensity"),
        "velocity_zscore": row.get("velocity_zscore"),
        "acceleration_zscore": row.get("acceleration_zscore"),
        "pmi_distance_percent": row.get("pmi_distance_percent"),
        "markov_p_up_given_up": row.get("markov_p_up_given_up"),
        "markov_p_down_given_down": row.get("markov_p_down_given_down"),
        "shannon_entropy": row.get("shannon_entropy"),
        "kalman_residual_zscore": row.get("kalman_residual_zscore"),
    }
    missing = [name for name, value in checks.items() if pd.isna(value)]
    if missing:
        return False, f"metricas ausentes: {', '.join(missing)}"

    if float(checks["hurst_exponent"]) >= config.max_hurst_exponent:
        return False, "hurst acima do limite"
    if abs(int(checks["tick_imbalance"])) >= config.max_abs_tick_imbalance:
        return False, "tick imbalance fora da lateralizacao"
    if float(checks["hawkes_intensity"]) > config.max_hawkes_intensity:
        return False, "intensidade Hawkes ativa"
    if abs(float(checks["velocity_zscore"])) > config.max_velocity_zscore:
        return False, "velocidade do preco acima do limite"
    if abs(float(checks["acceleration_zscore"])) > config.max_acceleration_zscore:
        return False, "aceleracao do preco acima do limite"
    if float(checks["pmi_distance_percent"]) > config.max_pmi_distance_percent:
        return False, "preco distante do centro de massa integral"
    if float(checks["markov_p_up_given_up"]) >= config.max_markov_continuation_prob:
        return False, "markov continuidade de alta acima do limite"
    if float(checks["markov_p_down_given_down"]) >= config.max_markov_continuation_prob:
        return False, "markov continuidade de queda acima do limite"
    if float(checks["shannon_entropy"]) < config.min_shannon_entropy:
        return False, "entropia de Shannon abaixo do limite"
    if abs(float(checks["kalman_residual_zscore"])) > config.max_kalman_residual_zscore:
        return False, "residual de Kalman acima do limite"
    return True, "ok"


def _calculate_tick_imbalance(close: pd.Series, window: int) -> pd.Series:
    signs = np.sign(close.diff()).fillna(0.0)
    return signs.rolling(window).sum()


def _calculate_hawkes_intensity(df: pd.DataFrame, config: AccumulatorStrategyConfig) -> pd.Series:
    close = df["close"].to_numpy(dtype=float)
    atr_price = (df["tick_atr_percent"].fillna(0.0).to_numpy(dtype=float) / 100) * close
    threshold = atr_price * config.hawkes_jump_atr_multiplier
    intensities: list[float] = []
    intensity = 0.0
    decay = float(np.exp(-config.hawkes_beta))

    for index, price in enumerate(close):
        if index == 0:
            intensities.append(intensity)
            continue
        intensity *= decay
        delta_price = abs(price - close[index - 1])
        if threshold[index] > 0 and delta_price > threshold[index]:
            intensity += config.hawkes_alpha
        intensities.append(intensity)

    return pd.Series(intensities, index=df.index)


def _hurst_exponent_from_prices(prices: np.ndarray) -> float:
    prices = np.asarray(prices, dtype=float)
    if len(prices) < 4 or np.any(prices <= 0):
        return np.nan
    returns = np.diff(np.log(prices))
    std = np.std(returns, ddof=1)
    if std <= 0 or np.isnan(std):
        return 0.0
    profile = np.cumsum(returns - np.mean(returns))
    rescaled_range = (np.max(profile) - np.min(profile)) / std
    if rescaled_range <= 0:
        return 0.0
    return float(np.log(rescaled_range) / np.log(len(returns)))


def _finite_velocity(close: pd.Series) -> pd.Series:
    # The live tick has no P[t+1], so the executable signal must use the
    # causal finite difference instead of a lookahead central value.
    return close.diff()


def _finite_acceleration(close: pd.Series) -> pd.Series:
    return close - (2 * close.shift(1)) + close.shift(2)


def _rolling_abs_zscore(series: pd.Series, window: int) -> pd.Series:
    magnitude = series.abs()
    rolling_mean = magnitude.rolling(window).mean()
    rolling_std = magnitude.rolling(window).std(ddof=0)
    zscore = (magnitude - rolling_mean) / rolling_std.replace(0, np.nan)
    return zscore.fillna(0.0)


def _integral_mean_price(prices: np.ndarray) -> float:
    prices = np.asarray(prices, dtype=float)
    if len(prices) == 0:
        return np.nan
    duration = max(len(prices) - 1, 1)
    return float(integrate_trapezoid(prices, dx=1.0) / duration)


def _markov_transition_probabilities(close: pd.Series, window: int = 50) -> pd.DataFrame:
    states = np.sign(close.diff()).fillna(0.0).to_numpy(dtype=int)
    up_given_up: list[float] = [np.nan] * len(states)
    down_given_down: list[float] = [np.nan] * len(states)

    for end in range(window, len(states)):
        window_states = states[end - window + 1 : end + 1]
        previous = window_states[:-1]
        current = window_states[1:]

        up_mask = previous == 1
        down_mask = previous == -1
        up_given_up[end] = float(np.mean(current[up_mask] == 1)) if np.any(up_mask) else 0.0
        down_given_down[end] = float(np.mean(current[down_mask] == -1)) if np.any(down_mask) else 0.0

    return pd.DataFrame(
        {
            "markov_p_up_given_up": up_given_up,
            "markov_p_down_given_down": down_given_down,
        },
        index=close.index,
    )


def _shannon_entropy(close: pd.Series, window: int = 30) -> pd.Series:
    returns = close.diff().fillna(0.0)
    scale = returns.abs().rolling(window).median().replace(0, np.nan)
    normalized = returns / scale
    categories = pd.cut(
        normalized,
        bins=[-np.inf, -1.0, -0.01, 0.01, 1.0, np.inf],
        labels=False,
        include_lowest=True,
    ).fillna(2)

    return categories.rolling(window).apply(_normalized_entropy, raw=True)


def _normalized_entropy(categories: np.ndarray) -> float:
    values = np.asarray(categories, dtype=int)
    counts = np.bincount(values, minlength=5).astype(float)
    probabilities = counts[counts > 0] / counts.sum()
    if probabilities.size == 0:
        return np.nan
    entropy = -np.sum(probabilities * np.log2(probabilities))
    return float(entropy / np.log2(5))


def _kalman_filter_metrics(close: pd.Series, process_variance: float, measurement_variance: float) -> pd.DataFrame:
    prices = close.to_numpy(dtype=float)
    estimates: list[float] = []
    covariances: list[float] = []
    residuals: list[float] = []

    estimate = prices[0] if len(prices) else np.nan
    covariance = 1.0
    process_variance = max(float(process_variance), 1e-12)
    measurement_variance = max(float(measurement_variance), 1e-12)

    for price in prices:
        predicted_estimate = estimate
        predicted_covariance = covariance + process_variance
        gain = predicted_covariance / (predicted_covariance + measurement_variance)
        residual = price - predicted_estimate
        estimate = predicted_estimate + gain * residual
        covariance = (1 - gain) * predicted_covariance

        estimates.append(float(estimate))
        covariances.append(float(covariance))
        residuals.append(float(residual))

    return pd.DataFrame(
        {
            "kalman_estimate": estimates,
            "kalman_covariance": covariances,
            "kalman_residual": residuals,
        },
        index=close.index,
    )


# ---------------------------------------------------------------------------
# Advanced Calculus & Statistical Helpers (universal)
# ---------------------------------------------------------------------------

def _finite_jerk(close: pd.Series) -> pd.Series:
    """3rd derivative of price — rate of change of acceleration.

    Jerk detects regime transitions: a spike in jerk means acceleration itself
    is changing rapidly, signaling the START or END of a move, not the move itself.
    J[t] = P[t] - 3*P[t-1] + 3*P[t-2] - P[t-3]  (forward finite diff, 3rd order)
    """
    return close - 3 * close.shift(1) + 3 * close.shift(2) - close.shift(3)


def _price_curvature(close: pd.Series) -> pd.Series:
    """Discrete curvature κ of the price curve.

    κ = |v'| / (1 + v²)^(3/2)  where v = dP/dt, v' = d²P/dt²

    High curvature → sharp bend (inflection point / reversal).
    Low curvature → straight move or flat.
    Returns absolute curvature (unsigned — direction comes from other indicators).
    """
    v = close.diff()                # velocity
    a = v.diff()                    # acceleration
    denom = (1.0 + v ** 2) ** 1.5
    # Protect against zero denominator (perfectly flat)
    denom = denom.replace(0, np.nan)
    return (a.abs() / denom).fillna(0.0)


def _integral_momentum_divergence(
    close: pd.Series,
    ema_slow: pd.Series,
    window: int,
) -> pd.Series:
    """Signed integral divergence: ∫(price - EMA_slow) dt over rolling window.

    Positive → price has been persistently above the slow EMA (bullish pressure).
    Negative → persistently below (bearish pressure).
    Near zero → oscillating around EMA (no clear trend).

    This is analogous to the MACD histogram area — accumulated divergence energy.
    """
    diff = close - ema_slow
    return diff.rolling(window).apply(
        lambda x: float(integrate_trapezoid(x, dx=1.0)), raw=True,
    ).fillna(0.0)


def _derivative_energy(velocity: pd.Series, window: int) -> pd.Series:
    """Rolling kinetic energy proxy: ∫v² dt over window (trapezoidal rule).

    Analogous to kinetic energy ½mv². High energy = large sustained moves.
    Low energy = calm market. This is scale-free (no price level bias).
    """
    v2 = velocity ** 2
    return v2.rolling(window).apply(
        lambda x: float(integrate_trapezoid(x, dx=1.0)), raw=True,
    ).fillna(0.0)


def _trend_exhaustion(close: pd.Series, window: int) -> pd.Series:
    """Signed trend exhaustion: ∫(price - SMA) / SMA dt.

    Measures accumulated percentage deviation from rolling mean.
    Large positive → overbought (trend exhaustion, reversal risk).
    Large negative → oversold.
    Near zero → fair value zone.
    """
    sma = close.rolling(window).mean()
    pct_dev = (close - sma) / sma.replace(0, np.nan)
    pct_dev = pct_dev.fillna(0.0)
    return pct_dev.rolling(window).apply(
        lambda x: float(integrate_trapezoid(x, dx=1.0)), raw=True,
    ).fillna(0.0)


def _rolling_return_zscore(close: pd.Series, window: int) -> pd.Series:
    """Z-score of latest return vs rolling distribution.

    Detects fat-tail / extreme moves. |z| > 3 = outlier.
    Signed: positive = unusually large up-move, negative = large down-move.
    """
    ret = close.diff()
    mu = ret.rolling(window).mean()
    sigma = ret.rolling(window).std(ddof=0).replace(0, np.nan)
    return ((ret - mu) / sigma).fillna(0.0)


def _rolling_lyapunov(prices: np.ndarray) -> float:
    """Approximation of the maximal Lyapunov exponent from price series.

    Positive λ → chaotic / sensitive to perturbation → hard to predict.
    Negative or near-zero λ → more predictable dynamics.

    Uses the simple Rosenstein method approximation: mean log divergence
    of nearby trajectories in embedding space (delay=1, dim=2).
    """
    n = len(prices)
    if n < 10:
        return np.nan
    returns = np.diff(prices)
    if len(returns) < 8:
        return np.nan
    # Embedding: (r[t], r[t+1]) pairs
    m = len(returns) - 1
    if m < 4:
        return np.nan
    phase = np.column_stack([returns[:-1], returns[1:]])
    # For each point, find nearest neighbor (not itself)
    log_divs = []
    for i in range(m):
        dists = np.sqrt(np.sum((phase - phase[i]) ** 2, axis=1))
        dists[i] = np.inf  # exclude self
        j = int(np.argmin(dists))
        d0 = dists[j]
        if d0 < 1e-15:
            continue
        # Check divergence after 1 step
        i2, j2 = min(i + 1, m - 1), min(j + 1, m - 1)
        d1 = np.sqrt(np.sum((phase[i2] - phase[j2]) ** 2))
        if d1 < 1e-15:
            continue
        log_divs.append(np.log(d1 / d0))
    if not log_divs:
        return 0.0
    return float(np.mean(log_divs))


# ---------------------------------------------------------------------------
# Advanced Mathematical Intelligence Filters
# ---------------------------------------------------------------------------

def _bayesian_prob_up(close: pd.Series, window: int = 30) -> pd.Series:
    """Sequential Bayesian posterior P(next tick up) using Beta conjugate prior.

    Starts with uniform Beta(1,1) prior and updates with each tick direction.
    Returns P(up) from Beta(α, β) posterior: α/(α+β).

    Near 0.5 = uncertain. Near 0/1 = strong directional conviction.
    Uses rolling window so stale evidence decays.
    """
    signs = (close.diff() > 0).astype(float).fillna(0.5)

    def _beta_prob(x: np.ndarray) -> float:
        alpha = 1.0 + float(x.sum())          # prior=1 + observed ups
        beta = 1.0 + float(len(x) - x.sum())  # prior=1 + observed downs
        return alpha / (alpha + beta)

    return signs.rolling(window).apply(_beta_prob, raw=True).fillna(0.5)


def _renyi_entropy(close: pd.Series, window: int = 30, alpha: float = 0.5) -> pd.Series:
    """Rényi entropy of order α on discretized returns.

    Generalization of Shannon entropy. For α < 1, Rényi entropy is MORE
    sensitive to rare events (tail risk). For α > 1, it emphasizes common
    patterns. At α → 1 it converges to Shannon entropy.

    H_α = (1/(1-α)) * log₂(∑ pᵢ^α)

    Returns normalized to [0,1] range (divided by log₂(n_bins)).
    Low value = highly concentrated/predictable. High = dispersed/random.
    """
    returns = close.diff().fillna(0.0)
    scale = returns.abs().rolling(window).median().replace(0, np.nan)
    normalized = returns / scale
    bins = [-np.inf, -2.0, -1.0, -0.01, 0.01, 1.0, 2.0, np.inf]
    n_bins = len(bins) - 1
    categories = pd.cut(normalized, bins=bins, labels=False, include_lowest=True).fillna(n_bins // 2)

    def _renyi(x: np.ndarray) -> float:
        counts = np.bincount(x.astype(int), minlength=n_bins).astype(float)
        probs = counts[counts > 0] / counts.sum()
        if probs.size <= 1:
            return 0.0
        # Rényi formula: H_α = 1/(1-α) * log₂(∑ p^α)
        sum_pa = float(np.sum(probs ** alpha))
        if sum_pa <= 0:
            return 0.0
        h = (1.0 / (1.0 - alpha)) * np.log2(sum_pa)
        return float(h / np.log2(n_bins))  # normalize to [0,1]

    return categories.rolling(window).apply(_renyi, raw=True).fillna(0.5)


def _fisher_information(close: pd.Series, window: int = 30) -> pd.Series:
    """Fisher Information of the return distribution (estimated from sample).

    FI = 1/σ² for Gaussian returns. Higher FI = more concentrated returns
    around the mean → more predictable. Lower FI = wide dispersion → noisy.

    For non-Gaussian, uses the discrete approximation:
    FI ≈ ∑ (p'(x))² / p(x) over histogram bins.

    We use the simpler parametric estimate: FI = n/σ² scaled to [0, ∞).
    """
    returns = close.diff().fillna(0.0)
    var = returns.rolling(window).var(ddof=1).replace(0, np.nan)
    # FI = 1/variance (parametric estimate for location parameter)
    fi = (1.0 / var).fillna(0.0)
    # Scale by window to make it comparable across window sizes
    return fi / window


def _wavelet_energy_ratio(close: pd.Series, window: int = 32) -> pd.Series:
    """Haar wavelet decomposition signal-to-noise energy ratio.

    Decomposes price into approximation (signal) and detail (noise) at
    log₂(window) levels using the Haar wavelet transform (no scipy needed).

    Returns ratio: E_signal / E_total. High = clean trend. Low = noisy.
    Range [0, 1]. Values above 0.7 = strong signal. Below 0.3 = noise-dominated.
    """
    def _haar_snr(prices: np.ndarray) -> float:
        n = len(prices)
        if n < 4:
            return np.nan
        # Pad to nearest power of 2
        p2 = 1
        while p2 < n:
            p2 <<= 1
        x = np.zeros(p2)
        x[:n] = prices - prices.mean()  # center

        detail_energy = 0.0
        length = p2
        while length > 1:
            half = length >> 1
            approx = np.zeros(half)
            detail = np.zeros(half)
            for i in range(half):
                approx[i] = (x[2 * i] + x[2 * i + 1]) / np.sqrt(2)
                detail[i] = (x[2 * i] - x[2 * i + 1]) / np.sqrt(2)
            detail_energy += float(np.sum(detail ** 2))
            x[:half] = approx
            length = half

        total_energy = float(np.sum((prices - prices.mean()) ** 2))
        if total_energy < 1e-15:
            return 1.0  # perfectly flat = pure signal
        signal_energy = max(total_energy - detail_energy, 0.0)
        return signal_energy / total_energy

    return close.rolling(window).apply(_haar_snr, raw=True).fillna(0.5)


def _cusum_score(close: pd.Series, window: int = 30) -> pd.Series:
    """CUSUM (Cumulative Sum) regime change detector score.

    Tracks cumulative deviations of returns from their rolling mean.
    When the CUSUM exceeds a threshold, a regime change is likely occurring.

    Returns absolute CUSUM score normalized by rolling std.
    High values (>3) = regime transition in progress.
    Low values (<1) = stable regime.
    """
    returns = close.diff().fillna(0.0)
    mu = returns.rolling(window).mean()
    sigma = returns.rolling(window).std(ddof=0).replace(0, np.nan)

    # Compute running CUSUM within each rolling window
    def _cusum(x: np.ndarray) -> float:
        n = len(x)
        if n < 4:
            return 0.0
        m = float(np.mean(x))
        s = float(np.std(x, ddof=0))
        if s < 1e-15:
            return 0.0
        # One-sided CUSUM (positive and negative)
        cusum_pos = 0.0
        cusum_neg = 0.0
        max_cusum = 0.0
        for v in x:
            z = (v - m) / s
            cusum_pos = max(0.0, cusum_pos + z)
            cusum_neg = max(0.0, cusum_neg - z)
            max_cusum = max(max_cusum, cusum_pos, cusum_neg)
        return max_cusum

    return returns.rolling(window).apply(_cusum, raw=True).fillna(0.0)


def _copula_tail_dependence(close: pd.Series, window: int = 50) -> pd.Series:
    """Empirical tail dependence coefficient between velocity and acceleration.

    Measures the probability that both velocity AND acceleration are in their
    extreme tails simultaneously. High tail dependence = extreme moves cluster
    (velocity spike + acceleration spike happen together).

    λ_upper = P(V > V_q | A > A_q) where q = 90th percentile.
    Returns average of upper and lower tail dependence.

    High values (>0.3) = dangerous clustering of extremes.
    Low values (<0.1) = extremes are independent → safer market.
    """
    vel = close.diff().fillna(0.0)
    accel = vel.diff().fillna(0.0)

    def _tail_dep(window_data: np.ndarray) -> float:
        # window_data is velocity; we need to reconstruct accel from it
        n = len(window_data)
        if n < 10:
            return 0.0
        v = window_data
        a = np.diff(v)
        if len(a) < 5:
            return 0.0
        v_trunc = v[1:]  # align with a
        # 90th percentile thresholds
        v_q = np.percentile(np.abs(v_trunc), 90)
        a_q = np.percentile(np.abs(a), 90)
        if v_q < 1e-15 or a_q < 1e-15:
            return 0.0
        # Tail events
        v_extreme = np.abs(v_trunc) > v_q
        a_extreme = np.abs(a) > a_q
        # Conditional probability: P(both extreme | at least one extreme)
        both = v_extreme & a_extreme
        either = v_extreme | a_extreme
        n_either = float(np.sum(either))
        if n_either < 1:
            return 0.0
        return float(np.sum(both)) / n_either

    return vel.rolling(window).apply(_tail_dep, raw=True).fillna(0.0)


def _mutual_information_flow(close: pd.Series, window: int = 30, lag: int = 1) -> pd.Series:
    """Mutual information between past returns and future returns.

    MI(past, future) measures how much knowing the past tick direction
    tells us about the next tick direction. Non-zero MI = exploitable
    predictability. Zero MI = IID (pure random walk, no edge).

    Uses discrete MI: MI = ∑∑ p(x,y) * log₂(p(x,y) / (p(x)*p(y)))
    on discretized return categories {down, flat, up}.

    Returns MI in bits. Typical range [0, 0.3] for financial data.
    Values above 0.1 = significant predictability.
    """
    returns = close.diff().fillna(0.0)
    # Discretize: -1=down, 0=flat, 1=up
    categories = np.sign(returns).astype(int) + 1  # map to 0,1,2

    def _mi(x: np.ndarray) -> float:
        n = len(x)
        if n < lag + 4:
            return 0.0
        past = x[:-lag].astype(int)
        future = x[lag:].astype(int)
        m = len(past)
        # Joint and marginal distributions
        joint = np.zeros((3, 3))
        for i in range(m):
            p, f = past[i], future[i]
            if 0 <= p < 3 and 0 <= f < 3:
                joint[p, f] += 1
        if joint.sum() < 1:
            return 0.0
        joint /= joint.sum()
        p_past = joint.sum(axis=1)
        p_future = joint.sum(axis=0)
        mi = 0.0
        for i in range(3):
            for j in range(3):
                if joint[i, j] > 0 and p_past[i] > 0 and p_future[j] > 0:
                    mi += joint[i, j] * np.log2(joint[i, j] / (p_past[i] * p_future[j]))
        return max(0.0, mi)  # MI is non-negative

    return categories.rolling(window).apply(_mi, raw=True).fillna(0.0)


# ---------------------------------------------------------------------------
# RF directional indicator helpers
# ---------------------------------------------------------------------------

def _ols_slope(prices: np.ndarray) -> float:
    """OLS linear regression slope, normalised by mean price (% per tick)."""
    n = len(prices)
    if n < 2:
        return np.nan
    t = np.arange(n, dtype=float)
    st = t.sum()
    sp = float(prices.sum())
    stp = float((t * prices).sum())
    st2 = float((t ** 2).sum())
    denom = n * st2 - st ** 2
    if denom == 0:
        return 0.0
    slope = (n * stp - st * sp) / denom
    mean_p = float(np.mean(prices))
    return float(slope / mean_p * 100) if mean_p != 0 else 0.0


def _run_length(close: pd.Series) -> pd.Series:
    """Signed run-length: +N for N consecutive up-ticks, -N for down-ticks.

    Flat ticks (zero diff) reset the run to 0.
    """
    signs = np.sign(close.diff().fillna(0)).to_numpy(dtype=float)
    runs = np.zeros(len(signs))
    for i in range(1, len(signs)):
        s = signs[i]
        if s > 0:
            runs[i] = runs[i - 1] + 1 if runs[i - 1] > 0 else 1.0
        elif s < 0:
            runs[i] = runs[i - 1] - 1 if runs[i - 1] < 0 else -1.0
        # s == 0: flat tick — run breaks, stays 0
    return pd.Series(runs, index=close.index)


def _second_order_markov(close: pd.Series, window: int = 50) -> pd.DataFrame:
    """2nd-order Markov: P(up|up,up) and P(dn|dn,dn) over a rolling window."""
    states = np.sign(close.diff().fillna(0)).to_numpy(dtype=int)
    puu: list[float] = [np.nan] * len(states)
    pdd: list[float] = [np.nan] * len(states)

    for end in range(window, len(states)):
        s = states[end - window + 1: end + 1]
        prev2 = s[:-2]
        prev1 = s[1:-1]
        curr = s[2:]
        uu_mask = (prev2 == 1) & (prev1 == 1)
        dd_mask = (prev2 == -1) & (prev1 == -1)
        puu[end] = float(np.mean(curr[uu_mask] == 1)) if np.any(uu_mask) else 0.5
        pdd[end] = float(np.mean(curr[dd_mask] == -1)) if np.any(dd_mask) else 0.5

    return pd.DataFrame({"puu": puu, "pdd": pdd}, index=close.index)


def _rolling_return_autocorr(close: pd.Series, lag: int, window: int) -> pd.Series:
    """Rolling autocorrelation of price returns at the given lag.

    Positive value = momentum (returns persist); negative = mean-reversion.
    """
    returns = close.diff().fillna(0.0)

    def _autocorr(x: np.ndarray) -> float:
        if len(x) < lag + 2:
            return 0.0
        x1 = x[:-lag]
        x2 = x[lag:]
        std1 = float(np.std(x1, ddof=0))
        std2 = float(np.std(x2, ddof=0))
        if std1 == 0.0 or std2 == 0.0:
            return 0.0
        return float(np.mean((x1 - x1.mean()) * (x2 - x2.mean())) / (std1 * std2))

    return returns.rolling(window).apply(_autocorr, raw=True).fillna(0.0)


def _fft_dominant_period(prices: np.ndarray) -> float:
    """Dominant cycle period (in ticks) via FFT on de-trended price window.

    Returns NaN if the window is too small. On IID data this will be noisy
    and close to Nyquist; any consistent value > 4 ticks is worth investigating.
    """
    n = len(prices)
    if n < 8:
        return np.nan
    detrended = prices - np.linspace(float(prices[0]), float(prices[-1]), n)
    spectrum = np.abs(np.fft.rfft(detrended))
    spectrum[0] = 0.0  # remove DC
    if len(spectrum) < 2:
        return np.nan
    dominant_bin = int(np.argmax(spectrum[1:])) + 1
    return float(n / dominant_bin)


# ---------------------------------------------------------------------------
# HMM Regime Detection
# ---------------------------------------------------------------------------

def hmm_regime_is_high_variance(close: pd.Series, n_states: int = 2, window: int = 200) -> bool:
    """Return True if the HMM identifies the current market as a high-variance regime.

    Uses a Gaussian HMM on log-returns. The state with the higher emission variance
    is labeled as the "high-variance regime". If hmmlearn is not installed the
    function always returns False (conservative: never blocks a trade for regime).
    """
    if not _HMM_AVAILABLE:
        return False

    if len(close) < max(window // 2, 20):
        return False

    prices = close.iloc[-window:].to_numpy(dtype=float)
    if len(prices) < 10 or np.any(prices <= 0):
        return False

    returns = np.diff(np.log(prices)).reshape(-1, 1)
    if len(returns) < 10:
        return False

    try:
        model = _hmm_lib.GaussianHMM(
            n_components=n_states,
            covariance_type="full",
            n_iter=100,
            random_state=42,
        )
        model.fit(returns)
        hidden_states = model.predict(returns)
        current_state = int(hidden_states[-1])
        # The high-variance state is the one with the largest variance
        variances = [float(model.covars_[s][0, 0]) for s in range(n_states)]
        high_var_state = int(np.argmax(variances))
        return current_state == high_var_state
    except Exception:  # pragma: no cover - hmmlearn convergence failures
        return False


# ---------------------------------------------------------------------------
# Ensemble Scorer (Logistic Regression on shadow dataset)
# ---------------------------------------------------------------------------

#: Feature columns used by the ensemble scorer (must match shadow CSV columns).
ENSEMBLE_FEATURE_COLS = [
    "hurst_exponent",
    "shannon_entropy",
    "tick_imbalance",
    "hawkes_intensity",
    "velocity_zscore",
    "acceleration_zscore",
    "kalman_residual_zscore",
    "pmi_distance_percent",
    "markov_p_up_given_up",
    "markov_p_down_given_down",
    "bb_width_percent",
    "tick_atr_percent",
    "recent_move_percent",
]


class EnsembleScorer:
    """XGBoost Booster inference — loads pre-trained model from disk.
    Predicts P(LOSS): high value = dangerous entry. Block when >= ensemble_min_prob.
    """

    def __init__(
        self,
        model_path: str = "models/pegasus_xgb_v1.json",
        features_path: str = "models/pegasus_features_v1.json",
    ) -> None:
        if not _XGB_AVAILABLE:
            raise RuntimeError("xgboost nao instalado. Execute: pip install xgboost")
        import json as _json
        with open(features_path) as _f:
            self.feature_names: list[str] = _json.load(_f)
        self.booster = xgb.Booster()
        self.booster.load_model(model_path)
        logger.info(
            "EnsembleScorer XGBoost carregado: %d features, modelo=%s",
            len(self.feature_names),
            model_path,
        )

    def predict_loss_probability(self, row: pd.Series) -> float:
        """Return P(LOSS) for a single indicator row. LOSS=1 = positive class."""
        values = [float(row.get(feat, 0.0)) for feat in self.feature_names]
        if any(v != v for v in values):  # NaN guard
            return 0.0
        x = np.array([values], dtype=float)
        dmatrix = xgb.DMatrix(x, feature_names=self.feature_names)
        return float(self.booster.predict(dmatrix)[0])


# ---------------------------------------------------------------------------
# Rise/Fall (binary options) strategy
# ---------------------------------------------------------------------------

#: Features used for Rise/Fall direction prediction — signed directional features
#: plus volatility context for the ensemble model.
RF_FEATURES = [
    # Signed velocity / trend indicators
    "price_velocity",        # causal finite-difference velocity (signed)
    "ols_slope",             # OLS regression slope over window (% per tick)
    "price_momentum",        # net return over last N ticks (%)
    "ema_diff",              # EMA(5) - EMA(20): positive = short-term uptrend
    "run_length",            # consecutive same-direction tick count (signed)
    # Flow imbalance
    "tick_imbalance",        # net up-ticks minus down-ticks in window
    # 1st-order Markov regime
    "markov_p_up_given_up",
    "markov_p_down_given_down",
    # 2nd-order Markov (run continuation)
    "markov2_puu",           # P(up | up, up)
    "markov2_pdd",           # P(dn | dn, dn)
    # Return distribution statistics
    "return_autocorr_lag1",  # lag-1 autocorrelation: + = momentum, - = mean-rev
    "return_skewness",       # rolling skewness of returns
    # Spectral
    "fft_dominant_period",   # dominant cycle period in ticks
    # Volatility context (unsigned)
    "hurst_exponent",
    "hawkes_intensity",
    "velocity_zscore",
    "acceleration_zscore",
    "bb_width_percent",
    "tick_atr_percent",
    "recent_move_percent",
    "shannon_entropy",
    "kalman_residual_zscore",
]


def generate_calm_accu_signal(
    prices: list[float],
    threshold: float = 7.3e-7,
    lookback: int = 10,
    df: pd.DataFrame | None = None,
    config: AccumulatorStrategyConfig | None = None,
    ensemble_scorer: "EnsembleScorer | None" = None,
) -> tuple[Optional[str], int, Optional[float]]:
    """Calm-entry accumulator signal for BOOM1000.

    Uses a multi-layer filtering approach:
    1. Calm filter: avg |return| below threshold (core BOOM1000 edge)
    2. Full indicator voting (10 primary + 10 quant + 7 advanced = 27 max)
    3. HMM regime detection (blocks high-variance regimes)
    4. P(LOSS) XGBoost ensemble gate (probabilistic final check)

    Returns ("ACCU", score, p_loss) on entry, (None, 0, None) otherwise.
    """
    config = config or AccumulatorStrategyConfig()

    if len(prices) < lookback + 1:
        return None, 0, None

    recent = prices[-(lookback + 1):]
    abs_returns = [abs(recent[i] / recent[i - 1] - 1) for i in range(1, len(recent))]
    avg_abs_ret = sum(abs_returns) / len(abs_returns)

    if avg_abs_ret >= threshold:
        logger.debug(
            "CALM ACCU: avg_abs_ret=%.2e >= threshold=%.2e → skip",
            avg_abs_ret, threshold,
        )
        return None, 0, None

    # --- Full indicator voting system ---
    p_loss: Optional[float] = None
    if df is not None and len(df) >= config.minimum_ticks:
        last = df.iloc[-1]

        # Layer 1+2: Base score from 13 indicators (3 primary + 10 quant) — max 20
        score = score_accumulator_row(last, config)

        # Layer 3: Advanced indicator votes — 7 additional points
        def _val(name: str, default: float = 0.0) -> float:
            v = last.get(name, default)
            try:
                f = float(v if v is not None else default)
            except (TypeError, ValueError):
                f = default
            return default if f != f else f  # NaN → default

        # Bayesian P(up) near 0.5 = uncertain direction = good for ACCU
        bayesian = _val("bayesian_prob_up", 0.5)
        if 0.30 <= bayesian <= 0.70:
            score += 1

        # Rényi entropy >= 0.4 = dispersed returns = random = good
        renyi = _val("renyi_entropy", 0.5)
        if renyi >= 0.40:
            score += 1

        # Fisher information > 0 = returns concentrated = predictable
        fisher = _val("fisher_information", 0.0)
        if fisher > 0.0:
            score += 1

        # Wavelet SNR < 0.7 = noise-dominated = no clear trend
        wavelet = _val("wavelet_energy_ratio", 0.5)
        if wavelet < 0.70:
            score += 1

        # CUSUM < 5.0 = stable regime (no structural break)
        cusum = _val("cusum_score", 0.0)
        if cusum < 5.0:
            score += 1

        # Tail dependence < 0.3 = extremes independent = safer
        tail_dep = _val("tail_dependence", 0.0)
        if tail_dep < 0.30:
            score += 1

        # MI flow < 0.15 = low predictability = random walk = ACCU-friendly
        mi = _val("mi_flow", 0.0)
        if mi < 0.15:
            score += 1

        # max score = 27 (20 base + 7 advanced)

        # --- Hard blocks: only extreme danger (very relaxed) ---
        hurst = _val("hurst_exponent", 0.5)
        if hurst > 0.70:
            logger.info(
                "CALM ACCU HARD BLOCK: hurst=%.3f > 0.70 — trending extremo",
                hurst,
            )
            return None, 0, None

        if cusum > 8.0:
            logger.info(
                "CALM ACCU HARD BLOCK: cusum=%.2f > 8.0 — regime break severo",
                cusum,
            )
            return None, 0, None

        # --- HMM regime gate ---
        if config.hmm_high_variance_blocks and _HMM_AVAILABLE:
            if hmm_regime_is_high_variance(
                df["close"], n_states=config.hmm_n_states, window=config.hmm_window,
            ):
                logger.info("CALM ACCU BLOCKED: HMM regime de alta variância")
                return None, 0, None

        # --- Minimum score gate (configurable via CALM_ACCU_MIN_SCORE) ---
        min_score = config.calm_min_score
        if score < min_score:
            logger.info(
                "CALM ACCU score=%d < min=%d/27 | H=%.3f cusum=%.2f → skip",
                score, min_score, hurst, cusum,
            )
            return None, 0, None

        # --- P(LOSS) XGBoost ensemble gate (final AI check) ---
        if config.use_ensemble and ensemble_scorer is not None:
            p_loss = ensemble_scorer.predict_loss_probability(last)
            if p_loss >= config.ensemble_min_prob:
                logger.warning(
                    "⛔ CALM ACCU BLOCKED: P(LOSS)=%.4f >= %.4f — IA vetou",
                    p_loss, config.ensemble_min_prob,
                )
                return None, 0, None
            logger.info("CALM ACCU P(LOSS)=%.4f (limiar=%.4f) ✓", p_loss, config.ensemble_min_prob)

        logger.info(
            "CALM ACCU ENTRY: score=%d/27 | H=%.3f | cusum=%.2f | P(LOSS)=%s",
            score, hurst, cusum,
            f"{p_loss:.4f}" if p_loss is not None else "N/A",
        )
    else:
        score = 20

    return "ACCU", score, p_loss


@dataclass(frozen=True)
class RiseFallStrategyConfig:
    """Configuration for Rise/Fall directional signal generation."""

    min_votes: int = 4          # votes needed out of 6 direction indicators
    min_imbalance: float = 1.0  # |tick_imbalance| threshold for directional vote
    min_ols_slope: float = 0.0  # minimum |ols_slope| to count as directional
    min_momentum: float = 0.0   # minimum |price_momentum| to count as directional
    use_ensemble: bool = False
    ensemble_min_prob: float = 0.52  # P(correct direction) threshold

    @property
    def minimum_ticks(self) -> int:
        return AccumulatorStrategyConfig().minimum_ticks


def generate_rise_fall_signal(
    df: pd.DataFrame,
    config: RiseFallStrategyConfig | None = None,
    ensemble_scorer: "EnsembleScorerRF | None" = None,
) -> tuple[Optional[str], int, Optional[float]]:
    """Return ("CALL"/"PUT"/None, votes, p_direction).

    Direction signal from 6 aligned indicators (rule-based) or ensemble model:
      1. price_velocity > 0 / < 0    (instantaneous signed momentum)
      2. ols_slope > 0 / < 0         (linear regression trend direction)
      3. price_momentum > 0 / < 0    (N-tick net return)
      4. ema_diff > 0 / < 0          (EMA fast/slow crossover)
      5. tick_imbalance ≥ / ≤ threshold (flow imbalance)
      6. markov_up > markov_dn        (1st-order persistence)
    """
    config = config or RiseFallStrategyConfig()
    if len(df) < config.minimum_ticks:
        return None, 0, None

    last = df.iloc[-1]

    # Ensemble gate (optional): use trained direction model
    if config.use_ensemble and ensemble_scorer is not None:
        p_up = ensemble_scorer.predict_up_probability(last)
        if p_up >= config.ensemble_min_prob:
            logger.info("EnsembleScorerRF P(UP)=%.4f >= %.4f → CALL", p_up, config.ensemble_min_prob)
            return "CALL", 6, p_up
        if p_up <= 1.0 - config.ensemble_min_prob:
            logger.info("EnsembleScorerRF P(UP)=%.4f <= %.4f → PUT", p_up, 1.0 - config.ensemble_min_prob)
            return "PUT", 6, 1.0 - p_up
        logger.debug("EnsembleScorerRF P(UP)=%.4f — sem sinal direcional", p_up)
        return None, 0, None

    def _get(name: str, default: float = 0.0) -> float:
        v = last.get(name, default)
        try:
            f = float(v if v is not None else default)
        except (TypeError, ValueError):
            f = default
        return default if f != f else f  # NaN guard

    velocity  = _get("price_velocity")
    imbalance = _get("tick_imbalance")
    ols       = _get("ols_slope")
    momentum  = _get("price_momentum")
    ema_d     = _get("ema_diff")
    markov_up = _get("markov_p_up_given_up", 0.5)
    markov_dn = _get("markov_p_down_given_down", 0.5)

    up_votes = (
        int(velocity > 0)
        + int(imbalance >= config.min_imbalance)
        + int(ols > config.min_ols_slope)
        + int(momentum > config.min_momentum)
        + int(ema_d > 0)
        + int(markov_up > markov_dn)
    )
    dn_votes = (
        int(velocity < 0)
        + int(imbalance <= -config.min_imbalance)
        + int(ols < -config.min_ols_slope)
        + int(momentum < -config.min_momentum)
        + int(ema_d < 0)
        + int(markov_dn > markov_up)
    )

    logger.debug(
        "RF vel=%.5f ols=%.5f mom=%.4f ema=%.5f imb=%.1f mUp=%.3f mDn=%.3f → up=%d dn=%d",
        velocity, ols, momentum, ema_d, imbalance, markov_up, markov_dn, up_votes, dn_votes,
    )

    if up_votes >= config.min_votes:
        return "CALL", up_votes, None
    if dn_votes >= config.min_votes:
        return "PUT", dn_votes, None
    return None, 0, None


class EnsembleScorerRF:
    """XGBoost Booster — predicts P(UP) for Rise/Fall direction.

    Returns probability that price will be higher at t+N ticks.
    Train with train_rf_model.py to generate models/pegasus_rf_v1.json.
    """

    def __init__(
        self,
        model_path: str = "models/pegasus_rf_v1.json",
        features_path: str = "models/pegasus_rf_features_v1.json",
    ) -> None:
        if not _XGB_AVAILABLE:
            raise RuntimeError("xgboost nao instalado. Execute: pip install xgboost")
        import json as _json
        with open(features_path) as _f:
            self.feature_names: list[str] = _json.load(_f)
        self.booster = xgb.Booster()
        self.booster.load_model(model_path)
        logger.info(
            "EnsembleScorerRF XGBoost carregado: %d features, modelo=%s",
            len(self.feature_names),
            model_path,
        )

    def predict_up_probability(self, row: pd.Series) -> float:
        """Return P(UP) in [0,1]. Values above 0.52 suggest CALL; below 0.48 suggest PUT."""
        values = [float(row.get(feat, 0.0) or 0.0) for feat in self.feature_names]
        if any(v != v for v in values):  # NaN guard
            return 0.5
        x = np.array([values], dtype=float)
        dmatrix = xgb.DMatrix(x, feature_names=self.feature_names)
        return float(self.booster.predict(dmatrix)[0])


# ---------------------------------------------------------------------------
# Jump Index momentum strategy (JD10, JD25, JD50, JD75, JD100)
# ---------------------------------------------------------------------------
# Based on empirical analysis of 5000-tick datasets showing statistically
# significant edge (CI above 50%) on Jump Indices.
#
# Three strategies, combined as an ensemble:
#   1. MomCont_5t: after 5 consecutive ticks in same direction → bet continuation
#   2. EMA crossover: fast EMA crosses slow EMA → bet in cross direction
#   3. Reversal: after 7 consecutive ticks in same direction → bet reversal
#
# Confidence levels and signal quality scored 1-6 based on agreement.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JumpMomentumConfig:
    """Configuration for Jump Index momentum signal generation.

    21-vote system: each validator casts 1 vote (UP or DOWN or abstains).
    Signal fires when winning side has >= min_score votes AND confidence >= min_confidence.
    """

    # Momentum continuation: N consecutive ticks → bet continuation
    mom_lookback: int = 5       # how many consecutive same-direction ticks needed
    mom_horizon: int = 5        # bet duration in ticks (Rise/Fall)
    # Short momentum
    short_mom_lookback: int = 3 # short-term momentum window
    # EMA crossover
    ema_fast: int = 5           # fast EMA period
    ema_slow: int = 20          # slow EMA period
    # Reversal after long streak
    rev_lookback: int = 7       # streak length to trigger reversal bet
    # Minimum votes to trigger signal (out of 21 validators)
    min_score: int = 7          # needs at least 7 validators agreeing
    # Minimum confidence ratio to trigger (winning_votes / total_votes)
    min_confidence: float = 0.60  # at least 60% of voting validators must agree
    # Minimum ticks in buffer before generating signals
    min_ticks: int = 30
    # Hard block thresholds (return None immediately)
    lyapunov_chaos: float = 2.0         # positive lyapunov above this → too chaotic to trade
    return_z_extreme: float = 3.0       # |return_zscore| above this = extreme move
    cusum_regime_alert: float = 8.0     # CUSUM above this = regime shift in progress
    jerk_regime_z: float = 3.0          # jerk z-score → regime transition, block
    tail_dep_danger: float = 0.6        # tail dependence above this = extreme clustering, block
    # Validator thresholds
    curvature_reversal_z: float = 2.0   # curvature z-score → high = inflection
    energy_calm_pctile: float = 30.0    # derivative energy below this pctile = calm
    exhaustion_extreme: float = 0.02    # |trend_exhaustion| above this = overbought/oversold
    bayesian_strong_prob: float = 0.55  # P(up) above/below this = directional signal
    renyi_low_entropy: float = 0.4      # Rényi below this = concentrated → predictable
    fisher_info_min: float = 0.05       # Fisher info above this = tight distribution
    wavelet_snr_min: float = 0.50       # wavelet ratio above this = clean signal
    mi_flow_min: float = 0.03           # MI above this = exploitable predictability
    hurst_trending: float = 0.55        # hurst above this = trending market
    hurst_reverting: float = 0.40       # hurst below this = mean-reverting market
    # ── Quality gate filters (post-vote rejection) ─────────────────────
    # These reject signals that pass the vote threshold but lack indicator
    # conviction.  At least ONE quality gate must pass for signal to fire.
    quality_gate_enabled: bool = True
    qg_min_abs_imbalance: float = 6.0   # |tick_imbalance| must reach this (6+ proven profitable)
    qg_bayes_strong: float = 0.70       # bayesian_prob_up > X or < (1-X)
    qg_hurst_max: float = 0.50          # hurst below this for bayes+hurst combo


def _ema_series(prices: list[float], period: int) -> list[float]:
    """Compute exponential moving average from a list of prices."""
    k = 2.0 / (period + 1)
    result = [prices[0]]
    for p in prices[1:]:
        result.append(p * k + result[-1] * (1.0 - k))
    return result


def generate_jump_momentum_signal(
    tick_buffer: list[dict],
    config: JumpMomentumConfig | None = None,
    df: pd.DataFrame | None = None,
) -> tuple[Optional[str], int, Optional[float]]:
    """Return ("CALL"/"PUT"/None, score, confidence).

    21-vote universal validation system.
    Each validator independently casts exactly 1 vote (UP or DOWN) or abstains.
    Signal fires only when:
      - winning_votes >= min_score (default 7)
      - winning_votes / total_votes >= min_confidence (default 0.60)

    Hard safety blocks (return None immediately):
      - Lyapunov exponent > threshold → chaotic market
      - |return_zscore| > threshold → extreme spike
      - CUSUM > threshold → regime shift in progress
      - |jerk_zscore| > threshold → regime transition
      - tail_dependence > threshold → extreme clustering

    Validators (21 total):
      Price Action:  1-Momentum, 2-EMA crossover, 3-EMA alignment,
                     4-Short momentum, 5-Reversal streak
      Velocity:      6-Velocity direction, 7-Acceleration confirms,
                     8-Curvature inflection, 9-Integral divergence
      Statistical:   10-Bayesian posterior, 11-Kalman residual,
                     12-Hurst regime, 13-Markov transition, 14-Tick imbalance
      Info Theory:   15-Shannon entropy gate, 16-Rényi concentration,
                     17-MI flow structure, 18-Wavelet SNR
      Quality:       19-Fisher precision, 20-Calm market, 21-Trend exhaustion
    """
    config = config or JumpMomentumConfig()
    n = len(tick_buffer)
    if n < config.min_ticks:
        return None, 0, None

    quotes = [t["quote"] for t in tick_buffer]

    up_votes = 0
    dn_votes = 0

    # ══════════════════════════════════════════════════════════════════════
    # HARD SAFETY BLOCKS — return None immediately if any triggered
    # ══════════════════════════════════════════════════════════════════════
    if df is not None and len(df) >= config.min_ticks:
        last = df.iloc[-1]

        def _g(name: str, default: float = 0.0) -> float:
            v = last.get(name, default)
            try:
                f = float(v if v is not None else default)
            except (TypeError, ValueError):
                f = default
            return default if f != f else f  # NaN guard

        lyap     = _g("lyapunov_exponent")
        ret_z    = _g("return_zscore")
        cusum    = _g("cusum_score", 0.0)
        jerk_z   = _g("jerk_zscore")
        tail_dep = _g("tail_dependence", 0.0)

        if lyap > config.lyapunov_chaos:
            logger.debug(
                "JumpMom BLOCK: Lyapunov=%.3f > %.3f → chaotic.",
                lyap, config.lyapunov_chaos,
            )
            return None, 0, None

        if abs(ret_z) > config.return_z_extreme:
            logger.debug(
                "JumpMom BLOCK: return_z=%.2f → extreme spike.",
                ret_z,
            )
            return None, 0, None

        if cusum > config.cusum_regime_alert:
            logger.debug(
                "JumpMom BLOCK: CUSUM=%.2f → regime shift.",
                cusum,
            )
            return None, 0, None

        if abs(jerk_z) > config.jerk_regime_z:
            logger.debug(
                "JumpMom BLOCK: jerk_z=%.2f → regime transition.",
                jerk_z,
            )
            return None, 0, None

        if tail_dep > config.tail_dep_danger:
            logger.debug(
                "JumpMom BLOCK: tail_dep=%.3f → extreme clustering.",
                tail_dep,
            )
            return None, 0, None

    # ══════════════════════════════════════════════════════════════════════
    # PRICE ACTION VALIDATORS (1-5) — tick buffer only
    # ══════════════════════════════════════════════════════════════════════

    # ── V1: Momentum majority (majority of last N ticks same direction) ─
    lb = config.mom_lookback
    if n > lb:
        tick_dirs = [quotes[i] > quotes[i - 1] for i in range(n - lb, n)]
        up_count = sum(tick_dirs)
        dn_count = sum(not d for d in tick_dirs)
        # Majority (>=60%) in one direction votes
        if up_count >= lb * 0.6:
            up_votes += 1
        elif dn_count >= lb * 0.6:
            dn_votes += 1

    # ── V2: EMA crossover or strong gap ────────────────────────────────
    if n > config.ema_slow + 1:
        ema_f = _ema_series(quotes, config.ema_fast)
        ema_s = _ema_series(quotes, config.ema_slow)
        fast_above_now = ema_f[-1] > ema_s[-1]
        fast_above_prev = ema_f[-2] > ema_s[-2]
        if fast_above_now and not fast_above_prev:
            up_votes += 1
        elif not fast_above_now and fast_above_prev:
            dn_votes += 1
        else:
            # Also vote on strong EMA gap (>0.005% of price)
            gap_pct = abs(ema_f[-1] - ema_s[-1]) / ema_s[-1] * 100
            if gap_pct > 0.005:
                if ema_f[-1] > ema_s[-1]:
                    up_votes += 1
                else:
                    dn_votes += 1

    # ── V3: EMA alignment (price positioning relative to slow EMA) ─────
    if n > config.ema_slow + 1:
        ema_s = _ema_series(quotes, config.ema_slow)
        if quotes[-1] > ema_s[-1]:
            up_votes += 1
        elif quotes[-1] < ema_s[-1]:
            dn_votes += 1

    # ── V4: Short momentum majority (last 3 ticks) ────────────────────
    slb = config.short_mom_lookback
    if n > slb:
        short_dirs = [quotes[i] > quotes[i - 1] for i in range(n - slb, n)]
        up_short = sum(short_dirs)
        dn_short = sum(not d for d in short_dirs)
        # Majority (>=2/3) votes
        if up_short >= 2:
            up_votes += 1
        elif dn_short >= 2:
            dn_votes += 1

    # ── V5: Reversal after streak (counter-trend, shorter threshold) ───
    rl = config.rev_lookback
    if n > rl:
        streak_dirs = [quotes[i] > quotes[i - 1] for i in range(n - rl, n)]
        up_streak = sum(streak_dirs)
        dn_streak = sum(not d for d in streak_dirs)
        # 5+ out of 7 in one direction → expect reversal
        if up_streak >= rl - 2:
            dn_votes += 1  # overbought → expect pullback
        elif dn_streak >= rl - 2:
            up_votes += 1  # oversold → expect bounce

    # ══════════════════════════════════════════════════════════════════════
    # ADVANCED VALIDATORS (6-21) — require DataFrame with indicators
    # ══════════════════════════════════════════════════════════════════════
    if df is not None and len(df) >= config.min_ticks:
        last = df.iloc[-1]

        def _g(name: str, default: float = 0.0) -> float:
            v = last.get(name, default)
            try:
                f = float(v if v is not None else default)
            except (TypeError, ValueError):
                f = default
            return default if f != f else f  # NaN guard

        vel      = _g("price_velocity")
        accel    = _g("price_acceleration", 0.0)
        curv_z   = _g("curvature_zscore")
        int_div  = _g("integral_momentum_div")
        energy   = _g("derivative_energy")
        bayes_up = _g("bayesian_prob_up", 0.5)
        kalman_z = _g("kalman_residual_zscore")
        hurst    = _g("hurst_exponent", 0.5)
        markov_up   = _g("markov_p_up_given_up", 0.5)
        markov_dn   = _g("markov_p_down_given_down", 0.5)
        imbalance   = _g("tick_imbalance", 0.0)
        shannon     = _g("shannon_entropy", 1.0)
        renyi       = _g("renyi_entropy", 0.5)
        mi          = _g("mi_flow", 0.0)
        wavelet     = _g("wavelet_energy_ratio", 0.5)
        fisher      = _g("fisher_information", 0.0)
        exhaust     = _g("trend_exhaustion")
        vel_z       = _g("velocity_zscore", 0.0)
        accel_z     = _g("acceleration_zscore", 0.0)

        # ── V6: Velocity direction ─────────────────────────────────────
        if vel > 0:
            up_votes += 1
        elif vel < 0:
            dn_votes += 1

        # ── V7: Acceleration confirms velocity (same sign) ─────────────
        if vel > 0 and accel > 0:
            up_votes += 1
        elif vel < 0 and accel < 0:
            dn_votes += 1

        # ── V8: Curvature inflection (high curvature = turning point) ──
        if curv_z > config.curvature_reversal_z:
            # Inflection: if decelerating upward motion → turning down
            if vel > 0 and accel < 0:
                dn_votes += 1
            elif vel < 0 and accel > 0:
                up_votes += 1

        # ── V9: Integral momentum divergence direction ─────────────────
        if energy > 1e-10:
            norm_div = int_div / energy
            if norm_div > 1.0:
                up_votes += 1
            elif norm_div < -1.0:
                dn_votes += 1

        # ── V10: Bayesian posterior probability ────────────────────────
        if bayes_up > config.bayesian_strong_prob:
            up_votes += 1
        elif bayes_up < (1.0 - config.bayesian_strong_prob):
            dn_votes += 1

        # ── V11: Kalman residual direction (filter sees trend) ─────────
        if kalman_z > 1.0:
            up_votes += 1
        elif kalman_z < -1.0:
            dn_votes += 1

        # ── V12: Hurst exponent regime ─────────────────────────────────
        # Trending market (hurst > 0.55): trust momentum direction
        # Mean-reverting (hurst < 0.40): trust reversal direction
        if hurst > config.hurst_trending:
            # Trending → agree with velocity
            if vel > 0:
                up_votes += 1
            elif vel < 0:
                dn_votes += 1
        elif hurst < config.hurst_reverting:
            # Mean-reverting → counter velocity
            if vel > 0:
                dn_votes += 1
            elif vel < 0:
                up_votes += 1

        # ── V13: Markov transition probabilities ───────────────────────
        if markov_up > 0.55 and markov_up > markov_dn:
            up_votes += 1
        elif markov_dn > 0.55 and markov_dn > markov_up:
            dn_votes += 1

        # ── V14: Tick imbalance (buy/sell pressure) ────────────────────
        if imbalance > 0.1:
            up_votes += 1
        elif imbalance < -0.1:
            dn_votes += 1

        # ── V15: Shannon entropy gate (low = predictable → trust dir) ──
        if shannon < 0.7:
            # Low entropy = concentrated returns → trust dominant direction
            if vel > 0:
                up_votes += 1
            elif vel < 0:
                dn_votes += 1

        # ── V16: Rényi concentration (low = strong pattern) ────────────
        if renyi < config.renyi_low_entropy:
            if vel > 0:
                up_votes += 1
            elif vel < 0:
                dn_votes += 1

        # ── V17: Mutual information flow (high = structure exists) ─────
        if mi > config.mi_flow_min:
            # Predictable structure → trust direction from velocity z-score
            if vel_z > 0.5:
                up_votes += 1
            elif vel_z < -0.5:
                dn_votes += 1

        # ── V18: Wavelet signal-to-noise (clean trend signal) ──────────
        if wavelet > config.wavelet_snr_min:
            if vel > 0:
                up_votes += 1
            elif vel < 0:
                dn_votes += 1

        # ── V19: Fisher information (tight params = reliable) ──────────
        if fisher > config.fisher_info_min:
            # Tight distribution → trust acceleration z-score direction
            if accel_z > 0.5:
                up_votes += 1
            elif accel_z < -0.5:
                dn_votes += 1

        # ── V20: Calm market (low energy + low hurst) ──────────────────
        if energy < config.energy_calm_pctile and hurst < 0.50:
            # Calm and predictable → trust EMA alignment
            if n > config.ema_slow:
                ema_s_local = _ema_series(quotes, config.ema_slow)
                if quotes[-1] > ema_s_local[-1]:
                    up_votes += 1
                elif quotes[-1] < ema_s_local[-1]:
                    dn_votes += 1

        # ── V21: Trend exhaustion (overbought/oversold reversal) ───────
        if abs(exhaust) > config.exhaustion_extreme:
            if exhaust > config.exhaustion_extreme:
                dn_votes += 1  # overbought → expect fall
            else:
                up_votes += 1  # oversold → expect rise

    # ══════════════════════════════════════════════════════════════════════
    # DECISION: require min_score votes AND min_confidence ratio
    # ══════════════════════════════════════════════════════════════════════
    total_votes = up_votes + dn_votes
    if total_votes == 0:
        return None, 0, None

    winning_votes = max(up_votes, dn_votes)
    confidence = winning_votes / total_votes

    if winning_votes >= config.min_score and confidence >= config.min_confidence:
        # ── QUALITY GATE: at least one indicator filter must pass ───────
        if config.quality_gate_enabled and df is not None and len(df) >= config.min_ticks:
            last = df.iloc[-1]

            def _qg(name: str, default: float = 0.0) -> float:
                v = last.get(name, default)
                try:
                    f = float(v if v is not None else default)
                except (TypeError, ValueError):
                    f = default
                return default if f != f else f

            qg_imbalance = _qg("tick_imbalance", 0.0)
            qg_bayes = _qg("bayesian_prob_up", 0.5)
            qg_hurst = _qg("hurst_exponent", 0.5)

            # Gate 1: strong tick imbalance (|imbalance| >= threshold)
            gate_imbalance = abs(qg_imbalance) >= config.qg_min_abs_imbalance

            # Gate 2: strong bayesian conviction
            gate_bayes = qg_bayes > config.qg_bayes_strong or qg_bayes < (1.0 - config.qg_bayes_strong)

            # Gate 3: bayes + hurst combo (strongest filter: 77.8% WR)
            gate_bayes_hurst = gate_bayes and qg_hurst < config.qg_hurst_max

            if not gate_imbalance:
                logger.debug(
                    "JumpMom QUALITY GATE REJECT: imb=%.1f (need ±%.1f), "
                    "bayes=%.2f, hurst=%.2f",
                    qg_imbalance, config.qg_min_abs_imbalance,
                    qg_bayes, qg_hurst,
                )
                return None, 0, None

            # Log which gate(s) passed
            gates = [f"imb={qg_imbalance:+.1f}"]
            if gate_bayes_hurst:
                gates.append(f"bayes={qg_bayes:.2f}+hurst={qg_hurst:.2f}")
            elif gate_bayes:
                gates.append(f"bayes={qg_bayes:.2f}")
            logger.info("JumpMom QUALITY GATE PASS: %s", ", ".join(gates))

        if up_votes > dn_votes:
            logger.info(
                "JumpMom CALL: ↑%d ↓%d (conf=%.0f%%, %d/%d votes)",
                up_votes, dn_votes, confidence * 100, winning_votes, total_votes,
            )
            return "CALL", up_votes, confidence
        else:
            logger.info(
                "JumpMom PUT: ↑%d ↓%d (conf=%.0f%%, %d/%d votes)",
                up_votes, dn_votes, confidence * 100, winning_votes, total_votes,
            )
            return "PUT", dn_votes, confidence

    logger.debug(
        "JumpMom NO SIGNAL: ↑%d ↓%d (need %d votes @ %.0f%% conf, got %d @ %.0f%%)",
        up_votes, dn_votes, config.min_score, config.min_confidence * 100,
        winning_votes, confidence * 100,
    )
    return None, 0, None
