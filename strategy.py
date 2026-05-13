from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import ta

from logger import logger

try:
    from scipy.integrate import trapezoid as integrate_trapezoid
except ImportError:  # pragma: no cover - numpy keeps the bot usable without scipy installed.
    integrate_trapezoid = np.trapezoid if hasattr(np, "trapezoid") else np.trapz


@dataclass(frozen=True)
class AccumulatorStrategyConfig:
    min_score: int = 7
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

    return df


def generate_accumulator_signal(
    df: pd.DataFrame,
    config: AccumulatorStrategyConfig | None = None,
) -> tuple[Optional[str], int]:
    config = config or AccumulatorStrategyConfig()
    if len(df) < config.minimum_ticks:
        return None, 0

    last = df.iloc[-1]
    score = score_accumulator_row(last, config)

    if score == 0 and last[["bb_width_percent", "tick_atr_percent", "recent_move_percent"]].isna().any():
        return None, 0

    quant_pass, reason = accumulator_quant_filters_pass(last, config)

    logger.info(
        (
            "ACCU score=%s | BBWidth%%=%.4f | TickATR%%=%.4f | RecentMove%%=%.4f | "
            "H=%.4f | imbalance=%s | hawkes=%.4f | vel_z=%.4f | accel_z=%.4f | pmi_dist%%=%.5f"
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
    )

    if score >= config.min_score and quant_pass:
        return "ACCU", score

    if score >= config.min_score:
        logger.info("Setup ACCU bloqueado pelo filtro quantitativo: %s", reason)

    return None, 0


def score_accumulator_row(row: pd.Series, config: AccumulatorStrategyConfig | None = None) -> int:
    config = config or AccumulatorStrategyConfig()
    required = ["bb_width_percent", "tick_atr_percent", "recent_move_percent"]
    if row[required].isna().any():
        return 0

    score = 0
    if row["bb_width_percent"] <= config.max_bb_width_percent:
        score += config.squeeze_weight
    if row["tick_atr_percent"] <= config.max_tick_atr_percent:
        score += config.atr_weight
    if row["recent_move_percent"] <= config.max_recent_move_percent:
        score += config.stability_weight
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
