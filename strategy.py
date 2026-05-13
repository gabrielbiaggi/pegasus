from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import ta

from logger import logger


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

    @property
    def minimum_ticks(self) -> int:
        return max(self.bb_window + 2, self.atr_window + 2, self.recent_window + 2)


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

    logger.info(
        "ACCU score=%s | BBWidth%%=%.4f | TickATR%%=%.4f | RecentMove%%=%.4f",
        score,
        last["bb_width_percent"],
        last["tick_atr_percent"],
        last["recent_move_percent"],
    )

    if score >= config.min_score:
        return "ACCU", score

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
