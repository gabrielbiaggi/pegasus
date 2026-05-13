from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import ta

from logger import logger


@dataclass(frozen=True)
class StrategyConfig:
    min_score: int = 5
    use_trend_filter: bool = True
    trend_ema_window: int = 200
    use_atr_filter: bool = True
    atr_window: int = 14
    min_atr_percent: float = 0.05
    rsi_extreme_weight: int = 3
    rsi_soft_weight: int = 1
    macd_cross_weight: int = 3
    bollinger_touch_weight: int = 2
    ema_cross_weight: int = 2

    @property
    def minimum_candles(self) -> int:
        trend_need = self.trend_ema_window + 2 if self.use_trend_filter else 35
        atr_need = self.atr_window + 2 if self.use_atr_filter else 35
        return max(35, trend_need, atr_need)


def calculate_indicators(candles: list[dict], config: StrategyConfig | None = None) -> pd.DataFrame:
    config = config or StrategyConfig(use_trend_filter=False, use_atr_filter=False)
    df = pd.DataFrame(candles)
    if df.empty:
        return df

    required = {"epoch", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Candles sem campos obrigatorios: {sorted(missing)}")

    df = df.drop_duplicates(subset=["epoch"], keep="last").sort_values("epoch").reset_index(drop=True)
    df["epoch"] = df["epoch"].astype(int)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if len(df) < 35:
        return df

    df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()

    macd = ta.trend.MACD(df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()

    bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()

    df["ema9"] = ta.trend.EMAIndicator(df["close"], window=9).ema_indicator()
    df["ema21"] = ta.trend.EMAIndicator(df["close"], window=21).ema_indicator()
    df["ema_trend"] = ta.trend.EMAIndicator(df["close"], window=config.trend_ema_window).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=config.atr_window,
    ).average_true_range()
    df["atr_percent"] = df["atr"] / df["close"] * 100

    return df


def generate_signal(
    df: pd.DataFrame,
    min_score: int | None = None,
    config: StrategyConfig | None = None,
) -> tuple[Optional[str], int]:
    config = config or StrategyConfig(use_trend_filter=False, use_atr_filter=False)
    min_score = config.min_score if min_score is None else min_score

    if len(df) < config.minimum_candles:
        return None, 0

    last = df.iloc[-1]
    prev = df.iloc[-2]
    required = ["rsi", "macd_diff", "bb_upper", "bb_lower", "ema9", "ema21"]
    if config.use_trend_filter:
        required.append("ema_trend")
    if config.use_atr_filter:
        required.append("atr_percent")

    if last[required].isna().any() or prev[required].isna().any():
        return None, 0

    if config.use_atr_filter and last["atr_percent"] < config.min_atr_percent:
        logger.info(
            "Filtro ATR bloqueou sinal: atr_percent=%.4f minimo=%.4f",
            last["atr_percent"],
            config.min_atr_percent,
        )
        return None, 0

    call_score = 0
    put_score = 0

    if last["rsi"] < 30:
        call_score += config.rsi_extreme_weight
    elif last["rsi"] < 40:
        call_score += config.rsi_soft_weight

    if last["rsi"] > 70:
        put_score += config.rsi_extreme_weight
    elif last["rsi"] > 60:
        put_score += config.rsi_soft_weight

    if prev["macd_diff"] < 0 and last["macd_diff"] > 0:
        call_score += config.macd_cross_weight
    elif prev["macd_diff"] > 0 and last["macd_diff"] < 0:
        put_score += config.macd_cross_weight

    if last["close"] <= last["bb_lower"]:
        call_score += config.bollinger_touch_weight
    elif last["close"] >= last["bb_upper"]:
        put_score += config.bollinger_touch_weight

    if prev["ema9"] < prev["ema21"] and last["ema9"] > last["ema21"]:
        call_score += config.ema_cross_weight
    elif prev["ema9"] > prev["ema21"] and last["ema9"] < last["ema21"]:
        put_score += config.ema_cross_weight

    logger.info(
        "Score CALL=%s | PUT=%s | RSI=%.1f | ATR%%=%.4f | close=%s",
        call_score,
        put_score,
        last["rsi"],
        last.get("atr_percent", 0.0),
        last["close"],
    )

    if config.use_trend_filter:
        if last["close"] <= last["ema_trend"]:
            call_score = 0
        if last["close"] >= last["ema_trend"]:
            put_score = 0

    if call_score >= min_score and call_score > put_score:
        return "CALL", call_score
    if put_score >= min_score and put_score > call_score:
        return "PUT", put_score

    return None, 0
