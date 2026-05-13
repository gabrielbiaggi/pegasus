from __future__ import annotations

from typing import Optional

import pandas as pd
import ta

from logger import logger


def calculate_indicators(candles: list[dict]) -> pd.DataFrame:
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

    return df


def generate_signal(df: pd.DataFrame, min_score: int = 5) -> tuple[Optional[str], int]:
    if len(df) < 35:
        return None, 0

    last = df.iloc[-1]
    prev = df.iloc[-2]
    required = ["rsi", "macd_diff", "bb_upper", "bb_lower", "ema9", "ema21"]

    if last[required].isna().any() or prev[required].isna().any():
        return None, 0

    call_score = 0
    put_score = 0

    if last["rsi"] < 30:
        call_score += 3
    elif last["rsi"] < 40:
        call_score += 1

    if last["rsi"] > 70:
        put_score += 3
    elif last["rsi"] > 60:
        put_score += 1

    if prev["macd_diff"] < 0 and last["macd_diff"] > 0:
        call_score += 3
    elif prev["macd_diff"] > 0 and last["macd_diff"] < 0:
        put_score += 3

    if last["close"] <= last["bb_lower"]:
        call_score += 2
    elif last["close"] >= last["bb_upper"]:
        put_score += 2

    if prev["ema9"] < prev["ema21"] and last["ema9"] > last["ema21"]:
        call_score += 2
    elif prev["ema9"] > prev["ema21"] and last["ema9"] < last["ema21"]:
        put_score += 2

    logger.info(
        "Score CALL=%s | PUT=%s | RSI=%.1f | close=%s",
        call_score,
        put_score,
        last["rsi"],
        last["close"],
    )

    if call_score >= min_score and call_score > put_score:
        return "CALL", call_score
    if put_score >= min_score and put_score > call_score:
        return "PUT", put_score

    return None, 0
