import unittest

import pandas as pd

from logger import logger
from strategy import StrategyConfig, generate_signal


logger.disabled = True


def signal_frame(rows: int = 205, close: float = 100, ema_trend: float = 90, atr_percent: float = 0.1) -> pd.DataFrame:
    data = []
    for _ in range(rows):
        data.append(
            {
                "close": close,
                "rsi": 50,
                "macd_diff": 0,
                "bb_upper": close + 10,
                "bb_lower": close - 10,
                "ema9": close,
                "ema21": close,
                "ema_trend": ema_trend,
                "atr_percent": atr_percent,
            }
        )
    return pd.DataFrame(data)


class StrategyTest(unittest.TestCase):
    def test_trend_filter_allows_call_above_ema200(self) -> None:
        config = StrategyConfig(min_score=5, use_trend_filter=True, use_atr_filter=True)
        df = signal_frame(close=110, ema_trend=100)
        df.loc[df.index[-2], "macd_diff"] = -1
        df.loc[df.index[-1], "macd_diff"] = 1
        df.loc[df.index[-1], "rsi"] = 25

        self.assertEqual(generate_signal(df, config=config), ("CALL", 6))

    def test_trend_filter_blocks_call_below_ema200(self) -> None:
        config = StrategyConfig(min_score=5, use_trend_filter=True, use_atr_filter=True)
        df = signal_frame(close=90, ema_trend=100)
        df.loc[df.index[-2], "macd_diff"] = -1
        df.loc[df.index[-1], "macd_diff"] = 1
        df.loc[df.index[-1], "rsi"] = 25

        self.assertEqual(generate_signal(df, config=config), (None, 0))

    def test_atr_filter_blocks_flat_market(self) -> None:
        config = StrategyConfig(min_score=5, use_trend_filter=False, use_atr_filter=True, min_atr_percent=0.1)
        df = signal_frame(close=100, atr_percent=0.01)
        df.loc[df.index[-2], "macd_diff"] = -1
        df.loc[df.index[-1], "macd_diff"] = 1
        df.loc[df.index[-1], "rsi"] = 25

        self.assertEqual(generate_signal(df, config=config), (None, 0))


if __name__ == "__main__":
    unittest.main()
