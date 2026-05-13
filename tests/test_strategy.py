import unittest

from logger import logger
from strategy import AccumulatorStrategyConfig, calculate_tick_indicators, generate_accumulator_signal


logger.disabled = True


def pd_isna(value: object) -> bool:
    return value != value


class StrategyTest(unittest.TestCase):
    def test_accumulator_signal_accepts_compressed_ticks(self) -> None:
        config = AccumulatorStrategyConfig(
            min_score=7,
            max_bb_width_percent=0.2,
            max_tick_atr_percent=0.05,
            max_recent_move_percent=0.05,
            max_markov_continuation_prob=1.0,
            min_shannon_entropy=0.0,
            max_kalman_residual_zscore=10.0,
        )
        ticks = [{"epoch": 1_700_000_000 + i, "quote": 100 + (i % 2) * 0.001} for i in range(80)]
        df = calculate_tick_indicators(ticks, config=config)

        self.assertEqual(generate_accumulator_signal(df, config=config), ("ACCU", 10))

    def test_accumulator_signal_blocks_wide_ticks(self) -> None:
        config = AccumulatorStrategyConfig(
            min_score=7,
            max_bb_width_percent=0.01,
            max_tick_atr_percent=0.001,
            max_recent_move_percent=0.001,
        )
        ticks = [{"epoch": 1_700_000_000 + i, "quote": 100 + i * 0.1} for i in range(80)]
        df = calculate_tick_indicators(ticks, config=config)

        self.assertEqual(generate_accumulator_signal(df, config=config), (None, 0))

    def test_calculate_tick_indicators_requires_tick_schema(self) -> None:
        with self.assertRaises(ValueError):
            calculate_tick_indicators([{"epoch": 1_700_000_000, "close": 100}])

    def test_calculate_tick_indicators_adds_stochastic_metrics(self) -> None:
        ticks = [{"epoch": 1_700_000_000 + i, "quote": 100 + (i % 3 - 1) * 0.001} for i in range(120)]
        df = calculate_tick_indicators(ticks)
        last = df.iloc[-1]

        for column in (
            "markov_p_up_given_up",
            "markov_p_down_given_down",
            "shannon_entropy",
            "kalman_residual_zscore",
        ):
            self.assertIn(column, df.columns)
            self.assertFalse(pd_isna(last[column]))


if __name__ == "__main__":
    unittest.main()
