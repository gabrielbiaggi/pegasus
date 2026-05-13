import unittest

from logger import logger
from strategy import AccumulatorStrategyConfig, calculate_tick_indicators, generate_accumulator_signal


logger.disabled = True


class StrategyTest(unittest.TestCase):
    def test_accumulator_signal_accepts_compressed_ticks(self) -> None:
        config = AccumulatorStrategyConfig(
            min_score=7,
            max_bb_width_percent=0.2,
            max_tick_atr_percent=0.05,
            max_recent_move_percent=0.05,
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


if __name__ == "__main__":
    unittest.main()
