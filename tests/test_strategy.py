import unittest

from logger import logger
from strategy import (
    AccumulatorStrategyConfig,
    MultiplierContinuationConfig,
    calculate_tick_indicators,
    generate_accumulator_signal,
    generate_multiplier_continuation_snapshot_signal,
)


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

        signal, score, p_loss = generate_accumulator_signal(df, config=config)
        self.assertEqual(signal, "ACCU")
        self.assertIsNone(p_loss)
        # Score now includes all 13 indicators (max 20); quant contributions push above old max of 10
        self.assertGreater(score, 10)

    def test_accumulator_signal_blocks_wide_ticks(self) -> None:
        config = AccumulatorStrategyConfig(
            min_score=7,
            max_bb_width_percent=0.01,
            max_tick_atr_percent=0.001,
            max_recent_move_percent=0.001,
        )
        ticks = [{"epoch": 1_700_000_000 + i, "quote": 100 + i * 0.1} for i in range(80)]
        df = calculate_tick_indicators(ticks, config=config)

        self.assertEqual(generate_accumulator_signal(df, config=config), (None, 0, None))

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

    def test_multiplier_continuation_signal_accepts_boom1000_up_regime_without_mi_wavelet_edge(self) -> None:
        quotes = [100.0]
        for delta in (
            0.0001, 0.0001, 0.0002, -0.00005, 0.0002, 0.00015, 0.0001, 0.0002, 0.00015, 0.0001,
            0.0002, 0.00015, 0.0001, 0.00025, 0.00015, 0.0001, 0.0002, 0.00015, 0.0001, 0.0002,
            0.00015, 0.0001, 0.0002, 0.00015, 0.0001, 0.0002, 0.00015, 0.0001, 0.0002, 0.00015,
            0.0001, 0.0002, 0.00015, 0.0001, 0.0002, 0.00015, 0.0001, 0.0002, 0.00015,
        ):
            quotes.append(quotes[-1] + delta)

        row = {
            "price_velocity": 0.00018,
            "price_acceleration": 0.00005,
            "velocity_zscore": 1.3,
            "acceleration_zscore": 0.9,
            "tick_imbalance": 3.2,
            "markov_p_up_given_up": 0.68,
            "markov_p_down_given_down": 0.44,
            "bayesian_prob_up": 0.53,
            "hurst_exponent": 0.61,
            "shannon_entropy": 0.61,
            "renyi_entropy": 0.32,
            "fisher_information": 0.08,
            "mi_flow": 0.0,
            "wavelet_energy_ratio": 0.0,
        }

        signal, score, confidence = generate_multiplier_continuation_snapshot_signal(
            quotes,
            row,
            config=MultiplierContinuationConfig(
                min_score=4,
                min_confidence=0.57,
                min_up_ticks=4,
                max_down_ticks=1,
            ),
        )

        self.assertEqual(signal, "CALL")
        self.assertGreaterEqual(score, 4)
        self.assertGreater(confidence, 0.57)


if __name__ == "__main__":
    unittest.main()
