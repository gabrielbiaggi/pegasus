import unittest
from unittest.mock import patch

import backtest_engine
import pandas as pd


class MultiplierBacktestTest(unittest.TestCase):
    def test_indicator_cache_stale_detection_flags_degenerate_sampled_frame(self) -> None:
        df = pd.DataFrame(
            {
                "tick_imbalance": [-10.0] * 120,
                "bayesian_prob_up": [0.5] * 120,
                "mi_flow": [0.0] * 120,
                "wavelet_energy_ratio": [0.5] * 120,
                "cusum_score": [0.0] * 120,
                "hurst_exponent": [float("nan")] * 120,
            }
        )
        with patch.object(backtest_engine, "SAMPLE_EVERY", 60):
            self.assertTrue(backtest_engine._indicator_cache_is_stale(df))

    def test_indicator_cache_stale_detection_accepts_recomputed_frame(self) -> None:
        df = pd.DataFrame(
            {
                "tick_imbalance": [-10.0] * 120,
                "bayesian_prob_up": [0.03125] * 120,
                "mi_flow": [0.0] * 120,
                "wavelet_energy_ratio": [1e-12] * 120,
                "cusum_score": [4.7] * 120,
                "hurst_exponent": [0.49] * 120,
            }
        )
        with patch.object(backtest_engine, "SAMPLE_EVERY", 60):
            self.assertFalse(backtest_engine._indicator_cache_is_stale(df))

    def test_multiplier_profit_caps_at_take_profit(self) -> None:
        with patch.object(backtest_engine, "MULTIPLIER_VALUE", 100), patch.object(
            backtest_engine, "MULTIPLIER_TAKE_PROFIT", 0.50
        ), patch.object(backtest_engine, "MULTIPLIER_STOP_LOSS", 1.00):
            profit = backtest_engine._simulate_multiplier_profit(
                1.0,
                "MULTDOWN",
                [-0.001, -0.01],
            )

        self.assertEqual(profit, 0.50)

    def test_multiplier_profit_caps_at_stop_loss(self) -> None:
        with patch.object(backtest_engine, "MULTIPLIER_VALUE", 100), patch.object(
            backtest_engine, "MULTIPLIER_TAKE_PROFIT", 0.50
        ), patch.object(backtest_engine, "MULTIPLIER_STOP_LOSS", 1.00):
            profit = backtest_engine._simulate_multiplier_profit(
                1.0,
                "MULTUP",
                [-0.001, -0.02],
            )

        self.assertEqual(profit, -1.00)

    def test_multiplier_profit_marks_to_market_without_barrier_hit(self) -> None:
        with patch.object(backtest_engine, "MULTIPLIER_VALUE", 100), patch.object(
            backtest_engine, "MULTIPLIER_TAKE_PROFIT", 0.50
        ), patch.object(backtest_engine, "MULTIPLIER_STOP_LOSS", 1.00):
            profit = backtest_engine._simulate_multiplier_profit(
                2.0,
                "MULTUP",
                [0.0004, 0.0008],
            )

        self.assertEqual(profit, 0.12)

    def test_multiplier_direction_can_be_forced_up_or_down(self) -> None:
        with patch.object(backtest_engine, "MULTIPLIER_DIRECTION", "up"):
            self.assertEqual(backtest_engine._multiplier_direction_from_signal("PUT"), "MULTUP")
        with patch.object(backtest_engine, "MULTIPLIER_DIRECTION", "down"):
            self.assertEqual(backtest_engine._multiplier_direction_from_signal("CALL"), "MULTDOWN")
        with patch.object(backtest_engine, "MULTIPLIER_DIRECTION", "signal"):
            self.assertEqual(backtest_engine._multiplier_direction_from_signal("CALL"), "MULTUP")


if __name__ == "__main__":
    unittest.main()
