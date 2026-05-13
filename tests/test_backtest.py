import unittest

from backtest import max_drawdown, run_accumulator_backtest
from logger import logger
from strategy import AccumulatorStrategyConfig


logger.disabled = True


def compressed_ticks(total: int = 90, spike_after_setup: bool = False) -> list[dict]:
    ticks = []
    for index in range(total):
        quote = 100 + (index % 2) * 0.001
        if spike_after_setup and index == 22:
            quote = 100.2
        ticks.append({"epoch": 1_700_000_000 + index, "quote": quote})
    return ticks


class BacktestTest(unittest.TestCase):
    def test_max_drawdown(self) -> None:
        self.assertEqual(max_drawdown([1000, 1100, 990]), 0.1)

    def test_accumulator_backtest_returns_metrics(self) -> None:
        result = run_accumulator_backtest(
            ticks=compressed_ticks(),
            initial_balance=1000,
            stake=1,
            growth_rate=0.03,
            take_profit_percent=3,
            barrier_percent=0.05,
            max_hold_ticks=8,
            cooldown_ticks=1,
            strategy_config=AccumulatorStrategyConfig(
                min_score=7,
                max_bb_width_percent=0.2,
                max_tick_atr_percent=0.05,
                max_recent_move_percent=0.05,
            ),
        )

        self.assertGreater(result["total_trades"], 0)
        self.assertIn("ending_balance", result)
        self.assertIn("max_drawdown_pct", result)
        self.assertIsInstance(result["trades"], list)

    def test_accumulator_backtest_can_hit_barrier(self) -> None:
        result = run_accumulator_backtest(
            ticks=compressed_ticks(spike_after_setup=True),
            initial_balance=1000,
            stake=1,
            growth_rate=0.03,
            take_profit_percent=5,
            barrier_percent=0.05,
            max_hold_ticks=8,
            cooldown_ticks=1,
            strategy_config=AccumulatorStrategyConfig(
                min_score=7,
                max_bb_width_percent=0.2,
                max_tick_atr_percent=0.05,
                max_recent_move_percent=0.05,
            ),
        )

        self.assertTrue(any(trade["exit_reason"] == "barrier" for trade in result["trades"]))


if __name__ == "__main__":
    unittest.main()
