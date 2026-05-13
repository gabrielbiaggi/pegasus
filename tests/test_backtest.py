import unittest

from backtest import max_drawdown, run_backtest
from logger import logger


logger.disabled = True


def sample_candles(total: int = 90) -> list[dict]:
    candles = []
    price = 100.0
    for index in range(total):
        price += 0.25
        candles.append(
            {
                "epoch": 1_700_000_000 + index * 60,
                "open": price - 0.1,
                "high": price + 0.2,
                "low": price - 0.2,
                "close": price,
            }
        )
    return candles


class BacktestTest(unittest.TestCase):
    def test_max_drawdown(self) -> None:
        self.assertEqual(max_drawdown([1000, 1100, 990]), 0.1)

    def test_backtest_returns_metrics(self) -> None:
        result = run_backtest(
            candles=sample_candles(),
            min_score=1,
            initial_balance=1000,
            stake=1,
            duration_candles=5,
            payout=0.85,
            cooldown_candles=1,
        )

        self.assertIn("total_trades", result)
        self.assertIn("ending_balance", result)
        self.assertIn("max_drawdown_pct", result)
        self.assertIsInstance(result["trades"], list)


if __name__ == "__main__":
    unittest.main()
