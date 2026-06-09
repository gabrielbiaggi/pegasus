import os
import unittest
from unittest.mock import patch

import backtest_engine


class DigitsBacktestTest(unittest.TestCase):
    def test_quote_last_digit_uses_symbol_pip_size(self) -> None:
        self.assertEqual(backtest_engine._quote_last_digit(123.45, "1HZ100V"), 5)
        self.assertEqual(backtest_engine._quote_last_digit(123.456, "BOOM1000"), 6)

    def test_digits_contract_outcome_matches_expected_digit_rule(self) -> None:
        self.assertTrue(backtest_engine._digits_contract_wins("DIGITODD", 5, None))
        self.assertFalse(backtest_engine._digits_contract_wins("DIGITEVEN", 5, None))
        self.assertTrue(backtest_engine._digits_contract_wins("DIGITDIFF", 5, 0))
        self.assertFalse(backtest_engine._digits_contract_wins("DIGITMATCH", 5, 0))

    def test_digits_optimizer_run_keeps_full_tick_sampling(self) -> None:
        with patch.dict(
            os.environ,
            {
                "CONTRACT_MODE": "digits",
                "PEGASUS_OPTIMIZER_RUN": "true",
                "BACKTEST_SAMPLE_EVERY": "60",
                "DIGITS_DURATION_TICKS": "1",
            },
            clear=False,
        ):
            backtest_engine.apply_config(dict(os.environ))

        self.assertEqual(backtest_engine.SAMPLE_EVERY, 1)


if __name__ == "__main__":
    unittest.main()
