import unittest

from bot import is_unsupported_rf_contract_error


class BotContractErrorsTest(unittest.TestCase):
    def test_trading_duration_error_disables_rf_retries(self) -> None:
        self.assertTrue(is_unsupported_rf_contract_error("TradingDurationNotAllowed"))

    def test_unrelated_error_does_not_disable_rf_retries(self) -> None:
        self.assertFalse(is_unsupported_rf_contract_error("WrongResponse"))


if __name__ == "__main__":
    unittest.main()
