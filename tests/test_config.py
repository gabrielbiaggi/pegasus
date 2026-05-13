import os
import unittest
from unittest.mock import patch

from config import load_config


class ConfigTest(unittest.TestCase):
    def test_loads_defaults(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            config = load_config()

        self.assertEqual(config.bot_name, "Pegasus")
        self.assertEqual(config.account_mode, "demo")
        self.assertEqual(config.contract_mode, "accumulator")
        self.assertEqual(config.symbol, "1HZ100V")
        self.assertTrue(config.dry_run)
        self.assertEqual(config.max_trades_per_day, 50)
        self.assertEqual(config.tick_count, 300)
        self.assertEqual(config.blocked_utc_hours, ())
        self.assertEqual(config.accumulator_growth_rate, 0.03)

    def test_parses_blocked_hours(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "BLOCKED_UTC_HOURS": "0,2-4,23",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            config = load_config()

        self.assertEqual(config.blocked_utc_hours, (0, 2, 3, 4, 23))

    def test_rejects_invalid_account_mode(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "ACCOUNT_MODE": "paper",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                load_config()

    def test_rejects_non_accumulator_mode(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "CONTRACT_MODE": "rise_fall",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                load_config()


if __name__ == "__main__":
    unittest.main()
