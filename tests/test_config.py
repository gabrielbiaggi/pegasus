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
        with patch.dict(os.environ, env, clear=True):
            config = load_config()

        self.assertEqual(config.bot_name, "Pegasus")
        self.assertEqual(config.account_mode, "demo")
        self.assertTrue(config.dry_run)
        self.assertEqual(config.max_trades_per_day, 50)

    def test_rejects_invalid_account_mode(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "ACCOUNT_MODE": "paper",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                load_config()


if __name__ == "__main__":
    unittest.main()
