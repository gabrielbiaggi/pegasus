import os
import unittest
from unittest.mock import patch

import backtest_engine


class OptimizerFastSamplingTest(unittest.TestCase):
    def test_optimizer_multiplier_keeps_configured_sample_every(self) -> None:
        env = {
            "CONTRACT_MODE": "multiplier",
            "PEGASUS_OPTIMIZER_RUN": "true",
            "BACKTEST_SAMPLE_EVERY": "60",
            "SYMBOL": "BOOM1000",
        }

        with patch.dict(os.environ, env, clear=False):
            backtest_engine.apply_config(env)

        self.assertEqual(backtest_engine.CONTRACT_MODE, "multiplier")
        self.assertEqual(backtest_engine.SAMPLE_EVERY, 60)

    def test_live_multiplier_still_uses_full_tick_sampling(self) -> None:
        env = {
            "CONTRACT_MODE": "multiplier",
            "PEGASUS_OPTIMIZER_RUN": "false",
            "BACKTEST_SAMPLE_EVERY": "60",
            "SYMBOL": "BOOM1000",
        }

        with patch.dict(os.environ, env, clear=False):
            backtest_engine.apply_config(env)

        self.assertEqual(backtest_engine.CONTRACT_MODE, "multiplier")
        self.assertEqual(backtest_engine.SAMPLE_EVERY, 1)


if __name__ == "__main__":
    unittest.main()
