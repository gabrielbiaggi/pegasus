import json
import os
import unittest
from unittest.mock import patch

from bot import DerivBot
from config import load_config


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send(self, message: str) -> None:
        self.messages.append(json.loads(message))


class BotTest(unittest.IsolatedAsyncioTestCase):
    async def test_accumulator_proposal_payload(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "CONTRACT_MODE": "accumulator",
            "SYMBOL": "1HZ100V",
            "DRY_RUN": "false",
            "TICK_COUNT": "300",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            config = load_config()

        ws = FakeWebSocket()
        bot = DerivBot(config)

        await bot.request_accumulator_proposal(ws, 1.0, 7, 1_700_000_000)

        payload = ws.messages[0]
        self.assertEqual(payload["contract_type"], "ACCU")
        self.assertEqual(payload["growth_rate"], 0.03)
        self.assertEqual(payload["underlying_symbol"], "1HZ100V")
        self.assertNotIn("symbol", payload)  # new API uses underlying_symbol, not symbol
        self.assertNotIn("duration", payload)

    async def test_multiplier_proposal_payload(self) -> None:
        env = {
            "DERIV_TOKEN": "token",
            "DERIV_APP_ID": "1089",
            "CONTRACT_MODE": "multiplier",
            "SYMBOL": "BOOM1000",
            "DRY_RUN": "false",
            "MULTIPLIER_VALUE": "100",
            "MULTIPLIER_TAKE_PROFIT": "0.50",
            "MULTIPLIER_STOP_LOSS": "1.00",
        }
        with patch("config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            config = load_config()

        ws = FakeWebSocket()
        bot = DerivBot(config)

        await bot.request_multiplier_proposal(ws, 1.0, "MULTDOWN", 25, 1_700_000_000)

        payload = ws.messages[0]
        self.assertEqual(payload["contract_type"], "MULTDOWN")
        self.assertEqual(payload["underlying_symbol"], "BOOM1000")
        self.assertEqual(payload["multiplier"], 100)
        self.assertEqual(payload["limit_order"], {"take_profit": 0.5, "stop_loss": 1.0})
        self.assertNotIn("duration", payload)
        self.assertNotIn("duration_unit", payload)


if __name__ == "__main__":
    unittest.main()
