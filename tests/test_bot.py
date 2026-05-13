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
        self.assertEqual(payload["symbol"], "1HZ100V")
        self.assertNotIn("duration", payload)


if __name__ == "__main__":
    unittest.main()
