import tempfile
import unittest
from pathlib import Path

from logger import logger
from risk_manager import RiskManager


logger.disabled = True


class RiskManagerTest(unittest.TestCase):
    def make_risk(self, path: Path) -> RiskManager:
        return RiskManager(
            balance=1000,
            max_loss_day=5,
            max_profit_day=10,
            max_trades_day=3,
            daily_trailing_start=0,
            daily_trailing_lock=0,
            max_stake_pct=0.02,
            fixed_stake=1,
            min_stake=0.35,
            max_stake=100,
            max_consecutive_losses=2,
            use_soros=False,
            soros_max_steps=1,
            soros_profit_factor=1.0,
            state_path=str(path),
        )

    def test_blocks_after_daily_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = self.make_risk(Path(tmp) / "risk.json")
            risk.update(profit=-3, buy_price=3)
            risk.update(profit=-2, buy_price=2)

            self.assertFalse(risk.can_trade())

    def test_blocks_after_max_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = self.make_risk(Path(tmp) / "risk.json")
            risk.update(profit=0.1, buy_price=1)
            risk.update(profit=0.1, buy_price=1)
            risk.update(profit=0.1, buy_price=1)

            self.assertFalse(risk.can_trade())

    def test_persists_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "risk.json"
            risk = self.make_risk(path)
            risk.update(profit=-1, buy_price=1)

            restored = self.make_risk(path)
            self.assertEqual(restored.daily_loss, 1)
            self.assertEqual(restored.trades_today, 1)

    def test_trailing_daily_profit_blocks_risking_locked_profit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=20,
                max_profit_day=0,
                max_trades_day=10,
                daily_trailing_start=10,
                daily_trailing_lock=5,
                max_stake_pct=0.02,
                fixed_stake=6,
                min_stake=0.35,
                max_stake=100,
                max_consecutive_losses=5,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                state_path=str(Path(tmp) / "risk.json"),
            )
            risk.update(profit=10, buy_price=1)

            self.assertFalse(risk.can_trade())

    def test_soros_uses_previous_profit_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=20,
                max_profit_day=0,
                max_trades_day=10,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.02,
                fixed_stake=1,
                min_stake=0.35,
                max_stake=100,
                max_consecutive_losses=5,
                use_soros=True,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                state_path=str(Path(tmp) / "risk.json"),
            )
            risk.update(profit=0.85, buy_price=1)

            self.assertEqual(risk.get_stake(), 1.85)


if __name__ == "__main__":
    unittest.main()
