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

    def test_wins_offset_losses_for_stop(self) -> None:
        """Stop is based on net P&L, not gross losses. Wins that recover losses should not trigger stop."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=5,
                max_profit_day=0,
                max_trades_day=300,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.02,
                fixed_stake=1,
                min_stake=0.35,
                max_stake=100,
                max_consecutive_losses=100,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                max_losses_in_window=100,  # disable frequency guard for this test
                state_path=str(Path(tmp) / "risk.json"),
            )
            # max_loss_day=5; lose 4, win back 4, lose 4 → net = -4, gross_loss = 8
            # Old logic would block (8 >= 5); new logic must NOT block (-4 > -5)
            risk.update(profit=-4, buy_price=4)
            risk.update(profit=4, buy_price=1)
            risk.update(profit=-4, buy_price=4)
            self.assertTrue(risk.can_trade())  # net = -4, still above -5 threshold
            # One more loss pushes net to -5 → should block
            risk.update(profit=-1, buy_price=1)
            self.assertFalse(risk.can_trade())  # net = -5 == -max_loss_day

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

    def test_martingale_recovery_stake_formula(self) -> None:
        """Gale stake deve recuperar todas as perdas + lucro original no payout configurado."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=100_000,
                max_loss_day=50_000,
                max_profit_day=0,
                max_trades_day=50,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.5,
                fixed_stake=10,
                min_stake=0.35,
                max_stake=0,  # sem cap absoluto para testar a formula pura
                max_consecutive_losses=10,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=3,
                martingale_payout_rate=0.15,
                state_path=str(Path(tmp) / "risk.json"),
            )
            # Gale 0: aposta base
            self.assertEqual(risk.martingale_step, 0)
            s0 = risk.get_stake()
            self.assertAlmostEqual(s0, 10.0, places=2)

            # Apos loss: gale 1 = acumulado/payout + base = 10/0.15 + 10 = 76.67
            risk.update(profit=-s0, buy_price=s0)
            self.assertEqual(risk.martingale_step, 1)
            s1 = risk.get_stake()
            expected_s1 = round(s0 / 0.15 + s0, 2)
            self.assertAlmostEqual(s1, expected_s1, places=2)
            # Verificacao: WIN em s1 recupera s0 e ainda lucra s0*payout
            self.assertAlmostEqual(s1 * 0.15, s0 + s0 * 0.15, places=1)

            # Apos 2 losses: gale 2 = (s0+s1)/0.15 + s0
            risk.update(profit=-s1, buy_price=s1)
            self.assertEqual(risk.martingale_step, 2)
            s2 = risk.get_stake()
            expected_s2 = round((s0 + s1) / 0.15 + s0, 2)
            self.assertAlmostEqual(s2, expected_s2, places=2)
            # Verificacao: WIN em s2 recupera s0+s1 e ainda lucra s0*payout
            self.assertAlmostEqual(s2 * 0.15, s0 + s1 + s0 * 0.15, places=1)

    def test_martingale_resets_on_win(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=100_000,
                max_loss_day=50_000,
                max_profit_day=0,
                max_trades_day=50,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.5,
                fixed_stake=10,
                min_stake=0.35,
                max_stake=0,
                max_consecutive_losses=10,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=3,
                martingale_payout_rate=0.15,
                state_path=str(Path(tmp) / "risk.json"),
            )
            s0 = risk.get_stake()   # 10.0
            risk.update(profit=-s0, buy_price=s0)   # gale 1
            s1 = risk.get_stake()
            risk.update(profit=-s1, buy_price=s1)   # gale 2
            self.assertEqual(risk.martingale_step, 2)
            accum_before = risk.martingale_accumulated_loss  # s0 + s1

            # Partial win: profit = s1*0.15 < accumulated_loss → stay in recovery
            partial_profit = round(s1 * 0.15, 2)
            risk.update(profit=partial_profit, buy_price=s1)
            self.assertEqual(risk.martingale_step, 2)  # still recovering
            self.assertAlmostEqual(
                risk.martingale_accumulated_loss,
                round(max(0.0, accum_before - partial_profit), 2),
                places=2,
            )

            # Full win: profit covers remaining accumulated_loss → reset
            big_profit = risk.martingale_accumulated_loss + 1.0  # more than enough
            risk.update(profit=big_profit, buy_price=s1)
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)
            self.assertEqual(risk.martingale_base_stake, 0.0)
            self.assertAlmostEqual(risk.get_stake(), s0, places=2)  # back to base

    def test_martingale_stops_after_max_gales(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=200,
                max_profit_day=0,
                max_trades_day=50,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.5,
                fixed_stake=1,
                min_stake=0.35,
                max_stake=100,
                max_consecutive_losses=4,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=3,
                martingale_payout_rate=0.15,
                state_path=str(Path(tmp) / "risk.json"),
            )
            # 4 consecutive losses → stop (max_consecutive_losses=4)
            risk.update(profit=-1, buy_price=1)   # G0 perde → step=1
            risk.update(profit=-1, buy_price=1)   # G1 perde → step=2
            risk.update(profit=-1, buy_price=1)   # G2 perde → step=3
            risk.update(profit=-1, buy_price=1)   # G3 (last gale) perde → RESET → step=0
            self.assertFalse(risk.can_trade())
            # Após perder o último gale, a sequência reseta para G0 (começa de novo)
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)


if __name__ == "__main__":
    unittest.main()
