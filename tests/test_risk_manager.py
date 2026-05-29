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
        """MAX_TRADES_PER_DAY já não bloqueia — somente STOP_LOSS/STOP_GAIN controlam.
        Verifica que o bot CONTINUA operando após atingir o limite de trades."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = self.make_risk(Path(tmp) / "risk.json")
            risk.update(profit=0.1, buy_price=1)
            risk.update(profit=0.1, buy_price=1)
            risk.update(profit=0.1, buy_price=1)

            # Bot continua — limite de trades não é mais um stop
            self.assertTrue(risk.can_trade())

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
                daily_trailing_start=10, # 10% of 1000 = 100 USD
                daily_trailing_lock=5,   # 5% of 1000 = 50 USD
                max_stake_pct=0.5,
                fixed_stake=60,
                min_stake=0.35,
                max_stake=100,
                max_consecutive_losses=5,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                dynamic_stake_base_pct=0,
                state_path=str(Path(tmp) / "risk.json"),
            )
            # Profit = 100 USD (reaches 10% trailing start)
            risk.update(profit=100, buy_price=1)

            # Since trailing is active, lock is 50 USD.
            # Remaining budget = P&L - lock = 100 - 50 = 50 USD.
            # Fixed stake is 60 USD.
            # Capped stake should be min(60, budget=50) = 50.
            self.assertEqual(risk.get_stake(), 50.0)
            self.assertTrue(risk.can_trade()) # Can trade with the capped stake of 50.0

            # Now lose 50.0. Net profit drops to 50.0 (exactly at lock).
            risk.update(profit=-50.0, buy_price=50.0)

            # Budget is now 50 - 50 = 0.0, which is below min_stake (0.35).
            # Stake becomes 0.0 and trading is blocked.
            self.assertEqual(risk.get_stake(), 0.0)
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
                dynamic_stake_base_pct=0.001,  # 1000 * 0.001 = 1
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
                dynamic_stake_base_pct=0.0001,  # 100000 * 0.0001 = 10
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
                dynamic_stake_base_pct=0.0001,  # 100000 * 0.0001 = 10
                state_path=str(Path(tmp) / "risk.json"),
            )
            s0 = risk.get_stake()  # 10.0
            risk.update(profit=-s0, buy_price=s0)  # gale 1
            s1 = risk.get_stake()
            risk.update(profit=-s1, buy_price=s1)  # gale 2
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
            risk.update(profit=-1, buy_price=1)  # G0 perde → step=1
            risk.update(profit=-1, buy_price=1)  # G1 perde → step=2
            risk.update(profit=-1, buy_price=1)  # G2 perde → step=3
            risk.update(profit=-1, buy_price=1)  # G3 (last gale) perde → RESET → step=0
            # Após perder o último gale, a sequência reseta para G0 e o bot continua.
            # Regra atual: somente STOP_LOSS/STOP_GAIN bloqueiam can_trade().
            self.assertTrue(risk.can_trade())
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)

    def test_pct_based_stop_loss(self) -> None:
        """stop_loss_pct=5 on balance=1000 → effective limit = 50. Fixed max_loss_day is ignored."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=9999,  # high fixed $, should be ignored when pct > 0
                max_profit_day=0,
                max_trades_day=100,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=1.0,
                fixed_stake=10,
                min_stake=0.35,
                max_stake=500,
                max_consecutive_losses=100,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                max_losses_in_window=100,
                state_path=str(Path(tmp) / "risk.json"),
                stop_loss_pct=5.0,  # 5% of 1000 = 50
            )
            # Simulate Deriv balance stream updating balance after loss
            risk.update(profit=-49, buy_price=49)
            risk.balance = 951  # 1000 - 49
            # start_of_day_balance = 951 - (-49) = 1000 → effective limit = 50
            self.assertTrue(risk.can_trade())  # net=-49, limit=-50 → OK
            risk.update(profit=-2, buy_price=2)
            risk.balance = 949  # 951 - 2
            # start_of_day_balance = 949 - (-51) = 1000 → effective limit = 50
            self.assertFalse(risk.can_trade())  # net=-51, limit=-50 → BLOCKED

    def test_pct_based_stop_gain(self) -> None:
        """stop_gain_pct=10 on balance=1000 → effective profit target = 100."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=1000,
                max_loss_day=500,
                max_profit_day=0,  # no fixed $ target
                max_trades_day=100,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=1.0,
                fixed_stake=10,
                min_stake=0.35,
                max_stake=500,
                max_consecutive_losses=100,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                max_losses_in_window=100,
                state_path=str(Path(tmp) / "risk.json"),
                stop_gain_pct=10.0,  # 10% of 1000 = 100
            )
            # Simulate Deriv balance stream updating balance after win
            risk.update(profit=99, buy_price=10)
            risk.balance = 1099  # 1000 + 99
            # start_of_day_balance = 1099 - 99 = 1000 → effective target = 100
            self.assertTrue(risk.can_trade())  # net=99, target=100 → OK
            risk.update(profit=2, buy_price=10)
            risk.balance = 1101  # 1099 + 2
            # start_of_day_balance = 1101 - 101 = 1000 → effective target = 100
            self.assertFalse(risk.can_trade())  # net=101, target=100 → BLOCKED

    def test_fibonacci_stake_sequence(self) -> None:
        """Fibonacci mode uses FIB_SEQUENCE multipliers instead of classic martingale formula."""
        from risk_manager import FIB_SEQUENCE

        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=100_000,
                max_loss_day=50_000,
                max_profit_day=0,
                max_trades_day=50,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.5,
                fixed_stake=0.35,
                min_stake=0.35,
                max_stake=0,
                max_consecutive_losses=20,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=7,
                martingale_payout_rate=0.953,
                martingale_mode="fibonacci",
                dynamic_stake_base_pct=0,
                max_losses_in_window=100,
                state_path=str(Path(tmp) / "risk.json"),
            )
            # G0: base stake
            self.assertEqual(risk.get_stake(), 0.35)

            # Lose G0 → step=1, stake = 0.35 * FIB[1] = 0.35 * 1 = 0.35
            risk.update(profit=-0.35, buy_price=0.35)
            self.assertEqual(risk.martingale_step, 1)
            self.assertAlmostEqual(risk.get_stake(), 0.35, places=2)

            # Lose G1 → step=2, stake = 0.35 * FIB[2] = 0.35 * 2 = 0.70
            risk.update(profit=-0.35, buy_price=0.35)
            self.assertEqual(risk.martingale_step, 2)
            self.assertAlmostEqual(risk.get_stake(), 0.70, places=2)

            # Lose G2 → step=3, stake = 0.35 * FIB[3] = 0.35 * 3 = 1.05
            risk.update(profit=-0.70, buy_price=0.70)
            self.assertEqual(risk.martingale_step, 3)
            self.assertAlmostEqual(risk.get_stake(), 1.05, places=2)

            # Win at step 3 → step = max(0, 3-2) = 1
            risk.update(profit=1.00, buy_price=1.05)
            self.assertEqual(risk.martingale_step, 1)

            # Win at step 1 → step = max(0, 1-2) = 0 → full reset
            risk.update(profit=0.33, buy_price=0.35)
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)
            self.assertEqual(risk.martingale_base_stake, 0.0)

    def test_fibonacci_absorbs_at_max(self) -> None:
        """Fibonacci absorbs losses and resets when step >= min(max_gales, len(FIB)-1)."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=100_000,
                max_loss_day=50_000,
                max_profit_day=0,
                max_trades_day=50,
                daily_trailing_start=0,
                daily_trailing_lock=0,
                max_stake_pct=0.5,
                fixed_stake=0.35,
                min_stake=0.35,
                max_stake=0,
                max_consecutive_losses=20,
                use_soros=False,
                soros_max_steps=1,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=3,  # max 3 gales
                martingale_payout_rate=0.953,
                martingale_mode="fibonacci",
                dynamic_stake_base_pct=0,
                max_losses_in_window=100,
                state_path=str(Path(tmp) / "risk.json"),
            )
            # Lose 4 times: step reaches max_gales=3 on 3rd loss, 4th loss triggers absorb+reset
            risk.update(profit=-0.35, buy_price=0.35)  # step 0→1
            risk.update(profit=-0.35, buy_price=0.35)  # step 1→2
            risk.update(profit=-0.70, buy_price=0.70)  # step 2→3
            self.assertEqual(risk.martingale_step, 3)  # not yet absorbed
            risk.update(profit=-1.05, buy_price=1.05)  # step 3 >= max_gales=3 → RESET
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)
            # Lose one more → should start fresh at step=1
            risk.update(profit=-0.35, buy_price=0.35)
            self.assertEqual(risk.martingale_step, 1)

    def test_simulated_balance_soros_martingale(self) -> None:
        """Verifica se a banca simulada de $50 com stake base de $5, Soros e Martingale ativos
        calcula as stakes, deduz saldos, acumula ganhos e recupera perdas de forma correta."""
        with tempfile.TemporaryDirectory() as tmp:
            risk = RiskManager(
                balance=10000.0,  # real balance is high
                max_loss_day=9999.0,
                max_profit_day=9999.0,
                max_trades_day=50,
                daily_trailing_start=0.0,
                daily_trailing_lock=0.0,
                max_stake_pct=1.0,
                fixed_stake=5.0,
                min_stake=5.0,
                max_stake=100.0,
                max_consecutive_losses=3,
                use_soros=True,
                soros_max_steps=2,
                soros_profit_factor=1.0,
                use_martingale=True,
                martingale_max_gales=2,
                martingale_payout_rate=0.50,
                dynamic_stake_base_pct=0.0,
                state_path=str(Path(tmp) / "risk.json"),
                simulated_balance=50.0,  # simulated balance mode active!
            )
            # 1. Init: simulated_balance should override self.balance and self.start_of_day_balance
            self.assertTrue(risk.simulated_balance_mode)
            self.assertEqual(risk.balance, 50.0)
            self.assertEqual(risk.start_of_day_balance, 50.0)

            # 2. Get G0 stake
            s0 = risk.get_stake()
            self.assertEqual(s0, 5.0)

            # Simulate buying G0: in production bot.py deducts this immediately
            risk.balance = round(risk.balance - s0, 2)
            risk._pending_stake_deduction = s0
            self.assertEqual(risk.balance, 45.0)

            # Win G0: profit is 50% = 2.50. Update should add stake (5.0) back + profit (2.50)
            risk.update(profit=2.50, buy_price=5.0)
            self.assertEqual(risk.balance, 52.50)
            self.assertEqual(risk.daily_net_profit, 2.50)
            self.assertEqual(risk.soros_step, 1)
            self.assertEqual(risk.soros_profit, 2.50)

            # 3. Get Soros Step 1 stake (base + accumulated profit = 5.0 + 2.50 = 7.50)
            s1 = risk.get_stake()
            self.assertEqual(s1, 7.50)

            # Simulate buying Soros Step 1
            risk.balance = round(risk.balance - s1, 2)
            risk._pending_stake_deduction = s1
            self.assertEqual(risk.balance, 45.0)

            # Lose Soros Step 1: profit is -7.50. Risk resets Soros and enters Martingale Step 1.
            # As planned, Martingale base stake should be self.fixed_stake = 5.0, NOT the Soros stake of 7.50.
            risk.update(profit=-7.50, buy_price=7.50)
            self.assertEqual(risk.balance, 45.0)  # already deducted during buy, so unchanged on loss
            self.assertEqual(risk.soros_step, 0)
            self.assertEqual(risk.soros_profit, 0.0)
            self.assertEqual(risk.martingale_step, 1)
            self.assertEqual(risk.martingale_base_stake, 5.0)
            self.assertEqual(risk.martingale_accumulated_loss, 5.0)

            # 4. Get Martingale Step 1 stake (accumulated_loss / payout + base = 5.0 / 0.50 + 5.0 = 15.0)
            sm1 = risk.get_stake()
            self.assertEqual(sm1, 15.0)

            # Simulate buying Martingale Step 1
            risk.balance = round(risk.balance - sm1, 2)
            risk._pending_stake_deduction = sm1
            self.assertEqual(risk.balance, 30.0)

            # Win Martingale Step 1: profit is 15.0 * 0.50 = 7.50.
            # Update should add back stake (15.0) + profit (7.50) = 22.50.
            # Net balance becomes 30.0 + 22.50 = 52.50.
            # PNL becomes recovered (+2.50 from the first trade).
            # Martingale recovers and resets.
            risk.update(profit=7.50, buy_price=15.0)
            self.assertEqual(risk.balance, 52.50)
            self.assertEqual(risk.martingale_step, 0)
            self.assertEqual(risk.martingale_accumulated_loss, 0.0)


if __name__ == "__main__":
    unittest.main()
