import unittest

from cooldown_rules import dynamic_cooldown_resume_ok


class DynamicCooldownResumeTest(unittest.TestCase):
    def test_boom_rf_uses_strategy_thresholds_instead_of_hardcoded_cusum(self) -> None:
        self.assertTrue(
            dynamic_cooldown_resume_ok(
                symbol="BOOM1000",
                max_abs_ret=2.0e-6,
                cusum=5.4,
                velocity=-0.00015,
                imbalance=-10.0,
                hurst=0.53,
                p_loss=0.18,
                cusum_limit=8.0,
                velocity_limit=0.001,
                imbalance_limit=1.5,
                ensemble_loss_threshold=0.52,
            )
        )

    def test_boom_rf_keeps_cooldown_when_loss_probability_is_high(self) -> None:
        self.assertFalse(
            dynamic_cooldown_resume_ok(
                symbol="BOOM1000",
                max_abs_ret=2.0e-6,
                cusum=5.4,
                velocity=-0.00015,
                imbalance=-10.0,
                hurst=0.53,
                p_loss=0.61,
                cusum_limit=8.0,
                velocity_limit=0.001,
                imbalance_limit=1.5,
                ensemble_loss_threshold=0.52,
            )
        )

    def test_boom_rf_keeps_cooldown_on_recent_spike(self) -> None:
        self.assertFalse(
            dynamic_cooldown_resume_ok(
                symbol="BOOM1000",
                max_abs_ret=2.0e-4,
                cusum=1.0,
                velocity=-0.0001,
                imbalance=-10.0,
                hurst=0.53,
                p_loss=0.18,
                cusum_limit=8.0,
                velocity_limit=0.001,
                imbalance_limit=1.5,
                ensemble_loss_threshold=0.52,
            )
        )


if __name__ == "__main__":
    unittest.main()
