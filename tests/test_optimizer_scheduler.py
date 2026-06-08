import unittest
from unittest.mock import patch

import optimize_loop


class OptimizerSchedulerTest(unittest.TestCase):
    def test_optimization_months_respects_configured_range(self) -> None:
        months = optimize_loop.optimization_months()

        self.assertEqual(months[0]["start"], "2026-01-01")
        self.assertEqual(months[-1]["end"], "2026-05-31")
        self.assertEqual(len(months), 5)

    def test_month_for_worker_spreads_workers_across_all_months(self) -> None:
        months = optimize_loop.optimization_months()

        with patch.object(optimize_loop, "N_WORKERS", 9):
            round_zero = [optimize_loop.month_for_worker(months, 0, idx)["name"] for idx in range(9)]
            round_one = [optimize_loop.month_for_worker(months, 1, idx)["name"] for idx in range(9)]

        self.assertEqual(set(round_zero), {m["name"] for m in months})
        self.assertEqual(set(round_one), {m["name"] for m in months})
        self.assertNotEqual(round_zero[0], round_one[0])


if __name__ == "__main__":
    unittest.main()
