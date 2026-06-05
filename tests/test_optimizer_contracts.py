import json
import os
import tempfile
import time
import unittest
from pathlib import Path

import dashboard.app as dashboard_app
import backtest_engine
import optimize_loop


class OptimizerContractsTest(unittest.TestCase):
    def test_sanitize_params_removes_secrets_and_keeps_strategy_context(self) -> None:
        params = {
            "DERIV_TOKEN": "secret",
            "DERIV_PAT": "pat_secret",
            "PG_DSN": "postgresql://secret",
            "SYMBOL": "BOOM1000",
            "CONTRACT_MODE": "rise_fall",
            "STAKE": "13.0",
            "FRANKENSTEIN_USE_SOROS": "true",
        }

        safe = optimize_loop.sanitize_params_for_storage(params)

        self.assertNotIn("DERIV_TOKEN", safe)
        self.assertNotIn("DERIV_PAT", safe)
        self.assertNotIn("PG_DSN", safe)
        self.assertEqual(safe["SYMBOL"], "BOOM1000")
        self.assertEqual(safe["CONTRACT_MODE"], "rise_fall")
        self.assertEqual(safe["STAKE"], "13.0")
        self.assertEqual(safe["_optimizer_context"]["symbol"], "BOOM1000")
        self.assertEqual(safe["_optimizer_context"]["contract_mode"], "rise_fall")

    def test_optimizer_context_rejects_old_market_champion(self) -> None:
        current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000"})
        old_champion = {
            "SYMBOL": "1HZ100V",
            "CONTRACT_MODE": "calm_accu",
        }

        self.assertFalse(optimize_loop.params_match_context(old_champion, current))

    def test_optimizer_context_accepts_current_boom_rise_fall_champion(self) -> None:
        current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000"})
        champion = {
            "_optimizer_context": {
                "symbol": "BOOM1000",
                "contract_mode": "rise_fall",
            },
            "STAKE": "13.0",
        }

        self.assertTrue(optimize_loop.params_match_context(champion, current))

    def test_read_optimizer_workers_prefers_active_monthly_worker_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            stale = logs / "backtest_worker_w0.json"
            stale.write_text(json.dumps({
                "current_day_index": 31,
                "total_days": 31,
                "elapsed_s": 10,
                "current_month": "Maio",
            }))
            old_ts = time.time() - 3600
            os.utime(stale, (old_ts, old_ts))

            active = logs / "backtest_worker_Mar_r2_w0.json"
            active.write_text(json.dumps({
                "current_day_index": 14,
                "total_days": 28,
                "elapsed_s": 70,
                "current_month": "Fevereiro",
                "current_day": "2026-02-14",
            }))

            workers = dashboard_app._read_optimizer_workers(logs, now=time.time())

        self.assertEqual(len(workers), 2)
        self.assertEqual(workers[0]["worker_id"], "Mar_r2_w0")
        self.assertEqual(workers[0]["progress_pct"], 50.0)
        self.assertFalse(workers[0]["stale"])
        self.assertEqual(workers[1]["worker_id"], "w0")
        self.assertTrue(workers[1]["stale"])

    def test_sanitize_metrics_for_state_removes_runtime_payloads(self) -> None:
        metrics = {
            "avg_daily_profit": 12.3,
            "_env": {"DERIV_TOKEN": "secret"},
            "summary": {
                "results": [{"date": "2026-01-01", "pnl": 1.0}],
                "strategies": {"Super-Frankenstein": {"total_pnl": 12.3}},
            },
            "monthly_breakdown": {"Super-Frankenstein": {"Jan/26": {"pnl": 12.3}}},
        }

        safe = optimize_loop.sanitize_metrics_for_state(metrics)
        serialized = json.dumps(safe)

        self.assertNotIn("_env", safe)
        self.assertNotIn("results", safe["summary"])
        self.assertNotIn("secret", serialized)
        self.assertEqual(safe["avg_daily_profit"], 12.3)
        self.assertIn("monthly_breakdown", safe)

    def test_sanitize_env_for_worker_removes_dashboard_context_dict(self) -> None:
        env = {
            "STAKE": 13.0,
            "START_DATE": "2026-01-01",
            "_optimizer_context": {"symbol": "BOOM1000"},
            "BAD_LIST": [1, 2],
        }

        safe = optimize_loop.sanitize_env_for_worker(env)

        self.assertEqual(safe["STAKE"], "13.0")
        self.assertEqual(safe["START_DATE"], "2026-01-01")
        self.assertNotIn("_optimizer_context", safe)
        self.assertNotIn("BAD_LIST", safe)

    def test_compile_summary_metrics_tolerates_missing_strategy_keys(self) -> None:
        results = [
            {
                "date": "2026-01-01",
                "strategies": {
                    "Super-Frankenstein": {
                        "pnl": 5.0,
                        "trades": 1,
                        "signal_wr": 100.0,
                        "busted": False,
                    }
                },
            },
            {
                "date": "2026-01-02",
                "strategies": {
                    "Super-Frankenstein": {
                        "pnl": 3.0,
                        "trades": 1,
                        "signal_wr": 100.0,
                        "busted": False,
                    },
                    "Pegasus Live Sniper (9% TP)": {
                        "pnl": 1.0,
                        "trades": 1,
                        "signal_wr": 100.0,
                        "busted": False,
                    },
                },
            },
        ]

        metrics = backtest_engine.compile_summary_metrics(results, {}, 50.0)

        self.assertIsNotNone(metrics)
        self.assertEqual(
            metrics["summary"]["strategies"]["Super-Frankenstein"]["total_pnl"],
            8.0,
        )


if __name__ == "__main__":
    unittest.main()
