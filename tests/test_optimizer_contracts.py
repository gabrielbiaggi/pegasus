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
            "CONTRACT_MODE": "multiplier",
            "STAKE": "13.0",
            "FRANKENSTEIN_USE_SOROS": "true",
        }

        safe = optimize_loop.sanitize_params_for_storage(params)

        self.assertNotIn("DERIV_TOKEN", safe)
        self.assertNotIn("DERIV_PAT", safe)
        self.assertNotIn("PG_DSN", safe)
        self.assertEqual(safe["SYMBOL"], "BOOM1000")
        self.assertEqual(safe["CONTRACT_MODE"], "multiplier")
        self.assertEqual(safe["STAKE"], "13.0")
        self.assertEqual(safe["_optimizer_context"]["symbol"], "BOOM1000")
        self.assertEqual(safe["_optimizer_context"]["contract_mode"], "multiplier")

    def test_optimizer_context_rejects_old_market_champion(self) -> None:
        current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000"})
        old_champion = {
            "SYMBOL": "1HZ100V",
            "CONTRACT_MODE": "calm_accu",
        }

        self.assertFalse(optimize_loop.params_match_context(old_champion, current))

    def test_optimizer_context_accepts_current_boom_multiplier_champion(self) -> None:
        current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000"})
        champion = {
            "_optimizer_context": {
                "symbol": "BOOM1000",
                "contract_mode": "multiplier",
            },
            "STAKE": "13.0",
        }

        self.assertTrue(optimize_loop.params_match_context(champion, current))

    def test_live_deploy_gate_rejects_negative_multiplier_candidate(self) -> None:
        self.assertFalse(
            optimize_loop.is_live_deployable(
                {
                    "avg_daily_profit": -44.37,
                    "consistency_pct": 0.0,
                    "worst_day_pnl": -50.0,
                    "active_days": 155,
                }
            )
        )

    def test_live_deploy_gate_accepts_consistent_daily_doubler(self) -> None:
        self.assertTrue(
            optimize_loop.is_live_deployable(
                {
                    "avg_daily_profit": 55.0,
                    "consistency_pct": 85.0,
                    "worst_day_pnl": -10.0,
                    "active_days": 155,
                }
            )
        )

    def test_parse_optimizer_workers_clamps_invalid_and_extreme_values(self) -> None:
        self.assertEqual(optimize_loop.parse_optimizer_workers({}, default=6), 6)
        self.assertEqual(optimize_loop.parse_optimizer_workers({"PEGASUS_OPTIMIZER_WORKERS": "2"}, default=6), 2)
        self.assertEqual(optimize_loop.parse_optimizer_workers({"PEGASUS_OPTIMIZER_WORKERS": "99"}, default=6), 12)
        self.assertEqual(optimize_loop.parse_optimizer_workers({"PEGASUS_OPTIMIZER_WORKERS": "bad"}, default=6), 6)
        self.assertEqual(optimize_loop.parse_optimizer_workers({"PEGASUS_OPTIMIZER_WORKERS": "0"}, default=6), 1)

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

    def test_read_optimizer_workers_can_filter_stale_files_for_live_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            stale = logs / "backtest_worker_old.json"
            stale.write_text(json.dumps({
                "current_day_index": 31,
                "total_days": 31,
                "elapsed_s": 10,
                "current_month": "Janeiro",
            }))
            old_ts = time.time() - 3600
            os.utime(stale, (old_ts, old_ts))

            active = logs / "backtest_worker_Jan_r0_w0.json"
            active.write_text(json.dumps({
                "current_day_index": 5,
                "total_days": 31,
                "elapsed_s": 20,
                "current_month": "Janeiro",
            }))

            workers = dashboard_app._read_optimizer_workers(logs, now=time.time(), include_stale=False)

        self.assertEqual([worker["worker_id"] for worker in workers], ["Jan_r0_w0"])

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

    def test_build_crossover_env_removes_dashboard_context_dict(self) -> None:
        champ_info = {
            "params": {
                "STAKE": "15.0",
                "CONTRACT_MODE": "multiplier",
                "SYMBOL": "BOOM1000",
                "_optimizer_context": {"contract_mode": "multiplier", "symbol": "BOOM1000"},
            }
        }

        env = optimize_loop.build_crossover_env(champ_info)

        self.assertEqual(env["STAKE"], "15.0")
        self.assertEqual(env["START_DATE"], "2026-01-01")
        self.assertEqual(env["END_DATE"], "2026-06-04")
        self.assertNotIn("_optimizer_context", env)
        self.assertTrue(all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()))

    def test_build_monthly_champion_entry_keeps_dashboard_metrics(self) -> None:
        params = {"STAKE": "17.8", "CONTRACT_MODE": "multiplier", "SYMBOL": "BOOM1000"}
        metrics = {
            "score": 2344.7469,
            "avg_daily_profit": 103.13,
            "total_pnl": 2887.64,
            "consistency_pct": 92.9,
            "positive_days": 26,
            "active_days": 28,
            "worst_day_pnl": -12.5,
            "max_drawdown": 36.2,
        }

        entry = optimize_loop.build_monthly_champion_entry(params, metrics)

        self.assertEqual(entry["score"], 2344.7469)
        self.assertEqual(entry["avg_daily_profit"], 103.13)
        self.assertEqual(entry["consistency_pct"], 92.9)
        self.assertEqual(entry["positive_days"], 26)
        self.assertEqual(entry["active_days"], 28)
        self.assertEqual(entry["params"]["STAKE"], "17.8")

    def test_merge_optimizer_candidates_preserves_live_workers_not_in_saved_state(self) -> None:
        saved = [{"worker_id": "Jan_r0_w0", "status": "Finalizado"}]
        workers = [
            {"worker_id": "Jan_r0_w0", "status": "Finalizado", "progress_pct": 100.0},
            {"worker_id": "Mar_r1_w8", "status": "Simulando...", "progress_pct": 42.0},
        ]

        merged = dashboard_app._merge_optimizer_candidates(saved, workers)

        self.assertEqual([item["worker_id"] for item in merged], ["Mar_r1_w8", "Jan_r0_w0"])
        self.assertEqual(merged[0]["progress_pct"], 42.0)
        self.assertEqual(merged[1]["progress_pct"], 100.0)

    def test_merge_optimizer_candidates_ignores_unsaved_finished_worker_files(self) -> None:
        saved = [{"worker_id": "Fev_r4_w0", "status": "Simulando..."}]
        workers = [
            {"worker_id": "Fev_r4_w0", "status": "Simulando...", "progress_pct": 30.0},
            {"worker_id": "Fev_r0_w0", "status": "Finalizado", "progress_pct": 100.0},
        ]

        merged = dashboard_app._merge_optimizer_candidates(saved, workers)

        self.assertEqual([item["worker_id"] for item in merged], ["Fev_r4_w0"])

    def test_optimizer_dashboard_cards_prefer_current_live_workers_and_worker_limit(self) -> None:
        saved = [
            {"worker_id": "Jan_r0_w0", "status": "Simulando...", "STAKE": "11"},
            {"worker_id": "Jan_r0_w1", "status": "Finalizado", "STAKE": "12"},
            {"worker_id": "Jan_r0_w2", "status": "Finalizado", "STAKE": "13"},
            {"worker_id": "Jan_r0_w3", "status": "Finalizado", "STAKE": "14"},
            {"worker_id": "Jan_r0_w4", "status": "Finalizado", "STAKE": "15"},
            {"worker_id": "Jan_r0_w5", "status": "Finalizado", "STAKE": "16"},
            {"worker_id": "Jan_r0_w6", "status": "Finalizado", "STAKE": "17"},
            {"worker_id": "Jan_r0_w7", "status": "Finalizado", "STAKE": "18"},
            {"worker_id": "Jan_r0_w8", "status": "Finalizado", "STAKE": "19"},
        ]
        workers = [
            {"worker_id": "Abr_r3_w0", "status": "Simulando...", "progress_pct": 76.7},
            {"worker_id": "Abr_r3_w1", "status": "Finalizado", "progress_pct": 100.0},
            {"worker_id": "Abr_r3_w2", "status": "Finalizado", "progress_pct": 100.0},
            {"worker_id": "Abr_r3_w3", "status": "Finalizado", "progress_pct": 100.0},
            {"worker_id": "Abr_r3_w4", "status": "Finalizado", "progress_pct": 100.0},
            {"worker_id": "Abr_r3_w5", "status": "Finalizado", "progress_pct": 100.0},
        ]

        cards = dashboard_app._optimizer_dashboard_cards(saved, workers, n_workers=6, running=True)

        self.assertEqual([item["worker_id"] for item in cards], [f"Abr_r3_w{i}" for i in range(6)])
        self.assertEqual(cards[0]["progress_pct"], 76.7)

    def test_optimizer_dashboard_cards_keep_state_declared_current_round(self) -> None:
        saved = [{"worker_id": f"Fev_r4_w{i}", "status": "Simulando..."} for i in range(6)]
        workers = [
            {"worker_id": "Fev_r4_w0", "status": "Simulando...", "progress_pct": 96.4},
            {"worker_id": "cross_Junho", "status": "Simulando...", "progress_pct": 48.4},
            {"worker_id": "cross_Maio", "status": "Simulando...", "progress_pct": 54.8},
            {"worker_id": "par_0_Jan", "status": "Finalizado", "progress_pct": 100.0},
        ]

        cards = dashboard_app._optimizer_dashboard_cards(saved, workers, n_workers=6, running=True)

        self.assertEqual([item["worker_id"] for item in cards], [f"Fev_r4_w{i}" for i in range(6)])
        self.assertEqual(cards[0]["progress_pct"], 96.4)

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
