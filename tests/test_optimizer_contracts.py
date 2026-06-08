import json
import os
import random
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import dashboard.app as dashboard_app
import backtest_engine
import optimize_loop


class OptimizerContractsTest(unittest.TestCase):
    def test_rise_fall_non_crash_fast_path_can_emit_call_when_up_votes_dominate(self) -> None:
        signal, score = backtest_engine._resolve_rise_fall_fast_signal(
            symbol="1HZ25V",
            velocity_v=0.0020,
            imbalance_v=2.4,
            ols_v=0.0012,
            momentum_v=0.0031,
            ema_diff_v=0.0015,
            markov_up_v=0.72,
            markov_dn_v=0.31,
        )

        self.assertEqual(signal, "CALL")
        self.assertEqual(score, 6)

    def test_normalize_rise_fall_candidate_keeps_ensemble_and_selective_bounds(self) -> None:
        candidate = optimize_loop.normalize_candidate_params(
            {
                "SYMBOL": "1HZ25V",
                "CONTRACT_MODE": "rise_fall",
                "RISE_FALL_USE_ENSEMBLE": "false",
                "RISE_FALL_MIN_VOTES": "1",
                "RISE_FALL_COOLDOWN_TICKS": "1",
            }
        )

        self.assertEqual(candidate["RISE_FALL_USE_ENSEMBLE"], "true")
        self.assertGreaterEqual(int(candidate["RISE_FALL_MIN_VOTES"]), 3)
        self.assertGreaterEqual(int(candidate["RISE_FALL_COOLDOWN_TICKS"]), 6)

    def test_boom1000_global_search_biases_toward_spike_families(self) -> None:
        random.seed(7)
        params = {
            "SYMBOL": "BOOM1000",
            "CONTRACT_MODE": "multiplier",
        }

        candidate = optimize_loop.inject_global_multiplier_search(params)

        self.assertIn(candidate["MULTIPLIER_DIRECTION"], {"up", "signal", "down"})
        self.assertGreaterEqual(int(candidate["RISE_FALL_MIN_VOTES"]), 2)
        self.assertLessEqual(int(candidate["RISE_FALL_MIN_VOTES"]), 6)
        self.assertGreaterEqual(int(candidate["MULTIPLIER_MAX_HOLD_TICKS"]), 3)
        self.assertLessEqual(int(candidate["MULTIPLIER_MAX_HOLD_TICKS"]), 30)
        self.assertLessEqual(float(candidate["STAKE"]), 12.0)
        self.assertEqual(candidate["RISE_FALL_USE_ENSEMBLE"], "true")
        self.assertLessEqual(int(candidate["MULTIPLIER_VALUE"]), 25)
        self.assertIn(candidate["FRANKENSTEIN_USE_SOROS"], {"true", "false"})
        self.assertIn(candidate["FRANKENSTEIN_USE_MARTINGALE"], {"true", "false"})
        self.assertIn("TICK_COUNT", candidate)
        self.assertGreaterEqual(int(candidate["TICK_COUNT"]), 70)
        self.assertLessEqual(int(candidate["TICK_COUNT"]), 160)
        self.assertIn("PCS_XGB_BYPASS_LIMIT", candidate)
        self.assertIn("ENSEMBLE_MIN_PROB", candidate)

    def test_boom1000_sparse_metrics_force_directional_probe(self) -> None:
        random.seed(11)
        base = {
            "SYMBOL": "BOOM1000",
            "CONTRACT_MODE": "multiplier",
        }
        metrics = {"total_trades": 3, "avg_daily_profit": 0.0, "negative_days": 0, "consistency_pct": 0.0}

        candidate = optimize_loop.rand_params(base, metrics)

        self.assertIn(candidate["MULTIPLIER_DIRECTION"], {"up", "signal", "down"})
        self.assertEqual(candidate["RISE_FALL_USE_ENSEMBLE"], "true")
        self.assertGreaterEqual(int(candidate["RISE_FALL_MIN_VOTES"]), 2)
        self.assertLessEqual(int(candidate["RISE_FALL_MIN_VOTES"]), 6)
        self.assertIn("TICK_COUNT", candidate)
        self.assertIn("PCS_XGB_BYPASS_LIMIT", candidate)
        self.assertIn("ENSEMBLE_MIN_PROB", candidate)

    def test_normalize_boom1000_candidate_prunes_bad_regions(self) -> None:
        candidate = optimize_loop.normalize_candidate_params(
            {
                "SYMBOL": "BOOM1000",
                "CONTRACT_MODE": "multiplier",
                "MULTIPLIER_DIRECTION": "down",
                "MULTIPLIER_VALUE": "40",
                "MULTIPLIER_TAKE_PROFIT": "3.0",
                "MULTIPLIER_STOP_LOSS": "0.2",
                "MULTIPLIER_MAX_HOLD_TICKS": "45",
                "RISE_FALL_MIN_VOTES": "1",
                "RISE_FALL_COOLDOWN_TICKS": "40",
                "RISE_FALL_USE_ENSEMBLE": "false",
                "RISE_FALL_ENSEMBLE_MIN_PROB": "0.10",
                "RISE_FALL_MAX_CUSUM": "8.5",
                "RISE_FALL_MAX_VELOCITY": "0.005",
                "RISE_FALL_MAX_IMBALANCE": "5.0",
                "MULTIPLIER_JUMP_MIN_CONFIDENCE": "0.90",
                "MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE": "0.5",
                "MULTIPLIER_JUMP_BAYES_STRONG_PROB": "0.90",
                "MULTIPLIER_JUMP_MIN_SCORE": "9",
                "MULTIPLIER_JUMP_HURST_TRENDING": "0.90",
                "MULTIPLIER_JUMP_HURST_REVERTING": "0.60",
                "MULTIPLIER_JUMP_MI_FLOW_MIN": "0.30",
                "MULTIPLIER_JUMP_WAVELET_SNR_MIN": "0.10",
                "TICK_COUNT": "20",
                "PCS_XGB_BYPASS_LIMIT": "0.60",
                "ENSEMBLE_MIN_PROB": "0.02",
            }
        )

        self.assertEqual(candidate["MULTIPLIER_DIRECTION"], "down")
        self.assertEqual(candidate["MULTIPLIER_VALUE"], "25")
        self.assertEqual(candidate["RISE_FALL_USE_ENSEMBLE"], "true")
        self.assertEqual(candidate["RISE_FALL_MIN_VOTES"], "2")
        self.assertEqual(candidate["MULTIPLIER_MAX_HOLD_TICKS"], "30")
        self.assertEqual(candidate["MULTIPLIER_JUMP_MIN_CONFIDENCE"], "0.8")
        self.assertEqual(candidate["MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE"], "2.0")
        self.assertEqual(candidate["MULTIPLIER_JUMP_BAYES_STRONG_PROB"], "0.8")
        self.assertEqual(candidate["MULTIPLIER_JUMP_MIN_SCORE"], "7")
        self.assertEqual(candidate["TICK_COUNT"], "70")
        self.assertEqual(candidate["PCS_XGB_BYPASS_LIMIT"], "0.4")
        self.assertEqual(candidate["ENSEMBLE_MIN_PROB"], "0.12")

    def test_monthly_candidate_viability_rejects_sparse_month(self) -> None:
        self.assertFalse(
            optimize_loop.is_monthly_candidate_viable(
                {
                    "avg_daily_profit": 0.0,
                    "total_pnl": 0.0,
                    "active_days": 2,
                    "total_trades": 4,
                    "positive_days": 0,
                    "consistency_pct": 0.0,
                    "worst_day_pnl": -0.5,
                }
            )
        )

    def test_monthly_candidate_viability_accepts_dense_month(self) -> None:
        self.assertTrue(
            optimize_loop.is_monthly_candidate_viable(
                {
                    "avg_daily_profit": 0.72,
                    "total_pnl": 12.96,
                    "active_days": 18,
                    "total_trades": 64,
                    "positive_days": 6,
                    "consistency_pct": 33.3,
                    "worst_day_pnl": -8.0,
                }
            )
        )

    def test_monthly_candidate_viability_rejects_negative_dense_month(self) -> None:
        self.assertFalse(
            optimize_loop.is_monthly_candidate_viable(
                {
                    "avg_daily_profit": -0.08,
                    "total_pnl": -2.4,
                    "active_days": 18,
                    "total_trades": 64,
                    "positive_days": 6,
                    "consistency_pct": 33.3,
                    "worst_day_pnl": -8.0,
                }
            )
        )

    def test_build_monthly_champion_entry_marks_sparse_month_as_not_viable(self) -> None:
        entry = optimize_loop.build_monthly_champion_entry(
            {"SYMBOL": "BOOM1000", "CONTRACT_MODE": "multiplier", "STAKE": "5.0"},
            {
                "score": -25450.0,
                "avg_daily_profit": 0.0,
                "total_pnl": 0.0,
                "active_days": 0,
                "total_trades": 0,
                "positive_days": 0,
                "negative_days": 0,
                "consistency_pct": 0.0,
                "worst_day_pnl": 0.0,
            },
        )

        self.assertFalse(entry["candidate_viable"])
        self.assertFalse(entry["deployable"])

    def test_crossover_candidate_viability_rejects_single_month_concentration(self) -> None:
        self.assertFalse(
            optimize_loop.is_crossover_candidate_viable(
                {
                    "total_pnl": 5.61,
                    "active_days": 155,
                    "total_trades": 80,
                    "consistency_pct": 8.0,
                    "monthly_breakdown": {
                        "Super-Frankenstein": {
                            "2026-01": {"pnl": 0.0},
                            "2026-02": {"pnl": 0.0},
                            "2026-03": {"pnl": -0.01},
                            "2026-04": {"pnl": 0.0},
                            "2026-05": {"pnl": 5.62},
                            "2026-06": {"pnl": 0.0},
                        }
                    },
                }
            )
        )

    def test_crossover_candidate_viability_accepts_multi_month_distribution(self) -> None:
        self.assertTrue(
            optimize_loop.is_crossover_candidate_viable(
                {
                    "total_pnl": 12.5,
                    "active_days": 155,
                    "total_trades": 120,
                    "consistency_pct": 12.0,
                    "monthly_breakdown": {
                        "Super-Frankenstein": {
                            "2026-01": {"pnl": 2.0},
                            "2026-02": {"pnl": 1.8},
                            "2026-03": {"pnl": -0.5},
                            "2026-04": {"pnl": 1.2},
                            "2026-05": {"pnl": 5.0},
                            "2026-06": {"pnl": 3.0},
                        }
                    },
                }
            )
        )

    def test_compute_score_penalizes_sparse_near_zero_activity(self) -> None:
        results = []
        for idx in range(155):
            trades = 1 if idx == 0 else 0
            pnl = 0.01 if idx == 0 else 0.0
            results.append({
                "date": f"2026-01-{(idx % 31) + 1:02d}",
                "strategies": {"Super-Frankenstein": {"pnl": pnl, "trades": trades}},
            })

        metrics = optimize_loop.compute_score(results)

        self.assertEqual(metrics["active_days"], 1)
        self.assertEqual(metrics["total_trades"], 1)
        self.assertLess(metrics["score"], -500.0)

    def test_compute_score_rewards_broad_positive_coverage(self) -> None:
        results = []
        for idx in range(30):
            results.append({
                "date": f"2026-02-{idx + 1:02d}",
                "strategies": {"Super-Frankenstein": {"pnl": 2.0, "trades": 3}},
            })

        metrics = optimize_loop.compute_score(results)

        self.assertEqual(metrics["active_days"], 30)
        self.assertEqual(metrics["total_trades"], 90)
        self.assertGreater(metrics["score"], 0.0)

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

        with patch.dict(
            os.environ,
            {"OPTIMIZER_TARGET_SYMBOL": "", "OPTIMIZER_TARGET_CONTRACT_MODE": ""},
            clear=False,
        ):
            safe = optimize_loop.sanitize_params_for_storage(params)

        self.assertNotIn("DERIV_TOKEN", safe)
        self.assertNotIn("DERIV_PAT", safe)
        self.assertNotIn("PG_DSN", safe)
        self.assertEqual(safe["SYMBOL"], "BOOM1000")
        self.assertEqual(safe["CONTRACT_MODE"], "multiplier")
        self.assertEqual(safe["STAKE"], "13.0")
        self.assertEqual(safe["_optimizer_context"]["symbol"], "BOOM1000")
        self.assertEqual(safe["_optimizer_context"]["contract_mode"], "multiplier")

    def test_sanitize_env_clamps_stake_to_real_bounds(self) -> None:
        safe = optimize_loop.sanitize_env_for_worker(
            {
                "SYMBOL": "BOOM1000",
                "CONTRACT_MODE": "multiplier",
                "MIN_STAKE": "5",
                "MAX_STAKE": "100",
                "STAKE": "0.4",
            }
        )

        self.assertEqual(safe["STAKE"], "5.0")
        self.assertEqual(safe["MIN_STAKE"], "5.0")
        self.assertEqual(safe["MAX_STAKE"], "35.0")

    def test_multiplier_progression_knobs_are_searchable(self) -> None:
        self.assertNotIn("FRANKENSTEIN_USE_SOROS", optimize_loop.FROZEN_PARAMS)
        self.assertNotIn("FRANKENSTEIN_SOROS_STEPS", optimize_loop.FROZEN_PARAMS)
        self.assertNotIn("FRANKENSTEIN_USE_MARTINGALE", optimize_loop.FROZEN_PARAMS)
        self.assertNotIn("FRANKENSTEIN_MAX_GALES", optimize_loop.FROZEN_PARAMS)

    def test_translate_frankenstein_params_preserves_multiplier_progressions(self) -> None:
        with patch.dict(
            os.environ,
            {"OPTIMIZER_TARGET_SYMBOL": "", "OPTIMIZER_TARGET_CONTRACT_MODE": ""},
            clear=False,
        ):
            translated = optimize_loop.translate_frankenstein_params(
                {
                    "SYMBOL": "BOOM1000",
                    "CONTRACT_MODE": "multiplier",
                    "STAKE": "5",
                    "MULTIPLIER_TAKE_PROFIT": "0.50",
                    "FRANKENSTEIN_USE_SOROS": "true",
                    "FRANKENSTEIN_SOROS_STEPS": "2",
                    "FRANKENSTEIN_USE_MARTINGALE": "true",
                    "FRANKENSTEIN_MAX_GALES": "1",
                }
            )

        self.assertEqual(translated["USE_SOROS"], "true")
        self.assertEqual(translated["SOROS_MAX_STEPS"], "2")
        self.assertEqual(translated["USE_MARTINGALE"], "true")
        self.assertEqual(translated["MARTINGALE_MAX_GALES"], "1")
        self.assertEqual(translated["MARTINGALE_PAYOUT_RATE"], "0.1")

    def test_translate_frankenstein_params_respects_non_multiplier_optimizer_target(self) -> None:
        with patch.dict(
            os.environ,
            {"OPTIMIZER_TARGET_SYMBOL": "1HZ25V", "OPTIMIZER_TARGET_CONTRACT_MODE": "rise_fall"},
            clear=False,
        ):
            translated = optimize_loop.translate_frankenstein_params(
                {
                    "SYMBOL": "BOOM1000",
                    "CONTRACT_MODE": "multiplier",
                    "STAKE": "5",
                    "FRANKENSTEIN_USE_SOROS": "false",
                }
            )

        self.assertEqual(translated["SYMBOL"], "1HZ25V")
        self.assertEqual(translated["CONTRACT_MODE"], "rise_fall")
        self.assertNotIn("MULTIPLIER_VALUE", translated)

    def test_optimizer_context_rejects_old_market_champion(self) -> None:
        current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000"})
        old_champion = {
            "SYMBOL": "1HZ100V",
            "CONTRACT_MODE": "calm_accu",
        }

        self.assertFalse(optimize_loop.params_match_context(old_champion, current))

    def test_optimizer_context_accepts_current_boom_multiplier_champion(self) -> None:
        with patch.dict(
            os.environ,
            {"OPTIMIZER_TARGET_SYMBOL": "", "OPTIMIZER_TARGET_CONTRACT_MODE": ""},
            clear=False,
        ):
            current = optimize_loop.optimizer_context({"SYMBOL": "BOOM1000", "CONTRACT_MODE": "multiplier"})
            champion = {
                "_optimizer_context": {
                    "symbol": "BOOM1000",
                    "contract_mode": "multiplier",
                },
                "STAKE": "13.0",
            }

            self.assertTrue(optimize_loop.params_match_context(champion, current))

    def test_optimizer_context_can_follow_explicit_target_market(self) -> None:
        with patch.dict(
            os.environ,
            {"OPTIMIZER_TARGET_SYMBOL": "1HZ25V", "OPTIMIZER_TARGET_CONTRACT_MODE": "rise_fall"},
            clear=False,
        ):
            current = optimize_loop.optimizer_context({})

        self.assertEqual(current["symbol"], "1HZ25V")
        self.assertEqual(current["contract_mode"], "rise_fall")

    def test_optimizer_context_follows_primary_env_when_target_override_is_empty(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SYMBOL": "1HZ25V",
                "CONTRACT_MODE": "rise_fall",
                "OPTIMIZER_TARGET_SYMBOL": "",
                "OPTIMIZER_TARGET_CONTRACT_MODE": "",
            },
            clear=False,
        ):
            current = optimize_loop.optimizer_context({})

        self.assertEqual(current["symbol"], "1HZ25V")
        self.assertEqual(current["contract_mode"], "rise_fall")

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

    def test_read_optimizer_workers_filters_by_current_run_id_and_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            (logs / "backtest_worker_Jan_r0_w0.json").write_text(json.dumps({
                "current_day_index": 6,
                "total_days": 31,
                "elapsed_s": 12,
                "current_month": "Janeiro",
                "optimizer_run_id": "run-new",
                "optimizer_context": {"symbol": "1HZ25V", "contract_mode": "rise_fall"},
            }))
            (logs / "backtest_worker_Fev_r0_w1.json").write_text(json.dumps({
                "current_day_index": 6,
                "total_days": 28,
                "elapsed_s": 12,
                "current_month": "Fevereiro",
                "optimizer_run_id": "run-old",
                "optimizer_context": {"symbol": "BOOM1000", "contract_mode": "multiplier"},
            }))

            workers = dashboard_app._read_optimizer_workers(
                logs,
                now=time.time(),
                include_stale=False,
                run_id="run-new",
                optimizer_context={"symbol": "1HZ25V", "contract_mode": "rise_fall"},
            )

        self.assertEqual([worker["worker_id"] for worker in workers], ["Jan_r0_w0"])

    def test_read_optimizer_workers_accepts_refinement_worker_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            (logs / "backtest_worker_ref_0.json").write_text(json.dumps({
                "current_day_index": 12,
                "total_days": 31,
                "elapsed_s": 10,
                "current_day": "2026-01-12",
                "current_month": "Janeiro",
                "optimizer_run_id": "run-ref",
                "optimizer_context": {"symbol": "1HZ25V", "contract_mode": "rise_fall"},
            }))

            workers = dashboard_app._read_optimizer_workers(
                logs,
                now=time.time(),
                include_stale=False,
                run_id="run-ref",
                optimizer_context={"symbol": "1HZ25V", "contract_mode": "rise_fall"},
            )

        self.assertEqual([worker["worker_id"] for worker in workers], ["ref_0"])

    def test_reset_optimizer_runtime_state_removes_stale_worker_progress_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            (logs / "backtest_worker_Jan_r0_w0.json").write_text("{}")
            (logs / "backtest_worker_Fev_r0_w1.json").write_text("{}")
            state_path = logs / "optimizer_state.json"
            state_path.write_text(json.dumps({"phase": "old"}))

            run_id = optimize_loop.reset_optimizer_runtime_state(
                logs_dir=logs,
                state_path=state_path,
                context={"symbol": "1HZ25V", "contract_mode": "rise_fall"},
            )

            self.assertTrue(run_id)
            self.assertFalse((logs / "backtest_worker_Jan_r0_w0.json").exists())
            self.assertFalse((logs / "backtest_worker_Fev_r0_w1.json").exists())
            self.assertTrue(state_path.exists())

    def test_ensure_optimizer_db_healthy_rotates_malformed_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            db_path = logs / "results.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.write_text("not a sqlite database")

            ok = optimize_loop.ensure_optimizer_db_healthy(db_path)

            self.assertTrue(ok)
            self.assertTrue(db_path.exists())
            backups = list(logs.glob("results.db.corrupt-*"))
            self.assertTrue(backups)

    def test_build_refinement_seed_pool_keeps_search_alive_without_crossover_winner(self) -> None:
        monthly_states = {
            "2026-01": {
                "best_env": {"SYMBOL": "1HZ25V", "CONTRACT_MODE": "rise_fall", "STAKE": "7.0"},
                "best_metrics": {"score": -12.0, "avg_daily_profit": -0.2, "positive_days": 8, "active_days": 20},
            },
            "2026-02": {
                "best_env": {"SYMBOL": "1HZ25V", "CONTRACT_MODE": "rise_fall", "STAKE": "9.0"},
                "best_metrics": {"score": -8.0, "avg_daily_profit": 0.1, "positive_days": 9, "active_days": 18},
            },
        }

        pool = optimize_loop.build_refinement_seed_pool(
            monthly_states=monthly_states,
            crossover_results=[],
            best_env={"SYMBOL": "1HZ25V", "CONTRACT_MODE": "rise_fall", "STAKE": "5.0"},
            best_data={"score": -20.0, "avg_daily_profit": -1.0},
        )

        self.assertGreaterEqual(len(pool), 3)
        stakes = sorted({env["STAKE"] for env, _metrics in pool})
        self.assertEqual(stakes, ["5.0", "7.0", "9.0"])

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
        self.assertEqual(env["END_DATE"], "2026-05-31")
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

    def test_optimizer_dashboard_cards_keep_declared_round_before_progress_files_exist(self) -> None:
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

        self.assertEqual([item["worker_id"] for item in cards], [f"Jan_r0_w{i}" for i in range(6)])
        self.assertNotIn("progress_pct", cards[0])

    def test_optimizer_dashboard_cards_keep_state_declared_current_round(self) -> None:
        saved = [{"worker_id": f"Fev_r4_w{i}", "status": "Simulando..."} for i in range(6)]
        workers = [
            {"worker_id": "Fev_r4_w0", "status": "Simulando...", "progress_pct": 96.4},
            {"worker_id": "cross_Maio", "status": "Simulando...", "progress_pct": 48.4},
            {"worker_id": "cross_Maio", "status": "Simulando...", "progress_pct": 54.8},
            {"worker_id": "par_0_Jan", "status": "Finalizado", "progress_pct": 100.0},
        ]

        cards = dashboard_app._optimizer_dashboard_cards(saved, workers, n_workers=6, running=True)

        self.assertEqual([item["worker_id"] for item in cards], [f"Fev_r4_w{i}" for i in range(6)])
        self.assertEqual(cards[0]["progress_pct"], 96.4)

    def test_optimizer_status_cards_do_not_mix_external_live_workers_into_active_round(self) -> None:
        saved = [{"worker_id": f"Mar_r3_w{i}", "status": "Simulando..."} for i in range(6)]
        workers = [
            {"worker_id": "Mar_r3_w0", "status": "Simulando...", "progress_pct": 80.0},
            {"worker_id": "Mar_r3_w1", "status": "Simulando...", "progress_pct": 70.0},
            {"worker_id": "cross_Abril", "status": "Simulando...", "progress_pct": 10.0},
        ]

        merged_first = dashboard_app._merge_optimizer_candidates(saved, workers)
        cards = dashboard_app._optimizer_dashboard_cards(saved, workers, n_workers=6, running=True)

        self.assertIn("cross_Abril", [item["worker_id"] for item in merged_first])
        self.assertEqual([item["worker_id"] for item in cards], [f"Mar_r3_w{i}" for i in range(6)])

    def test_optimizer_runtime_summary_exposes_live_progress_even_without_history(self) -> None:
        summary = dashboard_app._derive_optimizer_runtime_summary(
            {
                "running": True,
                "phase": "monthly:Janeiro:round:0",
                "current_iteration": 1,
                "iterations": [],
                "baseline": {"total_trades": 0, "active_days": 0, "avg_daily_profit": 0.0},
                "best": {"total_trades": 0, "active_days": 0, "avg_daily_profit": 0.0},
                "optimizer_workers": [
                    {
                        "worker_id": "Jan_r0_w0",
                        "status": "Simulando...",
                        "progress_pct": 20.0,
                        "idle": False,
                    },
                    {
                        "worker_id": "Jan_r0_w1",
                        "status": "Simulando...",
                        "progress_pct": 40.0,
                        "idle": False,
                    },
                    {
                        "worker_id": "slot_3",
                        "status": "Sem job nesta fase",
                        "progress_pct": 0.0,
                        "idle": True,
                    },
                ],
            }
        )

        self.assertEqual(summary["active_workers"], 2)
        self.assertEqual(summary["worker_slots"], 3)
        self.assertEqual(summary["avg_progress_pct"], 30.0)
        self.assertTrue(summary["history_empty"])
        self.assertTrue(summary["zero_trade_baseline"])
        self.assertIn("Janeiro", summary["status_text"])

    def test_monthly_dashboard_history_entry_exposes_winner_metrics(self) -> None:
        metrics = {
            "score": 123.4,
            "avg_daily_profit": 55.5,
            "total_pnl": 1555.0,
            "positive_days": 25,
            "active_days": 31,
            "consistency_pct": 80.6,
            "max_drawdown": 12.3,
        }

        entry = optimize_loop.build_dashboard_history_entry(101, metrics, "monthly:Janeiro", True)

        self.assertEqual(entry["iteration"], 101)
        self.assertEqual(entry["phase"], "monthly:Janeiro")
        self.assertEqual(entry["avg_daily_profit"], 55.5)
        self.assertEqual(entry["avg_daily"], 55.5)
        self.assertTrue(entry["is_best"])

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
