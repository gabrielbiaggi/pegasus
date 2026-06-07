#!/usr/bin/env python3
"""
Pegasus Auto-Optimizer v3 — PARALLEL Hill Climber
==================================================
Roda N workers em paralelo usando todos os cores do i7-9700 (8c/8t).
Cada worker perturba os parâmetros atuais e avalia com backtest real.
O melhor resultado global é aceito e o bot é reiniciado automaticamente.

OBJETIVO: Maximizar LUCRO DIÁRIO REAL com banca de $50 fixo (sem compounding).
Maio 2026 (01/05–31/05) como conjunto de avaliação.

PARALELISMO: N_WORKERS backtests simultâneos = N_WORKERS x velocidade.
"""

# ── Saída sem buffer (logs em tempo real) ────────────────────────────────────
import sys
import os
os.environ["PYTHONUNBUFFERED"] = "1"
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

import json
import time
import random
import socket
import shutil
import subprocess
import threading
import tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import backtest_engine

# ── Configuração ──────────────────────────────────────────────────────────────
START_DATE    = "2026-01-01"
END_DATE      = "2026-06-04"
START_BALANCE = "50.0"       # NUNCA mude: banca base real do usuário

ENV_PATH      = Path(".env")
STATE_PATH    = Path("logs/optimizer_state.json")
LOG_PATH      = Path("logs/optimizer_v2.log")

# ── Persistência de Otimização em Banco de Dados SQLite ───────────────────────
import sqlite3

def _init_opt_db():
    db_path = Path("logs/results.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS optimizer_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            iteration INTEGER NOT NULL,
            avg_daily REAL,
            positive_days INTEGER,
            negative_days INTEGER,
            consistency_pct REAL,
            score REAL,
            pnl REAL,
            roi REAL,
            sharpe REAL,
            sortino REAL,
            drawdown REAL,
            elapsed_s REAL,
            is_best INTEGER,
            params TEXT NOT NULL,
            live_avg_daily REAL,
            live_positive_days INTEGER,
            live_total_pnl REAL,
            live_sharpe REAL,
            live_sortino REAL,
            live_drawdown REAL,
            best_day_pnl REAL,
            worst_day_pnl REAL
        )
        """)
        conn.commit()
    except Exception as e:
        print(f"[WARN] _init_opt_db error: {e}", flush=True)
    finally:
        conn.close()

def _save_opt_iteration(entry: dict, params: dict) -> None:
    db_path = Path("logs/results.db")
    _init_opt_db()
    conn = sqlite3.connect(str(db_path))
    try:
        safe_params = sanitize_params_for_storage(params)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """INSERT INTO optimizer_history
               (timestamp, iteration, avg_daily, positive_days, negative_days,
                consistency_pct, score, pnl, roi, sharpe, sortino, drawdown, elapsed_s, is_best, params,
                live_avg_daily, live_positive_days, live_total_pnl, live_sharpe, live_sortino, live_drawdown,
                best_day_pnl, worst_day_pnl)
               VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry["iteration"],
                entry["avg_daily"],
                entry["positive_days"],
                entry["negative_days"],
                entry["consistency_pct"],
                entry["score"],
                entry["pnl"],
                entry["roi"],
                entry["sharpe"],
                entry["sortino"],
                entry["drawdown"],
                entry["elapsed_s"],
                1 if entry["is_best"] else 0,
                json.dumps(safe_params),
                entry.get("live_avg_daily"),
                entry.get("live_positive_days"),
                entry.get("live_total_pnl"),
                entry.get("live_sharpe"),
                entry.get("live_sortino"),
                entry.get("live_drawdown"),
                entry.get("best_day_pnl"),
                entry.get("worst_day_pnl")
            )
        )
        conn.commit()
    except Exception as e:
        print(f"[WARN] _save_opt_iteration error: {e}", flush=True)
    finally:
        conn.close()

def _load_opt_history() -> list[dict]:
    db_path = Path("logs/results.db")
    if not db_path.exists():
        return []
    _init_opt_db()
    history = []
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM optimizer_history ORDER BY id DESC LIMIT 200"
        )
        rows = cursor.fetchall()
        for r in rows:
            history.append({
                "iteration": r["iteration"],
                "roi": r["roi"],
                "pnl": r["pnl"],
                "busted": r["negative_days"],
                "elapsed_s": r["elapsed_s"],
                "is_best": bool(r["is_best"]),
                "avg_daily": r["avg_daily"],
                "positive_days": r["positive_days"],
                "negative_days": r["negative_days"],
                "consistency_pct": r["consistency_pct"],
                "score": r["score"],
                "sharpe": r["sharpe"],
                "sortino": r["sortino"],
                "drawdown": r["drawdown"],
                "live_avg_daily": r["live_avg_daily"] if "live_avg_daily" in r.keys() else None,
                "live_positive_days": r["live_positive_days"] if "live_positive_days" in r.keys() else None,
                "live_total_pnl": r["live_total_pnl"] if "live_total_pnl" in r.keys() else None,
                "live_sharpe": r["live_sharpe"] if "live_sharpe" in r.keys() else None,
                "live_sortino": r["live_sortino"] if "live_sortino" in r.keys() else None,
                "live_drawdown": r["live_drawdown"] if "live_drawdown" in r.keys() else None,
                "best_day_pnl": r["best_day_pnl"] if "best_day_pnl" in r.keys() else None,
                "worst_day_pnl": r["worst_day_pnl"] if "worst_day_pnl" in r.keys() else None,
                "ts": time.time(),
            })
        history.reverse()
    except Exception as e:
        print(f"[WARN] _load_opt_history error: {e}", flush=True)
    finally:
        conn.close()
    return history

def _load_best_opt_run() -> tuple[dict, dict] | None:
    db_path = Path("logs/results.db")
    if not db_path.exists():
        return None
    _init_opt_db()
    target_context = optimizer_context()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM optimizer_history WHERE is_best = 1 ORDER BY score DESC LIMIT 50"
        )
        for r in cursor.fetchall():
            try:
                params = json.loads(r["params"])
            except Exception:
                params = {}
            if not params_match_context(params, target_context):
                continue
            best_data = {
                "iteration": r["iteration"],
                "roi_pct": r["roi"],
                "total_pnl": r["pnl"],
                "avg_daily_profit": r["avg_daily"],
                "positive_days": r["positive_days"],
                "negative_days": r["negative_days"],
                "consistency_pct": r["consistency_pct"],
                "score": r["score"],
                "sharpe_ratio": r["sharpe"],
                "sortino_ratio": r["sortino"],
                "max_drawdown": r["drawdown"],
                "elapsed_s": r["elapsed_s"],
                "active_days": r["positive_days"] + r["negative_days"],
                "live_avg_daily": r["live_avg_daily"] if "live_avg_daily" in r.keys() else None,
                "live_positive_days": r["live_positive_days"] if "live_positive_days" in r.keys() else None,
                "live_total_pnl": r["live_total_pnl"] if "live_total_pnl" in r.keys() else None,
                "live_sharpe": r["live_sharpe"] if "live_sharpe" in r.keys() else None,
                "live_sortino": r["live_sortino"] if "live_sortino" in r.keys() else None,
                "live_drawdown": r["live_drawdown"] if "live_drawdown" in r.keys() else None,
                "best_day_pnl": r["best_day_pnl"] if "best_day_pnl" in r.keys() else None,
                "worst_day_pnl": r["worst_day_pnl"] if "worst_day_pnl" in r.keys() else None,
                "reason": "Recuperado do Banco de Dados",
            }
            return best_data, params
    except Exception as e:
        print(f"[WARN] _load_best_opt_run error: {e}", flush=True)
    finally:
        conn.close()
    return None

def _load_top_champions() -> list[dict]:
    db_path = Path("logs/results.db")
    if not db_path.exists():
        return []
    _init_opt_db()
    target_context = optimizer_context()
    champions = []
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT DISTINCT params FROM optimizer_history ORDER BY score DESC LIMIT 5"
        )
        rows = cursor.fetchall()
        for r in rows:
            try:
                p = json.loads(r["params"])
                if p and params_match_context(p, target_context):
                    champions.append(p)
            except Exception:
                pass
    except Exception as e:
        print(f"[WARN] _load_top_champions error: {e}", flush=True)
    finally:
        conn.close()
    return champions


def _history_entry(iteration_num: int, metrics: dict, elapsed_s: float, is_best: bool) -> dict:
    return {
        "iteration": iteration_num,
        "avg_daily": metrics.get("avg_daily_profit", 0.0),
        "positive_days": metrics.get("positive_days", 0),
        "negative_days": metrics.get("negative_days", 0),
        "consistency_pct": metrics.get("consistency_pct", 0.0),
        "score": metrics.get("score", 0.0),
        "pnl": metrics.get("total_pnl", 0.0),
        "roi": metrics.get("roi_pct", 0.0),
        "sharpe": metrics.get("sharpe_ratio", 0.0),
        "sortino": metrics.get("sortino_ratio", 0.0),
        "drawdown": metrics.get("max_drawdown", 0.0),
        "elapsed_s": round(elapsed_s, 1),
        "is_best": is_best,
        "live_avg_daily": metrics.get("live_avg_daily"),
        "live_positive_days": metrics.get("live_positive_days"),
        "live_total_pnl": metrics.get("live_total_pnl"),
        "live_sharpe": metrics.get("live_sharpe"),
        "live_sortino": metrics.get("live_sortino"),
        "live_drawdown": metrics.get("live_drawdown"),
        "best_day_pnl": metrics.get("best_day_pnl"),
        "worst_day_pnl": metrics.get("worst_day_pnl"),
        "ts": time.time(),
    }

# ── Modo Ultra-Estresse ────────────────────────────────────────────────────────
def _read_stress_config() -> bool:
    path = Path("logs/stress_config.json")
    if not path.exists():
        return False
    try:
        import json
        data = json.loads(path.read_text())
        return bool(data.get("ultra_stress", False))
    except Exception:
        return False

# ── Carrega Símbolo Ativo e Volatilidade Mediana no escopo global ──────────────
_env_for_vol = {}
if Path(".env").exists():
    try:
        for line in Path(".env").read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                _env_for_vol[k.strip()] = v.strip()
    except Exception:
        pass
            
ACTIVE_SYMBOL = _env_for_vol.get("SYMBOL", "1HZ100V")

def get_median_volatility(symbol: str) -> float:
    symbol_upper = symbol.upper()
    defaults = {
        "BOOM1000": 1.0e-6,
        "1HZ100V": 1.4e-4,
        "1HZ10V": 1.5e-5,
    }
    try:
        data_dir = Path("data")
        max_path = data_dir / f"ticks_{symbol_upper}_max.csv"
        if not max_path.exists():
            files = list(data_dir.glob(f"ticks_{symbol_upper}_*.csv"))
            if files:
                max_path = files[0]
                
        if max_path.exists():
            import pandas as pd
            import numpy as np
            df = pd.read_csv(max_path, nrows=50000, usecols=["quote"])
            df["quote"] = pd.to_numeric(df["quote"], errors="coerce")
            df = df.dropna().reset_index(drop=True)
            returns = df["quote"].pct_change().abs().dropna()
            rolling_vol = returns.rolling(10).mean().dropna()
            if not rolling_vol.empty:
                val = float(rolling_vol.median())
                print(f"[Optimizer] Volatilidade mediana calculada para {symbol_upper}: {val:.2e}", flush=True)
                return val
    except Exception as e:
        print(f"[Optimizer] Erro ao calcular volatilidade mediana para {symbol}: {e}", flush=True)
        
    fallback = defaults.get(symbol_upper, 1.4e-4)
    print(f"[Optimizer] Usando volatilidade mediana padrão para {symbol_upper}: {fallback:.2e}", flush=True)
    return fallback

MEDIAN_VOL = get_median_volatility(ACTIVE_SYMBOL)

def parse_optimizer_workers(env: dict | None = None, default: int = 6) -> int:
    """Return a bounded optimizer worker count for memory-safe parallelism."""
    env = os.environ if env is None else env
    raw = env.get("PEGASUS_OPTIMIZER_WORKERS", str(default))
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = default
    return max(1, min(12, value))


# Each backtest worker holds tick data in memory. Keep this configurable so the
# server can reserve RAM for cloudflared, dashboard, k3s, n8n, MinIO and cache.
N_WORKERS = parse_optimizer_workers()

PARAM_SPACE = {
    # Stake base para operar: permite micro-risco e agressividade controlada.
    "STAKE":                        {"type": "float", "min": 5.0, "max": 35.0, "step": 0.25},
    # Cooldown entre entradas de ticks: precisa conseguir bloquear overtrading.
    "RISE_FALL_COOLDOWN_TICKS":     {"type": "int",   "min": 1,   "max": 60,   "step": 1},
    # Gate principal de entrada direcional: sem isso o optimizer só ajusta TP/SL
    # depois de entrar demais em mercado ruim.
    "RISE_FALL_MIN_VOTES":          {"type": "int",   "min": 1,   "max": 6,    "step": 1},
    # Payout minimo de Rise/Fall: busca entre 0.0040 e 0.0080
    "RISE_FALL_MIN_PAYOUT_PCT":     {"type": "float", "min": 0.0040, "max": 0.0080, "step": 0.0005},
    # Filtros do BOOM1000: expande para regimes muito calmos e regimes de spike.
    "RISE_FALL_BOOM_MAX_CUSUM":     {"type": "float", "min": 0.5, "max": 12.0, "step": 0.25},
    "RISE_FALL_BOOM_MAX_VELOCITY":  {"type": "float", "min": 0.00002, "max": 0.0050, "step": 0.00005},
    "RISE_FALL_BOOM_MAX_IMBALANCE": {"type": "float", "min": 0.1, "max": 5.0, "step": 0.05},
    "MULTIPLIER_JUMP_MIN_CONFIDENCE": {"type": "float", "min": 0.55, "max": 0.85, "step": 0.01},
    "MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE": {"type": "float", "min": 2.0, "max": 8.0, "step": 0.25},
    "MULTIPLIER_JUMP_BAYES_STRONG_PROB": {"type": "float", "min": 0.55, "max": 0.80, "step": 0.01},
    "MULTIPLIER_JUMP_MIN_SCORE": {"type": "int", "min": 3, "max": 7},
    "MULTIPLIER_JUMP_HURST_TRENDING": {"type": "float", "min": 0.50, "max": 0.75, "step": 0.01},
    "MULTIPLIER_JUMP_HURST_REVERTING": {"type": "float", "min": 0.20, "max": 0.45, "step": 0.01},
    "MULTIPLIER_JUMP_MI_FLOW_MIN": {"type": "float", "min": 0.00, "max": 0.01, "step": 0.001},
    "MULTIPLIER_JUMP_WAVELET_SNR_MIN": {"type": "float", "min": 0.00, "max": 0.08, "step": 0.005},

    # Parâmetros de gerenciamento de risco da estratégia Frankenstein
    "FRANKENSTEIN_USE_SOROS":       {"type": "bool"},
    "FRANKENSTEIN_SOROS_STEPS":     {"type": "int",   "min": 0,   "max": 3,   "step": 1},
    "FRANKENSTEIN_USE_MARTINGALE":  {"type": "bool"},
    "FRANKENSTEIN_MAX_GALES":       {"type": "int",   "min": 0,   "max": 2,   "step": 1},
    "FRANKENSTEIN_MODE":            {"type": "choice", "choices": ["flat", "dynamic_10"]},

    # Filtro XGBoost para Rise/Fall
    "RISE_FALL_USE_ENSEMBLE":       {"type": "bool"},
    "RISE_FALL_ENSEMBLE_MIN_PROB":  {"type": "float", "min": 0.05, "max": 0.65, "step": 0.01},
    "MULTIPLIER_VALUE":             {"type": "int",   "min": 5,   "max": 200,  "step": 5},
    "MULTIPLIER_DIRECTION":         {"type": "choice", "choices": ["signal", "up", "down"]},
    "MULTIPLIER_TAKE_PROFIT":       {"type": "float", "min": 0.02, "max": 3.00, "step": 0.02},
    "MULTIPLIER_STOP_LOSS":         {"type": "float", "min": 0.05, "max": 5.00, "step": 0.05},
    "MULTIPLIER_MAX_HOLD_TICKS":    {"type": "int",   "min": 1,   "max": 120,  "step": 1},
}

FROZEN_PARAMS = {
    # Multipliers no BOOM1000 nao usam Soros/Martingale no bot real; manter
    # esses knobs fora da exploracao evita iteracoes sem efeito operacional.
    "FRANKENSTEIN_USE_SOROS",
    "FRANKENSTEIN_SOROS_STEPS",
    "FRANKENSTEIN_USE_MARTINGALE",
    "FRANKENSTEIN_MAX_GALES",
    "FRANKENSTEIN_MODE",
    "RISE_FALL_MIN_PAYOUT_PCT",
}

SENSITIVE_PARAM_MARKERS = (
    "TOKEN",
    "PAT",
    "SECRET",
    "PASSWORD",
    "PASS",
    "DSN",
    "KEY",
    "APP_ID",
)

SAFE_PARAM_EXACT = {
    "ACCOUNT_MODE",
    "CONTRACT_MODE",
    "SYMBOL",
    "CURRENCY",
    "STAKE",
    "MIN_STAKE",
    "MAX_STAKE",
    "MAX_STAKE_PERCENT",
    "STOP_LOSS_PCT",
    "STOP_GAIN_PCT",
    "USE_SOROS",
    "SOROS_MAX_STEPS",
    "SOROS_PROFIT_FACTOR",
    "SOROS_POST_LOSS_COOLDOWN",
    "USE_MARTINGALE",
    "MARTINGALE_MAX_GALES",
    "MARTINGALE_MODE",
    "MARTINGALE_MULTIPLIER",
    "MARTINGALE_PAYOUT_RATE",
    "DYNAMIC_STAKE_BASE_PCT",
    "OPTIMIZER_CHAMPION_ITERATION",
}

SAFE_PARAM_PREFIXES = (
    "FRANKENSTEIN_",
    "RISE_FALL_",
    "CALM_ACCU_",
    "ACCUMULATOR_",
    "ENSEMBLE_",
    "MULTIPLIER_",
)

# ── Funções utilitárias ───────────────────────────────────────────────────────

def _norm_symbol(value: str | None) -> str:
    return (value or "").strip().upper()


def _norm_contract_mode(value: str | None) -> str:
    return (value or "").strip().lower()


def optimizer_context(env_vars: dict | None = None) -> dict:
    """Contexto real usado pelo optimizer para comparar campeões equivalentes."""
    env_vars = env_vars or {}
    symbol = _norm_symbol(env_vars.get("SYMBOL") or ACTIVE_SYMBOL)
    # BOOM/Crash na API PAT/OTP atual oferece Multipliers, não RF/ACCU.
    return {"contract_mode": "multiplier", "symbol": symbol}


def params_match_context(params: dict, context: dict | None = None) -> bool:
    context = context or optimizer_context()
    saved_ctx = params.get("_optimizer_context")
    if isinstance(saved_ctx, dict):
        mode = saved_ctx.get("contract_mode")
        symbol = saved_ctx.get("symbol")
    else:
        mode = params.get("CONTRACT_MODE")
        symbol = params.get("SYMBOL")

    return (
        _norm_contract_mode(mode) == _norm_contract_mode(context.get("contract_mode"))
        and _norm_symbol(symbol) == _norm_symbol(context.get("symbol"))
    )


def _is_sensitive_param(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SENSITIVE_PARAM_MARKERS)


def _is_safe_strategy_param(key: str) -> bool:
    return (
        key in PARAM_SPACE
        or key in SAFE_PARAM_EXACT
        or any(key.startswith(prefix) for prefix in SAFE_PARAM_PREFIXES)
    )


def sanitize_params_for_storage(params: dict) -> dict:
    """Remove credenciais antes de persistir params no DB/dashboard."""
    normalized = normalize_candidate_params(params)
    safe = {}
    for key, value in normalized.items():
        if _is_sensitive_param(key):
            continue
        if _is_safe_strategy_param(key):
            if key == "RISE_FALL_MIN_VOTES":
                try:
                    value = max(1, min(6, int(float(value))))
                except (TypeError, ValueError):
                    value = 4
            safe[key] = str(value)

    ctx = optimizer_context(params)
    safe["CONTRACT_MODE"] = ctx["contract_mode"]
    safe["SYMBOL"] = ctx["symbol"]
    safe["_optimizer_context"] = ctx
    return safe

def sanitize_env_for_worker(env_vars: dict) -> dict[str, str]:
    """Return only string env overrides accepted by os.environ/backtest workers."""
    env_vars = normalize_candidate_params(env_vars)
    safe: dict[str, str] = {}
    for key, value in (env_vars or {}).items():
        if not isinstance(key, str) or key.startswith("_"):
            continue
        if isinstance(value, (dict, list, tuple, set)):
            continue
        if key == "RISE_FALL_MIN_VOTES":
            try:
                value = max(1, min(6, int(float(value))))
            except (TypeError, ValueError):
                value = 4
        safe[key] = str(value)
    return safe

def load_env(path: Path = ENV_PATH) -> dict:
    env = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def save_env(env_vars: dict, path: Path = ENV_PATH) -> None:
    if not path.exists():
        path.write_text("\n".join(f"{k}={v}" for k, v in env_vars.items()) + "\n")
        return
    lines = path.read_text(encoding="utf-8").splitlines()
    new_lines, updated = [], set()
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in env_vars:
                new_lines.append(f"{k}={env_vars[k]}")
                updated.add(k)
                continue
        new_lines.append(line)
    for k in env_vars:
        if k not in updated:
            new_lines.append(f"{k}={env_vars[k]}")
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def random_space_value(key: str) -> str:
    space = PARAM_SPACE[key]
    if space["type"] == "int":
        step = int(space.get("step", 1))
        count = int((space["max"] - space["min"]) / step)
        return str(int(space["min"] + random.randint(0, count) * step))
    if space["type"] == "float":
        step = float(space.get("step", 0.01))
        count = int(round((space["max"] - space["min"]) / step))
        value = float(space["min"]) + random.randint(0, count) * step
        return str(round(min(float(space["max"]), value), 4))
    if space["type"] == "bool":
        return "true" if random.random() < 0.5 else "false"
    if space["type"] == "choice":
        return str(random.choice(space["choices"]))
    return str(space.get("min", ""))


def _configured_float(env_vars: dict | None, key: str, default: float) -> float:
    raw = None
    if env_vars:
        raw = env_vars.get(key)
    if raw is None:
        raw = os.environ.get(key)
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError, AttributeError):
        return default


def effective_stake_bounds(env_vars: dict | None = None) -> tuple[float, float]:
    floor = max(0.35, _configured_float(env_vars, "MIN_STAKE", 5.0))
    ceiling = _configured_float(env_vars, "MAX_STAKE", PARAM_SPACE["STAKE"]["max"])
    if ceiling <= 0:
        ceiling = PARAM_SPACE["STAKE"]["max"]
    ceiling = max(floor, min(PARAM_SPACE["STAKE"]["max"], ceiling))
    return floor, ceiling


def normalize_candidate_params(params: dict | None) -> dict:
    if not params:
        return {}
    out = dict(params)
    min_stake, max_stake = effective_stake_bounds(out)
    out["MIN_STAKE"] = str(min_stake)
    out["MAX_STAKE"] = str(max_stake)

    if "STAKE" in out:
        try:
            stake = float(out["STAKE"])
        except (TypeError, ValueError):
            stake = min_stake
        out["STAKE"] = str(round(max(min_stake, min(max_stake, stake)), 2))

    if "RISE_FALL_MIN_VOTES" in out:
        try:
            out["RISE_FALL_MIN_VOTES"] = str(max(1, min(6, int(float(out["RISE_FALL_MIN_VOTES"])))))
        except (TypeError, ValueError):
            out["RISE_FALL_MIN_VOTES"] = "4"

    symbol = _norm_symbol(out.get("SYMBOL") or ACTIVE_SYMBOL)
    contract_mode = _norm_contract_mode(out.get("CONTRACT_MODE") or "multiplier")
    if contract_mode == "multiplier" and symbol == "BOOM1000":
        out["MULTIPLIER_VALUE"] = str(max(5, min(10, int(float(out.get("MULTIPLIER_VALUE", 5))))))
        direction = str(out.get("MULTIPLIER_DIRECTION", "signal")).strip().lower()
        if direction not in {"signal", "up"}:
            direction = "signal"
        out["MULTIPLIER_DIRECTION"] = direction

        tp = float(out.get("MULTIPLIER_TAKE_PROFIT", 0.75) or 0.75)
        sl = float(out.get("MULTIPLIER_STOP_LOSS", 0.85) or 0.85)
        hold = int(float(out.get("MULTIPLIER_MAX_HOLD_TICKS", 12)) or 12)
        votes = int(float(out.get("RISE_FALL_MIN_VOTES", 5)) or 5)
        cooldown = int(float(out.get("RISE_FALL_COOLDOWN_TICKS", 12)) or 12)
        ens_prob = float(out.get("RISE_FALL_ENSEMBLE_MIN_PROB", 0.32) or 0.32)
        max_cusum = float(out.get("RISE_FALL_BOOM_MAX_CUSUM", 3.8) or 3.8)
        max_vel = float(out.get("RISE_FALL_BOOM_MAX_VELOCITY", 0.0026) or 0.0026)
        max_imb = float(out.get("RISE_FALL_BOOM_MAX_IMBALANCE", 2.2) or 2.2)
        jump_conf = float(out.get("MULTIPLIER_JUMP_MIN_CONFIDENCE", 0.62) or 0.62)
        jump_qg_imb = float(out.get("MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE", 4.0) or 4.0)
        jump_bayes = float(out.get("MULTIPLIER_JUMP_BAYES_STRONG_PROB", 0.62) or 0.62)
        jump_min_score = int(float(out.get("MULTIPLIER_JUMP_MIN_SCORE", 5) or 5))
        jump_hurst_tr = float(out.get("MULTIPLIER_JUMP_HURST_TRENDING", 0.58) or 0.58)
        jump_hurst_rev = float(out.get("MULTIPLIER_JUMP_HURST_REVERTING", 0.38) or 0.38)
        jump_mi = float(out.get("MULTIPLIER_JUMP_MI_FLOW_MIN", 0.02) or 0.02)
        jump_wavelet = float(out.get("MULTIPLIER_JUMP_WAVELET_SNR_MIN", 0.48) or 0.48)

        out["MULTIPLIER_TAKE_PROFIT"] = str(round(max(0.45, min(1.60, tp)), 2))
        out["MULTIPLIER_STOP_LOSS"] = str(round(max(0.45, min(1.35, sl)), 2))
        out["MULTIPLIER_MAX_HOLD_TICKS"] = str(max(4, min(18, hold)))
        out["RISE_FALL_MIN_VOTES"] = str(max(3, min(5, votes)))
        out["RISE_FALL_COOLDOWN_TICKS"] = str(max(6, min(24, cooldown)))
        out["RISE_FALL_USE_ENSEMBLE"] = "true"
        out["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(max(0.24, min(0.42, ens_prob)), 2))
        out["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(max(2.2, min(5.2, max_cusum)), 4))
        out["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(max(0.0014, min(0.0042, max_vel)), 6))
        out["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(max(1.4, min(3.6, max_imb)), 4))
        out["MULTIPLIER_JUMP_MIN_CONFIDENCE"] = str(round(max(0.55, min(0.80, jump_conf)), 2))
        out["MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE"] = str(round(max(2.0, min(8.0, jump_qg_imb)), 2))
        out["MULTIPLIER_JUMP_BAYES_STRONG_PROB"] = str(round(max(0.55, min(0.80, jump_bayes)), 2))
        out["MULTIPLIER_JUMP_MIN_SCORE"] = str(max(3, min(7, jump_min_score)))
        out["MULTIPLIER_JUMP_HURST_TRENDING"] = str(round(max(0.50, min(0.72, jump_hurst_tr)), 2))
        out["MULTIPLIER_JUMP_HURST_REVERTING"] = str(round(max(0.22, min(0.45, jump_hurst_rev)), 2))
        if float(out["MULTIPLIER_JUMP_HURST_REVERTING"]) >= float(out["MULTIPLIER_JUMP_HURST_TRENDING"]) - 0.06:
            out["MULTIPLIER_JUMP_HURST_REVERTING"] = str(round(float(out["MULTIPLIER_JUMP_HURST_TRENDING"]) - 0.06, 2))
        out["MULTIPLIER_JUMP_MI_FLOW_MIN"] = str(round(max(0.0, min(0.01, jump_mi)), 3))
        out["MULTIPLIER_JUMP_WAVELET_SNR_MIN"] = str(round(max(0.0, min(0.08, jump_wavelet)), 3))

    return out


def inject_global_multiplier_search(params: dict) -> dict:
    """Build a broad multiplier candidate instead of only local hill-climbing."""
    p = params.copy()
    symbol = _norm_symbol(p.get("SYMBOL") or ACTIVE_SYMBOL)
    contract_mode = _norm_contract_mode(p.get("CONTRACT_MODE") or "multiplier")

    if contract_mode == "multiplier" and symbol == "BOOM1000":
        regime = random.choices(
            ["spike_long", "balanced_probe", "trend_follow"],
            weights=[0.58, 0.27, 0.15],
            k=1,
        )[0]
        if regime == "spike_long":
            p["MULTIPLIER_DIRECTION"] = "up"
            p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
            p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.55, 1.30), 2))
            p["MULTIPLIER_STOP_LOSS"] = str(round(random.uniform(0.55, 1.10), 2))
            p["MULTIPLIER_MAX_HOLD_TICKS"] = str(random.randint(4, 14))
            p["RISE_FALL_MIN_VOTES"] = str(random.choice([3, 4, 5]))
            p["RISE_FALL_COOLDOWN_TICKS"] = str(random.randint(6, 18))
            p["RISE_FALL_USE_ENSEMBLE"] = "true"
            p["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(random.uniform(0.26, 0.40), 2))
            p["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(random.uniform(2.4, 4.8), 4))
            p["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(random.uniform(0.0018, 0.0040), 6))
            p["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(random.uniform(1.6, 3.4), 4))
            p["MULTIPLIER_JUMP_MIN_CONFIDENCE"] = str(round(random.uniform(0.57, 0.68), 2))
            p["MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE"] = str(round(random.uniform(2.5, 5.5), 2))
            p["MULTIPLIER_JUMP_BAYES_STRONG_PROB"] = str(round(random.uniform(0.58, 0.72), 2))
            p["MULTIPLIER_JUMP_MIN_SCORE"] = str(random.randint(3, 5))
            p["MULTIPLIER_JUMP_HURST_TRENDING"] = str(round(random.uniform(0.54, 0.66), 2))
            p["MULTIPLIER_JUMP_HURST_REVERTING"] = str(round(random.uniform(0.26, 0.40), 2))
            p["MULTIPLIER_JUMP_MI_FLOW_MIN"] = str(round(random.uniform(0.0, 0.004), 3))
            p["MULTIPLIER_JUMP_WAVELET_SNR_MIN"] = str(round(random.uniform(0.0, 0.03), 3))
            min_stake, _ = effective_stake_bounds(p)
            p["STAKE"] = str(round(random.uniform(min_stake, 6.0), 2))
            return normalize_candidate_params(p)
        if regime == "balanced_probe":
            p["MULTIPLIER_DIRECTION"] = "signal"
            p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
            p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.50, 1.50), 2))
            p["MULTIPLIER_STOP_LOSS"] = str(round(random.uniform(0.55, 1.25), 2))
            p["MULTIPLIER_MAX_HOLD_TICKS"] = str(random.randint(6, 18))
            p["RISE_FALL_MIN_VOTES"] = str(random.choice([3, 4, 5]))
            p["RISE_FALL_COOLDOWN_TICKS"] = str(random.randint(8, 24))
            p["RISE_FALL_USE_ENSEMBLE"] = "true"
            p["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(random.uniform(0.24, 0.38), 2))
            p["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(random.uniform(2.2, 5.2), 4))
            p["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(random.uniform(0.0014, 0.0039), 6))
            p["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(random.uniform(1.4, 3.6), 4))
            p["MULTIPLIER_JUMP_MIN_CONFIDENCE"] = str(round(random.uniform(0.58, 0.70), 2))
            p["MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE"] = str(round(random.uniform(2.0, 5.0), 2))
            p["MULTIPLIER_JUMP_BAYES_STRONG_PROB"] = str(round(random.uniform(0.58, 0.72), 2))
            p["MULTIPLIER_JUMP_MIN_SCORE"] = str(random.randint(3, 5))
            p["MULTIPLIER_JUMP_HURST_TRENDING"] = str(round(random.uniform(0.54, 0.66), 2))
            p["MULTIPLIER_JUMP_HURST_REVERTING"] = str(round(random.uniform(0.24, 0.38), 2))
            p["MULTIPLIER_JUMP_MI_FLOW_MIN"] = str(round(random.uniform(0.0, 0.004), 3))
            p["MULTIPLIER_JUMP_WAVELET_SNR_MIN"] = str(round(random.uniform(0.0, 0.03), 3))
            min_stake, _ = effective_stake_bounds(p)
            p["STAKE"] = str(round(random.uniform(min_stake, 8.0), 2))
            return normalize_candidate_params(p)
        p["MULTIPLIER_DIRECTION"] = random.choice(["signal", "up"])
        p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
        p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.65, 1.60), 2))
        p["MULTIPLIER_STOP_LOSS"] = str(round(random.uniform(0.50, 1.20), 2))
        p["MULTIPLIER_MAX_HOLD_TICKS"] = str(random.randint(4, 16))
        p["RISE_FALL_MIN_VOTES"] = str(random.choice([3, 4, 5]))
        p["RISE_FALL_COOLDOWN_TICKS"] = str(random.randint(8, 20))
        p["RISE_FALL_USE_ENSEMBLE"] = "true"
        p["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(random.uniform(0.28, 0.42), 2))
        p["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(random.uniform(2.4, 5.0), 4))
        p["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(random.uniform(0.0016, 0.0042), 6))
        p["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(random.uniform(1.6, 3.4), 4))
        p["MULTIPLIER_JUMP_MIN_CONFIDENCE"] = str(round(random.uniform(0.58, 0.72), 2))
        p["MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE"] = str(round(random.uniform(2.5, 6.0), 2))
        p["MULTIPLIER_JUMP_BAYES_STRONG_PROB"] = str(round(random.uniform(0.60, 0.74), 2))
        p["MULTIPLIER_JUMP_MIN_SCORE"] = str(random.randint(3, 5))
        p["MULTIPLIER_JUMP_HURST_TRENDING"] = str(round(random.uniform(0.55, 0.68), 2))
        p["MULTIPLIER_JUMP_HURST_REVERTING"] = str(round(random.uniform(0.24, 0.38), 2))
        p["MULTIPLIER_JUMP_MI_FLOW_MIN"] = str(round(random.uniform(0.0, 0.005), 3))
        p["MULTIPLIER_JUMP_WAVELET_SNR_MIN"] = str(round(random.uniform(0.0, 0.04), 3))
        min_stake, _ = effective_stake_bounds(p)
        p["STAKE"] = str(round(random.uniform(min_stake, max(min_stake, 6.0)), 2))
        return normalize_candidate_params(p)

    for key in (
        "STAKE",
        "RISE_FALL_MIN_VOTES",
        "RISE_FALL_COOLDOWN_TICKS",
        "RISE_FALL_BOOM_MAX_CUSUM",
        "RISE_FALL_BOOM_MAX_VELOCITY",
        "RISE_FALL_BOOM_MAX_IMBALANCE",
        "RISE_FALL_USE_ENSEMBLE",
        "RISE_FALL_ENSEMBLE_MIN_PROB",
        "MULTIPLIER_VALUE",
        "MULTIPLIER_DIRECTION",
        "MULTIPLIER_TAKE_PROFIT",
        "MULTIPLIER_STOP_LOSS",
        "MULTIPLIER_MAX_HOLD_TICKS",
    ):
        p[key] = random_space_value(key)

    stake = float(p.get("STAKE", 1.0))
    mult = int(float(p.get("MULTIPLIER_VALUE", 50)))
    commission = stake * mult * 0.0002
    tp = float(p.get("MULTIPLIER_TAKE_PROFIT", 0.5))
    if commission > tp * 0.45:
        min_stake, _ = effective_stake_bounds(p)
        p["STAKE"] = str(round(random.uniform(min_stake, 8.0), 2))
        p["MULTIPLIER_VALUE"] = str(random.choice([5, 10, 15, 20, 25, 30, 40, 50]))
        p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.25, 2.5), 2))
    return normalize_candidate_params(p)


def rand_params(base: dict, metrics: dict | None = None) -> dict:
    """Perturba parâmetros com heurística de direção baseada em métricas anteriores."""
    p = base.copy()
    if random.random() < 0.35:
        return inject_global_multiplier_search(p)
    
    # Heurística inteligente baseada em performance anterior:
    if metrics:
        avg_d = metrics.get("avg_daily_profit", 0.0) or metrics.get("avg_daily", 0.0) or 0.0
        neg_days = metrics.get("negative_days", 0) or metrics.get("busted", 0) or 0
        consist = metrics.get("consistency_pct", 0.0)
        total_trades = int(metrics.get("total_trades", 0) or 0)
        contract_mode = _norm_contract_mode(p.get("CONTRACT_MODE") or "multiplier")
        symbol = _norm_symbol(p.get("SYMBOL") or ACTIVE_SYMBOL)

        if contract_mode == "multiplier" and symbol == "BOOM1000" and total_trades < 40:
            p["MULTIPLIER_DIRECTION"] = random.choice(["up", "signal"])
            p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
            p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.55, 1.30), 2))
            p["MULTIPLIER_STOP_LOSS"] = str(round(random.uniform(0.55, 1.10), 2))
            p["MULTIPLIER_MAX_HOLD_TICKS"] = str(random.randint(4, 14))
            p["RISE_FALL_MIN_VOTES"] = str(random.choice([5, 6]))
            p["RISE_FALL_COOLDOWN_TICKS"] = str(random.randint(6, 18))
            p["RISE_FALL_USE_ENSEMBLE"] = "true"
            p["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(random.uniform(0.26, 0.38), 2))
            return normalize_candidate_params(p)

        if contract_mode == "multiplier" and symbol == "BOOM1000" and -0.20 <= avg_d <= 0.20:
            action = random.choice(["tp_sl_pair", "hold", "votes", "cooldown", "filters", "direction"])
            if action == "tp_sl_pair":
                p["MULTIPLIER_TAKE_PROFIT"] = str(round(random.uniform(0.55, 1.25), 2))
                p["MULTIPLIER_STOP_LOSS"] = str(round(random.uniform(0.55, 1.10), 2))
            elif action == "hold":
                p["MULTIPLIER_MAX_HOLD_TICKS"] = str(random.randint(4, 14))
            elif action == "votes":
                p["RISE_FALL_MIN_VOTES"] = str(random.choice([5, 6]))
                p["RISE_FALL_ENSEMBLE_MIN_PROB"] = str(round(random.uniform(0.26, 0.40), 2))
            elif action == "cooldown":
                p["RISE_FALL_COOLDOWN_TICKS"] = str(random.randint(6, 20))
            elif action == "filters":
                p["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(random.uniform(2.4, 4.8), 4))
                p["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(random.uniform(0.0016, 0.0040), 6))
                p["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(random.uniform(1.6, 3.4), 4))
            else:
                p["MULTIPLIER_DIRECTION"] = random.choice(["signal", "up"])
            return normalize_candidate_params(p)
        
        # 1. Se tem perdas (dias negativos), a prioridade absoluta é reduzir o risco
        # Aperta os filtros de spikes e aumenta o cooldown ticks!
        if neg_days > 0 or consist < 95.0:
            action = random.choice(["votes", "cusum", "velocity", "imbalance", "cooldown", "mult_sl", "mult_tp", "mult_value", "mult_dir", "exposure"])
            if action == "votes" and "RISE_FALL_MIN_VOTES" in p:
                val = int(p["RISE_FALL_MIN_VOTES"])
                p["RISE_FALL_MIN_VOTES"] = str(min(PARAM_SPACE["RISE_FALL_MIN_VOTES"]["max"], val + 1))
            if action == "cusum" and "RISE_FALL_BOOM_MAX_CUSUM" in p:
                val = float(p["RISE_FALL_BOOM_MAX_CUSUM"])
                p["RISE_FALL_BOOM_MAX_CUSUM"] = str(round(max(PARAM_SPACE["RISE_FALL_BOOM_MAX_CUSUM"]["min"], val * 0.85), 4))
            elif action == "velocity" and "RISE_FALL_BOOM_MAX_VELOCITY" in p:
                val = float(p["RISE_FALL_BOOM_MAX_VELOCITY"])
                p["RISE_FALL_BOOM_MAX_VELOCITY"] = str(round(max(PARAM_SPACE["RISE_FALL_BOOM_MAX_VELOCITY"]["min"], val * 0.80), 6))
            elif action == "imbalance" and "RISE_FALL_BOOM_MAX_IMBALANCE" in p:
                val = float(p["RISE_FALL_BOOM_MAX_IMBALANCE"])
                p["RISE_FALL_BOOM_MAX_IMBALANCE"] = str(round(max(PARAM_SPACE["RISE_FALL_BOOM_MAX_IMBALANCE"]["min"], val * 0.85), 4))
            elif action == "cooldown" and "RISE_FALL_COOLDOWN_TICKS" in p:
                val = int(p["RISE_FALL_COOLDOWN_TICKS"])
                p["RISE_FALL_COOLDOWN_TICKS"] = str(min(PARAM_SPACE["RISE_FALL_COOLDOWN_TICKS"]["max"], val + random.choice([1, 2, 3, 5])))
            elif action == "mult_sl" and "MULTIPLIER_STOP_LOSS" in p:
                val = float(p["MULTIPLIER_STOP_LOSS"])
                p["MULTIPLIER_STOP_LOSS"] = str(round(max(PARAM_SPACE["MULTIPLIER_STOP_LOSS"]["min"], val * 0.75), 2))
            elif action == "mult_value" and "MULTIPLIER_VALUE" in p:
                val = int(p["MULTIPLIER_VALUE"])
                p["MULTIPLIER_VALUE"] = str(max(PARAM_SPACE["MULTIPLIER_VALUE"]["min"], val - random.choice([5, 10, 20])))
            elif action == "mult_tp" and "MULTIPLIER_TAKE_PROFIT" in p:
                val = float(p["MULTIPLIER_TAKE_PROFIT"])
                p["MULTIPLIER_TAKE_PROFIT"] = str(round(min(PARAM_SPACE["MULTIPLIER_TAKE_PROFIT"]["max"], val * random.choice([1.2, 1.5, 2.0])), 2))
            elif action == "mult_dir":
                current = str(p.get("MULTIPLIER_DIRECTION", "signal"))
                p["MULTIPLIER_DIRECTION"] = "up" if current == "signal" else "signal"
            elif action == "exposure":
                min_stake, _ = effective_stake_bounds(p)
                p["STAKE"] = str(round(random.uniform(min_stake, 8.0), 2))
                p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
            
            # Opcional: reduz o stake um pouco para proteger o capital
            if "STAKE" in p and random.random() < 0.3:
                val = float(p["STAKE"])
                p["STAKE"] = str(round(max(PARAM_SPACE["STAKE"]["min"], val - 1.0), 1))
            
            return normalize_candidate_params(p)

        # 2. Se a estratégia é consistente (sem dias negativos) mas o ganho diário está abaixo de $50 (dobrar a banca):
        # Aumentamos o STAKE, diminuímos o cooldown (mais trades), ou aumentamos Soros/Martingale!
        if avg_d < 50.0:
            action = random.choice(["stake", "votes", "cooldown", "multiplier", "mult_tp", "mult_dir", "filters"])
            if action == "stake" and "STAKE" in p:
                val = float(p["STAKE"])
                factor = 50.0 / max(1.0, avg_d)
                target = val * factor * random.choice([0.9, 1.0, 1.1])
                target = round(max(val + 0.5, min(PARAM_SPACE["STAKE"]["max"], target)), 1)
                p["STAKE"] = str(target)
            elif action == "cooldown" and "RISE_FALL_COOLDOWN_TICKS" in p:
                val = int(p["RISE_FALL_COOLDOWN_TICKS"])
                p["RISE_FALL_COOLDOWN_TICKS"] = str(max(PARAM_SPACE["RISE_FALL_COOLDOWN_TICKS"]["min"], val - 1))
            elif action == "votes" and "RISE_FALL_MIN_VOTES" in p:
                val = int(p["RISE_FALL_MIN_VOTES"])
                p["RISE_FALL_MIN_VOTES"] = str(max(PARAM_SPACE["RISE_FALL_MIN_VOTES"]["min"], val - 1))
            elif action == "multiplier" and "MULTIPLIER_VALUE" in p:
                p["MULTIPLIER_VALUE"] = str(random.choice([5, 10]))
            elif action == "mult_tp" and "MULTIPLIER_TAKE_PROFIT" in p:
                val = float(p["MULTIPLIER_TAKE_PROFIT"])
                p["MULTIPLIER_TAKE_PROFIT"] = str(round(min(PARAM_SPACE["MULTIPLIER_TAKE_PROFIT"]["max"], val + 0.10), 2))
            elif action == "mult_dir":
                current = str(p.get("MULTIPLIER_DIRECTION", "signal"))
                p["MULTIPLIER_DIRECTION"] = "up" if current == "signal" else "signal"
            elif action == "filters":
                # Afrouxa de leve os filtros se o número de trades for muito baixo
                for f_key in ["RISE_FALL_BOOM_MAX_CUSUM", "RISE_FALL_BOOM_MAX_VELOCITY", "RISE_FALL_BOOM_MAX_IMBALANCE"]:
                    if f_key in p:
                        val = float(p[f_key])
                        p[f_key] = str(round(min(PARAM_SPACE[f_key]["max"], val * 1.15), 4))
            
            return normalize_candidate_params(p)

    # 3. Caso padrão (exploração puramente aleatória / perturbação de 1-4 parâmetros)
    eligible = [k for k in PARAM_SPACE if k not in FROZEN_PARAMS]
    num = random.randint(1, min(4, len(eligible)))
    keys = random.sample(eligible, num)

    for key in keys:
        space = PARAM_SPACE[key]
        curr = p.get(key)

        if space["type"] == "int":
            step = space.get("step", 1)
            val  = int(curr) if curr is not None else (space["min"] + space["max"]) // 2
            delta = random.choice([-3,-2,-1,1,2,3]) * step
            val   = max(space["min"], min(space["max"], val + delta))
            p[key] = str(int(val))

        elif space["type"] == "float":
            step = space.get("step", 0.01)
            val  = float(curr) if curr is not None else (space["min"] + space["max"]) / 2
            delta = random.choice([-3,-2,-1,1,2,3]) * step
            val   = round(max(space["min"], min(space["max"], val + delta)), 4)
            p[key] = str(val)

        elif space["type"] == "float_sci":
            val = float(curr) if curr is not None else (space["min"] + space["max"]) / 2
            factor = random.choice([0.6, 0.75, 0.85, 1.0, 1.15, 1.30, 1.50, 1.75])
            val    = max(space["min"], min(space["max"], val * factor))
            p[key] = f"{val:.2e}"

        elif space["type"] == "bool":
            curr_bool = (str(curr).lower() == "true") if curr is not None else False
            val = not curr_bool
            p[key] = "true" if val else "false"

        elif space["type"] == "choice":
            choices = space["choices"]
            p[key] = str(random.choice(choices))

    return normalize_candidate_params(p)


def compute_score(results: list, strategy: str = "Super-Frankenstein") -> dict:
    """
    Métricas REAIS de lucro diário com banca de $50 fixo.
    Retorna dict com avg_daily_profit, positive_days, score, etc.
    Inclui penalização de volatilidade (Standard Deviation) e pior dia (Drawdown).
    """
    days = []
    for r in results:
        s    = r.get("strategies", {}).get(strategy, {})
        pnl  = s.get("pnl", 0.0)
        trd  = s.get("trades", 0)
        days.append({"pnl": pnl, "trades": trd, "date": r.get("date", "?")})

    active = [d for d in days if d["trades"] > 0]
    pos    = [d for d in days if d["pnl"] > 0]
    neg    = [d for d in days if d["pnl"] < 0]
    total  = sum(d["pnl"] for d in days)
    n_act  = len(active)
    n_pos  = len(pos)
    n_neg  = len(neg)
    total_days = len(days)
    total_trades = sum(max(0, int(d["trades"])) for d in days)

    avg_day   = (sum(d["pnl"] for d in active) / n_act) if n_act > 0 else 0.0
    consist   = (n_pos / n_act * 100) if n_act > 0 else 0.0
    best_day  = max((d["pnl"] for d in days), default=0.0)
    worst_day = min((d["pnl"] for d in days), default=0.0)

    # 1. Volatilidade Diária (Standard Deviation) de dias ativos
    active_pnls = [d["pnl"] for d in active]
    if len(active_pnls) > 1:
        mean_pnl = sum(active_pnls) / len(active_pnls)
        variance = sum((x - mean_pnl) ** 2 for x in active_pnls) / len(active_pnls)
        std_dev = variance ** 0.5
    else:
        std_dev = 0.0

    # 2. Penalização de Drawdown / Pior Dia
    dd_penalty = 0.0
    if worst_day < -15.0:
        dd_penalty = abs(worst_day) * 4.0   # Penalização severa para perdas críticas (>30% da banca)
    elif worst_day < -5.0:
        dd_penalty = abs(worst_day) * 1.5   # Penalização moderada

    # 3. Penalidade Catástrofe (dias com grandes perdas)
    bust_pen = sum(20.0 for d in days if d["pnl"] < -20.0)

    # 4. Score Base — normalizado por dias ativos para funcionar em qualquer período (1 mês ou 5 meses)
    # n_pos e n_neg são normalizados: divide pelo número de dias ativos para manter escala
    norm = n_act if n_act > 0 else 1
    consist_bonus = (n_pos / norm)  # 0.0–1.0 — fracção de dias positivos
    neg_rate      = (n_neg / norm)  # 0.0–1.0 — fracção de dias negativos
    coverage = (n_act / total_days) if total_days > 0 else 0.0
    avg_trades_per_active_day = (total_trades / n_act) if n_act > 0 else 0.0

    inactivity_penalty = 0.0
    if coverage < 0.20:
        inactivity_penalty += (0.20 - coverage) * 4000.0
    if n_act < 10:
        inactivity_penalty += (10 - n_act) * 120.0
    if total_trades < 40:
        inactivity_penalty += (40 - total_trades) * 8.0
    if total_trades == 0:
        inactivity_penalty += 12000.0
    if n_act == 0:
        inactivity_penalty += 8000.0
    if total <= 0.50:
        inactivity_penalty += (0.50 - total) * 160.0
    if avg_trades_per_active_day < 1.0:
        inactivity_penalty += (1.0 - avg_trades_per_active_day) * 250.0

    score = (
        avg_day        * 12.0    # Lucro médio/dia — independente de período
        + consist_bonus * 600.0  # Bônus de consistência normalizado (substitui n_pos * 4)
        + consist       * 0.6    # % Consistência (reforço)
        - neg_rate      * 1500.0 # Penalidade por dias negativos normalizada (substitui n_neg * 10)
        - std_dev       * 3.0    # Penalidade por volatilidade diária
        - dd_penalty             # Penalidade por rebaixamento / pior dia
        - bust_pen               # Penalidade catástrofe
        - inactivity_penalty     # Penaliza falso positivo que quase não opera
    )

    if n_act == 0:
        score = -25000.0
    elif total <= 0.0 and n_pos == 0:
        score = min(score, -9000.0)


    return {
        "avg_daily_profit": round(avg_day, 4),
        "positive_days":    n_pos,
        "negative_days":    n_neg,
        "active_days":      n_act,
        "total_days":       total_days,
        "total_trades":     total_trades,
        "consistency_pct":  round(consist, 1),
        "total_pnl":        round(total, 2),
        "score":            round(score, 4),
        "best_day_pnl":     round(best_day, 2),
        "worst_day_pnl":    round(worst_day, 2),
        "roi_pct":          round(total / 50.0 * 100, 1),
    }


# ── Worker de backtest (roda em processo separado) ───────────────────────────

def _run_one(args) -> dict | None:
    """
    Roda um backtest diretamente em memória. Executa em processo filho (multiprocessing).
    Evita sobrecarga de subprocessos e I/O de disco.
    """
    env_vars, worker_id = args
    
    # Define prioridade baixa de CPU (nice -n 19) no processo worker
    try:
        os.nice(19)
    except Exception:
        pass

    import backtest_engine
    
    # Garante flags de otimização no dicionário de overrides
    env_vars = sanitize_env_for_worker(env_vars)
    
    # Extrai datas personalizadas se fornecidas
    s_date = env_vars.pop("START_DATE", START_DATE)
    e_date = env_vars.pop("END_DATE", END_DATE)
    
    env_vars["BACKTEST_COMPOUNDING"] = "false"   # $50 fixo por dia — obrigatório!
    env_vars["PEGASUS_OPTIMIZER_RUN"] = "true"
    env_vars["CONTRACT_MODE"] = "multiplier"
    symbol_upper = ACTIVE_SYMBOL.upper()
    env_vars["SYMBOL"] = symbol_upper
    is_boom_crash = "BOOM" in symbol_upper or "CRASH" in symbol_upper
    env_vars["RISE_FALL_PAYOUT_RATE"] = "0.95"

    try:
        return backtest_engine.run_backtest_direct(
            start_date_str=s_date,
            end_date_str=e_date,
            start_balance=float(START_BALANCE),
            env_overrides=env_vars,
            worker_id=worker_id,
        )
    except Exception as e:
        print(f"   [worker {worker_id}] erro: {e}", flush=True)
        return None


# ── Estado e dashboard ────────────────────────────────────────────────────────

_state_lock = threading.Lock()

_STATE_DROP_KEYS = {
    "_env",
    "env",
    "env_overrides",
    "raw_results",
    "results",
    "daily_results",
    "trades",
    "signals",
}

def sanitize_metrics_for_state(value):
    """Keep optimizer dashboard state small and free of runtime secrets."""
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            if str(key) in _STATE_DROP_KEYS:
                continue
            clean[key] = sanitize_metrics_for_state(item)
        return clean
    if isinstance(value, list):
        return [sanitize_metrics_for_state(item) for item in value[:500]]
    return value

def write_state(iteration: int, baseline: dict, best: dict | None,
                history: list, running: bool = True,
                evaluating_candidates: list | None = None,
                monthly_champions: dict | None = None,
                phase: str | None = None,
                crossover_results: list | None = None) -> None:
    """Grava estado para o dashboard ler (thread-safe)."""
    try:
        existing_candidates = None
        existing_champions = None
        
        if STATE_PATH.exists():
            try:
                old_data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
                if evaluating_candidates is None:
                    existing_candidates = old_data.get("evaluating_candidates")
                if monthly_champions is None:
                    existing_champions = old_data.get("monthly_champions")
            except Exception:
                pass
                
        if evaluating_candidates is not None:
            existing_candidates = evaluating_candidates
        if monthly_champions is not None:
            existing_champions = monthly_champions

        payload = {
            "running":           running,
            "current_iteration": iteration,
            "baseline":          sanitize_metrics_for_state(baseline),
            "best":              sanitize_metrics_for_state(best),
            "iterations":        sanitize_metrics_for_state(history[-200:]),
            "last_update":       time.time(),
            "start_date":        START_DATE,
            "end_date":          END_DATE,
            "n_workers":         N_WORKERS,
            "optimizer_context": optimizer_context(),
        }
        if phase is not None:
            payload["phase"] = phase
        if existing_candidates is not None:
            payload["evaluating_candidates"] = sanitize_metrics_for_state(existing_candidates)
        if existing_champions is not None:
            payload["monthly_champions"] = sanitize_metrics_for_state(existing_champions)
        if crossover_results is not None:
            payload["crossover_results"] = sanitize_metrics_for_state(crossover_results)

        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                       encoding="utf-8")
        tmp.replace(STATE_PATH)    # rename atômico
    except Exception as e:
        print(f"[WARN] write_state: {e}", flush=True)


def update_monthly_champions(monthly_champions: dict, iteration: int, m: dict, params: dict) -> bool:
    updated = False
    sf_breakdown = m.get("monthly_breakdown", {}).get("Super-Frankenstein", {})
    if not sf_breakdown:
        return False
        
    all_months_pnls = {
        month: data["pnl"]
        for month, data in sf_breakdown.items()
    }
    
    for month, data in sf_breakdown.items():
        pnl = data["pnl"]
        if pnl <= 0.0 or data["active_days"] == 0:
            continue
            
        current_champ = monthly_champions.get(month)
        if current_champ is None or pnl > current_champ["pnl"]:
            monthly_champions[month] = {
                "iteration": iteration,
                "pnl": round(pnl, 2),
                "params": sanitize_params_for_storage(params),
                "monthly_pnls": all_months_pnls,
                "avg_daily_profit": round(data["avg_daily_profit"], 2),
                "consistency_pct": round(data["consistency_pct"], 1),
                "active_days": data["active_days"]
            }
            updated = True
    return updated


def build_monthly_champion_entry(params: dict, metrics: dict | None) -> dict:
    """Build the compact monthly champion payload consumed by the dashboard."""
    metrics = metrics or {}
    avg_day = float(metrics.get("avg_daily_profit", metrics.get("avg_daily", 0.0)) or 0.0)
    consistency = float(metrics.get("consistency_pct", 0.0) or 0.0)
    worst_day = float(metrics.get("worst_day_pnl", 0.0) or 0.0)
    active_days = int(metrics.get("active_days", 0) or 0)
    candidate_viable = is_monthly_candidate_viable(metrics)
    entry = {
        "score": round(float(metrics.get("score", -999999.0) or -999999.0), 4),
        "params": sanitize_params_for_storage(params),
        "candidate_viable": candidate_viable,
        "deployable": (
            avg_day >= 50.0
            and consistency >= 80.0
            and worst_day >= -25.0
            and active_days >= 20
        ),
    }
    for key in (
        "avg_daily_profit",
        "total_pnl",
        "consistency_pct",
        "positive_days",
        "negative_days",
        "active_days",
        "total_trades",
        "worst_day_pnl",
        "max_drawdown",
        "sharpe",
        "sortino",
    ):
        value = metrics.get(key)
        if value is None:
            continue
        entry[key] = round(value, 4) if isinstance(value, float) else value
    monthly_breakdown = metrics.get("monthly_breakdown")
    if monthly_breakdown:
        entry["monthly_breakdown"] = sanitize_metrics_for_state(monthly_breakdown)
    return entry


def build_dashboard_history_entry(
    iteration_num: int,
    metrics: dict,
    phase: str,
    is_best: bool = False,
    elapsed_s: float = 0.0,
) -> dict:
    """Compact optimizer result row for the live dashboard tables."""
    return {
        "iteration": iteration_num,
        "phase": phase,
        "avg_daily_profit": round(float(metrics.get("avg_daily_profit", 0.0) or 0.0), 4),
        "avg_daily": round(float(metrics.get("avg_daily_profit", 0.0) or 0.0), 4),
        "pnl": round(float(metrics.get("total_pnl", 0.0) or 0.0), 2),
        "total_pnl": round(float(metrics.get("total_pnl", 0.0) or 0.0), 2),
        "score": round(float(metrics.get("score", 0.0) or 0.0), 4),
        "positive_days": int(metrics.get("positive_days", 0) or 0),
        "active_days": int(metrics.get("active_days", 0) or 0),
        "consistency_pct": round(float(metrics.get("consistency_pct", 0.0) or 0.0), 1),
        "drawdown": round(float(metrics.get("max_drawdown", metrics.get("drawdown", 0.0)) or 0.0), 2),
        "max_drawdown": round(float(metrics.get("max_drawdown", metrics.get("drawdown", 0.0)) or 0.0), 2),
        "sharpe": round(float(metrics.get("sharpe", metrics.get("sharpe_ratio", 0.0)) or 0.0), 4),
        "sortino": round(float(metrics.get("sortino", metrics.get("sortino_ratio", 0.0)) or 0.0), 4),
        "elapsed_s": round(float(elapsed_s or 0.0), 2),
        "is_best": bool(is_best),
    }


def build_crossover_env(champ_info: dict) -> dict[str, str]:
    """Build a clean worker env for cross-month validation."""
    env_test = sanitize_env_for_worker((champ_info.get("params") or {}).copy())
    env_test["START_DATE"] = "2026-01-01"
    env_test["END_DATE"] = "2026-06-04"
    return env_test


# Estado global para debouncing de deploy
_last_deploy_time = 0.0
_pending_deploy_env = None
_pending_deploy_msg = ""
DEPLOY_COOLDOWN = 120.0  # 2 minutos de cooldown mínimo para o bot ao vivo respirar


def is_live_deployable(metrics: dict | None) -> bool:
    """Only deploy strategies that clear the live operating target."""
    if not metrics:
        return False
    avg_day = float(metrics.get("avg_daily_profit", metrics.get("avg_daily", 0.0)) or 0.0)
    consistency = float(metrics.get("consistency_pct", 0.0) or 0.0)
    worst_day = float(metrics.get("worst_day_pnl", 0.0) or 0.0)
    active_days = int(metrics.get("active_days", 0) or 0)
    return (
        avg_day >= 50.0
        and consistency >= 80.0
        and worst_day >= -25.0
        and active_days >= 20
    )


def is_monthly_candidate_viable(metrics: dict | None) -> bool:
    if not metrics:
        return False
    avg_day = float(metrics.get("avg_daily_profit", metrics.get("avg_daily", 0.0)) or 0.0)
    total_pnl = float(metrics.get("total_pnl", 0.0) or 0.0)
    active_days = int(metrics.get("active_days", 0) or 0)
    total_trades = int(metrics.get("total_trades", 0) or 0)
    positive_days = int(metrics.get("positive_days", 0) or 0)
    consistency = float(metrics.get("consistency_pct", 0.0) or 0.0)
    worst_day = float(metrics.get("worst_day_pnl", 0.0) or 0.0)
    return (
        avg_day > 0.0
        and total_pnl > 0.0
        and active_days >= 8
        and total_trades >= 40
        and positive_days >= 4
        and consistency >= 25.0
        and worst_day >= -25.0
    )


def is_crossover_candidate_viable(metrics: dict | None) -> bool:
    if not metrics:
        return False
    total_pnl = float(metrics.get("total_pnl", 0.0) or 0.0)
    active_days = int(metrics.get("active_days", 0) or 0)
    total_trades = int(metrics.get("total_trades", 0) or 0)
    consistency = float(metrics.get("consistency_pct", 0.0) or 0.0)
    breakdown = (metrics.get("monthly_breakdown") or {}).get("Super-Frankenstein", {})
    pnls = [float((data or {}).get("pnl", 0.0) or 0.0) for data in breakdown.values()]
    positive_months = sum(1 for pnl in pnls if pnl > 0.0)
    negative_months = sum(1 for pnl in pnls if pnl < 0.0)
    best_month = max(pnls) if pnls else 0.0
    concentration_ratio = (best_month / total_pnl) if total_pnl > 0 and best_month > 0 else 0.0
    return (
        total_pnl > 0.0
        and active_days >= 20
        and total_trades >= 60
        and consistency >= 5.0
        and positive_months >= 2
        and negative_months <= 3
        and concentration_ratio <= 0.85
    )


def try_deploy_winner(env_vars: dict, msg: str, force: bool = False) -> bool:
    global _last_deploy_time, _pending_deploy_env, _pending_deploy_msg
    now = time.time()
    if force or (now - _last_deploy_time >= DEPLOY_COOLDOWN):
        print(f"   🚀 Iniciando deploy do campeão...", flush=True)
        ok = deploy_winner(env_vars, msg)
        if ok:
            _last_deploy_time = now
            _pending_deploy_env = None
            _pending_deploy_msg = ""
        return ok
    else:
        _pending_deploy_env = env_vars.copy()
        _pending_deploy_msg = msg
        rem = int(DEPLOY_COOLDOWN - (now - _last_deploy_time))
        print(f"   ⏳ Deploy em cooldown. Campeão registrado para deploy automático em {rem}s para evitar race condition/loop de restart.", flush=True)
        return False


def _is_on_server() -> bool:
    return Path("/opt/pegasus").exists() and Path.cwd() == Path("/opt/pegasus")


def translate_frankenstein_params(env_vars: dict) -> dict:
    """Traduz as configurações de Frankenstein para as variáveis que o bot real carrega do .env."""
    out = env_vars.copy()
    
    if "FRANKENSTEIN_USE_SOROS" in out:
        out["USE_SOROS"] = out["FRANKENSTEIN_USE_SOROS"]
    if "FRANKENSTEIN_SOROS_STEPS" in out:
        out["SOROS_MAX_STEPS"] = out["FRANKENSTEIN_SOROS_STEPS"]
    if "FRANKENSTEIN_USE_MARTINGALE" in out:
        out["USE_MARTINGALE"] = out["FRANKENSTEIN_USE_MARTINGALE"]
    if "FRANKENSTEIN_MAX_GALES" in out:
        out["MARTINGALE_MAX_GALES"] = out["FRANKENSTEIN_MAX_GALES"]
        
    if "FRANKENSTEIN_MODE" in out:
        mode = out["FRANKENSTEIN_MODE"]
        if mode == "dynamic_10":
            out["DYNAMIC_STAKE_BASE_PCT"] = "0.10"
        else:
            out["DYNAMIC_STAKE_BASE_PCT"] = "0.0"
            
    # Remove as chaves FRANKENSTEIN do .env final para mantê-lo limpo
    for k in list(out.keys()):
        if k.startswith("FRANKENSTEIN_"):
            del out[k]
            
    # Garante que o bot real opera no contrato suportado pela API PAT/OTP atual.
    symbol_upper = ACTIVE_SYMBOL.upper()
    out["CONTRACT_MODE"] = "multiplier"
    out["SYMBOL"] = symbol_upper
    out["USE_MARTINGALE"] = "false"
    out["USE_SOROS"] = "false"
    out["MARTINGALE_PAYOUT_RATE"] = "1.0"
    out.setdefault("MULTIPLIER_VALUE", "100")
    out.setdefault("MULTIPLIER_DIRECTION", "signal")
    out.setdefault("MULTIPLIER_TAKE_PROFIT", "0.50")
    out.setdefault("MULTIPLIER_STOP_LOSS", "1.00")
    out.setdefault("MULTIPLIER_MAX_HOLD_TICKS", "30")
    
    return out


def deploy_winner(env_vars: dict, msg: str, min_pnl: float = 5.0) -> bool:
    """
    Salva .env e reinicia o bot ao vivo com os novos parâmetros.
    Garante que nunca haverá instâncias duplicadas do bot.
    """
    translated_env = translate_frankenstein_params(env_vars)
    save_env(translated_env)
    print(f"   💾 .env salvo com novos parâmetros traduzidos.", flush=True)

    if _is_on_server():
        try:
            # 1. Mata TODOS os bots existentes por PID (pgrep exato)
            pids_result = subprocess.run(
                ["pgrep", "-f", "python bot.py"],
                capture_output=True, timeout=5,
            )
            if pids_result.returncode == 0:
                pids = pids_result.stdout.decode().strip().split()
                for pid in pids:
                    try:
                        subprocess.run(["kill", pid], capture_output=True, timeout=3)
                    except Exception:
                        pass
                print(f"   🛑 Bots anteriores finalizados (PIDs: {' '.join(pids)})", flush=True)
                time.sleep(3)

            # 2. Encerra qualquer screen session 'pegasus' remanescente
            subprocess.run(["screen", "-S", "pegasus", "-X", "quit"],
                           capture_output=True, timeout=5)
            time.sleep(1)

            # 3. Cria UMA nova session screen
            subprocess.run(
                ["screen", "-dmS", "pegasus", "bash", "-c",
                 "cd /opt/pegasus && PEGASUS_LIVE_BOT=true .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log"],
                capture_output=True, timeout=15,
            )
            time.sleep(3)

            # 4. Confirma exatamente 1 instância
            check = subprocess.run(
                ["pgrep", "-f", "python bot.py"],
                capture_output=True, timeout=5,
            )
            pids_new = check.stdout.decode().strip().split() if check.returncode == 0 else []
            if pids_new:
                print(f"   🚀 Bot reiniciado! 1 instância ativa (PID={pids_new[0]})", flush=True)
                return True
            else:
                print(f"   ⚠️  Bot não iniciou — params salvos no .env", flush=True)
                return False
        except Exception as e:
            print(f"   ⚠️  Restart bot: {e} — params salvos no .env", flush=True)
            return False
    else:
        try:
            subprocess.run(
                ["./deploy.sh", msg, "--restart"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                check=True, timeout=120,
            )
            return True
        except Exception as e:
            print(f"   ❌ deploy.sh: {e}", flush=True)
            return False



# ── Formatação de métricas ────────────────────────────────────────────────────

def fmt(m: dict) -> str:
    return (
        f"avg_day=${m['avg_daily_profit']:.2f}"
        f" pos={m['positive_days']}/{m['active_days']}"
        f" consist={m['consistency_pct']:.0f}%"
        f" score={m['score']:.1f}"
        f" total=${m['total_pnl']:.2f}"
    )


# ── Métodos de Paralelização por Sub-intervalo Mensal ──────────────────────────

def split_range_into_months(start_date_str: str, end_date_str: str) -> list[dict]:
    from datetime import date, timedelta
    s_date = date.fromisoformat(start_date_str)
    e_date = date.fromisoformat(end_date_str)
    
    ranges = []
    months_pt = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    
    cur = s_date
    while cur <= e_date:
        if cur.month == 12:
            next_month = date(cur.year + 1, 1, 1)
        else:
            next_month = date(cur.year, cur.month + 1, 1)
        
        last_day_of_month = next_month - timedelta(days=1)
        segment_end = min(e_date, last_day_of_month)
        
        ranges.append({
            "start": cur.isoformat(),
            "end": segment_end.isoformat(),
            "name": months_pt[cur.month]
        })
        
        cur = segment_end + timedelta(days=1)
    return ranges


def run_backtest_parallel(env_vars: dict, start_date_str: str, end_date_str: str, max_workers: int = 9) -> dict | None:
    monthly_ranges = split_range_into_months(start_date_str, end_date_str)
    
    jobs = []
    for idx, r in enumerate(monthly_ranges):
        job_env = env_vars.copy()
        job_env["START_DATE"] = r["start"]
        job_env["END_DATE"] = r["end"]
        jobs.append((job_env, f"par_{idx}_{r['name'][:3]}"))
        
    if len(jobs) == 1:
        return _run_one(jobs[0])
        
    combined_results = []
    from concurrent.futures import ProcessPoolExecutor, as_completed
    
    with ProcessPoolExecutor(max_workers=min(max_workers, len(jobs))) as pool:
        futures = {pool.submit(_run_one, job): job for job in jobs}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res and "results" in res:
                    combined_results.extend(res["results"])
            except Exception as e:
                print(f"   [run_backtest_parallel] erro no subprocesso: {e}", flush=True)
                
    if not combined_results:
        return None
        
    combined_results.sort(key=lambda x: x.get("date", ""))
    import backtest_engine
    return backtest_engine.compile_summary_metrics(combined_results, env_vars, float(START_BALANCE))


# ── Loop principal ────────────────────────────────────────────────────────────

def main():
    SEP = "=" * 70
    print(SEP, flush=True)
    print(f"  🦅 PEGASUS OPTIMIZER v3 — PARALLEL ({N_WORKERS} workers simultâneos)", flush=True)
    print(f"  CPU alvo: i7-9700 8c | RAM: ~15GB disponível", flush=True)
    print(f"  Meta: LUCRO DIÁRIO MÁXIMO com banca de $50 fixo", flush=True)
    print(SEP, flush=True)

    best_env = load_env()
    original_env = best_env.copy()

    # Preenche parâmetros faltando com valores intermediários
    for key, space in PARAM_SPACE.items():
        if key not in best_env:
            if space["type"] == "int":
                best_env[key] = str((space["min"] + space["max"]) // 2)
            elif space["type"] == "float":
                best_env[key] = f"{(space['min'] + space['max']) / 2:.4f}"
            elif space["type"] == "float_sci":
                best_env[key] = f"{(space['min'] + space['max']) / 2:.2e}"

    # ── Baseline (parâmetros atuais) ─────────────────────────────────────────
    print(f"\n📊 Calculando baseline com parâmetros atuais...", flush=True)
    t0 = time.time()
    baseline_metrics = run_backtest_parallel(best_env, START_DATE, END_DATE)
    baseline_time = time.time() - t0

    if not baseline_metrics:
        print("❌ Backtest de baseline falhou. Verificar dados em data/cache/", flush=True)
        import sys as _s; _s.exit(1)

    print(f"   ✅ Baseline [{baseline_time:.0f}s]: {fmt(baseline_metrics)}", flush=True)
    print(f"\n   📌 Contexto real:", flush=True)
    print(f"      ROI total período: {baseline_metrics['roi_pct']:.1f}% = ${baseline_metrics['total_pnl']:.2f}", flush=True)
    print(f"      Lucro médio/dia: ${baseline_metrics['avg_daily_profit']:.2f}  (META: $50/dia)", flush=True)
    print(f"      Dias positivos:  {baseline_metrics['positive_days']}/{baseline_metrics['active_days']} ativos ({baseline_metrics['consistency_pct']:.0f}%)", flush=True)
    print(f"      Melhor dia: ${baseline_metrics['best_day_pnl']:.2f} | Pior dia: ${baseline_metrics['worst_day_pnl']:.2f}", flush=True)

    _init_opt_db()
    history = _load_opt_history()
    iteration = 1
    if history:
        iteration = max(h["iteration"] for h in history) + 1
        print(f"   📊 Carregado {len(history)} iterações do histórico do banco de dados (reiniciando no it#{iteration})", flush=True)

    # Carrega campeões mensais persistidos
    monthly_champions = {}
    if STATE_PATH.exists():
        try:
            state_data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            monthly_champions = state_data.get("monthly_champions", {})
            print(f"   📂 Carregado {len(monthly_champions)} campeões mensais salvos no arquivo de estado.", flush=True)
        except Exception:
            pass

    bot_was_synced = False
    db_best = _load_best_opt_run()
    if db_best:
        best_data, db_params = db_best
        best_score = best_data["score"]
        best_pos   = best_data["positive_days"]
        best_avg   = best_data["avg_daily_profit"]
        if db_params:
            for k in PARAM_SPACE:
                if k in db_params:
                    best_env[k] = str(db_params[k])
            best_env["OPTIMIZER_CHAMPION_ITERATION"] = str(best_data["iteration"])
        print(f"   🏆 Recorde anterior recuperado do DB: score={best_score:.2f} avg_day=${best_avg:.2f}/dia (it#{best_data['iteration']})", flush=True)
        
        # Calcula o breakdown do campeão carregado para exibir no frontend
        print("   ⚙️ Calculando breakdown mensal do campeão recuperado do DB...", flush=True)
        try:
            champ_res = run_backtest_parallel(best_env, START_DATE, END_DATE)
            if champ_res and "monthly_breakdown" in champ_res:
                best_data["monthly_breakdown"] = champ_res["monthly_breakdown"]
        except Exception as e:
            print(f"   [WARN] Erro ao carregar breakdown do campeão: {e}", flush=True)

        # Sincronização de startup se a iteração no .env for diferente da campeã do DB
        original_deployed_it = original_env.get("OPTIMIZER_CHAMPION_ITERATION")
        db_it_str = str(best_data["iteration"])
        if original_deployed_it != db_it_str:
            print(f"   🔄 [Startup Sync] Desalinhamento detectado: .env tem it#{original_deployed_it}, mas DB tem campeão it#{db_it_str}.", flush=True)
            print(f"      Iniciando deploy forçado do campeão it#{db_it_str}...", flush=True)
            if is_live_deployable(best_data):
                ok = try_deploy_winner(best_env, f"Startup Sync: deploy campeão it#{db_it_str} do banco de dados", force=True)
                if ok:
                    bot_was_synced = True
                    print(f"      ✅ Deploy de startup concluído com sucesso!", flush=True)
                else:
                    print(f"      ⚠️  Deploy de startup retornou falha, mas parâmetros foram salvos no .env", flush=True)
            else:
                print(
                    "      ⏸️  Campeão DB não passa gate live; mantendo bot offline e otimizando.",
                    flush=True,
                )
    else:
        best_score = baseline_metrics["score"]
        best_pos   = baseline_metrics["positive_days"]
        best_avg   = baseline_metrics["avg_daily_profit"]
        best_data  = {**baseline_metrics, "iteration": 0}

    write_state(iteration - 1, baseline_metrics, best_data, history, monthly_champions=monthly_champions)

    # Garante que o bot ao vivo está online no startup (se não estiver em Modo Ultra-Estresse e não foi sincronizado agora)
    is_stress = _read_stress_config()
    if not is_stress and not bot_was_synced:
        is_bot_running = False
        if _is_on_server():
            try:
                check = subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True, timeout=5)
                if check.returncode == 0:
                    is_bot_running = True
            except Exception:
                pass
        else:
            is_bot_running = True

        if not is_bot_running:
            if is_live_deployable(best_data):
                print("   ⚠️  Bot ao vivo está OFFLINE no startup. Inicializando campeão validado...", flush=True)
                try_deploy_winner(best_env, "Optimizer Startup: Live bot was offline, starting now", force=True)
            else:
                print("   ⏸️  Bot ao vivo OFFLINE: sem campeão validado para live; optimizer continua buscando.", flush=True)
    else:
        print("   🤖 [Ultra-Estresse] Bot ao vivo permanece em espera (OFFLINE) para prioridade de CPU/RAM.", flush=True)

    print(f"\n⚡ [Otimizador] Iniciando Otimização Evolutiva Mês a Mês...", flush=True)

    months = [
        {"name": "Janeiro", "start": "2026-01-01", "end": "2026-01-31"},
        {"name": "Fevereiro", "start": "2026-02-01", "end": "2026-02-28"},
        {"name": "Março", "start": "2026-03-01", "end": "2026-03-31"},
        {"name": "Abril", "start": "2026-04-01", "end": "2026-04-30"},
        {"name": "Maio", "start": "2026-05-01", "end": "2026-05-31"},
        {"name": "Junho", "start": "2026-06-01", "end": "2026-06-04"},
    ]

    monthly_champions = {}
    dashboard_result_seq = max(iteration, 1)

    # Executamos a busca para cada mês individualmente
    for m_info in months:
        m_name = m_info["name"]
        m_start = m_info["start"]
        m_end = m_info["end"]
        m_key = m_start[:7]
        print(f"\n📅 Otimizando para o mês: {m_name} ({m_start} a {m_end})...", flush=True)

        month_best_score = -999999.0
        month_best_env = best_env.copy()
        month_best_env["START_DATE"] = m_start
        month_best_env["END_DATE"] = m_end
        month_best_metrics = None

        # Mantem 9 workers simultaneos, mas aprofunda a busca mensal em mais
        # rodadas. Mais candidatos por mes melhora a chance de escapar de
        # vizinhancas ruins sem aumentar RAM ao mesmo tempo.
        rounds_per_month = int(os.getenv("PEGASUS_OPTIMIZER_ROUNDS_PER_MONTH", "10"))
        ITERS_PER_MONTH = max(N_WORKERS, N_WORKERS * max(1, rounds_per_month))
        with ProcessPoolExecutor(max_workers=N_WORKERS) as pool:
            for r in range(max(1, ITERS_PER_MONTH // N_WORKERS)):
                candidates = []
                candidates_ui = []
                for w in range(N_WORKERS):
                    cand = rand_params(month_best_env, month_best_metrics)
                    cand["START_DATE"] = m_start
                    cand["END_DATE"] = m_end
                    worker_id = f"{m_name[:3]}_r{r}_w{w}"
                    candidates.append((cand, worker_id))
                    candidates_ui.append({
                        **sanitize_params_for_storage(cand),
                        "worker_id": worker_id,
                        "current_month": m_name,
                        "status": "Simulando...",
                    })
                write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                            evaluating_candidates=candidates_ui,
                            monthly_champions=monthly_champions,
                            phase=f"monthly:{m_name}:round:{r}")

                futures = {pool.submit(_run_one, c): idx for idx, c in enumerate(candidates)}

                for fut in as_completed(futures):
                    idx = futures[fut]
                    try:
                        res = fut.result()
                        candidates_ui[idx]["status"] = "Finalizado" if res else "Falha"
                        if res:
                            dashboard_result_seq += 1
                            candidates_ui[idx].update({
                                "result_score": round(float(res.get("score", 0.0) or 0.0), 4),
                                "result_avg_daily_profit": round(float(res.get("avg_daily_profit", 0.0) or 0.0), 2),
                                "result_pnl": round(float(res.get("total_pnl", 0.0) or 0.0), 2),
                                "result_consistency_pct": round(float(res.get("consistency_pct", 0.0) or 0.0), 1),
                                "result_days": f"{res.get('positive_days', 0)}/{res.get('active_days', 0)}",
                            })
                            history.append(
                                build_dashboard_history_entry(
                                    dashboard_result_seq,
                                    res,
                                    f"monthly:{m_name}",
                                    bool(res["score"] > month_best_score),
                                )
                            )
                            if len(history) > 500:
                                history = history[-500:]
                        if res and res["score"] > month_best_score:
                            month_best_score = res["score"]
                            month_best_env = res["_env"].copy()
                            month_best_env["START_DATE"] = m_start
                            month_best_env["END_DATE"] = m_end
                            month_best_metrics = res
                            print(f"   ✨ Mês {m_name} (rodada {r}): Novo melhor score = {month_best_score:.4f} | Lucro/Dia = ${res['avg_daily_profit']:.2f}/dia", flush=True)
                    except Exception as e:
                        candidates_ui[idx]["status"] = "Erro"
                        print(f"   [Mês {m_name}] erro no worker {idx}: {e}", flush=True)
                    finally:
                        write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                                    evaluating_candidates=candidates_ui,
                                    monthly_champions=monthly_champions,
                                    phase=f"monthly:{m_name}:round:{r}")

        champ_params = month_best_env.copy()
        champ_params.pop("START_DATE", None)
        champ_params.pop("END_DATE", None)
        champ_params = sanitize_params_for_storage(champ_params)
        champion_viable = bool(is_monthly_candidate_viable(month_best_metrics))

        if champion_viable:
            monthly_champion_entry = build_monthly_champion_entry(champ_params, month_best_metrics)
            monthly_champion_entry["month_name"] = m_name
            monthly_champion_entry["month_key"] = m_key
            monthly_champion_entry["eligible_for_crossover"] = True
            monthly_champions[m_key] = monthly_champion_entry
            write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                        evaluating_candidates=[],
                        monthly_champions=monthly_champions,
                        phase=f"monthly:{m_name}:champion")
            print(f"🏆 Campeão Mensal de {m_name} encontrado! Score: {month_best_score:.4f}", flush=True)
            print(f"   Parâmetros seguros: {champ_params}", flush=True)
        else:
            monthly_champions.pop(m_key, None)
            write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                        evaluating_candidates=[],
                        monthly_champions=monthly_champions,
                        phase=f"monthly:{m_name}:no-viable-champion")
            print(
                f"   ⚠️  {m_name}: nenhum campeão viável neste ciclo "
                f"(score={month_best_score:.4f}, avg_day=${float((month_best_metrics or {}).get('avg_daily_profit', 0.0) or 0.0):.2f}, "
                f"trades={int((month_best_metrics or {}).get('total_trades', 0) or 0)}, "
                f"ativos={int((month_best_metrics or {}).get('active_days', 0) or 0)}).",
                flush=True,
            )

    # ── Crossover & Comparação Cruzada ───────────────────────────────────────
    print(f"\n🔄 [Crossover] Iniciando validação cruzada dos campeões mensais...", flush=True)

    crossover_results = []

    with ProcessPoolExecutor(max_workers=N_WORKERS) as pool:
        jobs = []
        candidates_ui = []
        for champ_name, champ_info in monthly_champions.items():
            if not champ_info.get("eligible_for_crossover"):
                print(f"   ⏭️  Pulando campeão de {champ_name}: densidade operacional insuficiente para crossover.", flush=True)
                continue
            env_test = build_crossover_env(champ_info)
            jobs.append((env_test, champ_name))
            candidates_ui.append({
                **sanitize_params_for_storage(env_test),
                "worker_id": f"cross_{champ_name}",
                "current_month": "Jan-Jun",
                "status": "Simulando...",
                "champ_name": champ_name,
            })

        write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                    evaluating_candidates=candidates_ui,
                    monthly_champions=monthly_champions,
                    phase="crossover",
                    crossover_results=crossover_results)

        futures = {
            pool.submit(_run_one, (job[0], f"cross_{job[1]}")): idx
            for idx, job in enumerate(jobs)
        }

        for fut in as_completed(futures):
            idx = futures[fut]
            champ_name = jobs[idx][1]
            try:
                res = fut.result()
                candidates_ui[idx]["status"] = "Finalizado" if res else "Falha"
                if res:
                    dashboard_result_seq += 1
                    row = {
                        "champ_name": champ_name,
                        "score": res["score"],
                        "avg_daily_profit": res["avg_daily_profit"],
                        "total_pnl": res["total_pnl"],
                        "consistency_pct": res["consistency_pct"],
                        "positive_days": res["positive_days"],
                        "active_days": res["active_days"],
                        "total_trades": int(res.get("total_trades", 0) or 0),
                        "worst_day_pnl": res.get("worst_day_pnl", 0.0),
                        "max_drawdown": res.get("max_drawdown", 0.0),
                        "params": sanitize_params_for_storage(res["_env"]),
                        "monthly_breakdown": res.get("monthly_breakdown", {})
                    }
                    crossover_results.append(row)
                    candidates_ui[idx].update({
                        "result_score": round(float(res.get("score", 0.0) or 0.0), 4),
                        "result_avg_daily_profit": round(float(res.get("avg_daily_profit", 0.0) or 0.0), 2),
                        "result_pnl": round(float(res.get("total_pnl", 0.0) or 0.0), 2),
                        "result_consistency_pct": round(float(res.get("consistency_pct", 0.0) or 0.0), 1),
                        "result_days": f"{res.get('positive_days', 0)}/{res.get('active_days', 0)}",
                    })
                    history.append(
                        build_dashboard_history_entry(
                            dashboard_result_seq,
                            res,
                            f"crossover:{champ_name}",
                            False,
                        )
                    )
                    if len(history) > 500:
                        history = history[-500:]
            except Exception as e:
                candidates_ui[idx]["status"] = "Erro"
                print(f"   [Crossover] erro ao testar campeão de {champ_name}: {e}", flush=True)
            finally:
                write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                            evaluating_candidates=candidates_ui,
                            monthly_champions=monthly_champions,
                            phase="crossover",
                            crossover_results=crossover_results)

    crossover_results = sorted(crossover_results, key=lambda x: x["score"], reverse=True)

    print(f"\n📋 Tabela Comparativa de Validação Cruzada (Período Jan-Jun Completo):", flush=True)
    print(f"{'-'*90}", flush=True)
    print(f"{'Campeão de':<12} | {'Score Global':<12} | {'Lucro/Dia':<10} | {'PnL Total':<10} | {'Consistência':<12} | {'Dias Pos':<8}", flush=True)
    print(f"{'-'*90}", flush=True)
    for r in crossover_results:
        print(f"{r['champ_name']:<12} | {r['score']:<12.4f} | ${r['avg_daily_profit']:<9.2f} | ${r['total_pnl']:<9.2f} | {r['consistency_pct']:<11.1f}% | {r['positive_days']}/{r['active_days']}", flush=True)
    print(f"{'-'*90}", flush=True)

    print(f"\n📊 Detalhamento Mensal de cada Campeão (PnL por Mês):", flush=True)
    print(f"{'-'*85}", flush=True)
    print(f"{'Campeão de':<12} | {'Jan':<9} | {'Fev':<9} | {'Mar':<9} | {'Abr':<9} | {'Mai':<9} | {'Jun':<9}", flush=True)
    print(f"{'-'*85}", flush=True)
    for r in crossover_results:
        breakdown = r["monthly_breakdown"].get("Super-Frankenstein", {})
        pnls = []
        for m_info in months:
            m_key = m_info["start"][:7]
            pnl_val = breakdown.get(m_key, {}).get("pnl", 0.0)
            pnls.append(f"${pnl_val:>6.2f}")
        pnls_str = " | ".join(pnls)
        print(f"{r['champ_name']:<12} | {pnls_str}", flush=True)
    print(f"{'-'*85}", flush=True)

    if not crossover_results:
        print("   ⚠️  Nenhum campeão passou pela validação cruzada; mantendo bot offline e reiniciando busca.", flush=True)
        write_state(iteration - 1, baseline_metrics, best_data, history,
                    evaluating_candidates=[],
                    monthly_champions=monthly_champions)
        return

    deployable_crossovers = [r for r in crossover_results if is_live_deployable(r) and is_crossover_candidate_viable(r)]
    if not deployable_crossovers:
        print("\n⏸️ Nenhum campeão da validação cruzada passou o gate live; mantendo melhor anterior e reiniciando busca.", flush=True)
        write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                    evaluating_candidates=[],
                    monthly_champions=monthly_champions,
                    phase="crossover:rejected",
                    crossover_results=crossover_results)
        return

    supreme_winner = deployable_crossovers[0]
    print(f"\n👑 Vencedor Supremo Selecionado: Campeão de {supreme_winner['champ_name']}", flush=True)
    print(f"   Métricas Globais: Score={supreme_winner['score']:.4f} | Lucro/Dia=${supreme_winner['avg_daily_profit']:.2f}/dia", flush=True)

    best_env = supreme_winner["params"].copy()
    if "START_DATE" in best_env:
        del best_env["START_DATE"]
    if "END_DATE" in best_env:
        del best_env["END_DATE"]

    best_score = supreme_winner["score"]
    best_pos = supreme_winner["positive_days"]
    best_avg = supreme_winner["avg_daily_profit"]
    best_data = {
        "score": supreme_winner["score"],
        "avg_daily_profit": supreme_winner["avg_daily_profit"],
        "total_pnl": supreme_winner["total_pnl"],
        "consistency_pct": supreme_winner["consistency_pct"],
        "positive_days": supreme_winner["positive_days"],
        "active_days": supreme_winner["active_days"],
        "worst_day_pnl": supreme_winner.get("worst_day_pnl", 0.0),
        "max_drawdown": supreme_winner.get("max_drawdown", 0.0),
        "iteration": 9999,
        "monthly_breakdown": supreme_winner.get("monthly_breakdown", {}),
        "phase": f"supreme:{supreme_winner['champ_name']}",
    }
    write_state(dashboard_result_seq, baseline_metrics, best_data, history,
                evaluating_candidates=[],
                monthly_champions=monthly_champions,
                phase=f"supreme:{supreme_winner['champ_name']}",
                crossover_results=crossover_results)

    # Faz deploy do vencedor supremo no bot ao vivo
    if is_live_deployable(best_data):
        print(f"\n🚀 Fazendo deploy do Vencedor Supremo no bot ao vivo...", flush=True)
        ok = try_deploy_winner(best_env, f"Supreme Winner from month {supreme_winner['champ_name']}: score={supreme_winner['score']:.4f} avg_day=${supreme_winner['avg_daily_profit']:.2f}/dia", force=True)
        if ok:
            print(f"✅ Vencedor Supremo ativo com sucesso no servidor!", flush=True)
    else:
        print("\n⏸️ Vencedor Supremo ainda não passa gate live; sem deploy ao bot.", flush=True)

    # ── Loop infinito de refinamento contínuo ─────────────────────────────────
    print(f"\n{'-'*70}", flush=True)
    print(f"  🚀 LOOP DE REFINAMENTO CONTÍNUO — {N_WORKERS} workers simultâneos", flush=True)
    print(f"  Aprimorando o Vencedor Supremo ({supreme_winner['champ_name']})...", flush=True)
    print(f"{'-'*70}\n", flush=True)

    iteration = 10000
    champion_pool = [(best_env, best_data)]

    with ProcessPoolExecutor(max_workers=N_WORKERS) as pool:
        while True:
            is_stress = _read_stress_config()
            if is_stress and _is_on_server():
                try:
                    check = subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True, timeout=2)
                    if check.returncode == 0:
                        print("   🤖 [Ultra-Estresse] Pausando Bot ao Vivo...", flush=True)
                        subprocess.run(["screen", "-S", "pegasus", "-X", "quit"], capture_output=True)
                except Exception:
                    pass

            candidates = []
            candidates_ui = []
            for i in range(N_WORKERS):
                base_env, base_metrics = random.choice(champion_pool)
                cand = rand_params(base_env, base_metrics)
                worker_id = f"ref_{i}"
                candidates.append((cand, worker_id))
                candidates_ui.append({
                    **sanitize_params_for_storage(cand),
                    "worker_id": worker_id,
                    "status": "Simulando...",
                })

            print(f"🔄 Iteração de Refinamento {iteration}–{iteration + N_WORKERS - 1}...", flush=True)
            write_state(iteration, baseline_metrics, best_data, history,
                        evaluating_candidates=candidates_ui,
                        monthly_champions=monthly_champions)

            t0 = time.time()
            futures = {pool.submit(_run_one, c): i for i, c in enumerate(candidates)}

            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    m = fut.result()
                    candidates_ui[idx]["status"] = "Finalizado" if m else "Inválido"
                    if not m:
                        write_state(iteration + idx, baseline_metrics, best_data, history,
                                    evaluating_candidates=candidates_ui,
                                    monthly_champions=monthly_champions)
                        continue
                except Exception as e:
                    candidates_ui[idx]["status"] = "Erro"
                    print(f"   [worker {idx}] erro: {e}", flush=True)
                    write_state(iteration + idx, baseline_metrics, best_data, history,
                                evaluating_candidates=candidates_ui,
                                monthly_champions=monthly_champions)
                    continue

                active = m.get("active_days", 0) or 0
                avg_d  = m["avg_daily_profit"]
                pnl_ok = (
                    m["total_pnl"] >= 0
                    and avg_d > 0
                    and avg_d <= 120.0
                    and active >= 5
                )

                is_better = False
                reason = ""

                if pnl_ok:
                    if m["score"] > best_score + 0.1:
                        is_better = True
                        reason = f"+score ({best_score:.1f}→{m['score']:.1f})"
                    elif abs(m["score"] - best_score) <= 0.1:
                        if m["positive_days"] > best_pos:
                            is_better = True
                            reason = f"+dias_pos ({best_pos}→{m['positive_days']})"
                        elif m["positive_days"] == best_pos and m["avg_daily_profit"] > best_avg + 0.05:
                            is_better = True
                            reason = f"+avg_day (${best_avg:.2f}→${m['avg_daily_profit']:.2f})"

                icon = "🔥 NOVO RECORD!" if is_better else "   ·"
                print(f"   [it#{iteration+idx}] {fmt(m)} → {icon}", flush=True)

                entry = _history_entry(iteration + idx, m, time.time() - t0, is_better)
                _save_opt_iteration(entry, m.get("_env", {}))
                history.append(entry)
                if len(history) > 500:
                    history = history[-500:]

                if is_better:
                    best_score = m["score"]
                    best_pos   = m["positive_days"]
                    best_avg   = m["avg_daily_profit"]
                    best_env   = m["_env"].copy()
                    best_env["OPTIMIZER_CHAMPION_ITERATION"] = str(iteration + idx)
                    best_data  = {**m, "iteration": iteration + idx, "reason": reason}

                    champion_pool.append((best_env, best_data))
                    if len(champion_pool) > 10:
                        champion_pool.pop(0)

                    print(f"\n{'★'*70}", flush=True)
                    print(f"  🏆 NOVO RECORDE GLOBAL! — {reason}", flush=True)
                    print(f"     Lucro médio/dia: ${m['avg_daily_profit']:.2f}", flush=True)
                    print(f"     Dias positivos:  {m['positive_days']}/{m['active_days']} ({m['consistency_pct']:.0f}%)", flush=True)
                    print(f"     PnL total período:  ${m['total_pnl']:.2f}", flush=True)
                    champ_params = {k: best_env.get(k) for k in PARAM_SPACE}
                    print(f"     Parâmetros: {champ_params}", flush=True)
                    print(f"{'★'*70}\n", flush=True)

                    if is_live_deployable(best_data):
                        try_deploy_winner(best_env, f"ref-opt it#{iteration+idx}: avg_day=${m['avg_daily_profit']:.2f} pos={m['positive_days']}")
                    else:
                        print("     ⏸️ Novo recorde não passa gate live; sem deploy.", flush=True)

                write_state(iteration + idx, baseline_metrics, best_data, history,
                            evaluating_candidates=candidates_ui,
                            monthly_champions=monthly_champions)

            iteration += N_WORKERS
            time.sleep(0.5)


if __name__ == "__main__":
    cycle = 1
    while True:
        try:
            print(f"\n♾️  OPTIMIZER MAIN LOOP #{cycle} — busca contínua até campeão validado", flush=True)
            main()
            cycle += 1
            print("\n🔁 main() encerrou sem campeão refinável; iniciando novo ciclo imediatamente...", flush=True)
            time.sleep(0.2)
        except KeyboardInterrupt:
            write_state(
                0,
                {},
                {},
                [],
                running=False,
                evaluating_candidates=[],
                phase="stopped",
            )
            raise
        except SystemExit as exc:
            print(f"\n[ERROR] optimizer requested exit: {exc}", flush=True)
            try:
                write_state(
                    0,
                    {},
                    {},
                    [],
                    running=False,
                    evaluating_candidates=[],
                    phase="error:system-exit",
                )
            except Exception:
                pass
            time.sleep(5.0)
            cycle += 1
        except Exception as exc:
            print(f"\n[ERROR] optimizer crashed: {exc}", flush=True)
            try:
                write_state(
                    0,
                    {},
                    {},
                    [],
                    running=False,
                    evaluating_candidates=[],
                    phase=f"error:{type(exc).__name__}",
                )
            except Exception:
                pass
            time.sleep(2.0)
            cycle += 1
