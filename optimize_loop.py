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
END_DATE      = "2026-05-31"
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
    conn = sqlite3.connect(str(db_path))
    try:
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
                json.dumps(params),
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
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM optimizer_history WHERE is_best = 1 ORDER BY score DESC LIMIT 1"
        )
        r = cursor.fetchone()
        if r:
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
            try:
                params = json.loads(r["params"])
            except Exception:
                params = {}
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
                if p:
                    champions.append(p)
            except Exception:
                pass
    except Exception as e:
        print(f"[WARN] _load_top_champions error: {e}", flush=True)
    finally:
        conn.close()
    return champions

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
            
ACTIVE_SYMBOL = _env_for_vol.get("SYMBOL", "BOOM1000")

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

# Paralelismo: usa todos os 8 cores com prioridade baixa (nice -n 19)
N_WORKERS = 8

# ── Espaço de busca focado no DNA do campeão it#508 (Jan-Mai 2026) ─────────────
# it#508: STAKE=11.0 CUSUM=7.0 MIN_SCORE=14 XGB=0.314 HURST=0.47
# Busca ±30% ao redor dos valores campeões — maximiza chance de superar
PARAM_SPACE = {
    # Score mínimo do ensemble: it#508=14 → busca 9–19
    "CALM_ACCU_MIN_SCORE":             {"type": "int",       "min": 9,    "max": 19,   "step": 1},
    # Prob mínima XGBoost: it#508=0.314 → busca 0.22–0.42
    "ENSEMBLE_MIN_PROB":               {"type": "float",     "min": 0.22, "max": 0.42, "step": 0.01},
    # CUSUM máximo: it#508=7.0 → busca 4.5–10.5
    "CALM_ACCU_MAX_ENTRY_CUSUM":       {"type": "float",     "min": 4.5,  "max": 10.5, "step": 0.5},
    # Hurst mínimo: it#508=0.47 → busca 0.35–0.55 (ligeiramente maior p/ robustez multi-mês)
    "ACCUMULATOR_MIN_HURST_EXPONENT":  {"type": "float",     "min": 0.35, "max": 0.55, "step": 0.01},
    # Limiar de volatilidade: dinâmico com base na volatilidade mediana do símbolo ativo
    "CALM_ACCU_THRESHOLD":             {"type": "float_sci", "min": 0.3 * MEDIAN_VOL, "max": 2.0 * MEDIAN_VOL},
    # XGB bypass: it#508=0.25 → busca 0.15–0.35
    "PCS_XGB_BYPASS_LIMIT":            {"type": "float",     "min": 0.15, "max": 0.35, "step": 0.01},
    # TP regime B+: it#508=18.0 → busca 12.0–28.0
    "PCS_REGIME_B_PLUS_TP":            {"type": "float",     "min": 12.0, "max": 28.0, "step": 1.0},
    # TP regime B-: it#508=11.0 → busca 6.0–14.0
    "PCS_REGIME_B_MINUS_TP":           {"type": "float",     "min": 6.0,  "max": 14.0, "step": 0.5},
    # Stake: it#508=11.0 → busca 9.0–13.0 (foco na zona lucrativa)
    "STAKE":                           {"type": "float",     "min": 9.0,  "max": 13.0, "step": 0.5},
}

FROZEN_PARAMS = set()  # todos os params são livres

# ── Funções utilitárias ───────────────────────────────────────────────────────

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


def rand_params(base: dict) -> dict:
    """Perturba 1-4 parâmetros aleatoriamente do espaço de busca."""
    p = base.copy()
    eligible = [k for k in PARAM_SPACE if k not in FROZEN_PARAMS]
    num = random.randint(1, min(4, len(eligible)))
    keys = random.sample(eligible, num)

    for key in keys:
        space = PARAM_SPACE[key]
        curr = p.get(key)

        if space["type"] == "int":
            step = space.get("step", 1)
            val  = int(curr) if curr else (space["min"] + space["max"]) // 2
            # Às vezes pula 2-3 steps para exploração mais ampla
            delta = random.choice([-3,-2,-1,1,2,3]) * step
            val   = max(space["min"], min(space["max"], val + delta))
            p[key] = str(int(val))

        elif space["type"] == "float":
            step = space.get("step", 0.01)
            val  = float(curr) if curr else (space["min"] + space["max"]) / 2
            delta = random.choice([-3,-2,-1,1,2,3]) * step
            val   = round(max(space["min"], min(space["max"], val + delta)), 4)
            p[key] = str(val)

        elif space["type"] == "float_sci":
            val = float(curr) if curr else (space["min"] + space["max"]) / 2
            # Multiplica por fator aleatório
            factor = random.choice([0.6, 0.75, 0.85, 1.0, 1.15, 1.30, 1.50, 1.75])
            val    = max(space["min"], min(space["max"], val * factor))
            p[key] = f"{val:.2e}"

    return p


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

    score = (
        avg_day        * 12.0    # Lucro médio/dia — independente de período
        + consist_bonus * 600.0  # Bônus de consistência normalizado (substitui n_pos * 4)
        + consist       * 0.6    # % Consistência (reforço)
        - neg_rate      * 1500.0 # Penalidade por dias negativos normalizada (substitui n_neg * 10)
        - std_dev       * 3.0    # Penalidade por volatilidade diária
        - dd_penalty             # Penalidade por rebaixamento / pior dia
        - bust_pen               # Penalidade catástrofe
    )


    return {
        "avg_daily_profit": round(avg_day, 4),
        "positive_days":    n_pos,
        "negative_days":    n_neg,
        "active_days":      n_act,
        "total_days":       len(days),
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
    env_vars = env_vars.copy()
    env_vars["BACKTEST_COMPOUNDING"] = "false"   # $50 fixo por dia — obrigatório!
    env_vars["PEGASUS_OPTIMIZER_RUN"] = "true"

    try:
        return backtest_engine.run_backtest_direct(
            start_date_str=START_DATE,
            end_date_str=END_DATE,
            start_balance=float(START_BALANCE),
            env_overrides=env_vars,
            worker_id=worker_id,
        )
    except Exception as e:
        print(f"   [worker {worker_id}] erro: {e}", flush=True)
        return None


# ── Estado e dashboard ────────────────────────────────────────────────────────

_state_lock = threading.Lock()

def write_state(iteration: int, baseline: dict, best: dict | None,
                history: list, running: bool = True,
                evaluating_candidates: list | None = None,
                monthly_champions: dict | None = None) -> None:
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
            "baseline":          baseline,
            "best":              best,
            "iterations":        history[-200:],
            "last_update":       time.time(),
            "start_date":        START_DATE,
            "end_date":          END_DATE,
            "n_workers":         N_WORKERS,
        }
        if existing_candidates is not None:
            payload["evaluating_candidates"] = existing_candidates
        if existing_champions is not None:
            payload["monthly_champions"] = existing_champions

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
                "params": params.copy(),
                "monthly_pnls": all_months_pnls,
                "avg_daily_profit": round(data["avg_daily_profit"], 2),
                "consistency_pct": round(data["consistency_pct"], 1),
                "active_days": data["active_days"]
            }
            updated = True
    return updated


# Estado global para debouncing de deploy
_last_deploy_time = 0.0
_pending_deploy_env = None
_pending_deploy_msg = ""
DEPLOY_COOLDOWN = 120.0  # 2 minutos de cooldown mínimo para o bot ao vivo respirar


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


def deploy_winner(env_vars: dict, msg: str, min_pnl: float = 5.0) -> bool:
    """
    Salva .env e reinicia o bot ao vivo com os novos parâmetros.
    Garante que nunca haverá instâncias duplicadas do bot.
    """
    save_env(env_vars)
    print(f"   💾 .env salvo com novos parâmetros.", flush=True)

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
    baseline_metrics = _run_one((best_env, "baseline"))
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
            champ_res = backtest_engine.run_backtest_direct(
                start_date_str=START_DATE,
                end_date_str=END_DATE,
                start_balance=float(START_BALANCE),
                env_overrides=best_env,
            )
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
            ok = try_deploy_winner(best_env, f"Startup Sync: deploy campeão it#{db_it_str} do banco de dados", force=True)
            if ok:
                bot_was_synced = True
                print(f"      ✅ Deploy de startup concluído com sucesso!", flush=True)
            else:
                print(f"      ⚠️  Deploy de startup retornou falha, mas parâmetros foram salvos no .env", flush=True)
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
            print("   ⚠️  Bot ao vivo está OFFLINE no startup. Inicializando...", flush=True)
            try_deploy_winner(best_env, "Optimizer Startup: Live bot was offline, starting now", force=True)
    else:
        print("   🤖 [Ultra-Estresse] Bot ao vivo permanece em espera (OFFLINE) para prioridade de CPU/RAM.", flush=True)

    print(f"\n{'─'*70}", flush=True)
    print(f"  🚀 LOOP INFINITO — {N_WORKERS} backtests em paralelo por rodada", flush=True)
    print(f"  Critério: 1) + dias positivos  2) + lucro/dia  3) + score", flush=True)
    print(f"{'─'*70}\n", flush=True)

    # ── Loop infinito de otimização ──────────────────────────────────────────
    with ProcessPoolExecutor(max_workers=N_WORKERS) as pool:
        while True:
            # 0. Verifica se há deploy pendente cujo cooldown expirou
            if _pending_deploy_env is not None and (time.time() - _last_deploy_time >= DEPLOY_COOLDOWN):
                print(f"   ⏰ Cooldown expirou. Executando deploy pendente do último campeão encontrado.", flush=True)
                try_deploy_winner(_pending_deploy_env, _pending_deploy_msg, force=True)

            # 1. Verifica e aplica o Modo Ultra-Estresse na rodada ativa
            is_stress = _read_stress_config()
            if is_stress and _is_on_server():
                try:
                    # Encerra o live bot para liberar 100% de CPU/RAM da VM
                    check = subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True, timeout=2)
                    if check.returncode == 0:
                        print("   🤖 [Ultra-Estresse] Pausando Bot ao Vivo para estresse máximo do Otimizador...", flush=True)
                        subprocess.run(["screen", "-S", "pegasus", "-X", "quit"], capture_output=True)
                except Exception:
                    pass

            # Carrega o pool de campeões históricos para busca evolutiva multi-champion
            champs = _load_top_champions()
            champion_pool = []
            for c in champs:
                champion_pool.append({k: str(v) for k, v in c.items()})
            if not champion_pool:
                champion_pool = [best_env]

            # Gera N_WORKERS candidatos perturbando campeões históricos aleatórios do pool
            candidates = [(rand_params(random.choice(champion_pool)), f"w{i}") for i in range(N_WORKERS)]

            print(f"🔄 Iterações {iteration}–{iteration + N_WORKERS - 1} "
                  f"({N_WORKERS} em paralelo)...", flush=True)

            # Atualiza o arquivo de estado indicando que estamos ativamente avaliando estes candidatos
            local_candidates = [
                {
                    **{k: c[0].get(k) for k in PARAM_SPACE if k in c[0]},
                    "status": "Simulando...",
                    "result_pnl": None,
                    "result_days": None,
                }
                for c in candidates
            ]
            try:
                with _state_lock:
                    state_data = {}
                    if STATE_PATH.exists():
                        try:
                            state_data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
                        except Exception:
                            pass
                    state_data["running"] = True
                    state_data["current_iteration"] = iteration
                    state_data["evaluating_candidates"] = local_candidates
                    tmp = STATE_PATH.with_suffix(".tmp")
                    tmp.write_text(json.dumps(state_data, indent=2, ensure_ascii=False), encoding="utf-8")
                    tmp.replace(STATE_PATH)
            except Exception as e:
                print(f"[WARN] pre-write_state: {e}", flush=True)

            t0 = time.time()
            futures = {pool.submit(_run_one, c): i for i, c in enumerate(candidates)}

            results_batch = []
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    m = fut.result()
                    if not m:
                        local_candidates[idx]["status"] = "Falha"
                        write_state(iteration + idx, baseline_metrics, best_data, history, evaluating_candidates=local_candidates, monthly_champions=monthly_champions)
                        continue
                except Exception as e:
                    print(f"   [worker {idx}] erro: {e}", flush=True)
                    local_candidates[idx]["status"] = "Erro"
                    write_state(iteration + idx, baseline_metrics, best_data, history, evaluating_candidates=local_candidates, monthly_champions=monthly_champions)
                    continue

                is_better = False
                reason    = ""

                # Sanity check: só aceita recorde se:
                # 1) PnL total >= 0 E avg_day > 0 (sem prejuízo)
                # 2) avg_daily <= 120.0 (teto de plausibilidade realista com stakes maiores)
                # 3) active_days >= 100 (bot realmente operou no período Jan-Mai)
                active = m.get("active_days", 0) or 0
                avg_d  = m["avg_daily_profit"]
                pnl_ok = (
                    m["total_pnl"] >= 0
                    and avg_d > 0
                    and avg_d <= 120.0     # teto de plausibilidade: max $120/dia com $50 banca e Soros
                    and active >= 100      # 5 meses: pelo menos 100 dias com operações (de ~144)
                )

                if pnl_ok:
                    local_candidates[idx]["status"] = f"Finalizado: ${avg_d:.2f}/dia"
                    local_candidates[idx]["result_pnl"] = round(avg_d, 2)
                    local_candidates[idx]["result_days"] = f"{m['positive_days']}/{active}"
                    updated_champ = update_monthly_champions(monthly_champions, iteration + idx, m, candidates[idx][0])
                    if updated_champ:
                        print(f"   🏆 [Campeão Mensal] Nova melhor performance mensal detectada!", flush=True)
                else:
                    local_candidates[idx]["status"] = f"Inválido (${avg_d:.2f}/dia)"
                    local_candidates[idx]["result_pnl"] = round(avg_d, 2)
                    local_candidates[idx]["result_days"] = f"{m['positive_days']}/{active}"

                if not pnl_ok:
                    pass  # resultado inválido ou implausível → descarta
                elif m["positive_days"] > best_pos:
                    is_better = True
                    reason = f"+dias_pos ({best_pos}→{m['positive_days']})"
                elif m["positive_days"] == best_pos:
                    if m["avg_daily_profit"] > best_avg + 0.05:
                        is_better = True
                        reason = f"+avg_day (${best_avg:.2f}→${m['avg_daily_profit']:.2f})"
                    elif abs(m["avg_daily_profit"] - best_avg) <= 0.05 and m["score"] > best_score + 1.0:
                        is_better = True
                        reason = f"+score ({best_score:.1f}→{m['score']:.1f})"

                icon = "🔥 NOVO RECORD!" if is_better else "   ·"
                print(f"   [it#{iteration+idx}] {fmt(m)} → {icon}", flush=True)

                est_elapsed = time.time() - t0
                entry = {
                    "iteration":      iteration + idx,
                    "roi":            m["roi_pct"],
                    "pnl":            m["total_pnl"],
                    "busted":         m["negative_days"],
                    "elapsed_s":      round(est_elapsed, 1),
                    "is_best":        is_better,
                    "avg_daily":      m["avg_daily_profit"],
                    "positive_days":  m["positive_days"],
                    "negative_days":  m["negative_days"],
                    "consistency_pct": m["consistency_pct"],
                    "score":          m["score"],
                    "sharpe":         m.get("sharpe_ratio", 0.0),
                    "sortino":        m.get("sortino_ratio", 0.0),
                    "drawdown":       m.get("max_drawdown", 0.0),
                    "live_avg_daily": m.get("live_avg_daily"),
                    "live_positive_days": m.get("live_positive_days"),
                    "live_total_pnl": m.get("live_total_pnl"),
                    "live_sharpe":    m.get("live_sharpe"),
                    "live_sortino":   m.get("live_sortino"),
                    "live_drawdown":  m.get("live_drawdown"),
                    "best_day_pnl":   m.get("best_day_pnl"),
                    "worst_day_pnl":  m.get("worst_day_pnl"),
                    "ts":             time.time(),
                }
                history.append(entry)
                _save_opt_iteration(entry, m["_env"])

                if is_better:
                    best_score = m["score"]
                    best_pos   = m["positive_days"]
                    best_avg   = m["avg_daily_profit"]
                    best_env   = m["_env"].copy()
                    best_env["OPTIMIZER_CHAMPION_ITERATION"] = str(iteration + idx)
                    best_data  = {**m, "iteration": iteration + idx, "reason": reason}

                    print(f"\n{'★'*70}", flush=True)
                    print(f"  🏆 NOVO RECORDE! — {reason}", flush=True)
                    print(f"     Lucro médio/dia: ${m['avg_daily_profit']:.2f}", flush=True)
                    print(f"     Dias positivos:  {m['positive_days']}/{m['active_days']} ({m['consistency_pct']:.0f}%)", flush=True)
                    print(f"     PnL total período:  ${m['total_pnl']:.2f}  (ROI {m['roi_pct']:.1f}%)", flush=True)
                    print(f"     Melhor dia: ${m['best_day_pnl']:.2f} | Pior dia: ${m['worst_day_pnl']:.2f}", flush=True)
                    print(f"     Sharpe: {m.get('sharpe_ratio', 0.0):.4f} | Sortino: {m.get('sortino_ratio', 0.0):.4f} | Max DD: ${m.get('max_drawdown', 0.0):.2f}", flush=True)
                    champ_params = {k: best_env.get(k) for k in PARAM_SPACE}
                    print(f"     Parâmetros campeões: {champ_params}", flush=True)
                    print(f"{'★'*70}\n", flush=True)

                    # Deploy no bot ao vivo
                    ok = try_deploy_winner(best_env,
                        f"opt-v3 it#{iteration+idx}: avg_day=${m['avg_daily_profit']:.2f} pos={m['positive_days']}")
                    if ok:
                        print(f"   ✅ Bot ao vivo reiniciado com novos parâmetros!\n", flush=True)
                        if is_stress:
                            print(f"   🤖 [Ultra-Estresse] Ativando Bot ao Vivo imediatamente para operar com a estratégia campeã!\n", flush=True)
                            if _is_on_server():
                                try:
                                    # Limpa sessões mortas e inicializa a sessão detached screen
                                    subprocess.run(["screen", "-wipe"], capture_output=True)
                                    cmd = "cd /opt/pegasus && PEGASUS_LIVE_BOT=true PYTHONUNBUFFERED=1 .venv/bin/python -u bot.py 2>&1 | tee -a logs/trades.log"
                                    subprocess.run(["screen", "-dmS", "pegasus", "bash", "-c", cmd])
                                except Exception as exc:
                                    print(f"   [ERR] Falha ao inicializar o Bot em Modo Ultra-Estresse: {exc}", flush=True)

                # Grava o estado de forma atômica e em tempo real para o dashboard ver
                write_state(iteration + idx, baseline_metrics, best_data, history, evaluating_candidates=local_candidates, monthly_champions=monthly_champions)

            elapsed = time.time() - t0
            throughput = N_WORKERS / elapsed * 60  # iterações/min

            print(f"   ⏱  Rodada completada em {elapsed:.0f}s ({N_WORKERS}/{N_WORKERS} ok | {throughput:.0f} iter/min estimado)", flush=True)
            iteration += N_WORKERS
            time.sleep(0.2)  # respiração mínima


if __name__ == "__main__":
    main()
