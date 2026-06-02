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

# ── Configuração ──────────────────────────────────────────────────────────────
START_DATE    = "2026-05-01"
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
            params TEXT NOT NULL
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
                consistency_pct, score, pnl, roi, sharpe, sortino, drawdown, elapsed_s, is_best, params)
               VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                json.dumps(params)
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

# Paralelismo: usa todos os 8 cores com prioridade baixa (nice -n 19)
N_WORKERS = 8

# ── Espaço de busca (hill-climbing gaussiano + perturbação discreta) ─────────
PARAM_SPACE = {
    # Score mínimo do ensemble (16–32): mais baixo = mais trades, menos filtro
    "CALM_ACCU_MIN_SCORE":          {"type": "int",       "min": 10, "max": 35,   "step": 1},
    # Prob mínima XGBoost (0.18–0.45): mais baixo = mais entradas
    "ENSEMBLE_MIN_PROB":            {"type": "float",     "min": 0.18, "max": 0.45, "step": 0.01},
    # CUSUM máximo p/ entrada (2.5–12): CRÍTICO — aumentar libera mais trades
    "CALM_ACCU_MAX_ENTRY_CUSUM":    {"type": "float",     "min": 2.5,  "max": 12.0, "step": 0.5},
    # Hurst mínimo (0.30–0.58): mais baixo = aceita mais regimes
    "ACCUMULATOR_MIN_HURST_EXPONENT": {"type": "float",  "min": 0.30, "max": 0.58, "step": 0.01},
    # Limiar de volatilidade (mais alto = mais trades em mercado volátil)
    "CALM_ACCU_THRESHOLD":          {"type": "float_sci", "min": 0.5e-6, "max": 5.0e-6},
    # XGBoost bypass (0.08–0.30): quando XGB pode ser ignorado
    "PCS_XGB_BYPASS_LIMIT":         {"type": "float",     "min": 0.08, "max": 0.30, "step": 0.01},
    # Take profit regime B+ agressivo (5–25 ticks)
    "PCS_REGIME_B_PLUS_TP":         {"type": "float",     "min": 3.0,  "max": 25.0, "step": 1.0},
    # Take profit regime B- defensivo (1–8 ticks)
    "PCS_REGIME_B_MINUS_TP":        {"type": "float",     "min": 1.0,  "max": 8.0,  "step": 0.5},
    # Stake base (1–8$): maior stake = mais lucro E mais risco
    "STAKE":                        {"type": "float",     "min": 1.0,  "max": 8.0,  "step": 0.5},
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

    # 4. Score Base (Super-Frankenstein)
    # Valoriza lucro e consistência, pune volatilidade, rebaixamentos e dias vermelhos
    score = (
        avg_day   * 12.0    # Lucro médio/dia — peso aumentado de 10 para 12
        + n_pos   * 4.0     # Cada dia positivo = +4 pts (aumentado de 3)
        + consist * 0.6     # % Consistência (aumentado de 0.5)
        - n_neg   * 10.0    # Penalidade por dia negativo (aumentado de 8)
        - std_dev * 3.0     # Penalidade por volatilidade diária
        - dd_penalty        # Penalidade por rebaixamento / pior dia
        - bust_pen          # Penalidade catástrofe
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
    Roda um backtest em subprocess. Executa em processo filho (multiprocessing).
    Usa arquivo temporário para não colidir com outros workers.
    """
    env_vars, worker_id = args
    tmp = Path(f"logs/backtest_worker_{worker_id}.json")

    # Prepara ambiente
    my_env = os.environ.copy()
    my_env.update(env_vars)
    my_env["BACKTEST_COMPOUNDING"] = "false"   # $50 fixo por dia — obrigatório!

    if tmp.exists():
        tmp.unlink()

    cmd = [
        "nice", "-n", "19",
        ".venv/bin/python", "backtest_engine.py",
        START_DATE, END_DATE, START_BALANCE, str(tmp),
    ]
    try:
        subprocess.run(
            cmd, env=my_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True, timeout=600,
        )
    except Exception as e:
        return None

    if not tmp.exists():
        return None

    try:
        data    = json.loads(tmp.read_text(encoding="utf-8"))
        results = data.get("results", [])
        if not results:
            return None
        summary_sf = data.get("summary", {}).get("strategies", {}).get("Super-Frankenstein", {})
        summary_live = data.get("summary", {}).get("strategies", {}).get("Pegasus Live Sniper (9% TP)", {})

        m = compute_score(results)   # Primary: Super-Frankenstein
        
        # Copia estatísticas avançadas do backtest
        m["sharpe_ratio"] = summary_sf.get("sharpe_ratio", 0.0)
        m["sortino_ratio"] = summary_sf.get("sortino_ratio", 0.0)
        m["max_drawdown"] = summary_sf.get("max_drawdown", 0.0)

        # Secondary: track Pegasus Live Sniper to monitor live bot correlation
        live = compute_score(results, "Pegasus Live Sniper (9% TP)")
        m["live_avg_daily"]   = live["avg_daily_profit"]
        m["live_positive_days"] = live["positive_days"]
        m["live_total_pnl"]   = live["total_pnl"]
        m["live_sharpe"]      = summary_live.get("sharpe_ratio", 0.0)
        m["live_sortino"]     = summary_live.get("sortino_ratio", 0.0)
        m["live_drawdown"]    = summary_live.get("max_drawdown", 0.0)
        
        m["_env"] = env_vars   # salva os params junto

        # Ponderação Avançada do Score Geral (Multi-Métrica + Risco)
        # Bônus por consistência estatística de Sharpe/Sortino
        sharpe_val = m["sharpe_ratio"]
        sortino_val = m["sortino_ratio"]
        max_dd_val = m["max_drawdown"]
        
        m["score"] += sharpe_val * 8.0
        m["score"] += sortino_val * 6.0
        
        # Penalização por rebaixamento máximo (drawdown)
        if max_dd_val > 15.0:
            m["score"] -= max_dd_val * 3.0
        elif max_dd_val > 5.0:
            m["score"] -= max_dd_val * 1.0

        # Evita overfitting com Live Bot
        live_avg = live["avg_daily_profit"]
        if live_avg < 0.0:
            m["score"] -= abs(live_avg) * 8.0
        else:
            m["score"] += live_avg * 3.0

        m["score"] = round(m["score"], 4)
        return m
    except Exception:
        return None
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


# ── Estado e dashboard ────────────────────────────────────────────────────────

_state_lock = threading.Lock()

def write_state(iteration: int, baseline: dict, best: dict | None,
                history: list, running: bool = True) -> None:
    """Grava estado para o dashboard ler (thread-safe)."""
    try:
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
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                       encoding="utf-8")
        tmp.replace(STATE_PATH)    # rename atômico
    except Exception as e:
        print(f"[WARN] write_state: {e}", flush=True)


# ── Deploy do vencedor ────────────────────────────────────────────────────────

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
                 "cd /opt/pegasus && .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log"],
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
    print(f"      ROI total maio: {baseline_metrics['roi_pct']:.1f}% = ${baseline_metrics['total_pnl']:.2f}", flush=True)
    print(f"      Lucro médio/dia: ${baseline_metrics['avg_daily_profit']:.2f}  (META: $50/dia)", flush=True)
    print(f"      Dias positivos:  {baseline_metrics['positive_days']}/{baseline_metrics['active_days']} ativos ({baseline_metrics['consistency_pct']:.0f}%)", flush=True)
    print(f"      Melhor dia: ${baseline_metrics['best_day_pnl']:.2f} | Pior dia: ${baseline_metrics['worst_day_pnl']:.2f}", flush=True)

    _init_opt_db()
    history = _load_opt_history()
    iteration = 1
    if history:
        iteration = max(h["iteration"] for h in history) + 1
        print(f"   📊 Carregado {len(history)} iterações do histórico do banco de dados (reiniciando no it#{iteration})", flush=True)

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
        print(f"   🏆 Recorde anterior recuperado do DB: score={best_score:.2f} avg_day=${best_avg:.2f}/dia (it#{best_data['iteration']})", flush=True)
    else:
        best_score = baseline_metrics["score"]
        best_pos   = baseline_metrics["positive_days"]
        best_avg   = baseline_metrics["avg_daily_profit"]
        best_data  = {**baseline_metrics, "iteration": 0}

    write_state(iteration - 1, baseline_metrics, best_data, history)

    # Garante que o bot ao vivo está online no startup (se não estiver em Modo Ultra-Estresse)
    is_stress = _read_stress_config()
    if not is_stress:
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
            deploy_winner(best_env, "Optimizer Startup: Live bot was offline, starting now")
    else:
        print("   🤖 [Ultra-Estresse] Bot ao vivo permanece em espera (OFFLINE) para prioridade de CPU/RAM.", flush=True)

    print(f"\n{'─'*70}", flush=True)
    print(f"  🚀 LOOP INFINITO — {N_WORKERS} backtests em paralelo por rodada", flush=True)
    print(f"  Critério: 1) + dias positivos  2) + lucro/dia  3) + score", flush=True)
    print(f"{'─'*70}\n", flush=True)

    # ── Loop infinito de otimização ──────────────────────────────────────────
    with ProcessPoolExecutor(max_workers=N_WORKERS) as pool:
        while True:
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

            # Gera N_WORKERS candidatos diferentes
            candidates = [(rand_params(best_env), f"w{i}") for i in range(N_WORKERS)]

            print(f"🔄 Iterações {iteration}–{iteration + N_WORKERS - 1} "
                  f"({N_WORKERS} em paralelo)...", flush=True)

            t0 = time.time()
            futures = {pool.submit(_run_one, c): i for i, c in enumerate(candidates)}

            results_batch = []
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    m = fut.result()
                    if not m:
                        continue
                except Exception as e:
                    print(f"   [worker {idx}] erro: {e}", flush=True)
                    continue

                is_better = False
                reason    = ""

                # Sanity check: só aceita recorde se:
                # 1) PnL total >= 0 E avg_day > 0 (sem prejuízo)
                # 2) avg_daily <= 30 (evita resultados implausíveis com stake<=8 e $50)
                # 3) active_days > 20 (bot realmente operou o mês inteiro)
                active = m.get("active_days", 0) or 0
                avg_d  = m["avg_daily_profit"]
                pnl_ok = (
                    m["total_pnl"] >= 0
                    and avg_d > 0
                    and avg_d <= 30.0      # teto de plausibilidade: max ~$30/dia com $50 banca
                    and active >= 20       # pelo menos 20 dias com operações
                )

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
                    "ts":             time.time(),
                }
                history.append(entry)
                _save_opt_iteration(entry, m["_env"])

                if is_better:
                    best_score = m["score"]
                    best_pos   = m["positive_days"]
                    best_avg   = m["avg_daily_profit"]
                    best_env   = m["_env"].copy()
                    best_data  = {**m, "iteration": iteration + idx, "reason": reason}

                    print(f"\n{'★'*70}", flush=True)
                    print(f"  🏆 NOVO RECORDE! — {reason}", flush=True)
                    print(f"     Lucro médio/dia: ${m['avg_daily_profit']:.2f}", flush=True)
                    print(f"     Dias positivos:  {m['positive_days']}/{m['active_days']} ({m['consistency_pct']:.0f}%)", flush=True)
                    print(f"     PnL total maio:  ${m['total_pnl']:.2f}  (ROI {m['roi_pct']:.1f}%)", flush=True)
                    print(f"     Melhor dia: ${m['best_day_pnl']:.2f} | Pior dia: ${m['worst_day_pnl']:.2f}", flush=True)
                    print(f"     Sharpe: {m.get('sharpe_ratio', 0.0):.4f} | Sortino: {m.get('sortino_ratio', 0.0):.4f} | Max DD: ${m.get('max_drawdown', 0.0):.2f}", flush=True)
                    champ_params = {k: best_env.get(k) for k in PARAM_SPACE}
                    print(f"     Parâmetros campeões: {champ_params}", flush=True)
                    print(f"{'★'*70}\n", flush=True)

                    # Deploy no bot ao vivo
                    ok = deploy_winner(best_env,
                        f"opt-v3 it#{iteration+idx}: avg_day=${m['avg_daily_profit']:.2f} pos={m['positive_days']}")
                    if ok:
                        print(f"   ✅ Bot ao vivo reiniciado com novos parâmetros!\n", flush=True)
                        if is_stress:
                            print(f"   🤖 [Ultra-Estresse] Ativando Bot ao Vivo imediatamente para operar com a estratégia campeã!\n", flush=True)
                            if _is_on_server():
                                try:
                                    # Limpa sessões mortas e inicializa a sessão detached screen
                                    subprocess.run(["screen", "-wipe"], capture_output=True)
                                    cmd = "cd /opt/pegasus && PYTHONUNBUFFERED=1 .venv/bin/python -u bot.py 2>&1 | tee -a logs/bot.log"
                                    subprocess.run(["screen", "-dmS", "pegasus", "bash", "-c", cmd])
                                except Exception as exc:
                                    print(f"   [ERR] Falha ao inicializar o Bot em Modo Ultra-Estresse: {exc}", flush=True)

                # Grava o estado de forma atômica e em tempo real para o dashboard ver
                write_state(iteration + idx, baseline_metrics, best_data, history)

            elapsed = time.time() - t0
            throughput = N_WORKERS / elapsed * 60  # iterações/min

            print(f"   ⏱  Rodada completada em {elapsed:.0f}s ({N_WORKERS}/{N_WORKERS} ok | {throughput:.0f} iter/min estimado)", flush=True)
            iteration += N_WORKERS
            time.sleep(0.2)  # respiração mínima


if __name__ == "__main__":
    main()
