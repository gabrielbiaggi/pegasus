#!/usr/bin/env python3
"""
Backtest engine real — código idêntico ao bot, amostragem a cada 60 ticks.
Uso: backtest_engine.py START_DATE END_DATE START_BALANCE OUTPUT_JSON

Processa dia a dia, salva resultados incrementalmente no OUTPUT_JSON.
Usa arquivos data/ticks_BOOM1000_YYYY-MM-DD.csv quando disponíveis,
senão filtra data/ticks_BOOM1000_max.csv por data.
"""

import json
import os
import sys
import time
from collections import deque
from datetime import date as _date
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Garante que imports do projeto funcionem mesmo rodando de outro dir
sys.path.insert(0, str(Path(__file__).parent))
from strategy import (
    AccumulatorStrategyConfig,
    calculate_tick_indicators,
    generate_calm_accu_signal,
)

load_dotenv()

# ── Parâmetros fixos ────────────────────────────────────────────────────────
STAKE = 5.0
TP_PCT = 0.30
MAX_HOLD = 80
GROWTH_RATE = 0.03
MAX_STAKE = 10.0
SOROS_STEPS = 3
SOROS_COOLDOWN = 2
STOP_GAIN = 1.00
TRAILING_S = 0.30
TRAILING_L = 0.05
CUSUM_MAX = 5.0
HURST_MIN = 0.45
CALM_THRESH = 1.5e-6
BOOM_THRESH = 0.000242
SAMPLE_EVERY = 60
TICK_COUNT = 100
CALM_MIN_SCORE = 20

# Lê BLOCKED_UTC_HOURS do .env (padrão: 5,6,7,8,9)
_blocked_raw = os.getenv("BLOCKED_UTC_HOURS", "5,6,7,8,9")
BLOCKED_HOURS: set[int] = set()
for _h in _blocked_raw.split(","):
    _h = _h.strip()
    if _h.isdigit():
        BLOCKED_HOURS.add(int(_h))

# ── Config de indicadores (idêntica ao backtest_live_clone) ─────────────────
accu_cfg = AccumulatorStrategyConfig(
    min_score=CALM_MIN_SCORE,
    bb_window=20,
    bb_std_dev=2.0,
    max_bb_width_percent=0.12,
    atr_window=20,
    max_tick_atr_percent=0.015,
    recent_window=5,
    max_recent_move_percent=0.05,
    hawkes_alpha=1.0,
    hawkes_beta=0.85,
    hawkes_jump_atr_multiplier=1.5,
    max_hawkes_intensity=1.0,
    imbalance_window=10,
    max_abs_tick_imbalance=4,
    hurst_window=30,
    max_hurst_exponent=0.60,
    derivative_window=20,
    max_velocity_zscore=2.0,
    max_acceleration_zscore=2.0,
    integral_window=20,
    max_pmi_distance_percent=0.02,
    markov_window=50,
    max_markov_continuation_prob=0.65,
    shannon_entropy_window=30,
    min_shannon_entropy=0.80,
    kalman_q=1e-5,
    kalman_r=1e-2,
    max_kalman_residual_zscore=2.0,
    use_ensemble=False,
    ensemble_min_prob=0.294,
    calm_min_score=CALM_MIN_SCORE,
)


def _write_state(out_path: Path, state: dict) -> None:
    """Salva estado incremental com escrita atômica via arquivo temporário."""
    tmp = out_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, default=str))
    tmp.replace(out_path)


def _load_day_df(day: _date, data_dir: Path) -> pd.DataFrame | None:
    """
    Carrega ticks de um dia. Prefere arquivo diário; cai no max.csv se ausente.
    Retorna None se não houver dados suficientes.
    """
    daily_path = data_dir / f"ticks_BOOM1000_{day.isoformat()}.csv"
    if daily_path.exists() and daily_path.stat().st_size > 1000:
        df = pd.read_csv(daily_path)
    else:
        max_path = data_dir / "ticks_BOOM1000_max.csv"
        if not max_path.exists():
            return None
        full = pd.read_csv(max_path)
        full["_date"] = pd.to_datetime(full["epoch"], unit="s", utc=True).dt.date
        df = full[full["_date"] == day].drop(columns=["_date"])

    if df.empty or len(df) < TICK_COUNT + 10:
        return None

    df["epoch"] = pd.to_numeric(df["epoch"], errors="coerce")
    df["quote"] = pd.to_numeric(df["quote"], errors="coerce")
    df = (
        df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
    )

    # Colunas auxiliares
    df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df["hour"] = df["dt"].dt.hour
    q = df["quote"].values
    rets = np.zeros(len(q))
    rets[1:] = np.abs(np.diff(q) / q[:-1])
    df["boom"] = rets > BOOM_THRESH
    df["avg_ret"] = pd.Series(rets).rolling(10).mean().values

    return df


def _sim_day(day: _date, day_df: pd.DataFrame, balance: float) -> dict:
    """Simula um dia completo. Retorna dict com métricas."""
    t0 = time.time()
    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    booms = day_df["boom"].values
    epochs = day_df["epoch"].values

    tick_buf: deque = deque(maxlen=TICK_COUNT)
    for w in range(min(TICK_COUNT, len(day_df))):
        tick_buf.append({"epoch": int(epochs[w]), "quote": float(prices[w])})

    sod = bal = balance
    peak = balance
    trail = False
    sor_step = sor_cd = 0
    sor_prof = 0.0
    wins = losses = 0
    stop_reason = None
    i = TICK_COUNT

    while i < len(day_df) - MAX_HOLD - 1:
        # Adiciona tick ao buffer (sempre, para manter contexto contínuo)
        tick_buf.append({"epoch": int(epochs[i]), "quote": float(prices[i])})

        # Filtra horário bloqueado mas mantém buffer consistente
        if hours[i] in BLOCKED_HOURS:
            i += 1
            continue

        # Amostragem: avalia apenas a cada SAMPLE_EVERY ticks
        if (i - TICK_COUNT) % SAMPLE_EVERY != 0:
            i += 1
            continue

        avg = avgs[i]
        if np.isnan(avg) or avg >= CALM_THRESH:
            i += 1
            continue

        # Indicadores reais
        try:
            df_ind = calculate_tick_indicators(list(tick_buf), config=accu_cfg)
        except Exception:
            i += 1
            continue
        if df_ind is None or df_ind.empty:
            i += 1
            continue

        df_ind = df_ind.reset_index(drop=True)

        # Sinal idêntico ao bot
        prices_list = [t["quote"] for t in list(tick_buf)]
        try:
            signal, score, p_loss = generate_calm_accu_signal(
                prices_list,
                threshold=CALM_THRESH,
                lookback=10,
                df=df_ind,
                config=accu_cfg,
                ensemble_scorer=None,
            )
        except Exception:
            i += 1
            continue

        if signal != "ACCU":
            i += 1
            continue

        # Quality gate (idêntico ao bot)
        row = df_ind.iloc[-1]
        cusum_v = float(row.get("cusum_score", 0) or 0)
        hurst_v = float(row.get("hurst_exponent", 0.5) or 0.5)
        if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
            i += 1
            continue

        # Stake com soros
        if sor_cd > 0:
            stake = STAKE
            sor_cd -= 1
        elif sor_step > 0:
            stake = round(min(STAKE + sor_prof, MAX_STAKE), 2)
        else:
            stake = STAKE
        stake = max(0.35, min(stake, MAX_STAKE, bal))
        if stake < 0.35:
            i += SAMPLE_EVERY
            continue

        # Simula trade com ticks reais
        profit = -stake
        is_win = False
        hold = MAX_HOLD
        for j in range(1, MAX_HOLD + 1):
            if i + j >= len(prices):
                break
            if booms[i + j]:
                hold = j
                break
            cv = stake * ((1 + GROWTH_RATE) ** j)
            if cv - stake >= stake * TP_PCT:
                profit = round(stake * TP_PCT, 2)
                is_win = True
                hold = j
                break

        if is_win:
            wins += 1
            sor_cd = 0
            if sor_step < SOROS_STEPS:
                sor_step += 1
                sor_prof = round(sor_prof + profit, 2)
            else:
                sor_step = 0
                sor_prof = 0.0
        else:
            losses += 1
            sor_step = 0
            sor_prof = 0.0
            sor_cd = SOROS_COOLDOWN

        bal = round(bal + profit, 2)
        peak = max(peak, bal)
        pnl = bal - sod
        i += hold + 6  # pula o hold + cooldown de barrier reset

        if pnl >= sod * STOP_GAIN:
            stop_reason = "DOBROU"
            break
        if not trail and pnl >= sod * TRAILING_S:
            trail = True
        if trail and pnl <= sod * TRAILING_L:
            stop_reason = "TRAILING"
            break

    total = wins + losses
    pnl_d = bal - sod
    return {
        "date": day.isoformat(),
        "trades": total,
        "wins": wins,
        "losses": losses,
        "wr": round(wins / total * 100, 1) if total else 0.0,
        "pnl": round(pnl_d, 2),
        "pnl_pct": round(pnl_d / sod * 100, 1) if sod > 0 else 0.0,
        "peak_pct": round((peak - sod) / sod * 100, 1) if sod > 0 else 0.0,
        "balance": round(bal, 2),
        "doubled": stop_reason == "DOBROU",
        "trailing": stop_reason == "TRAILING",
        "elapsed_s": round(time.time() - t0, 1),
    }


def main() -> None:
    if len(sys.argv) < 5:
        print(f"Usage: {sys.argv[0]} START_DATE END_DATE START_BALANCE OUTPUT_JSON")
        print(
            f"Example: {sys.argv[0]} 2026-05-06 2026-05-20 50.0 logs/backtest_live.json"
        )
        sys.exit(1)

    start_date = _date.fromisoformat(sys.argv[1])
    end_date = _date.fromisoformat(sys.argv[2])
    start_balance = float(sys.argv[3])
    out_path = Path(sys.argv[4])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Constrói lista de dias úteis (exclui fins de semana)
    days: list[_date] = []
    cur = start_date
    while cur <= end_date:
        if cur.weekday() < 5:  # 0=seg … 4=sex
            days.append(cur)
        cur += timedelta(days=1)

    data_dir = Path(__file__).parent / "data"

    t_global = time.time()
    state: dict = {
        "status": "running",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "start_balance": start_balance,
        "current_balance": start_balance,
        "current_day": start_date.isoformat(),
        "elapsed_s": 0,
        "results": [],
        "summary": {},
    }
    _write_state(out_path, state)

    balance = start_balance

    for day in days:
        state["current_day"] = day.isoformat()
        state["elapsed_s"] = round(time.time() - t_global, 1)
        _write_state(out_path, state)

        day_df = _load_day_df(day, data_dir)
        if day_df is None:
            print(f"  {day}: sem dados — pulando", flush=True)
            continue

        print(f"  {day}: simulando...", end=" ", flush=True)
        result = _sim_day(day, day_df, balance)
        balance = result["balance"]

        state["current_balance"] = balance
        state["results"].append(result)

        flag = (
            "🎯 DOBROU"
            if result["doubled"]
            else (
                "🔒 TRAILING"
                if result["trailing"]
                else ("✅" if result["pnl"] >= 0 else "❌")
            )
        )
        print(
            f"{result['trades']} trades | WR {result['wr']:.1f}% | "
            f"P&L {result['pnl']:+.2f} | Bal ${balance:.2f}  {flag}  [{result['elapsed_s']:.0f}s]",
            flush=True,
        )

    # Calcula sumário final
    results = state["results"]
    n = len(results)
    if n > 0:
        doubled = sum(1 for r in results if r["doubled"])
        positive = sum(1 for r in results if r["pnl"] > 0)
        total_wr = sum(r["wr"] for r in results)
        state["summary"] = {
            "total_days": n,
            "doubled": doubled,
            "positive": positive,
            "avg_wr": round(total_wr / n, 1),
            "final_balance": round(balance, 2),
        }

    state["status"] = "done"
    state["elapsed_s"] = round(time.time() - t_global, 1)
    state["current_balance"] = round(balance, 2)
    _write_state(out_path, state)

    print(f"\n✅ Concluído em {state['elapsed_s']:.0f}s | Saldo final: ${balance:.2f}")


if __name__ == "__main__":
    main()
