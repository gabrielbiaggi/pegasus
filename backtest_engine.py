#!/usr/bin/env python3
"""
Backtest engine real — código idêntico ao bot, amostragem a cada 60 ticks.
Uso: backtest_engine.py START_DATE END_DATE START_BALANCE OUTPUT_JSON

Processa dia a dia, salva resultados incrementalmente no OUTPUT_JSON.
Usa arquivos data/ticks_BOOM1000_YYYY-MM-DD.csv quando disponíveis,
senão filtra data/ticks_BOOM1000_max.csv por data.
"""

import asyncio
import json
import os
import sys
import time
from collections import deque
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import timedelta, timezone
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

# ── Credenciais Deriv para download automático de ticks ───────────────────────
TOKEN = os.getenv("DERIV_TOKEN", "")
APP_ID = os.getenv("DERIV_APP_ID", "1089")
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

# ── Parâmetros — lidos do .env para garantir fidel. ao bot real ─────────────────
STAKE = float(os.getenv("STAKE", "5"))
MAX_STAKE = float(os.getenv("MAX_STAKE", "10"))
GROWTH_RATE = float(os.getenv("ACCUMULATOR_GROWTH_RATE", "0.03"))
TP_PCT = float(os.getenv("ACCUMULATOR_TAKE_PROFIT_PERCENT", "30")) / 100.0
MAX_HOLD = int(os.getenv("ACCUMULATOR_MAX_HOLD_TICKS", "80"))
SOROS_STEPS = int(os.getenv("SOROS_MAX_STEPS", "3"))
SOROS_COOLDOWN = int(os.getenv("ACCUMULATOR_COOLDOWN_TICKS", "5"))
STOP_GAIN = 1.00
TRAILING_S = 0.30
TRAILING_L = 0.05
CUSUM_MAX = float(os.getenv("CALM_ACCU_MAX_ENTRY_CUSUM", "5.0"))
HURST_MIN = float(os.getenv("ACCUMULATOR_MIN_HURST_EXPONENT", "0.45"))
CALM_THRESH = float(os.getenv("CALM_ACCU_THRESHOLD", "1.5e-6"))
SAMPLE_EVERY = 60
TICK_COUNT = int(os.getenv("TICK_COUNT", "100"))
CALM_MIN_SCORE = int(os.getenv("CALM_ACCU_MIN_SCORE", "20"))

# ── Simulação de barreira REAL (per-tick, idêntica ao Accumulator da Deriv) ───────
# A Deriv recalcula a barreira A CADA TICK: o preço não pode mover mais que
# ±barrier_pct do tick ANTERIOR. Não é cumulativo desde a entrada.
# P50 real do shadow_ticks: 0.000245% = 0.00000245 ratio.
# Usamos 0.0000025 como padrão (calibrado para WR ~76% em dados reais,
# que com o calm filter do bot chega a ~80-83%).
PER_TICK_BARRIER = float(os.getenv("BACKTEST_PER_TICK_BARRIER", "0.0000025"))
SLIPPAGE = 1  # 1 tick de delay de execução (latência do bot real)

# WIN_TICKS: quantos ticks para atingir TP (30% com 3%/tick = 9 ticks)
_v = 1.0
WIN_TICKS = MAX_HOLD
for _j in range(1, MAX_HOLD + 1):
    _v *= 1 + GROWTH_RATE
    if _v - 1.0 >= TP_PCT:
        WIN_TICKS = _j
        break

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


# ── Download automático de ticks ─────────────────────────────────────────────────────────────
async def _download_day_async(day: _date, out_path: Path) -> int:
    """Baixa ticks de um dia completo da Deriv em chunks de 1 hora."""
    try:
        import websockets  # opcional — não falha se não instalado
    except ImportError:
        return 0

    dt = _datetime.combine(day, _datetime.min.time()).replace(tzinfo=timezone.utc)
    start_epoch = int(dt.timestamp())
    end_epoch = start_epoch + 86400 - 1
    ticks: list[str] = []

    try:
        async with websockets.connect(WS_URL, ping_interval=30, open_timeout=15) as ws:
            # Autoriza com o token do bot
            await ws.send(json.dumps({"authorize": TOKEN}))
            resp = json.loads(await ws.recv())
            if "error" in resp:
                print(f"Auth Deriv falhou: {resp['error']['message']}", flush=True)
                return 0

            # Busca em blocos de 1 hora (máx 5000 ticks cada)
            start = start_epoch
            while start < end_epoch:
                chunk_end = min(start + 3600, end_epoch)
                await ws.send(
                    json.dumps(
                        {
                            "ticks_history": "BOOM1000",
                            "start": start,
                            "end": chunk_end,
                            "style": "ticks",
                            "count": 5000,
                        }
                    )
                )
                resp = json.loads(await ws.recv())
                if "error" in resp:
                    break
                hist = resp.get("history", {})
                prices = hist.get("prices", [])
                times = hist.get("times", [])
                for t, p in zip(times, prices):
                    ticks.append(f"{t},{p}")
                if not times:
                    break
                start = max(times) + 1
    except Exception as e:
        print(f"  Websocket erro: {e}", flush=True)
        return 0

    if len(ticks) > 10:
        out_path.write_text("epoch,quote\n" + "\n".join(ticks))
    return len(ticks)


# Cache da cobertura do max.csv: lemos apenas uma vez o range de datas
_max_csv_range: tuple[_date, _date] | None = None  # (day_min, day_max)


def _get_max_csv_range(data_dir: Path) -> tuple[_date, _date] | None:
    """Lê o range de DATAS do max.csv uma única vez e guarda em cache."""
    global _max_csv_range
    if _max_csv_range is not None:
        return _max_csv_range
    max_path = data_dir / "ticks_BOOM1000_max.csv"
    if not max_path.exists():
        return None
    try:
        epochs = pd.read_csv(max_path, usecols=["epoch"])["epoch"]
        min_ep, max_ep = int(epochs.min()), int(epochs.max())
        day_min = _datetime.fromtimestamp(min_ep, tz=timezone.utc).date()
        day_max = _datetime.fromtimestamp(max_ep, tz=timezone.utc).date()
        _max_csv_range = (day_min, day_max)
        print(f"  max.csv cobre: {day_min} → {day_max}", flush=True)
        return _max_csv_range
    except Exception:
        return None


def _ensure_day_ticks(day: _date, data_dir: Path, state: dict | None = None) -> bool:
    """
    Garante que ticks do dia existem.
    1. Checa arquivo diário (ticks_BOOM1000_YYYY-MM-DD.csv)
    2. Checa se max.csv cobre o dia (cache do range de epochs)
    3. Tenta baixar da Deriv se TOKEN disponível
    Retorna True se dados estão disponíveis.
    """
    # BOOM1000 opera 24/7 incluindo fins de semana (BLOCK_WEEKENDS=false no bot real)
    daily = data_dir / f"ticks_BOOM1000_{day.isoformat()}.csv"
    if daily.exists() and daily.stat().st_size > 5_000:
        return True

    # Checa cobertura do max.csv (leitura única, depois usa cache de datas)
    csv_range = _get_max_csv_range(data_dir)
    if csv_range is not None:
        day_min, day_max = csv_range
        if day_min <= day <= day_max:
            return True  # dia coberto pelo max.csv

    # Não temos os dados — tenta baixar
    if not TOKEN:
        print(f"  {day}: sem ticks e DERIV_TOKEN não configurado — pulando", flush=True)
        return False

    msg = f"  {day}: ticks ausentes, baixando da Deriv..."
    print(msg, end=" ", flush=True)
    if state is not None:
        state["download_status"] = f"Baixando ticks de {day}..."

    try:
        n = asyncio.run(_download_day_async(day, daily))
        if n > 100:
            print(f"{n} ticks OK", flush=True)
            if state is not None:
                state.pop("download_status", None)
            return True
        else:
            print(f"poucos ticks ({n}) — dia provavelmente sem mercado", flush=True)
            return False
    except Exception as e:
        print(f"ERRO: {e}", flush=True)
        return False


def _write_state(out_path: Path, state: dict) -> None:
    """Salva estado incremental com escrita atômica via arquivo temporário."""
    state["last_update"] = round(time.time(), 3)  # timestamp para detectar staleness
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
    # avg_ret: usado pelo filtro calm (nao mais BOOM_THRESH por tick)
    df["avg_ret"] = pd.Series(rets).rolling(10).mean().values

    return df


# ── Estratégias de stake ─────────────────────────────────────────────────────────
FIB_SEQ = [1, 1, 2, 3, 5, 8, 13, 21]

STRATEGY_NAMES = [
    "flat",  # stake fixo, sem recovery
    "soros",  # bot atual: acumula lucro nos próximos stakes
    "martingale",  # dobra stake após loss
    "fibonacci",  # fib sequence após loss
    "soros_martingale",  # soros em wins + martingale em losses
    "soros_fibonacci",  # soros em wins + fib em losses
    "dalembert",  # +1 unidade após loss, -1 após win
]


def _replay_strategy(
    outcomes: list[bool],  # True=WIN, False=LOSS
    strategy: str,
    start_balance: float,
) -> dict:
    """Replay uma sequência de WIN/LOSS com uma estratégia de stake.
    Retorna métricas da simulação.
    """
    base = STAKE
    bal = start_balance
    peak = bal
    sod = bal
    wins = losses = 0
    max_dd_pct = 0.0
    # State vars
    sor_step = sor_profit = 0.0
    sor_cd = 0
    gale_step = 0
    gale_loss_accum = 0.0
    dale_units = 1  # D'Alembert unit counter
    trail = False
    stop_reason = None

    for is_win in outcomes:
        if bal < 0.35:
            break  # bust

        # Calcula stake baseado na estratégia
        if strategy == "flat":
            stake = base

        elif strategy == "soros":
            if sor_cd > 0:
                stake = base
            elif sor_step > 0:
                stake = min(base + sor_profit, MAX_STAKE)
            else:
                stake = base

        elif strategy == "martingale":
            if gale_step == 0:
                stake = base
            else:
                stake = min(base * (2**gale_step), bal, MAX_STAKE * 4)

        elif strategy == "fibonacci":
            if gale_step == 0:
                stake = base
            else:
                idx = min(gale_step, len(FIB_SEQ) - 1)
                stake = min(base * FIB_SEQ[idx], bal, MAX_STAKE * 4)

        elif strategy == "soros_martingale":
            if gale_step > 0:
                stake = min(base * (2**gale_step), bal, MAX_STAKE * 4)
            elif sor_cd > 0:
                stake = base
            elif sor_step > 0:
                stake = min(base + sor_profit, MAX_STAKE)
            else:
                stake = base

        elif strategy == "soros_fibonacci":
            if gale_step > 0:
                idx = min(gale_step, len(FIB_SEQ) - 1)
                stake = min(base * FIB_SEQ[idx], bal, MAX_STAKE * 4)
            elif sor_cd > 0:
                stake = base
            elif sor_step > 0:
                stake = min(base + sor_profit, MAX_STAKE)
            else:
                stake = base

        elif strategy == "dalembert":
            stake = min(base * dale_units, bal, MAX_STAKE * 2)

        else:
            stake = base

        stake = round(max(0.35, min(stake, bal)), 2)
        if stake < 0.35:
            break

        # Aplica resultado
        if is_win:
            profit = round(stake * TP_PCT, 2)
            wins += 1
            # Update strategy state
            if strategy in ("soros", "soros_martingale", "soros_fibonacci"):
                sor_cd = 0
                if sor_step < SOROS_STEPS:
                    sor_step += 1
                    sor_profit = round(sor_profit + profit, 2)
                else:
                    sor_step = 0
                    sor_profit = 0.0
            if strategy in (
                "martingale",
                "fibonacci",
                "soros_martingale",
                "soros_fibonacci",
            ):
                gale_step = 0
                gale_loss_accum = 0.0
            if strategy == "dalembert":
                dale_units = max(1, dale_units - 1)
        else:
            profit = -stake
            losses += 1
            if strategy in ("soros", "soros_martingale", "soros_fibonacci"):
                sor_step = 0
                sor_profit = 0.0
                sor_cd = SOROS_COOLDOWN
            if strategy in (
                "martingale",
                "fibonacci",
                "soros_martingale",
                "soros_fibonacci",
            ):
                gale_step = min(gale_step + 1, 6)  # cap 6 gales
                gale_loss_accum += stake
            if strategy == "dalembert":
                dale_units = min(dale_units + 1, 8)  # cap 8

        bal = round(bal + profit, 2)
        peak = max(peak, bal)
        if peak > 0:
            dd = (peak - bal) / peak * 100
            max_dd_pct = max(max_dd_pct, dd)

        # Stop diario
        pnl = bal - sod
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
        "trades": total,
        "wins": wins,
        "losses": losses,
        "wr": round(wins / total * 100, 1) if total else 0.0,
        "pnl": round(pnl_d, 2),
        "balance": round(bal, 2),
        "max_dd_pct": round(max_dd_pct, 1),
        "doubled": stop_reason == "DOBROU",
        "trailing": stop_reason == "TRAILING",
        "busted": bal < 0.35,
    }


def _collect_day_outcomes(
    day: _date,
    day_df: pd.DataFrame,
    state: dict | None = None,
    out_path: Path | None = None,
    t_global: float | None = None,
) -> tuple[list[bool], float]:
    """Coleta WIN/LOSS outcomes de um dia SEM aplicar estratégia de stake.
    Retorna (lista de bools True=WIN, elapsed_seconds).
    """
    t0 = time.time()
    t_last_progress = t0

    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    epochs = day_df["epoch"].values
    total_ticks = len(day_df)

    tick_buf: deque = deque(maxlen=TICK_COUNT)
    for w in range(min(TICK_COUNT, len(day_df))):
        tick_buf.append({"epoch": int(epochs[w]), "quote": float(prices[w])})

    outcomes: list[bool] = []
    i = TICK_COUNT

    while i < len(day_df) - MAX_HOLD - 1:
        # Progresso intra-dia
        now = time.time()
        if (
            state is not None
            and out_path is not None
            and (now - t_last_progress) >= 5.0
        ):
            w_count = sum(outcomes)
            l_count = len(outcomes) - w_count
            total_tr = len(outcomes)
            state["day_progress"] = {
                "ticks_done": int(i),
                "ticks_total": int(total_ticks),
                "pct": round(i / total_ticks * 100, 1),
                "trades": total_tr,
                "wins": w_count,
                "losses": l_count,
                "wr": round(w_count / total_tr * 100, 1) if total_tr else 0.0,
                "elapsed_day_s": round(now - t0, 1),
            }
            if t_global is not None:
                state["elapsed_s"] = round(now - t_global, 1)
            _write_state(out_path, state)
            t_last_progress = now

        # Adiciona tick ao buffer
        tick_buf.append({"epoch": int(epochs[i]), "quote": float(prices[i])})

        if hours[i] in BLOCKED_HOURS:
            i += 1
            continue

        if (i - TICK_COUNT) % SAMPLE_EVERY != 0:
            i += 1
            continue

        avg = avgs[i]
        if np.isnan(avg) or avg >= CALM_THRESH:
            i += 1
            continue

        try:
            df_ind = calculate_tick_indicators(list(tick_buf), config=accu_cfg)
        except Exception:
            i += 1
            continue
        if df_ind is None or df_ind.empty:
            i += 1
            continue
        df_ind = df_ind.reset_index(drop=True)

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

        row = df_ind.iloc[-1]
        cusum_v = float(row.get("cusum_score", 0) or 0)
        hurst_v = float(row.get("hurst_exponent", 0.5) or 0.5)
        if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
            i += 1
            continue

        # ── Barreira PER-TICK ────────────────────────────────────────
        entry_idx = i + SLIPPAGE
        if entry_idx >= len(prices) - WIN_TICKS:
            i += 1
            continue

        is_win = True
        hold = WIN_TICKS + SLIPPAGE
        for j in range(1, WIN_TICKS + 1):
            prev_idx = entry_idx + j - 1
            curr_idx = entry_idx + j
            if curr_idx >= len(prices):
                is_win = False
                hold = SLIPPAGE + j
                break
            tick_move = abs(prices[curr_idx] - prices[prev_idx]) / prices[prev_idx]
            if tick_move >= PER_TICK_BARRIER:
                is_win = False
                hold = SLIPPAGE + j
                break

        outcomes.append(is_win)
        i += hold + SOROS_COOLDOWN

    elapsed = round(time.time() - t0, 1)
    return outcomes, elapsed


def _sim_day_multi(
    day: _date,
    day_df: pd.DataFrame,
    balances: dict[str, float],
    state: dict | None = None,
    out_path: Path | None = None,
    t_global: float | None = None,
) -> dict:
    """Simula um dia com TODAS as estratégias. Retorna resultados comparativos."""
    outcomes, elapsed = _collect_day_outcomes(day, day_df, state, out_path, t_global)

    w = sum(outcomes)
    total = len(outcomes)
    day_result = {
        "date": day.isoformat(),
        "total_signals": total,
        "signal_wins": w,
        "signal_losses": total - w,
        "signal_wr": round(w / total * 100, 1) if total else 0.0,
        "elapsed_s": elapsed,
        "strategies": {},
    }

    for strat in STRATEGY_NAMES:
        bal = balances.get(strat, 50.0)
        r = _replay_strategy(outcomes, strat, bal)
        r["start_balance"] = round(bal, 2)
        day_result["strategies"][strat] = r
        balances[strat] = r["balance"]

    return day_result


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

    # BOOM1000 opera 24/7 — inclui fins de semana (fiel ao bot real: BLOCK_WEEKENDS=false)
    days: list[_date] = []
    cur = start_date
    while cur <= end_date:
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
        "total_days": len(days),
        "results": [],
        "summary": {},
    }
    _write_state(out_path, state)

    balance = start_balance

    # Fase 1: verifica e baixa ticks faltantes ANTES de simular
    print("\n[1/2] Verificando ticks disponíveis...", flush=True)
    days_with_data: list[_date] = []
    for day in days:
        state["current_day"] = day.isoformat()
        state["download_status"] = f"Verificando {day}..."
        _write_state(out_path, state)
        ok = _ensure_day_ticks(day, data_dir, state)
        if ok:
            days_with_data.append(day)
    state.pop("download_status", None)
    state["total_days"] = len(days_with_data)
    print(f"  {len(days_with_data)}/{len(days)} dias com dados", flush=True)

    # Fase 2: simulação MULTI-ESTRATÉGIA
    print("\n[2/2] Simulando 7 estratégias...", flush=True)
    print(f"  Estratégias: {', '.join(STRATEGY_NAMES)}")

    # Cada estratégia mantém seu próprio saldo
    balances = {s: start_balance for s in STRATEGY_NAMES}

    for day in days_with_data:
        state["current_day"] = day.isoformat()
        state["elapsed_s"] = round(time.time() - t_global, 1)
        _write_state(out_path, state)

        day_df = _load_day_df(day, data_dir)
        if day_df is None:
            print(f"  {day}: sem dados — pulando", flush=True)
            continue

        print(f"  {day}: simulando...", end=" ", flush=True)
        result = _sim_day_multi(
            day,
            day_df,
            balances,
            state=state,
            out_path=out_path,
            t_global=t_global,
        )

        # Limpa progresso intra-dia
        state.pop("day_progress", None)
        state["results"].append(result)
        state["current_balance"] = balances.get("soros", start_balance)

        # Print resumo do dia
        wr = result["signal_wr"]
        line = f"{result['total_signals']}T WR {wr:.0f}% | "
        for s in STRATEGY_NAMES:
            sr = result["strategies"][s]
            emoji = "✅" if sr["pnl"] > 0 else ("💥" if sr["busted"] else "❌")
            line += f"{s}:{emoji}${sr['balance']:.0f} "
        print(f"{line} [{result['elapsed_s']:.0f}s]", flush=True)

    # Sumário final
    results = state["results"]
    n = len(results)
    if n > 0:
        summary = {"total_days": n, "strategies": {}}
        for s in STRATEGY_NAMES:
            final_bal = balances[s]
            total_pnl = round(final_bal - start_balance, 2)
            busted_days = sum(1 for r in results if r["strategies"][s]["busted"])
            pos_days = sum(1 for r in results if r["strategies"][s]["pnl"] > 0)
            avg_wr = round(sum(r["signal_wr"] for r in results) / n, 1)
            summary["strategies"][s] = {
                "final_balance": round(final_bal, 2),
                "total_pnl": total_pnl,
                "roi_pct": round(total_pnl / start_balance * 100, 1),
                "positive_days": pos_days,
                "busted_days": busted_days,
                "avg_signal_wr": avg_wr,
            }
        state["summary"] = summary

        # Tabela final no console
        print("\n" + "=" * 80)
        print(
            f"  {'Estratégia':<20} {'Saldo':>8} {'PnL':>8} {'ROI':>7} {'Bust':>5} {'Dias+':>6}"
        )
        print("-" * 80)
        for s in STRATEGY_NAMES:
            ss = summary["strategies"][s]
            print(
                f"  {s:<20} ${ss['final_balance']:>7.2f} {ss['total_pnl']:>+7.2f} {ss['roi_pct']:>+6.1f}% {ss['busted_days']:>5} {ss['positive_days']:>5}/{n}"
            )
        print("=" * 80)

    state["status"] = "done"
    state["elapsed_s"] = round(time.time() - t_global, 1)
    _write_state(out_path, state)

    print(f"\n✅ Concluído em {state['elapsed_s']:.0f}s")


if __name__ == "__main__":
    main()
