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
    EnsembleScorer,
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
STOP_GAIN = float(os.getenv("STOP_GAIN_PCT", "100.0")) / 100.0
TRAILING_S = float(os.getenv("DAILY_TRAILING_START", "30.0")) / 100.0
TRAILING_L = float(os.getenv("DAILY_TRAILING_LOCK", "5.0")) / 100.0

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
    use_ensemble=True,
    ensemble_min_prob=0.294,
    calm_min_score=CALM_MIN_SCORE,
)

# ── XGBoost EnsembleScorer: filtra sinais ruins (idêntico ao bot real) ─────────
_ensemble_scorer: EnsembleScorer | None = None
try:
    _model_path = Path(__file__).parent / "models" / "pegasus_xgb_v3_pertick.json"
    _feat_path = Path(__file__).parent / "models" / "pegasus_features_v3_pertick.json"
    if _model_path.exists() and _feat_path.exists():
        _ensemble_scorer = EnsembleScorer(
            model_path=str(_model_path),
            features_path=str(_feat_path),
        )
        print(f"  XGBoost v3 PER-TICK carregado OK", flush=True)
    else:
        # Fallback para v1
        _model_path = Path(__file__).parent / "models" / "pegasus_xgb_v1.json"
        _feat_path = Path(__file__).parent / "models" / "pegasus_features_v1.json"
        if _model_path.exists():
            _ensemble_scorer = EnsembleScorer(str(_model_path), str(_feat_path))
            print(f"  XGBoost v1 fallback carregado", flush=True)
except Exception as _e:
    print(f"  XGBoost nao carregou: {_e}", flush=True)
    _ensemble_scorer = None

ENSEMBLE_MIN_PROB = float(os.getenv("ENSEMBLE_MIN_PROB", "0.30"))


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
    1. Checa se existem ticks no PostgreSQL (shadow_ticks / shadow_ticks_accumulator)
    2. Checa arquivo diário (ticks_BOOM1000_YYYY-MM-DD.csv)
    3. Checa se max.csv cobre o dia (cache do range de epochs)
    4. Tenta baixar da Deriv se TOKEN disponível
    Retorna True se dados estão disponíveis.
    """
    pg_dsn = os.getenv("PG_DSN")
    if pg_dsn:
        try:
            import psycopg2
            dt = _datetime.combine(day, _datetime.min.time()).replace(tzinfo=timezone.utc)
            start_ep = int(dt.timestamp())
            end_ep = start_ep + 86400 - 1
            conn = psycopg2.connect(pg_dsn)
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM shadow_ticks WHERE entry_epoch >= %s AND entry_epoch <= %s", (start_ep, end_ep))
            cnt = cur.fetchone()[0]
            if cnt < 1000:
                cur.execute("SELECT count(*) FROM shadow_ticks_accumulator WHERE entry_epoch >= %s AND entry_epoch <= %s", (start_ep, end_ep))
                cnt = cur.fetchone()[0]
            cur.close()
            conn.close()
            if cnt >= 1000:
                return True
        except Exception:
            pass

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


def _load_day_df_from_pg(day: _date) -> pd.DataFrame | None:
    pg_dsn = os.getenv("PG_DSN")
    if not pg_dsn:
        return None
    try:
        import psycopg2
        dt_start = _datetime.combine(day, _datetime.min.time()).replace(tzinfo=timezone.utc)
        epoch_start = int(dt_start.timestamp())
        epoch_end = epoch_start + 86400 - 1
        
        conn = psycopg2.connect(pg_dsn)
        query = """
            SELECT entry_epoch as epoch, entry_quote as quote 
            FROM shadow_ticks 
            WHERE entry_epoch >= %s AND entry_epoch <= %s 
            ORDER BY entry_epoch ASC
        """
        df = pd.read_sql(query, conn, params=(epoch_start, epoch_end))
        conn.close()
        if df.empty or len(df) < TICK_COUNT + 10:
            conn = psycopg2.connect(pg_dsn)
            query = """
                SELECT entry_epoch as epoch, entry_quote as quote 
                FROM shadow_ticks_accumulator 
                WHERE entry_epoch >= %s AND entry_epoch <= %s 
                ORDER BY entry_epoch ASC
            """
            df = pd.read_sql(query, conn, params=(epoch_start, epoch_end))
            conn.close()
            
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
        df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
        return df
    except Exception as e:
        print(f"  Erro ao carregar do PG para o dia {day}: {e}", flush=True)
        return None


def _load_day_df(day: _date, data_dir: Path) -> pd.DataFrame | None:
    """
    Carrega ticks de um dia. Prefere carregar do PostgreSQL; senão cai em arquivo diário e max.csv.
    Retorna None se não houver dados suficientes.
    """
    df = _load_day_df_from_pg(day)
    if df is not None:
        print(f"  [PG] {len(df)} ticks carregados", end=" ", flush=True)
        return df

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
DAILY_SL_PCT = float(os.getenv("STOP_LOSS_PCT", "100.0")) / 100.0
STOP_GAIN_PCT = float(os.getenv("STOP_GAIN_PCT", "100.0")) / 100.0


# Cada config: (nome, tp_pct, win_ticks, min_score, stake_mode, daily_sl, wr_stop)
# stake_mode: 'flat'=fixo $5, 'pct2'=2% da banca
# wr_stop: se WR < 70% após 20 trades, para o dia (Opção C)


def _calc_win_ticks(tp_pct: float) -> int:
    v = 1.0
    for j in range(1, 200):
        v *= 1 + GROWTH_RATE
        if v - 1.0 >= tp_pct:
            return j
    return 80


def _generate_strategy_configs() -> list[dict]:
    configs = []
    
    # 1. Mantém os 9 Sniperes base como referência crucial
    base_configs = [
        {"name": "Sniper Otimizado (30% TP)", "tp": 0.30, "score": 25, "mode": "flat10", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 2},
        {"name": "Sniper Só Soros (30% TP)", "tp": 0.30, "score": 25, "mode": "flat10", "use_soros": True, "soros_steps": 2, "use_martingale": False, "max_gales": 0},
        {"name": "Sniper Só Gale (30% TP)", "tp": 0.30, "score": 25, "mode": "flat10", "use_soros": False, "soros_steps": 0, "use_martingale": True, "max_gales": 2},
        {"name": "Sniper Flat $10 (30% TP)", "tp": 0.30, "score": 25, "mode": "flat10", "use_soros": False, "soros_steps": 0, "use_martingale": False, "max_gales": 0},
        {"name": "Conservador 3% TP", "tp": 0.03, "score": 25, "mode": "flat10", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 2},
        {"name": "Conservador Flat $10", "tp": 0.03, "score": 25, "mode": "flat10", "use_soros": False, "soros_steps": 0, "use_martingale": False, "max_gales": 0},
        {"name": "Pegasus Live Sniper (9% TP)", "tp": 0.09, "score": 25, "mode": "flat15", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 2},
        {"name": "Frankenstein Sniper (30% TP, $5)", "tp": 0.30, "score": 25, "mode": "flat5", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 1},
        {"name": "Super-Frankenstein", "tp": 0.30, "score": 25, "mode": "dynamic_10", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 1, "is_super_frank": True},
    ]
    configs.extend(base_configs)

    # 2. Gera variações estatísticas robustas combinando TP, Stakes e Recuperação (até atingir 50 configs)
    tps = [0.05, 0.15, 0.25, 0.35, 0.45]
    modes = ["flat5", "flat10", "dynamic_10", "dynamic_15"]
    soros_opts = [(True, 2), (False, 0)]
    gale_opts = [(True, 1), (True, 2), (False, 0)]
    
    idx = 10
    for tp in tps:
        for mode in modes:
            for use_soros, soros_steps in soros_opts:
                for use_martingale, max_gales in gale_opts:
                    if len(configs) >= 50:
                        break
                    
                    # Nome amigável identificando as propriedades da estratégia
                    name = f"Regime #{idx} (TP {int(tp*100)}% | {mode.replace('flat', '$')}"
                    if use_soros:
                        name += f" + S{soros_steps}"
                    if use_martingale:
                        name += f" + G{max_gales}"
                    name += ")"
                    
                    configs.append({
                        "name": name,
                        "tp": tp,
                        "score": 25,
                        "mode": mode,
                        "use_soros": use_soros,
                        "soros_steps": soros_steps,
                        "use_martingale": use_martingale,
                        "max_gales": max_gales
                    })
                    idx += 1
                    
    return configs

STRATEGY_CONFIGS = _generate_strategy_configs()
STRATEGY_NAMES = [c["name"] for c in STRATEGY_CONFIGS]


def _replay_strategy(
    outcomes: list[tuple],
    config: dict,
    start_balance: float,
) -> dict:
    """Replay WIN/LOSS com o RiskManager real do Pegasus para máxima fidelidade e cálculos simultâneos!"""
    from risk_manager import RiskManager
    import tempfile
    
    tp_pct = config["tp"]
    mode = config["mode"]
    is_super_frank = config.get("is_super_frank", False)
    
    use_soros = config.get("use_soros", False)
    use_martingale = config.get("use_martingale", False)
    
    # Mapeia modo para stake
    dynamic_stake_base_pct = 0.0
    if mode == "flat15":
        fixed_stake = 15.0
        min_stake = 15.0
    elif mode == "flat10":
        fixed_stake = 10.0
        min_stake = 10.0
    elif mode == "flat5":
        fixed_stake = 5.0
        min_stake = 5.0
    elif mode == "flat1":
        fixed_stake = 1.0
        min_stake = 1.0
    elif mode == "dynamic_10":
        fixed_stake = 5.0
        min_stake = 5.0
        dynamic_stake_base_pct = 0.10
    else:
        fixed_stake = STAKE
        min_stake = STAKE
        
    with tempfile.TemporaryDirectory() as tmp_dir:
        state_file = str(Path(tmp_dir) / "risk.json")
        risk = RiskManager(
            balance=start_balance,
            max_loss_day=99999.0,
            max_profit_day=99999.0,
            max_trades_day=99999,
            daily_trailing_start=TRAILING_S * 100.0,
            daily_trailing_lock=TRAILING_L * 100.0,
            max_stake_pct=1.0,
            fixed_stake=fixed_stake,
            min_stake=min_stake,
            max_stake=MAX_STAKE,
            max_consecutive_losses=10,
            use_soros=use_soros,
            soros_max_steps=config.get("soros_steps", SOROS_STEPS),
            soros_profit_factor=1.0,
            use_martingale=use_martingale,
            martingale_max_gales=config.get("max_gales", 2),
            martingale_payout_rate=tp_pct,
            dynamic_stake_base_pct=dynamic_stake_base_pct,
            state_path=state_file,
            simulated_balance=start_balance,
            stop_loss_pct=DAILY_SL_PCT * 100.0,
            stop_gain_pct=STOP_GAIN * 100.0,
        )
        
        wins = losses = 0
        stop_reason = None
        
        for item in outcomes:
            if len(item) == 9:
                is_win, epoch, avg, cusum_v, hurst_v, barrier_hit_at, shannon_v, kalman_v, p_loss = item
            elif len(item) == 6:
                is_win, epoch, avg, cusum_v, hurst_v, barrier_hit_at = item
                shannon_v = 0.0
                kalman_v = 0.0
                p_loss = 1.0
            else:
                is_win, epoch, avg, cusum_v, hurst_v = item
                barrier_hit_at = None
                shannon_v = 0.0
                kalman_v = 0.0
                p_loss = 1.0
                
            risk._sim_time = epoch
            risk._sim_monotonic_time = epoch
            
            # Se for Super-Frankenstein, aplica regime switching e gale standby dinâmicos!
            if is_super_frank:
                is_absolute_calm = False
                is_medium_calm = False
                
                # Calmaria Extrema (Regime A) Check:
                _pass_a_xgb = (p_loss < 0.22)
                if (
                    avg < 1.0e-6
                    and cusum_v < 2.5
                    and hurst_v > 0.48
                    and shannon_v > 0.85
                    and abs(kalman_v) < 1.5
                    and _pass_a_xgb
                ):
                    is_absolute_calm = True
                
                # Calmaria Moderada (Regime B+) Check:
                _pass_b_plus_xgb = (p_loss < 0.26)
                if (
                    avg < 2.2e-6
                    and cusum_v < 4.0
                    and hurst_v > 0.45
                    and _pass_b_plus_xgb
                ):
                    is_medium_calm = True
                
                _in_gale = risk.use_martingale and risk.martingale_step > 0
                
                if _in_gale:
                    # GALE STANDBY & BYPASS
                    _xgb_bypass = (p_loss < float(os.getenv("PCS_XGB_BYPASS_LIMIT", "0.15")))
                    if not is_absolute_calm and not _xgb_bypass:
                        continue  # Standby, aguarda calmaria extrema ou IA Bypass no próximo tick
                    
                    # Gale Fire em Regime A (30% TP, 9 ticks)
                    regime_tp = float(os.getenv("ACCUMULATOR_TAKE_PROFIT_PERCENT", "30.0")) / 100.0
                    regime_hold = int(os.getenv("ACCUMULATOR_MAX_HOLD_TICKS", "9"))
                    current_tp_pct = regime_tp
                    current_wt = regime_hold
                    risk.martingale_payout_rate = regime_tp
                    risk.use_soros = False
                else:
                    if is_absolute_calm:
                        # Regime A: Sniper 30% TP com Soros ATIVO
                        regime_tp = float(os.getenv("ACCUMULATOR_TAKE_PROFIT_PERCENT", "30.0")) / 100.0
                        regime_hold = int(os.getenv("ACCUMULATOR_MAX_HOLD_TICKS", "9"))
                        current_tp_pct = regime_tp
                        current_wt = regime_hold
                        risk.use_soros = True
                        risk.soros_max_steps = 2
                    elif is_medium_calm:
                        # Regime B+: Medium Harvester 9% TP com 3 Ticks e Soros DESATIVADO
                        regime_b_plus_tp = float(os.getenv("PCS_REGIME_B_PLUS_TP", "9.0"))
                        regime_b_plus_hold = int(os.getenv("PCS_REGIME_B_PLUS_HOLD", "3"))
                        current_tp_pct = regime_b_plus_tp / 100.0
                        current_wt = regime_b_plus_hold
                        risk.use_soros = False
                    else:
                        # Regime B-: Defensive Harvester 3% TP com 1 Tick e Soros DESATIVADO
                        regime_b_minus_tp = float(os.getenv("PCS_REGIME_B_MINUS_TP", "3.0"))
                        regime_b_minus_hold = int(os.getenv("PCS_REGIME_B_MINUS_HOLD", "1"))
                        current_tp_pct = regime_b_minus_tp / 100.0
                        current_wt = regime_b_minus_hold
                        risk.use_soros = False
                
                # Determina o resultado WIN/LOSS com base no win_ticks do regime ativo
                is_win_trade = not (barrier_hit_at is not None and barrier_hit_at <= current_wt)
            else:
                current_tp_pct = tp_pct
                is_win_trade = is_win
            
            # Dynamic Cooldown Bypass (same as live bot)
            if getattr(risk, "cooldown_until", 0.0) > 0:
                if (
                    avg < CALM_THRESH
                    and cusum_v < 3.0
                    and hurst_v > 0.45
                ):
                    risk.reset_cooldown_early()
            
            if not risk.can_trade():
                # If we are in session cooldown, skip this tick but do NOT break the daily loop
                if getattr(risk, "cooldown_until", 0.0) > 0:
                    continue
                # Otherwise, it's a permanent Stop Loss or Stop Gain block
                if risk.daily_net_profit >= risk._effective_profit_limit():
                    stop_reason = "DOBROU"
                elif risk.daily_net_profit <= -risk._effective_loss_limit():
                    stop_reason = "SL"
                elif risk.daily_trailing_active and risk.daily_net_profit <= risk._daily_trailing_lock_abs:
                    stop_reason = "TRAIL"
                else:
                    stop_reason = "BLOCKED"
                break

                
            stake = risk.get_stake()
            if stake <= 0.0:
                stop_reason = "BUST"
                break
                
            # Simula a dedução do stake antes da compra (igual no bot real)
            risk.balance = round(risk.balance - stake, 2)
            risk._pending_stake_deduction = stake
            
            if is_win_trade:
                profit = round(stake * current_tp_pct, 2)
                risk.update(profit=profit, buy_price=stake)
                wins += 1
            else:
                risk.update(profit=-stake, buy_price=stake)
                losses += 1
                
        total = wins + losses
        return {
            "trades": total,
            "wins": wins,
            "losses": losses,
            "wr": round(wins / total * 100, 1) if total else 0.0,
            "pnl": round(risk.balance - start_balance, 2),
            "balance": round(risk.balance, 2),
            "peak_pnl": round(risk.daily_peak_profit, 2),
            "doubled": stop_reason == "DOBROU",
            "trailing": stop_reason == "TRAIL",
            "busted": risk.balance < min_stake or stop_reason == "BUST",
            "stop": stop_reason or "",
        }


def _collect_day_outcomes(
    day: _date,
    day_df: pd.DataFrame,
    state: dict | None = None,
    out_path: Path | None = None,
    t_global: float | None = None,
) -> tuple[dict[str, list[tuple]], float]:
    """Coleta WIN/LOSS outcomes para TODAS as configs de TP e score.
    Retorna {config_name: [(is_win, epoch, avg, cusum_v, hurst_v, barrier_hit_at)]}, elapsed.
    """
    data_dir = Path(__file__).parent / "data"
    t0 = time.time()
    t_last_progress = t0

    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    epochs = day_df["epoch"].values
    total_ticks = len(day_df)

    # Determina quais índices serão avaliados (amostrados) no backtest
    sample_indices = []
    for w in range(TICK_COUNT, len(day_df)):
        if hours[w] in BLOCKED_HOURS:
            continue
        if (w - TICK_COUNT) % SAMPLE_EVERY != 0:
            continue
        avg = avgs[w]
        if np.isnan(avg) or avg >= CALM_THRESH:
            continue
        sample_indices.append(w)

    # ─── SISTEMA DE CACHE DE INDICADORES (Super Otimização com RAM Disk /dev/shm) ───
    shm_dir = Path("/dev/shm/pegasus_cache")
    disk_dir = data_dir / "cache"
    disk_dir.mkdir(parents=True, exist_ok=True)
    
    # Tenta usar o RAM disk (/dev/shm) se ele for gravável
    cache_dir = shm_dir
    if not cache_dir.exists():
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            cache_dir = disk_dir

    filename = f"indicators_BOOM1000_{day.isoformat()}.csv.gz"
    cache_path = cache_dir / filename
    disk_path = disk_dir / filename
    
    # Se o arquivo estiver no disco mas ausente do RAM disk, copia para RAM disk para acesso rápido subsequente
    if cache_dir != disk_dir and not cache_path.exists() and disk_path.exists():
        try:
            import shutil
            shutil.copy2(disk_path, cache_path)
        except Exception as e:
            print(f"  [SHM] Erro ao copiar {filename} para /dev/shm: {e}", flush=True)
            # Fallback temporário para o disco
            cache_path = disk_path

    day_indicators_df = None
    if cache_path.exists():
        try:
            day_indicators_df = pd.read_csv(cache_path, compression="gzip")
        except Exception as e:
            print(f"  Erro ao carregar cache {cache_path}: {e}. Recalculando...", flush=True)
            # Se deu erro e estamos no SHM, tenta no disco como backup
            if cache_path != disk_path and disk_path.exists():
                try:
                    day_indicators_df = pd.read_csv(disk_path, compression="gzip")
                    # Tenta corrigir a cópia no SHM
                    import shutil
                    shutil.copy2(disk_path, cache_path)
                except Exception:
                    pass

    if day_indicators_df is None:
        # Se não houver cache, calcula usando o superconjunto de indices de amostragem
        # (usando o limiar máximo de 2.5e-6 para cobrir qualquer candidato da busca)
        max_calm_thresh = 2.5e-6
        super_indices = []
        for w in range(TICK_COUNT, len(day_df)):
            if hours[w] in BLOCKED_HOURS:
                continue
            if (w - TICK_COUNT) % SAMPLE_EVERY != 0:
                continue
            avg = avgs[w]
            if np.isnan(avg) or avg >= max_calm_thresh:
                continue
            super_indices.append(w)
            
        day_ticks = [{"epoch": int(epochs[w]), "quote": float(prices[w])} for w in range(len(day_df))]
        try:
            day_indicators_df = calculate_tick_indicators(day_ticks, config=accu_cfg, sample_indices=super_indices)
            day_indicators_df = day_indicators_df.reset_index(drop=True)
            # Pre-calcula as probabilidades de perda do XGBoost em lote
            if _ensemble_scorer is not None:
                try:
                    day_indicators_df["p_loss"] = _ensemble_scorer.predict_loss_probability_batch(day_indicators_df)
                except Exception as e:
                    print(f"  Erro no batch predict XGBoost: {e}", flush=True)
                    day_indicators_df["p_loss"] = None
            else:
                day_indicators_df["p_loss"] = None
                
            # Salva no cache ativo (/dev/shm ou disco)
            day_indicators_df.to_csv(cache_path, index=False, compression="gzip")
            
            # Se salvou no RAM disk, persiste também no disco permanente
            if cache_dir != disk_dir:
                try:
                    import shutil
                    shutil.copy2(cache_path, disk_path)
                except Exception as e:
                    print(f"  [SHM] Erro ao persistir {filename} no disco: {e}", flush=True)
        except Exception as e:
            print(f"  Erro ao pre-calcular indicadores para o dia {day}: {e}", flush=True)
            return {c["name"]: [] for c in STRATEGY_CONFIGS}, 0.0


    # Pre-calcula win_ticks para cada TP unico
    tp_to_wt: dict[float, int] = {}
    for c in STRATEGY_CONFIGS:
        tp = c["tp"]
        if tp not in tp_to_wt:
            tp_to_wt[tp] = _calc_win_ticks(tp)
            
    # Garantimos que se tivermos Super-Frankenstein, o max_wt inclua o wt máximo dele (9 ticks)
    max_wt = max(tp_to_wt.values())
    for c in STRATEGY_CONFIGS:
        if c.get("is_super_frank", False):
            max_wt = max(max_wt, 9)

    # Score levels unicos
    score_levels = sorted(set(c["score"] for c in STRATEGY_CONFIGS))

    # Outcomes: uma lista separada para cada (tp, score)
    outcomes: dict[str, list[tuple]] = {c["name"]: [] for c in STRATEGY_CONFIGS}
    i = TICK_COUNT
    total_signals = 0

    while i < len(day_df) - max_wt - SLIPPAGE - 1:
        now = time.time()
        if (
            state is not None
            and out_path is not None
            and (now - t_last_progress) >= 5.0
        ):
            state["day_progress"] = {
                "ticks_done": int(i),
                "ticks_total": int(total_ticks),
                "pct": round(i / total_ticks * 100, 1),
                "trades": total_signals,
                "elapsed_day_s": round(now - t0, 1),
            }
            if t_global is not None:
                state["elapsed_s"] = round(now - t_global, 1)
            _write_state(out_path, state)
            t_last_progress = now

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

        # Slice do dataframe pré-calculado em O(1) de tempo!
        if i >= len(day_indicators_df):
            i += 1
            continue
            
        df_ind = day_indicators_df.iloc[i - TICK_COUNT + 1 : i + 1]
        if df_ind.empty or len(df_ind) < accu_cfg.minimum_ticks:
            i += 1
            continue
            
        df_ind = df_ind.reset_index(drop=True)
        prices_list = list(prices[i - TICK_COUNT + 1 : i + 1])

        try:
            signal, score, p_loss = generate_calm_accu_signal(
                prices_list,
                threshold=CALM_THRESH,
                lookback=10,
                df=df_ind,
                config=accu_cfg,
                ensemble_scorer=_ensemble_scorer,
            )
        except Exception:
            i += 1
            continue

        if signal != "ACCU":
            i += 1
            continue

        # XGBoost P(LOSS) filter — idêntico ao bot real
        if _ensemble_scorer is not None and p_loss is not None:
            if p_loss > ENSEMBLE_MIN_PROB:
                i += 1
                continue  # sinal fraco, pula

        row = df_ind.iloc[-1]
        cusum_v = float(row.get("cusum_score", 0) or 0)
        hurst_v = float(row.get("hurst_exponent", 0.5) or 0.5)
        shannon_v = float(row.get("shannon_entropy", 0) or 0)
        kalman_v = float(row.get("kalman_residual_zscore", 0) or 0)
        if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
            i += 1
            continue

        # Score do sinal
        actual_score = score
        total_signals += 1

        # Entry idx com slippage
        entry_idx = i + SLIPPAGE
        if entry_idx >= len(prices) - max_wt:
            i += 1
            continue

        # Verifica outcome para cada (TP, score) combo
        # Checa per-tick barrier para diferentes durações
        max_hold = max_wt
        barrier_hit_at = None  # tick onde a barreira foi atingida
        for j in range(1, max_hold + 1):
            prev_idx = entry_idx + j - 1
            curr_idx = entry_idx + j
            if curr_idx >= len(prices):
                barrier_hit_at = j
                break
            tick_move = abs(prices[curr_idx] - prices[prev_idx]) / prices[prev_idx]
            if tick_move >= PER_TICK_BARRIER:
                barrier_hit_at = j
                break

        # Para cada config, determina WIN/LOSS baseado no win_ticks e score
        best_hold = 0
        for c in STRATEGY_CONFIGS:
            wt = tp_to_wt[c["tp"]] if not c.get("is_super_frank", False) else 9
            if actual_score < c["score"]:
                continue  # score insuficiente para esta config
                
            is_win = not (barrier_hit_at is not None and barrier_hit_at <= wt)
            outcomes[c["name"]].append((
                is_win, 
                float(epochs[entry_idx]), 
                float(avg), 
                float(cusum_v), 
                float(hurst_v), 
                barrier_hit_at,
                float(shannon_v),
                float(kalman_v),
                float(p_loss if p_loss is not None else 1.0)
            ))

            best_hold = max(best_hold, wt)

        i += (
            (
                barrier_hit_at
                if barrier_hit_at and barrier_hit_at <= best_hold
                else best_hold
            )
            + SLIPPAGE
            + SOROS_COOLDOWN
        )

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
    all_outcomes, elapsed = _collect_day_outcomes(
        day, day_df, state, out_path, t_global
    )

    day_result = {
        "date": day.isoformat(),
        "elapsed_s": elapsed,
        "strategies": {},
    }

    for c in STRATEGY_CONFIGS:
        name = c["name"]
        oc = all_outcomes.get(name, [])
        bal = balances.get(name, 50.0)
        r = _replay_strategy(oc, c, bal)
        r["start_balance"] = round(bal, 2)
        r["signal_wr"] = round(sum(1 for x in oc if x[0]) / len(oc) * 100, 1) if oc else 0.0

        day_result["strategies"][name] = r
        balances[name] = r["balance"]

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
    print(f"\n[2/2] Simulando {len(STRATEGY_NAMES)} estratégias...", flush=True)
    print(f"  Estratégias: {', '.join(STRATEGY_NAMES)}")

    # Lê do .env se deve acumular saldo (compounding) entre os dias no backtest
    accumulate_balance = os.getenv("BACKTEST_COMPOUNDING", "false").strip().lower() == "true"
    if accumulate_balance:
        print(f"  [MODO COMPOSTO] Preservando saldos acumulados dia-a-dia!", flush=True)
    else:
        print(f"  [MODO DIÁRIO INDEPENDENTE] Reseta saldos para ${start_balance:.2f} no início de cada dia!", flush=True)

    # Cada estratégia mantém seu próprio saldo (com reset diário opcional!)
    total_pnls = {s: 0.0 for s in STRATEGY_NAMES}
    balances = {s: start_balance for s in STRATEGY_NAMES}

    for day in days_with_data:
        state["current_day"] = day.isoformat()
        state["elapsed_s"] = round(time.time() - t_global, 1)
        _write_state(out_path, state)

        day_df = _load_day_df(day, data_dir)
        if day_df is None:
            print(f"  {day}: sem dados — pulando", flush=True)
            continue

        if not accumulate_balance:
            # Reseta o saldo no início do dia para simular sessões independentes de risco
            balances = {s: start_balance for s in STRATEGY_NAMES}

        print(f"  {day}: simulando...", end=" ", flush=True)
        result = _sim_day_multi(
            day,
            day_df,
            balances,
            state=state,
            out_path=out_path,
            t_global=t_global,
        )

        # Acumula PnL do dia
        for s in STRATEGY_NAMES:
            total_pnls[s] += result["strategies"][s]["pnl"]

        # Limpa progresso intra-dia
        state.pop("day_progress", None)
        state["results"].append(result)
        # Print resumo do dia
        first_strat = STRATEGY_NAMES[0]
        first_sr = result["strategies"].get(first_strat, {})
        wr = first_sr.get("signal_wr", 0)
        line = f"{first_sr.get('trades', 0)}T WR {wr:.0f}% | "
        for s in STRATEGY_NAMES:
            sr = result["strategies"].get(s, {})
            stop = sr.get("stop", "")
            icon = (
                "🎯"
                if stop == "DOBROU"
                else (
                    "🔒"
                    if stop == "TRAIL"
                    else (
                        "⛔"
                        if stop == "SL"
                        else (
                            "💥"
                            if stop == "BUST"
                            else ("✅" if sr.get("pnl", 0) >= 0 else "❌")
                        )
                    )
                )
            )
            line += f"{s}:{icon}${sr.get('balance', 0):.0f} "
        print(f"{line} [{result['elapsed_s']:.0f}s]", flush=True)

    # Sumário final
    results = state["results"]
    n = len(results)
    if n > 0:
        summary = {"total_days": n, "strategies": {}}
        for s in STRATEGY_NAMES:
            total_pnl = round(total_pnls[s], 2)
            final_bal = start_balance + total_pnl
            busted_days = sum(
                1 for r in results if r["strategies"].get(s, {}).get("busted", False)
            )
            pos_days = sum(
                1 for r in results if r["strategies"].get(s, {}).get("pnl", 0) > 0
            )
            neg_days = sum(
                1 for r in results if r["strategies"].get(s, {}).get("pnl", 0) < 0
            )
            active_days = sum(
                1 for r in results if r["strategies"].get(s, {}).get("trades", 0) > 0
            )
            wrs = [r["strategies"].get(s, {}).get("signal_wr", 0) for r in results]
            avg_wr = round(sum(wrs) / len(wrs), 1) if wrs else 0

            # Lucro médio por dia ATIVO (dias que tiveram pelo menos 1 trade)
            active_pnls = [
                r["strategies"].get(s, {}).get("pnl", 0)
                for r in results
                if r["strategies"].get(s, {}).get("trades", 0) > 0
            ]
            avg_daily_profit = round(sum(active_pnls) / len(active_pnls), 4) if active_pnls else 0.0
            consistency_pct = round(pos_days / active_days * 100, 1) if active_days > 0 else 0.0

            # ── 1. Curva de Patrimônio e Max Drawdown ────────────────────────
            equity_curve = [start_balance]
            curr_bal = start_balance
            for r in results:
                curr_bal += r["strategies"].get(s, {}).get("pnl", 0.0)
                equity_curve.append(curr_bal)

            peak = start_balance
            max_dd = 0.0
            for bal in equity_curve:
                if bal > peak:
                    peak = bal
                dd = peak - bal
                if dd > max_dd:
                    max_dd = dd
            max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0

            # ── 2. Sharpe e Sortino Ratios ───────────────────────────────────
            daily_pnls = [r["strategies"].get(s, {}).get("pnl", 0.0) for r in results]
            mean_pnl = sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0.0
            
            # Desvio padrão
            if len(daily_pnls) > 1:
                variance = sum((x - mean_pnl) ** 2 for x in daily_pnls) / (len(daily_pnls) - 1)
                std_dev = variance ** 0.5
            else:
                std_dev = 0.0

            sharpe = (mean_pnl / std_dev) if std_dev > 0.001 else 0.0

            # Desvio de queda (Downside deviation)
            downside_pnls = [x for x in daily_pnls if x < 0.0]
            if downside_pnls:
                downside_variance = sum(x ** 2 for x in downside_pnls) / len(daily_pnls)
                downside_dev = downside_variance ** 0.5
            else:
                downside_dev = 0.0

            sortino = (mean_pnl / downside_dev) if downside_dev > 0.001 else (mean_pnl / 0.001 if mean_pnl > 0 else 0.0)

            summary["strategies"][s] = {
                "final_balance": round(final_bal, 2),
                "total_pnl": total_pnl,
                "roi_pct": round(total_pnl / start_balance * 100, 1),
                "positive_days": pos_days,
                "negative_days": neg_days,
                "busted_days": busted_days,
                "active_days": active_days,
                "avg_daily_profit": avg_daily_profit,
                "consistency_pct": consistency_pct,
                "avg_signal_wr": avg_wr,
                "sharpe_ratio": round(sharpe, 4),
                "sortino_ratio": round(sortino, 4),
                "max_drawdown": round(max_dd, 2),
                "max_drawdown_pct": round(max_dd_pct, 1),
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
