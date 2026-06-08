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
from cooldown_rules import dynamic_cooldown_resume_ok
from strategy import (
    AccumulatorStrategyConfig,
    EnsembleScorer,
    JumpMomentumConfig,
    MultiplierContinuationConfig,
    calculate_tick_indicators,
    generate_calm_accu_signal,
    generate_multiplier_continuation_snapshot_signal,
    RiseFallStrategyConfig,
    generate_jump_momentum_snapshot_signal,
    generate_rise_fall_signal,
)

load_dotenv()

# ── Credenciais Deriv para download automático de ticks ───────────────────────
TOKEN = os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or ""
APP_ID = os.getenv("DERIV_APP_ID", "1089")
if not APP_ID.isdigit():
    APP_ID = "1089"
WS_URL = f"wss://api.derivws.com/trading/v1/options/ws/public?app_id={APP_ID}"

# ── Símbolo Ativo e Auxiliares de Volatilidade ──────────────────────────────────
SYMBOL = os.getenv("SYMBOL", "BOOM1000")

def get_symbol_median_volatility(sym: str) -> float:
    sym_upper = sym.upper()
    baselines = {
        "BOOM1000": 1.0e-6,
        "1HZ100V": 1.4e-4,
        "1HZ10V": 1.5e-5,
    }
    return baselines.get(sym_upper, 1.4e-4)

def get_max_calm_thresh(sym: str, avgs_arr: np.ndarray = None) -> float:
    sym_upper = sym.upper()
    if sym_upper == "BOOM1000":
        return 2.5e-6
    if sym_upper == "1HZ100V":
        return 2.5e-4
    if sym_upper == "1HZ10V":
        return 3.0e-5
    if avgs_arr is not None and len(avgs_arr) > 0:
        valid_avgs = avgs_arr[~np.isnan(avgs_arr)]
        if len(valid_avgs) > 0:
            return float(np.median(valid_avgs)) * 2.0
    return 3.0e-4

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

CONTRACT_MODE = os.getenv("CONTRACT_MODE", "calm_accu").strip().lower()
RISE_FALL_DURATION_TICKS = int(os.getenv("RISE_FALL_DURATION_TICKS", "5"))
RISE_FALL_MIN_PAYOUT_PCT = float(os.getenv("RISE_FALL_MIN_PAYOUT_PCT", "0.0055"))
RISE_FALL_COOLDOWN_TICKS = int(os.getenv("RISE_FALL_COOLDOWN_TICKS", "3"))
RISE_FALL_BOOM_MAX_CUSUM = float(os.getenv("RISE_FALL_BOOM_MAX_CUSUM", "8.0"))
RISE_FALL_BOOM_MAX_VELOCITY = float(os.getenv("RISE_FALL_BOOM_MAX_VELOCITY", "0.001"))
RISE_FALL_BOOM_MAX_IMBALANCE = float(os.getenv("RISE_FALL_BOOM_MAX_IMBALANCE", "1.5"))
RISE_FALL_BOOM_ONLY_PUT = os.getenv("RISE_FALL_BOOM_ONLY_PUT", "true").lower() == "true"
RISE_FALL_MIN_VOTES = max(1, min(6, int(os.getenv("RISE_FALL_MIN_VOTES", "4"))))
RISE_FALL_USE_ENSEMBLE = os.getenv("RISE_FALL_USE_ENSEMBLE", "false").lower() == "true"
RISE_FALL_ENSEMBLE_MIN_PROB = float(os.getenv("RISE_FALL_ENSEMBLE_MIN_PROB", "0.52"))
JUMP_MIN_CONFIDENCE = float(os.getenv("JUMP_MIN_CONFIDENCE", "0.60"))
RISE_FALL_QUALITY_GATE = os.getenv("RISE_FALL_QUALITY_GATE", "true").lower() == "true"
RISE_FALL_QG_MIN_ABS_IMBALANCE = float(os.getenv("RISE_FALL_QG_MIN_ABS_IMBALANCE", "6.0"))
RISE_FALL_QG_BAYES_STRONG = float(os.getenv("RISE_FALL_QG_BAYES_STRONG", "0.70"))
RISE_FALL_QG_HURST_MAX = float(os.getenv("RISE_FALL_QG_HURST_MAX", "0.50"))
MULTIPLIER_JUMP_MIN_CONFIDENCE = float(os.getenv("MULTIPLIER_JUMP_MIN_CONFIDENCE", str(JUMP_MIN_CONFIDENCE)))
MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE = float(os.getenv("MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE", "4.0"))
MULTIPLIER_JUMP_BAYES_STRONG_PROB = float(os.getenv("MULTIPLIER_JUMP_BAYES_STRONG_PROB", "0.62"))
MULTIPLIER_JUMP_MIN_SCORE = int(float(os.getenv("MULTIPLIER_JUMP_MIN_SCORE", "5")) or 5)
MULTIPLIER_JUMP_HURST_TRENDING = float(os.getenv("MULTIPLIER_JUMP_HURST_TRENDING", "0.58"))
MULTIPLIER_JUMP_HURST_REVERTING = float(os.getenv("MULTIPLIER_JUMP_HURST_REVERTING", "0.38"))
MULTIPLIER_JUMP_MI_FLOW_MIN = float(os.getenv("MULTIPLIER_JUMP_MI_FLOW_MIN", "0.02"))
MULTIPLIER_JUMP_WAVELET_SNR_MIN = float(os.getenv("MULTIPLIER_JUMP_WAVELET_SNR_MIN", "0.02"))
MULTIPLIER_CONTINUATION_MIN_SCORE = int(float(os.getenv("MULTIPLIER_CONTINUATION_MIN_SCORE", "4")) or 4)
MULTIPLIER_CONTINUATION_MIN_CONFIDENCE = float(os.getenv("MULTIPLIER_CONTINUATION_MIN_CONFIDENCE", "0.57"))
MULTIPLIER_CONTINUATION_MIN_UP_TICKS = int(float(os.getenv("MULTIPLIER_CONTINUATION_MIN_UP_TICKS", "4")) or 4)
MULTIPLIER_CONTINUATION_MAX_DOWN_TICKS = int(float(os.getenv("MULTIPLIER_CONTINUATION_MAX_DOWN_TICKS", "1")) or 1)
MULTIPLIER_CONTINUATION_MIN_IMBALANCE = float(os.getenv("MULTIPLIER_CONTINUATION_MIN_IMBALANCE", "1.5"))
MULTIPLIER_CONTINUATION_MIN_MARKOV_EDGE = float(os.getenv("MULTIPLIER_CONTINUATION_MIN_MARKOV_EDGE", "0.04"))
MULTIPLIER_VALUE = int(os.getenv("MULTIPLIER_VALUE", "100"))
MULTIPLIER_DIRECTION = os.getenv("MULTIPLIER_DIRECTION", "signal").strip().lower()
MULTIPLIER_TAKE_PROFIT = float(os.getenv("MULTIPLIER_TAKE_PROFIT", "0.50"))
MULTIPLIER_STOP_LOSS = float(os.getenv("MULTIPLIER_STOP_LOSS", "1.00"))
MULTIPLIER_MAX_HOLD_TICKS = int(os.getenv("MULTIPLIER_MAX_HOLD_TICKS", "30"))
INDICATORS_CACHE_VERSION = "v3"

CUSUM_MAX = float(os.getenv("CALM_ACCU_MAX_ENTRY_CUSUM", "5.0"))
HURST_MIN = float(os.getenv("ACCUMULATOR_MIN_HURST_EXPONENT", "0.45"))
CALM_THRESH = float(os.getenv("CALM_ACCU_THRESHOLD", "1.5e-6"))
SAMPLE_EVERY = int(os.getenv("BACKTEST_SAMPLE_EVERY", "60"))
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
    use_ensemble=os.getenv("USE_ENSEMBLE", "true").lower() == "true",
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
            # Busca em blocos de 1 hora (máx 5000 ticks cada)
            start = start_epoch
            while start < end_epoch:
                chunk_end = min(start + 3600, end_epoch)
                await ws.send(
                    json.dumps(
                        {
                            "ticks_history": SYMBOL,
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

# Caches globais em RAM para otimização extrema
_day_df_cache: dict[_date, pd.DataFrame] = {}
_indicators_df_cache: dict[_date, pd.DataFrame] = {}
_indicators_list_cache: dict[_date, list[dict]] = {}

def _add_to_day_df_cache(day: _date, df: pd.DataFrame) -> None:
    global _day_df_cache
    _day_df_cache[day] = df
    if len(_day_df_cache) > 32:
        try:
            oldest = next(iter(_day_df_cache))
            _day_df_cache.pop(oldest, None)
        except Exception:
            pass

def _add_to_indicators_df_cache(day: _date, df: pd.DataFrame) -> None:
    global _indicators_df_cache
    _indicators_df_cache[day] = df
    if len(_indicators_df_cache) > 32:
        try:
            oldest = next(iter(_indicators_df_cache))
            _indicators_df_cache.pop(oldest, None)
        except Exception:
            pass

def _add_to_indicators_list_cache(day: _date, indicators_map: dict) -> None:
    global _indicators_list_cache
    _indicators_list_cache[day] = indicators_map
    if len(_indicators_list_cache) > 32:
        try:
            oldest = next(iter(_indicators_list_cache))
            _indicators_list_cache.pop(oldest, None)
        except Exception:
            pass



def _get_max_csv_range(data_dir: Path) -> tuple[_date, _date] | None:
    """Lê o range de DATAS do max.csv uma única vez e guarda em cache."""
    global _max_csv_range
    if _max_csv_range is not None:
        return _max_csv_range
    max_path = data_dir / f"ticks_{SYMBOL}_max.csv"
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
    2. Checa se existem ticks no PostgreSQL (shadow_ticks / shadow_ticks_accumulator)
    3. Checa se max.csv cobre o dia (cache do range de epochs)
    4. Tenta baixar da Deriv se TOKEN disponível
    Retorna True se dados estão disponíveis.
    """
    # O ativo opera 24/7 incluindo fins de semana (BLOCK_WEEKENDS=false no bot real)
    daily = data_dir / f"ticks_{SYMBOL}_{day.isoformat()}.csv"
    if daily.exists() and daily.stat().st_size > 5_000:
        return True

    pg_dsn = os.getenv("PG_DSN")
    if pg_dsn and SYMBOL == "BOOM1000":
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
    if SYMBOL != "BOOM1000":
        return None
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
    Carrega ticks de um dia. Prefere carregar de arquivo diário local ou max.csv;
    senão cai no PostgreSQL e salva localmente como cache diário para evitar queries futuras.
    """
    global _day_df_cache
    if day in _day_df_cache:
        return _day_df_cache[day]

    daily_path = data_dir / f"ticks_{SYMBOL}_{day.isoformat()}.csv"
    
    # 1. Primeiro verifica se existe o CSV diário local (é MUITO mais rápido que PG)
    if daily_path.exists() and daily_path.stat().st_size > 1000:
        try:
            df = pd.read_csv(daily_path)
            if not df.empty and len(df) >= TICK_COUNT + 10:
                df["epoch"] = pd.to_numeric(df["epoch"], errors="coerce")
                df["quote"] = pd.to_numeric(df["quote"], errors="coerce")
                df = df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
                df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
                df["hour"] = df["dt"].dt.hour
                q = df["quote"].values
                rets = np.zeros(len(q))
                rets[1:] = np.abs(np.diff(q) / q[:-1])
                df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
                _add_to_day_df_cache(day, df)
                return df
        except Exception:
            pass

    # 2. Se não tem CSV local, tenta carregar do PostgreSQL
    df = _load_day_df_from_pg(day)
    if df is not None:
        print(f"  [PG] {len(df)} ticks carregados", end=" ", flush=True)
        # ⚠️ CACHE INCRÍVEL: Salva o CSV localmente para que os próximos workers não batam no PG!
        try:
            df.to_csv(daily_path, index=False)
            print(f"(cached localmente)", end=" ", flush=True)
        except Exception as e:
            print(f"(erro ao salvar cache: {e})", end=" ", flush=True)
        _add_to_day_df_cache(day, df)
        return df

    # 3. Se PG falhar, tenta ler do max.csv
    max_path = data_dir / f"ticks_{SYMBOL}_max.csv"
    if not max_path.exists():
        return None
    try:
        full = pd.read_csv(max_path)
        full["_date"] = pd.to_datetime(full["epoch"], unit="s", utc=True).dt.date
        df = full[full["_date"] == day].drop(columns=["_date"])
        if not df.empty and len(df) >= TICK_COUNT + 10:
            df["epoch"] = pd.to_numeric(df["epoch"], errors="coerce")
            df["quote"] = pd.to_numeric(df["quote"], errors="coerce")
            df = df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
            df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
            df["hour"] = df["dt"].dt.hour
            q = df["quote"].values
            rets = np.zeros(len(q))
            rets[1:] = np.abs(np.diff(q) / q[:-1])
            df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
            # Também salva cache do max.csv no diário para acelerar leituras futures
            try:
                df.to_csv(daily_path, index=False)
            except Exception:
                pass
            _add_to_day_df_cache(day, df)
            return df
    except Exception:
        pass

    return None


def _indicator_cache_is_stale(df: pd.DataFrame | None) -> bool:
    """Reject cached indicator frames that were produced by older broken sampling logic."""
    if df is None or df.empty:
        return True
    required = {"tick_imbalance", "bayesian_prob_up", "mi_flow", "wavelet_energy_ratio", "cusum_score", "hurst_exponent"}
    if not required.issubset(df.columns):
        return True

    bayes = pd.to_numeric(df["bayesian_prob_up"], errors="coerce")
    hurst = pd.to_numeric(df["hurst_exponent"], errors="coerce")
    mi = pd.to_numeric(df["mi_flow"], errors="coerce")
    wavelet = pd.to_numeric(df["wavelet_energy_ratio"], errors="coerce")
    cusum = pd.to_numeric(df["cusum_score"], errors="coerce")

    informative_bayes = int((bayes.fillna(0.5) != 0.5).sum())
    informative_hurst = int(hurst.notna().sum())
    informative_mi = int((mi.fillna(0.0) != 0.0).sum())
    informative_wavelet = int((wavelet.fillna(0.5) != 0.5).sum())
    informative_cusum = int((cusum.fillna(0.0) != 0.0).sum())

    informative_rows = max(
        informative_bayes,
        informative_hurst,
        informative_mi,
        informative_wavelet,
        informative_cusum,
    )
    if informative_rows >= max(8, min(len(df) // 500, 128)):
        return False

    all_bayes_default = bool((bayes.fillna(0.5) == 0.5).all())
    all_hurst_missing = bool(hurst.isna().all())
    all_mi_zero = bool((mi.fillna(0.0) == 0.0).all())
    all_wavelet_default = bool((wavelet.fillna(0.5) == 0.5).all())
    all_cusum_zero = bool((cusum.fillna(0.0) == 0.0).all())

    return all_bayes_default and all_hurst_missing and all_mi_zero and all_wavelet_default and all_cusum_zero


def _build_indicator_sample_indices(
    hours,
    avgs,
    contract_mode: str,
    symbol: str,
) -> list[int]:
    """Return sampled tick indices for indicator precomputation."""
    mode = str(contract_mode or "").strip().lower()
    max_calm_thresh = get_max_calm_thresh(symbol, avgs)
    indices: list[int] = []
    for w in range(TICK_COUNT, len(hours)):
        if hours[w] in BLOCKED_HOURS:
            continue
        if (w - TICK_COUNT) % SAMPLE_EVERY != 0:
            continue
        if mode not in {"rise_fall", "multiplier"}:
            avg = avgs[w]
            if np.isnan(avg) or avg >= max_calm_thresh:
                continue
        indices.append(w)
    return indices


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
    # Se estiver rodando no loop de otimização, só precisamos das duas estratégias alvo (25x mais rápido!)
    if os.getenv("PEGASUS_OPTIMIZER_RUN") == "true":
        directional_score = (
            max(1, min(6, int(os.getenv("RISE_FALL_MIN_VOTES", "4"))))
            if os.getenv("CONTRACT_MODE", "").strip().lower() in {"rise_fall", "multiplier"}
            else 25
        )
        return [
            {"name": "Pegasus Live Sniper (9% TP)", "tp": 0.09, "score": directional_score, "mode": "flat15", "use_soros": True, "soros_steps": 2, "use_martingale": True, "max_gales": 2},
            {
                "name": "Super-Frankenstein",
                "tp": float(os.getenv("FRANKENSTEIN_TP", "0.30")),
                "score": directional_score,
                "mode": os.getenv("FRANKENSTEIN_MODE", "flat"),
                "use_soros": os.getenv("FRANKENSTEIN_USE_SOROS", "true").lower() == "true",
                "soros_steps": int(os.getenv("FRANKENSTEIN_SOROS_STEPS", "2")),
                "use_martingale": os.getenv("FRANKENSTEIN_USE_MARTINGALE", "true").lower() == "true",
                "max_gales": int(os.getenv("FRANKENSTEIN_MAX_GALES", "1")),
                "is_super_frank": True
            },
        ]

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


def _build_sampled_indicator_map(day_indicators_df: pd.DataFrame) -> dict[int, dict]:
    """Return indicator rows only for sampled backtest ticks."""
    target_indices = [
        w for w in range(TICK_COUNT, len(day_indicators_df))
        if (w - TICK_COUNT) % SAMPLE_EVERY == 0
    ]
    if not target_indices:
        return {}
    sub_df = day_indicators_df.iloc[target_indices].copy()
    return {
        idx: row
        for idx, row in zip(target_indices, sub_df.to_dict("records"))
    }


def _simulate_multiplier_profit(
    stake: float,
    direction: str,
    returns_path: list[float],
) -> float:
    sign = 1.0 if direction == "MULTUP" else -1.0
    # Commission is 0.02% of the exposure (stake * MULTIPLIER_VALUE)
    commission = stake * MULTIPLIER_VALUE * 0.0002
    
    last_net_profit = -commission
    for ret in returns_path:
        gross_profit = stake * MULTIPLIER_VALUE * sign * ret
        last_net_profit = round(gross_profit - commission, 2)
        if last_net_profit >= MULTIPLIER_TAKE_PROFIT:
            return round(MULTIPLIER_TAKE_PROFIT, 2)
        if last_net_profit <= -MULTIPLIER_STOP_LOSS:
            return round(-MULTIPLIER_STOP_LOSS, 2)
    return last_net_profit


def _multiplier_direction_from_signal(signal: str) -> str:
    if MULTIPLIER_DIRECTION == "up":
        return "MULTUP"
    if MULTIPLIER_DIRECTION == "down":
        return "MULTDOWN"
    return "MULTUP" if signal == "CALL" else "MULTDOWN"


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
            mult_direction = None
            mult_returns = None
            if len(item) == 11:
                (
                    is_win,
                    epoch,
                    avg,
                    cusum_v,
                    hurst_v,
                    barrier_hit_at,
                    shannon_v,
                    kalman_v,
                    p_loss,
                    velocity_v,
                    imbalance_v,
                ) = item
            elif len(item) == 13:
                (
                    _is_win,
                    epoch,
                    avg,
                    cusum_v,
                    hurst_v,
                    barrier_hit_at,
                    shannon_v,
                    kalman_v,
                    p_loss,
                    velocity_v,
                    imbalance_v,
                    mult_direction,
                    mult_returns,
                ) = item
                is_win = False
            elif len(item) == 9:
                is_win, epoch, avg, cusum_v, hurst_v, barrier_hit_at, shannon_v, kalman_v, p_loss = item
                velocity_v = 0.0
                imbalance_v = 0.0
            elif len(item) == 6:
                is_win, epoch, avg, cusum_v, hurst_v, barrier_hit_at = item
                shannon_v = 0.0
                kalman_v = 0.0
                p_loss = 1.0
                velocity_v = 0.0
                imbalance_v = 0.0
            else:
                is_win, epoch, avg, cusum_v, hurst_v = item
                barrier_hit_at = None
                shannon_v = 0.0
                kalman_v = 0.0
                p_loss = 1.0
                velocity_v = 0.0
                imbalance_v = 0.0
                
            risk._sim_time = epoch
            risk._sim_monotonic_time = epoch
            
            # Se for Rise/Fall, ignora o logic de regime switching de Acumulador
            if CONTRACT_MODE == "rise_fall":
                current_tp_pct = RISE_FALL_MIN_PAYOUT_PCT
                is_win_trade = is_win
                risk.martingale_payout_rate = RISE_FALL_MIN_PAYOUT_PCT
            elif CONTRACT_MODE == "multiplier":
                current_tp_pct = 0.0
                is_win_trade = False
                risk.use_martingale = False
                risk.use_soros = False
            # Se for Super-Frankenstein, aplica regime switching e gale standby dinâmicos!
            elif is_super_frank:
                is_absolute_calm = False
                is_medium_calm = False
                
                # Calmaria Extrema (Regime A) Check:
                use_ensemble = os.getenv("USE_ENSEMBLE", "true").lower() == "true"
                _pass_a_xgb = (not use_ensemble) or (p_loss < 0.22)
                
                median_vol = get_symbol_median_volatility(SYMBOL)
                if (
                    avg < 1.0 * median_vol
                    and cusum_v < 2.5
                    and hurst_v > 0.48
                    and shannon_v > 0.85
                    and abs(kalman_v) < 1.5
                    and _pass_a_xgb
                ):
                    is_absolute_calm = True
                
                # Calmaria Moderada (Regime B+) Check:
                _pass_b_plus_xgb = (not use_ensemble) or (p_loss < 0.26)
                if (
                    avg < 2.2 * median_vol
                    and cusum_v < 4.0
                    and hurst_v > 0.45
                    and _pass_b_plus_xgb
                ):
                    is_medium_calm = True
                
                _in_gale = risk.use_martingale and risk.martingale_step > 0
                
                if _in_gale:
                    # GALE STANDBY & BYPASS
                    _xgb_bypass = (not use_ensemble) or (p_loss < float(os.getenv("PCS_XGB_BYPASS_LIMIT", "0.15")))
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
                        # Regime B- desativado para evitar prejuízos com InvalidtoSell e spread
                        continue
                
                # Determina o resultado WIN/LOSS com base no win_ticks do regime ativo
                is_win_trade = not (barrier_hit_at is not None and barrier_hit_at <= current_wt)
            else:
                current_tp_pct = tp_pct
                is_win_trade = is_win
            
            # Dynamic Cooldown Bypass (same as live bot)
            if getattr(risk, "cooldown_until", 0.0) > 0:
                symbol_upper = SYMBOL.upper()
                is_boom = "BOOM" in symbol_upper
                is_crash = "CRASH" in symbol_upper
                
                if is_boom or is_crash:
                    _cusum_limit = (
                        RISE_FALL_BOOM_MAX_CUSUM
                        if CONTRACT_MODE in {"rise_fall", "jump_rise_fall", "multiplier"}
                        else CUSUM_MAX
                    )
                    _velocity_limit = (
                        RISE_FALL_BOOM_MAX_VELOCITY
                        if CONTRACT_MODE in {"rise_fall", "jump_rise_fall", "multiplier"}
                        else 0.0002
                    )
                    _imbalance_limit = (
                        RISE_FALL_BOOM_MAX_IMBALANCE
                        if CONTRACT_MODE in {"rise_fall", "jump_rise_fall", "multiplier"}
                        else 1.0
                    )
                    _ensemble_threshold = (
                        RISE_FALL_ENSEMBLE_MIN_PROB
                        if CONTRACT_MODE in {"rise_fall", "jump_rise_fall", "multiplier"}
                        else ENSEMBLE_MIN_PROB
                    )
                    if dynamic_cooldown_resume_ok(
                        symbol=SYMBOL,
                        max_abs_ret=avg,
                        cusum=cusum_v,
                        velocity=velocity_v,
                        imbalance=imbalance_v,
                        hurst=hurst_v,
                        p_loss=p_loss,
                        cusum_limit=_cusum_limit,
                        velocity_limit=_velocity_limit,
                        imbalance_limit=_imbalance_limit,
                        ensemble_loss_threshold=_ensemble_threshold,
                    ):
                        risk.reset_cooldown_early()
                else:
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
            
            if CONTRACT_MODE == "multiplier":
                profit = _simulate_multiplier_profit(
                    stake,
                    str(mult_direction or "MULTDOWN"),
                    list(mult_returns or []),
                )
                risk.update(profit=profit, buy_price=stake)
                if profit > 0:
                    wins += 1
                else:
                    losses += 1
            elif is_win_trade:
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


def _precalculate_metrics_for_row(row: dict, config: AccumulatorStrategyConfig) -> tuple[int, bool]:
    # Hard blocks
    hurst = row.get("hurst_exponent")
    cusum = row.get("cusum_score")
    hard_blocked = (hurst is not None and hurst > 0.70) or (cusum is not None and cusum > 8.0)
    
    required = ["bb_width_percent", "tick_atr_percent", "recent_move_percent"]
    if any(row.get(k) is None or pd.isna(row[k]) for k in required):
        return 0, hard_blocked

    score = 0
    bb_w = row.get("bb_width_percent")
    tick_atr = row.get("tick_atr_percent")
    recent_m = row.get("recent_move_percent")
    
    if bb_w is not None and bb_w <= config.max_bb_width_percent:
        score += config.squeeze_weight
    if tick_atr is not None and tick_atr <= config.max_tick_atr_percent:
        score += config.atr_weight
    if recent_m is not None and recent_m <= config.max_recent_move_percent:
        score += config.stability_weight
        
    h = row.get("hurst_exponent")
    if h is not None and not pd.isna(h) and float(h) < config.max_hurst_exponent:
        score += 1
    ti = row.get("tick_imbalance")
    if ti is not None and not pd.isna(ti) and abs(int(ti)) < config.max_abs_tick_imbalance:
        score += 1
    hi = row.get("hawkes_intensity")
    if hi is not None and not pd.isna(hi) and float(hi) <= config.max_hawkes_intensity:
        score += 1
    vz = row.get("velocity_zscore")
    if vz is not None and not pd.isna(vz) and abs(float(vz)) <= config.max_velocity_zscore:
        score += 1
    az = row.get("acceleration_zscore")
    if az is not None and not pd.isna(az) and abs(float(az)) <= config.max_acceleration_zscore:
        score += 1
    pd_ = row.get("pmi_distance_percent")
    if pd_ is not None and not pd.isna(pd_) and float(pd_) <= config.max_pmi_distance_percent:
        score += 1
    muu = row.get("markov_p_up_given_up")
    if muu is not None and not pd.isna(muu) and float(muu) < config.max_markov_continuation_prob:
        score += 1
    mdd = row.get("markov_p_down_given_down")
    if mdd is not None and not pd.isna(mdd) and float(mdd) < config.max_markov_continuation_prob:
        score += 1
    se = row.get("shannon_entropy")
    if se is not None and not pd.isna(se) and float(se) >= config.min_shannon_entropy:
        score += 1
    kz = row.get("kalman_residual_zscore")
    if kz is not None and not pd.isna(kz) and abs(float(kz)) <= config.max_kalman_residual_zscore:
        score += 1
        
    def _val(name: str, default: float = 0.0) -> float:
        v = row.get(name, default)
        try:
            f = float(v if v is not None else default)
        except (TypeError, ValueError):
            f = default
        return default if f != f else f

    bayesian = _val("bayesian_prob_up", 0.5)
    if 0.30 <= bayesian <= 0.70:
        score += 1
    renyi = _val("renyi_entropy", 0.5)
    if renyi >= 0.40:
        score += 1
    fisher = _val("fisher_information", 0.0)
    if fisher > 0.0:
        score += 1
    wavelet = _val("wavelet_energy_ratio", 0.5)
    if wavelet < 0.70:
        score += 1
    cusum_val = _val("cusum_score", 0.0)
    if cusum_val < 5.0:
        score += 1
    tail_dep = _val("tail_dependence", 0.0)
    if tail_dep < 0.30:
        score += 1
    mi = _val("mi_flow", 0.0)
    if mi < 0.15:
        score += 1

    deriv_energy = _val("derivative_energy", 0.0)
    de_median = row.get("deriv_energy_median_100", 1.0)
    if de_median > 0 and deriv_energy <= de_median:
        score += 1
    jerk_z = _val("jerk_zscore", 0.0)
    if jerk_z < 2.0:
        score += 1
    curv_z = _val("curvature_zscore", 0.0)
    if curv_z < 2.0:
        score += 1
    ret_z = _val("return_zscore", 0.0)
    if abs(ret_z) < 2.0:
        score += 1
    lyap = _val("lyapunov_exponent", 0.0)
    if lyap < 0.5:
        score += 1
    trend_ex = _val("trend_exhaustion", 0.0)
    if abs(trend_ex) < 0.01:
        score += 1
    int_mom = _val("integral_momentum_div", 0.0)
    if abs(int_mom) < 0.5:
        score += 1
    autocorr = _val("return_autocorr_lag1", 0.0)
    if abs(autocorr) < 0.3:
        score += 1
    skew = _val("return_skewness", 0.0)
    if abs(skew) < 1.0:
        score += 1
    run_len = _val("run_length", 0.0)
    if abs(run_len) < 5:
        score += 1
        
    return score, hard_blocked


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
    global _indicators_df_cache, _indicators_list_cache
    data_dir = Path(__file__).parent / "data"
    t0 = time.time()
    t_last_progress = t0

    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    epochs = day_df["epoch"].values
    total_ticks = len(day_df)

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

    cache_mode = str(CONTRACT_MODE or "default").strip().lower()
    filename = f"indicators_{SYMBOL}_{cache_mode}_{day.isoformat()}_sample{SAMPLE_EVERY}_{INDICATORS_CACHE_VERSION}.feather"
    cache_path = cache_dir / filename
    disk_path = disk_dir / filename

    day_indicators_df = None
    if day in _indicators_df_cache:
        day_indicators_df = _indicators_df_cache[day]
    else:
        # Se o arquivo estiver no disco mas ausente do RAM disk, copia para RAM disk para acesso rápido subsequente
        if cache_dir != disk_dir and not cache_path.exists() and disk_path.exists():
            try:
                import shutil
                shutil.copy2(disk_path, cache_path)
            except Exception as e:
                print(f"  [SHM] Erro ao copiar {filename} para /dev/shm: {e}", flush=True)
                cache_path = disk_path

        if cache_path.exists():
            try:
                day_indicators_df = pd.read_feather(cache_path)
                if _indicator_cache_is_stale(day_indicators_df):
                    print(f"  Cache {filename} com indicadores degenerados. Recalculando...", flush=True)
                    day_indicators_df = None
            except Exception as e:
                print(f"  Erro ao carregar cache {cache_path}: {e}. Recalculando...", flush=True)
                if cache_path != disk_path and disk_path.exists():
                    try:
                        day_indicators_df = pd.read_feather(disk_path)
                        if _indicator_cache_is_stale(day_indicators_df):
                            day_indicators_df = None
                            raise ValueError("cached indicators stale")
                        import shutil
                        shutil.copy2(disk_path, cache_path)
                    except Exception:
                        pass

        if day_indicators_df is None:
            super_indices = _build_indicator_sample_indices(
                hours,
                avgs,
                contract_mode=CONTRACT_MODE,
                symbol=SYMBOL,
            )
                
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
                    
                day_indicators_df.to_feather(cache_path)
                
                if cache_dir != disk_dir:
                    try:
                        import shutil
                        shutil.copy2(cache_path, disk_path)
                    except Exception as e:
                        print(f"  [SHM] Erro ao persistir {filename} no disco: {e}", flush=True)
            except Exception as e:
                import traceback
                print(f"  Erro ao pre-calcular indicadores para o dia {day}: {e}", flush=True)
                traceback.print_exc()
                return {c["name"]: [] for c in STRATEGY_CONFIGS}, 0.0

        if day_indicators_df is not None:
            _add_to_indicators_df_cache(day, day_indicators_df)

    # Convert to dictionary of records for target indices only, to avoid slow to_dict('records')
    if CONTRACT_MODE in {"rise_fall", "multiplier"}:
        # Extract numpy arrays directly from the indicators dataframe for O(1) indexing
        cusum_arr = day_indicators_df["cusum_score"].values if "cusum_score" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        velocity_arr = day_indicators_df["price_velocity"].values if "price_velocity" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        imbalance_arr = day_indicators_df["tick_imbalance"].values if "tick_imbalance" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        momentum_arr = day_indicators_df["price_momentum"].values if "price_momentum" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        ema_diff_arr = day_indicators_df["ema_diff"].values if "ema_diff" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        ols_arr = day_indicators_df["ols_slope"].values if "ols_slope" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        markov_up_arr = day_indicators_df["markov_p_up_given_up"].values if "markov_p_up_given_up" in day_indicators_df.columns else np.full(len(day_indicators_df), 0.5)
        markov_dn_arr = day_indicators_df["markov_p_down_given_down"].values if "markov_p_down_given_down" in day_indicators_df.columns else np.full(len(day_indicators_df), 0.5)
        hurst_arr = day_indicators_df["hurst_exponent"].values if "hurst_exponent" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        shannon_arr = day_indicators_df["shannon_entropy"].values if "shannon_entropy" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        kalman_arr = day_indicators_df["kalman_residual_zscore"].values if "kalman_residual_zscore" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        p_loss_arr = day_indicators_df["p_loss"].values if "p_loss" in day_indicators_df.columns else np.zeros(len(day_indicators_df))
        indicators_map = (
            _build_sampled_indicator_map(day_indicators_df)
            if CONTRACT_MODE == "multiplier"
            else {}
        )
    else:
        if day in _indicators_list_cache:
            indicators_map = _indicators_list_cache[day]
        else:
            # Pre-calcula a mediana rolante de 100 ticks da derivative_energy para evitar calcular no loop
            if "deriv_energy_median_100" not in day_indicators_df.columns:
                if "derivative_energy" in day_indicators_df.columns:
                    day_indicators_df["deriv_energy_median_100"] = day_indicators_df["derivative_energy"].rolling(100).median()
                else:
                    day_indicators_df["deriv_energy_median_100"] = 1.0
                    
            # Target indices only (aligned with SAMPLE_EVERY)
            target_indices = [
                w for w in range(TICK_COUNT, len(day_indicators_df))
                if (w - TICK_COUNT) % SAMPLE_EVERY == 0
            ]
            
            # Slice DataFrame and convert only target rows to dict records
            sub_df = day_indicators_df.iloc[target_indices].copy()
            sub_records = sub_df.to_dict('records')
            
            indicators_map = {}
            for idx, r in zip(target_indices, sub_records):
                sc, hb = _precalculate_metrics_for_row(r, accu_cfg)
                r["precalculated_score"] = sc
                r["hard_blocked"] = hb
                indicators_map[idx] = r
                
            _add_to_indicators_list_cache(day, indicators_map)

    # Pre-calcula win_ticks para cada TP unico ou define fixo para Rise/Fall
    tp_to_wt: dict[float, int] = {}
    if CONTRACT_MODE in {"rise_fall", "multiplier"}:
        max_wt = MULTIPLIER_MAX_HOLD_TICKS if CONTRACT_MODE == "multiplier" else RISE_FALL_DURATION_TICKS
        tp_to_wt = {c["tp"]: max_wt for c in STRATEGY_CONFIGS}
    else:
        for c in STRATEGY_CONFIGS:
            tp = c["tp"]
            if tp not in tp_to_wt:
                tp_to_wt[tp] = _calc_win_ticks(tp)
                
        # Garantimos que se tivermos Super-Frankenstein, o max_wt inclua o wt máximo dele (9 ticks)
        max_wt = max(tp_to_wt.values()) if tp_to_wt else 9
        for c in STRATEGY_CONFIGS:
            if c.get("is_super_frank", False):
                max_wt = max(max_wt, 9)

    # Outcomes: uma lista separada para cada (tp, score)
    outcomes: dict[str, list[tuple]] = {c["name"]: [] for c in STRATEGY_CONFIGS}
    i = TICK_COUNT
    total_signals = 0

    while i < len(day_df) - max_wt - SLIPPAGE - 1:
        # ─── OTIMIZAÇÃO DE ALINHAMENTO POR SALTO ───
        rem = (i - TICK_COUNT) % SAMPLE_EVERY
        if rem != 0:
            i += (SAMPLE_EVERY - rem)
            continue

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
            state["heartbeat_ts"] = now
            state["heartbeat_ms"] = int(now * 1000)
            _write_state(out_path, state)
            t_last_progress = now

        if hours[i] in BLOCKED_HOURS:
            i += SAMPLE_EVERY
            continue
        avg = avgs[i]
        if np.isnan(avg):
            i += SAMPLE_EVERY
            continue
        if CONTRACT_MODE not in {"rise_fall", "multiplier"} and avg >= CALM_THRESH:
            i += SAMPLE_EVERY
            continue

        if i >= len(day_indicators_df):
            i += SAMPLE_EVERY
            continue
            
        # --- VERIFICAÇÃO SUPER OTIMIZADA DE SINAL ---
        selected_signal = None
        if CONTRACT_MODE in {"rise_fall", "multiplier"}:
            cusum_v = float(cusum_arr[i])
            velocity_v = float(velocity_arr[i])
            imbalance_v = float(imbalance_arr[i])
            hurst_v = float(hurst_arr[i])
            shannon_v = float(shannon_arr[i])
            kalman_v = float(kalman_arr[i])
            p_loss = float(p_loss_arr[i])
            
            if RISE_FALL_USE_ENSEMBLE and p_loss >= RISE_FALL_ENSEMBLE_MIN_PROB:
                i += SAMPLE_EVERY
                continue

            if CONTRACT_MODE == "multiplier":
                row = indicators_map.get(i)
                if row is None:
                    i += SAMPLE_EVERY
                    continue
                quotes_window = prices[max(0, i - 39): i + 1].tolist()
                jm_cfg = JumpMomentumConfig(
                    min_score=max(3, MULTIPLIER_JUMP_MIN_SCORE),
                    min_confidence=MULTIPLIER_JUMP_MIN_CONFIDENCE,
                    min_ticks=30,
                    quality_gate_enabled=RISE_FALL_QUALITY_GATE,
                    qg_min_abs_imbalance=MULTIPLIER_JUMP_QG_MIN_ABS_IMBALANCE,
                    qg_bayes_strong=RISE_FALL_QG_BAYES_STRONG,
                    qg_hurst_max=RISE_FALL_QG_HURST_MAX,
                    bayesian_strong_prob=MULTIPLIER_JUMP_BAYES_STRONG_PROB,
                    hurst_trending=MULTIPLIER_JUMP_HURST_TRENDING,
                    hurst_reverting=MULTIPLIER_JUMP_HURST_REVERTING,
                    mi_flow_min=MULTIPLIER_JUMP_MI_FLOW_MIN,
                    wavelet_snr_min=MULTIPLIER_JUMP_WAVELET_SNR_MIN,
                )
                jm_signal, jm_score, jm_conf = generate_jump_momentum_snapshot_signal(
                    quotes_window,
                    row,
                    config=jm_cfg,
                )
                cont_cfg = MultiplierContinuationConfig(
                    min_score=max(3, MULTIPLIER_CONTINUATION_MIN_SCORE),
                    min_confidence=MULTIPLIER_CONTINUATION_MIN_CONFIDENCE,
                    min_ticks=30,
                    min_up_ticks=max(2, MULTIPLIER_CONTINUATION_MIN_UP_TICKS),
                    max_down_ticks=max(0, MULTIPLIER_CONTINUATION_MAX_DOWN_TICKS),
                    min_abs_imbalance=MULTIPLIER_CONTINUATION_MIN_IMBALANCE,
                    min_markov_edge=MULTIPLIER_CONTINUATION_MIN_MARKOV_EDGE,
                )
                cont_signal, cont_score, cont_conf = generate_multiplier_continuation_snapshot_signal(
                    quotes_window,
                    row,
                    config=cont_cfg,
                )
                if MULTIPLIER_DIRECTION == "up":
                    if jm_signal == "CALL":
                        selected_signal = "CALL"
                        actual_score = jm_score
                        p_loss = 1.0 - float(jm_conf) if jm_conf is not None else p_loss
                    elif cont_signal == "CALL":
                        selected_signal = "CALL"
                        actual_score = cont_score
                        p_loss = 1.0 - float(cont_conf) if cont_conf is not None else p_loss
                    else:
                        i += SAMPLE_EVERY
                        continue
                else:
                    if jm_signal in {"CALL", "PUT"}:
                        selected_signal = jm_signal
                        actual_score = jm_score
                        p_loss = 1.0 - float(jm_conf) if jm_conf is not None else p_loss
                    elif cont_signal == "CALL":
                        selected_signal = "CALL"
                        actual_score = cont_score
                        p_loss = 1.0 - float(cont_conf) if cont_conf is not None else p_loss
                    else:
                        i += SAMPLE_EVERY
                        continue
            else:
                momentum_v = float(momentum_arr[i]) if "momentum_arr" in locals() else 0.0
                ema_diff_v = float(ema_diff_arr[i]) if "ema_diff_arr" in locals() else 0.0
                ols_v = float(ols_arr[i]) if "ols_arr" in locals() else 0.0
                markov_up_v = float(markov_up_arr[i]) if "markov_up_arr" in locals() else 0.5
                markov_dn_v = float(markov_dn_arr[i]) if "markov_dn_arr" in locals() else 0.5
                up_votes = (
                    int(velocity_v > 0.0)
                    + int(imbalance_v >= 1.0)
                    + int(ols_v > 0.0)
                    + int(momentum_v > 0.0)
                    + int(ema_diff_v > 0.0)
                    + int(markov_up_v > markov_dn_v)
                )
                down_votes = (
                    int(velocity_v < 0.0)
                    + int(imbalance_v <= -1.0)
                    + int(ols_v < 0.0)
                    + int(momentum_v < 0.0)
                    + int(ema_diff_v < 0.0)
                    + int(markov_dn_v > markov_up_v)
                )
                is_crash = "CRASH" in SYMBOL.upper()
                selected_signal = "CALL" if is_crash else "PUT"
                actual_score = up_votes if is_crash else down_votes

            signal_vote_floor = RISE_FALL_MIN_VOTES
            if CONTRACT_MODE == "multiplier":
                signal_vote_floor = max(
                    2,
                    min(
                        RISE_FALL_MIN_VOTES,
                        MULTIPLIER_JUMP_MIN_SCORE,
                        MULTIPLIER_CONTINUATION_MIN_SCORE,
                    ),
                )
            if actual_score < signal_vote_floor:
                i += SAMPLE_EVERY
                continue

            # Direction-aware spike filter. A MULTDOWN entry is blocked during
            # upward shock risk; a MULTUP entry is blocked during downward shock risk.
            if selected_signal == "CALL":
                boom_multiplier_pullback = (
                    CONTRACT_MODE == "multiplier"
                    and "BOOM" in SYMBOL.upper()
                    and imbalance_v <= -RISE_FALL_BOOM_MAX_IMBALANCE
                    and cusum_v >= 0.0
                )
                if (
                    not boom_multiplier_pullback
                    and (
                        cusum_v < -RISE_FALL_BOOM_MAX_CUSUM
                        or velocity_v < -RISE_FALL_BOOM_MAX_VELOCITY
                        or imbalance_v < -RISE_FALL_BOOM_MAX_IMBALANCE
                    )
                ):
                    i += SAMPLE_EVERY
                    continue
            else:
                if (
                    cusum_v > RISE_FALL_BOOM_MAX_CUSUM
                    or velocity_v > RISE_FALL_BOOM_MAX_VELOCITY
                    or imbalance_v > RISE_FALL_BOOM_MAX_IMBALANCE
                ):
                    i += SAMPLE_EVERY
                    continue
        else:
            row = indicators_map.get(i)
            if row is None:
                i += SAMPLE_EVERY
                continue
            if row.get("hard_blocked", False):
                i += SAMPLE_EVERY
                continue
                
            score = row.get("precalculated_score", 0)
            if score < CALM_MIN_SCORE:
                i += SAMPLE_EVERY
                continue

            p_loss = row.get("p_loss")
            
            # XGBoost P(LOSS) filter
            use_ensemble = os.getenv("USE_ENSEMBLE", "true").lower() == "true"
            if use_ensemble and p_loss is not None:
                if p_loss > ENSEMBLE_MIN_PROB:
                    i += SAMPLE_EVERY
                    continue  # sinal fraco, pula

            cusum_v = float(row.get("cusum_score", 0) or 0)
            hurst_v = float(row.get("hurst_exponent", 0.5) or 0.5)
            shannon_v = float(row.get("shannon_entropy", 0) or 0)
            kalman_v = float(row.get("kalman_residual_zscore", 0) or 0)
            if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
                i += SAMPLE_EVERY
                continue

            actual_score = score

        total_signals += 1

        # Entry idx com slippage
        entry_idx = i + SLIPPAGE
        if entry_idx >= len(prices) - max_wt:
            i += SAMPLE_EVERY
            continue

        if CONTRACT_MODE in {"rise_fall", "multiplier"}:
            barrier_hit_at = None
        else:
            # Verifica outcome para cada (TP, score) combo
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
                
            if CONTRACT_MODE == "rise_fall":
                is_crash = "CRASH" in SYMBOL.upper()
                if is_crash:
                    is_win = prices[entry_idx + RISE_FALL_DURATION_TICKS] > prices[entry_idx]
                else:
                    is_win = prices[entry_idx + RISE_FALL_DURATION_TICKS] < prices[entry_idx]
                mult_direction = None
                mult_returns = None
            elif CONTRACT_MODE == "multiplier":
                signal = selected_signal or "PUT"
                mult_direction = _multiplier_direction_from_signal(signal)
                base_price = prices[entry_idx]
                mult_returns = [
                    float((prices[entry_idx + j] / base_price) - 1.0)
                    for j in range(1, min(MULTIPLIER_MAX_HOLD_TICKS, len(prices) - entry_idx - 1) + 1)
                ]
                is_win = False
            else:
                is_win = not (barrier_hit_at is not None and barrier_hit_at <= wt)
                mult_direction = None
                mult_returns = None
                
            base_outcome = (
                is_win,
                float(epochs[entry_idx]),
                float(avg),
                float(cusum_v),
                float(hurst_v),
                barrier_hit_at,
                float(shannon_v),
                float(kalman_v),
                float(p_loss if p_loss is not None else 1.0),
                float(velocity_v if CONTRACT_MODE in {"rise_fall", "multiplier"} else 0.0),
                float(imbalance_v if CONTRACT_MODE in {"rise_fall", "multiplier"} else 0.0),
            )
            if CONTRACT_MODE == "multiplier":
                outcomes[c["name"]].append(base_outcome + (mult_direction, mult_returns))
            else:
                outcomes[c["name"]].append(base_outcome)

            best_hold = max(best_hold, wt)

        if CONTRACT_MODE == "rise_fall":
            i += (
                RISE_FALL_DURATION_TICKS
                + SLIPPAGE
                + RISE_FALL_COOLDOWN_TICKS
            )
        elif CONTRACT_MODE == "multiplier":
            i += (
                MULTIPLIER_MAX_HOLD_TICKS
                + SLIPPAGE
                + RISE_FALL_COOLDOWN_TICKS
            )
        else:
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
    apply_config(dict(os.environ))
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

    for day_idx, day in enumerate(days_with_data):
        state["current_day"] = day.isoformat()
        state["current_day_index"] = day_idx + 1
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

            sortino = (mean_pnl / downside_dev) if downside_dev > 0.001 else (20.0 if mean_pnl > 0 else 0.0)

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


def apply_config(env_overrides: dict):
    global STAKE, MAX_STAKE, GROWTH_RATE, TP_PCT, MAX_HOLD, SOROS_STEPS, SOROS_COOLDOWN, STOP_GAIN, TRAILING_S, TRAILING_L
    global CUSUM_MAX, HURST_MIN, CALM_THRESH, TICK_COUNT, CALM_MIN_SCORE, ENSEMBLE_MIN_PROB, BLOCKED_HOURS, WIN_TICKS
    global STRATEGY_CONFIGS, STRATEGY_NAMES, accu_cfg, SAMPLE_EVERY
    global CONTRACT_MODE, RISE_FALL_DURATION_TICKS, RISE_FALL_MIN_PAYOUT_PCT, RISE_FALL_COOLDOWN_TICKS
    global RISE_FALL_BOOM_MAX_CUSUM, RISE_FALL_BOOM_MAX_VELOCITY, RISE_FALL_BOOM_MAX_IMBALANCE, RISE_FALL_BOOM_ONLY_PUT, RISE_FALL_MIN_VOTES
    global RISE_FALL_USE_ENSEMBLE, RISE_FALL_ENSEMBLE_MIN_PROB
    global MULTIPLIER_VALUE, MULTIPLIER_DIRECTION, MULTIPLIER_TAKE_PROFIT, MULTIPLIER_STOP_LOSS, MULTIPLIER_MAX_HOLD_TICKS
    global SYMBOL, _max_csv_range, _day_df_cache, _indicators_df_cache, _indicators_list_cache
    
    os.environ.update(env_overrides)
    if os.environ.get("PEGASUS_OPTIMIZER_RUN", "false").lower() == "true":
        import logging
        logging.getLogger("Pegasus").setLevel(logging.ERROR)
        
    new_symbol = os.environ.get("SYMBOL", "BOOM1000")
    if new_symbol != SYMBOL:
        SYMBOL = new_symbol
        _max_csv_range = None
        _day_df_cache.clear()
        _indicators_df_cache.clear()
        _indicators_list_cache.clear()
    
    STAKE = float(os.environ.get("STAKE", "5"))
    MAX_STAKE = float(os.environ.get("MAX_STAKE", "10"))
    GROWTH_RATE = float(os.environ.get("ACCUMULATOR_GROWTH_RATE", "0.03"))
    TP_PCT = float(os.environ.get("ACCUMULATOR_TAKE_PROFIT_PERCENT", "30")) / 100.0
    MAX_HOLD = int(os.environ.get("ACCUMULATOR_MAX_HOLD_TICKS", "80"))
    SOROS_STEPS = int(os.environ.get("SOROS_MAX_STEPS", "3"))
    SOROS_COOLDOWN = int(os.environ.get("ACCUMULATOR_COOLDOWN_TICKS", "5"))
    STOP_GAIN = float(os.environ.get("STOP_GAIN_PCT", "100.0")) / 100.0
    TRAILING_S = float(os.getenv("DAILY_TRAILING_START", "30.0")) / 100.0
    TRAILING_L = float(os.getenv("DAILY_TRAILING_LOCK", "5.0")) / 100.0

    CONTRACT_MODE = os.environ.get("CONTRACT_MODE", "calm_accu").strip().lower()
    RISE_FALL_DURATION_TICKS = int(os.environ.get("RISE_FALL_DURATION_TICKS", "5"))
    RISE_FALL_MIN_PAYOUT_PCT = float(os.environ.get("RISE_FALL_MIN_PAYOUT_PCT", "0.0055"))
    RISE_FALL_COOLDOWN_TICKS = int(os.environ.get("RISE_FALL_COOLDOWN_TICKS", "3"))
    RISE_FALL_BOOM_MAX_CUSUM = float(os.environ.get("RISE_FALL_BOOM_MAX_CUSUM", "8.0"))
    RISE_FALL_BOOM_MAX_VELOCITY = float(os.environ.get("RISE_FALL_BOOM_MAX_VELOCITY", "0.001"))
    RISE_FALL_BOOM_MAX_IMBALANCE = float(os.environ.get("RISE_FALL_BOOM_MAX_IMBALANCE", "1.5"))
    RISE_FALL_BOOM_ONLY_PUT = os.environ.get("RISE_FALL_BOOM_ONLY_PUT", "true").lower() == "true"
    RISE_FALL_MIN_VOTES = max(1, min(6, int(os.environ.get("RISE_FALL_MIN_VOTES", "4"))))
    RISE_FALL_USE_ENSEMBLE = os.environ.get("RISE_FALL_USE_ENSEMBLE", "false").lower() == "true"
    RISE_FALL_ENSEMBLE_MIN_PROB = float(os.environ.get("RISE_FALL_ENSEMBLE_MIN_PROB", "0.52"))
    MULTIPLIER_VALUE = int(os.environ.get("MULTIPLIER_VALUE", "100"))
    MULTIPLIER_DIRECTION = os.environ.get("MULTIPLIER_DIRECTION", "signal").strip().lower()
    MULTIPLIER_TAKE_PROFIT = float(os.environ.get("MULTIPLIER_TAKE_PROFIT", "0.50"))
    MULTIPLIER_STOP_LOSS = float(os.environ.get("MULTIPLIER_STOP_LOSS", "1.00"))
    MULTIPLIER_MAX_HOLD_TICKS = int(os.environ.get("MULTIPLIER_MAX_HOLD_TICKS", "30"))

    CUSUM_MAX = float(os.environ.get("CALM_ACCU_MAX_ENTRY_CUSUM", "5.0"))
    HURST_MIN = float(os.environ.get("ACCUMULATOR_MIN_HURST_EXPONENT", "0.45"))
    CALM_THRESH = float(os.environ.get("CALM_ACCU_THRESHOLD", "1.5e-6"))
    TICK_COUNT = int(os.environ.get("TICK_COUNT", "100"))
    CALM_MIN_SCORE = int(os.environ.get("CALM_ACCU_MIN_SCORE", "20"))
    ENSEMBLE_MIN_PROB = float(os.environ.get("ENSEMBLE_MIN_PROB", "0.30"))
    SAMPLE_EVERY = max(1, int(os.environ.get("BACKTEST_SAMPLE_EVERY", "60")))
    optimizer_fast_sampling = (
        os.environ.get("PEGASUS_OPTIMIZER_RUN", "false").lower() == "true"
        and os.environ.get("PEGASUS_OPTIMIZER_FULL_TICK", "false").lower() != "true"
    )
    if CONTRACT_MODE in {"rise_fall", "multiplier"} and not optimizer_fast_sampling:
        SAMPLE_EVERY = 1
    
    _v = 1.0
    WIN_TICKS = MAX_HOLD
    for _j in range(1, MAX_HOLD + 1):
        _v *= 1 + GROWTH_RATE
        if _v - 1.0 >= TP_PCT:
            WIN_TICKS = _j
            break
            
    _blocked_raw = os.environ.get("BLOCKED_UTC_HOURS", "5,6,7,8,9")
    BLOCKED_HOURS = set()
    for _h in _blocked_raw.split(","):
        _h = _h.strip()
        if _h.isdigit():
            BLOCKED_HOURS.add(int(_h))
            
    import dataclasses
    accu_cfg = dataclasses.replace(
        accu_cfg,
        min_score=CALM_MIN_SCORE,
        calm_min_score=CALM_MIN_SCORE,
        ensemble_min_prob=ENSEMBLE_MIN_PROB,
    )
    
    STRATEGY_CONFIGS = _generate_strategy_configs()
    STRATEGY_NAMES = [c["name"] for c in STRATEGY_CONFIGS]


def compute_monthly_breakdown(results: list, strategy: str) -> dict:
    monthly = {}
    for r in results:
        date_str = r.get("date", "")
        if len(date_str) < 7:
            continue
        month = date_str[:7]
        if month not in monthly:
            monthly[month] = []
        s = r.get("strategies", {}).get(strategy, {})
        monthly[month].append({
            "pnl": s.get("pnl", 0.0),
            "trades": s.get("trades", 0)
        })
    
    breakdown = {}
    for month, m_days in sorted(monthly.items()):
        active = [d for d in m_days if d["trades"] > 0]
        pos = [d for d in m_days if d["pnl"] > 0]
        total_pnl = sum(d["pnl"] for d in m_days)
        n_act = len(active)
        n_pos = len(pos)
        avg_day = (sum(d["pnl"] for d in active) / n_act) if n_act > 0 else 0.0
        consist = (n_pos / n_act * 100) if n_act > 0 else 0.0
        
        breakdown[month] = {
            "pnl": round(total_pnl, 2),
            "active_days": n_act,
            "positive_days": n_pos,
            "total_days": len(m_days),
            "avg_daily_profit": round(avg_day, 4),
            "consistency_pct": round(consist, 1)
        }
    return breakdown


def run_backtest_direct(
    start_date_str: str,
    end_date_str: str,
    start_balance: float,
    env_overrides: dict,
    worker_id: str = None,
) -> dict | None:
    apply_config(env_overrides)
    
    start_date = _date.fromisoformat(start_date_str)
    end_date = _date.fromisoformat(end_date_str)
    
    days: list[_date] = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur += timedelta(days=1)
        
    data_dir = Path(__file__).parent / "data"
    
    days_with_data = []
    for day in days:
        if _ensure_day_ticks(day, data_dir):
            days_with_data.append(day)
            
    accumulate_balance = os.environ.get("BACKTEST_COMPOUNDING", "false").strip().lower() == "true"
    
    total_pnls = {s: 0.0 for s in STRATEGY_NAMES}
    balances = {s: start_balance for s in STRATEGY_NAMES}
    results = []
    
    t_start = time.time()
    total_days = len(days_with_data)
    for day_idx, day in enumerate(days_with_data):
        day_df = _load_day_df(day, data_dir)
        if day_df is None:
            continue
            
        if not accumulate_balance:
            balances = {s: start_balance for s in STRATEGY_NAMES}
            
        if worker_id:
            progress_path = Path("logs") / f"backtest_worker_{worker_id}.json"
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                months_pt = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
                m_name = months_pt[day.month] if 1 <= day.month <= 12 else str(day.month)
                progress_path.write_text(json.dumps({
                    "current_day_index": day_idx + 1,
                    "total_days": total_days,
                    "elapsed_s": time.time() - t_start,
                    "current_day": day.isoformat(),
                    "current_month": m_name
                }), encoding="utf-8")
            except Exception:
                pass

        result = _sim_day_multi(
            day,
            day_df,
            balances,
            state=None,
            out_path=None,
            t_global=None,
        )
        
        for s in STRATEGY_NAMES:
            total_pnls[s] += result["strategies"][s]["pnl"]
            
        results.append(result)
        
    return compile_summary_metrics(results, env_overrides, start_balance)


def compile_summary_metrics(results: list, env_overrides: dict, start_balance: float = 50.0) -> dict | None:
    n = len(results)
    if n == 0:
        return None
        
    total_pnls = {s: 0.0 for s in STRATEGY_NAMES}
    for r in results:
        for s in STRATEGY_NAMES:
            total_pnls[s] += r.get("strategies", {}).get(s, {}).get("pnl", 0.0)
            
    summary = {"total_days": n, "strategies": {}}
    for s in STRATEGY_NAMES:
        total_pnl = round(total_pnls[s], 2)
        final_bal = start_balance + total_pnl
        busted_days = sum(1 for r in results if r["strategies"].get(s, {}).get("busted", False))
        pos_days = sum(1 for r in results if r["strategies"].get(s, {}).get("pnl", 0) > 0)
        neg_days = sum(1 for r in results if r["strategies"].get(s, {}).get("pnl", 0) < 0)
        active_days = sum(1 for r in results if r["strategies"].get(s, {}).get("trades", 0) > 0)
        wrs = [r["strategies"].get(s, {}).get("signal_wr", 0) for r in results]
        avg_wr = round(sum(wrs) / len(wrs), 1) if wrs else 0

        active_pnls = [r["strategies"].get(s, {}).get("pnl", 0) for r in results if r["strategies"].get(s, {}).get("trades", 0) > 0]
        avg_daily_profit = round(sum(active_pnls) / len(active_pnls), 4) if active_pnls else 0.0
        consistency_pct = round(pos_days / active_days * 100, 1) if active_days > 0 else 0.0

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

        daily_pnls = [r["strategies"].get(s, {}).get("pnl", 0.0) for r in results]
        mean_pnl = sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0.0
        
        if len(daily_pnls) > 1:
            variance = sum((x - mean_pnl) ** 2 for x in daily_pnls) / (len(daily_pnls) - 1)
            std_dev = variance ** 0.5
        else:
            std_dev = 0.0

        sharpe = (mean_pnl / std_dev) if std_dev > 0.001 else 0.0

        downside_pnls = [x for x in daily_pnls if x < 0.0]
        if downside_pnls:
            downside_variance = sum(x ** 2 for x in downside_pnls) / len(daily_pnls)
            downside_dev = downside_variance ** 0.5
        else:
            downside_dev = 0.0

        sortino = (mean_pnl / downside_dev) if downside_dev > 0.001 else (20.0 if mean_pnl > 0 else 0.0)

        total_trades = sum(r["strategies"].get(s, {}).get("trades", 0) for r in results)

        summary["strategies"][s] = {
            "final_balance": round(final_bal, 2),
            "total_pnl": total_pnl,
            "total_trades": total_trades,
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
        
    from optimize_loop import compute_score
    m = compute_score(results)
    
    summary_sf = summary["strategies"].get("Super-Frankenstein", {})
    summary_live = summary["strategies"].get("Pegasus Live Sniper (9% TP)", {})
    
    m["sharpe_ratio"] = summary_sf.get("sharpe_ratio", 0.0)
    m["sortino_ratio"] = summary_sf.get("sortino_ratio", 0.0)
    m["max_drawdown"] = summary_sf.get("max_drawdown", 0.0)

    live = compute_score(results, "Pegasus Live Sniper (9% TP)")
    m["live_avg_daily"]   = live["avg_daily_profit"]
    m["live_positive_days"] = live["positive_days"]
    m["live_total_pnl"]   = live["total_pnl"]
    m["live_sharpe"]      = summary_live.get("sharpe_ratio", 0.0)
    m["live_sortino"]     = summary_live.get("sortino_ratio", 0.0)
    m["live_drawdown"]    = summary_live.get("max_drawdown", 0.0)
    
    m["_env"] = env_overrides
    
    sharpe_val = m["sharpe_ratio"]
    sortino_val = m["sortino_ratio"]
    max_dd_val = m["max_drawdown"]
    
    m["score"] += sharpe_val * 8.0
    m["score"] += sortino_val * 6.0
    
    if max_dd_val > 15.0:
        m["score"] -= max_dd_val * 3.0
    elif max_dd_val > 5.0:
        m["score"] -= max_dd_val * 1.0

    live_avg = live["avg_daily_profit"]
    if live_avg < 0.0:
        m["score"] -= abs(live_avg) * 8.0
    else:
        m["score"] += live_avg * 3.0

    direction = str(env_overrides.get("MULTIPLIER_DIRECTION", "signal")).strip().lower()
    tp = float(env_overrides.get("MULTIPLIER_TAKE_PROFIT", 0.50) or 0.50)
    sl = float(env_overrides.get("MULTIPLIER_STOP_LOSS", 1.00) or 1.00)
    hold = int(float(env_overrides.get("MULTIPLIER_MAX_HOLD_TICKS", 30)) or 30)
    votes = int(float(env_overrides.get("RISE_FALL_MIN_VOTES", 4)) or 4)
    cooldown = int(float(env_overrides.get("RISE_FALL_COOLDOWN_TICKS", 3)) or 3)
    use_ensemble = str(env_overrides.get("RISE_FALL_USE_ENSEMBLE", "false")).lower() == "true"
    rr = (tp / sl) if sl > 0 else 99.0

    if direction == "down":
        m["score"] -= 180.0
    if not use_ensemble:
        m["score"] -= 120.0
    if votes < 5:
        m["score"] -= (5 - votes) * 80.0
    if hold > 18:
        m["score"] -= (hold - 18) * 18.0
    if cooldown > 24:
        m["score"] -= (cooldown - 24) * 10.0
    if rr > 1.9:
        m["score"] -= (rr - 1.9) * 180.0
    elif rr < 0.55:
        m["score"] -= (0.55 - rr) * 240.0

    m["summary"] = summary
    m["monthly_breakdown"] = {
        "Super-Frankenstein": compute_monthly_breakdown(results, "Super-Frankenstein"),
        "Pegasus Live Sniper (9% TP)": compute_monthly_breakdown(results, "Pegasus Live Sniper (9% TP)")
    }
    sf_months = list(m["monthly_breakdown"]["Super-Frankenstein"].values())
    sf_month_pnls = [float((month or {}).get("pnl", 0.0) or 0.0) for month in sf_months]
    positive_months = sum(1 for pnl in sf_month_pnls if pnl > 0.0)
    negative_months = sum(1 for pnl in sf_month_pnls if pnl < 0.0)
    best_month = max(sf_month_pnls) if sf_month_pnls else 0.0
    total_positive_pnl = sum(pnl for pnl in sf_month_pnls if pnl > 0.0)
    concentration_ratio = (best_month / total_positive_pnl) if total_positive_pnl > 0.0 else 0.0

    if positive_months <= 1:
        m["score"] -= 450.0
    elif positive_months == 2:
        m["score"] -= 120.0
    if negative_months >= 4:
        m["score"] -= negative_months * 45.0
    if concentration_ratio > 0.85:
        m["score"] -= (concentration_ratio - 0.85) * 1200.0
    elif concentration_ratio > 0.70:
        m["score"] -= (concentration_ratio - 0.70) * 500.0

    m["positive_months"] = positive_months
    m["negative_months"] = negative_months
    m["best_month_pnl"] = round(best_month, 2)
    m["concentration_ratio"] = round(concentration_ratio, 4)
    m["score"] = round(m["score"], 4)
    m["results"] = results
    return m


if __name__ == "__main__":
    main()
