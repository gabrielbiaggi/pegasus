"""shadow_collect_rf.py — Rise/Fall shadow data collector.

Two modes:
  --mode history   Fetch historical ticks via Deriv ticks_history API, compute
                   all RF directional indicators offline, write labeled rows to
                   the shadow_ticks_rf PostgreSQL table.
  --mode realtime  Subscribe to live ticks, compute indicators in a sliding
                   window, label rows with a 5-tick lookahead delay.

The RF direction labels (future_rf_direction_1t/3t/5t) are computed by pure
price comparison — no real-time barrier tracking is needed, so historical data
works perfectly for labeling.

Usage:
    python shadow_collect_rf.py --mode history  --batches 20 --pg-dsn $PG_DSN
    python shadow_collect_rf.py --mode realtime --ticks 10000 --pg-dsn $PG_DSN
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import deque
from typing import Any

import websockets
from dotenv import load_dotenv

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

from config import load_config
from strategy import AccumulatorStrategyConfig, RF_FEATURES, calculate_tick_indicators

# ---------------------------------------------------------------------------
# Table schema
# ---------------------------------------------------------------------------

_RF_FEATURES_ALL = RF_FEATURES  # 23 features (13 ACCU + 10 new directional)

_DDL = """
CREATE TABLE IF NOT EXISTS shadow_ticks_rf (
    id                      BIGSERIAL PRIMARY KEY,
    entry_epoch             BIGINT,
    entry_quote             DOUBLE PRECISION,
    future_rf_direction_1t  TEXT,
    future_rf_direction_3t  TEXT,
    future_rf_direction_5t  TEXT,
    -- directional velocity / trend
    price_velocity          DOUBLE PRECISION,
    ols_slope               DOUBLE PRECISION,
    price_momentum          DOUBLE PRECISION,
    ema_diff                DOUBLE PRECISION,
    run_length              DOUBLE PRECISION,
    -- flow imbalance
    tick_imbalance          DOUBLE PRECISION,
    -- 1st-order Markov
    markov_p_up_given_up    DOUBLE PRECISION,
    markov_p_down_given_down DOUBLE PRECISION,
    -- 2nd-order Markov
    markov2_puu             DOUBLE PRECISION,
    markov2_pdd             DOUBLE PRECISION,
    -- distribution statistics
    return_autocorr_lag1    DOUBLE PRECISION,
    return_skewness         DOUBLE PRECISION,
    -- spectral
    fft_dominant_period     DOUBLE PRECISION,
    -- volatility context
    hurst_exponent          DOUBLE PRECISION,
    hawkes_intensity        DOUBLE PRECISION,
    velocity_zscore         DOUBLE PRECISION,
    acceleration_zscore     DOUBLE PRECISION,
    bb_width_percent        DOUBLE PRECISION,
    tick_atr_percent        DOUBLE PRECISION,
    recent_move_percent     DOUBLE PRECISION,
    shannon_entropy         DOUBLE PRECISION,
    kalman_residual_zscore  DOUBLE PRECISION,
    inserted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shadow_ticks_rf_entry_epoch_idx ON shadow_ticks_rf (entry_epoch);
"""

_INSERT_COLS = [
    "entry_epoch", "entry_quote",
    "future_rf_direction_1t", "future_rf_direction_3t", "future_rf_direction_5t",
] + _RF_FEATURES_ALL

_INSERT_SQL = (
    f"INSERT INTO shadow_ticks_rf ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) ON CONFLICT DO NOTHING"
)


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _pg_init(pg_dsn: str) -> None:
    """Create the shadow_ticks_rf table if it does not exist."""
    with psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()


def _pg_write(pg_dsn: str, rows: list[dict[str, Any]]) -> int:
    """Batch-insert rows into shadow_ticks_rf. Returns number of rows written."""
    if not rows:
        return 0
    values = []
    for row in rows:
        values.append(tuple(
            row.get(col) if row.get(col) is not None else None
            for col in _INSERT_COLS
        ))
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, _INSERT_SQL, values, page_size=500)
            conn.commit()
        return len(rows)
    except Exception as exc:
        print(f"[shadow_collect_rf] PG write error: {exc}")
        return 0


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------

def _rf_direction(quotes: list[float], i: int, lookahead: int) -> str | None:
    if i + lookahead >= len(quotes):
        return None
    entry = quotes[i]
    future = quotes[i + lookahead]
    if future > entry:
        return "UP"
    if future < entry:
        return "DOWN"
    return "TIE"


def _metric(value: Any) -> float | None:
    """Coerce indicator value to float; return None for NaN/missing."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else round(f, 8)


# ---------------------------------------------------------------------------
# Historical mode: bulk download via ticks_history
# ---------------------------------------------------------------------------

async def collect_history(
    ws_url: str,
    token: str,
    symbol: str,
    pg_dsn: str,
    num_batches: int,
    batch_size: int,
    flush_every: int,
) -> int:
    """Download historical ticks in batches and compute RF labels offline."""
    config = AccumulatorStrategyConfig()
    min_ticks = config.minimum_ticks
    # Need min_ticks for warmup + 5 ticks of lookahead
    warmup = min_ticks + 5

    async with websockets.connect(ws_url, ping_interval=None, open_timeout=15) as ws:
        # Authorize
        await ws.send(json.dumps({"authorize": token}))
        auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        if auth.get("error"):
            raise RuntimeError(f"Auth error: {auth['error']}")
        loginid = str(auth["authorize"].get("loginid", ""))
        print(f"[history] Autorizado: {loginid}")

        # Fetch batches backwards in time
        all_ticks: list[dict[str, Any]] = []
        end_param: int | str = "latest"

        for batch_num in range(num_batches):
            req: dict[str, Any] = {
                "ticks_history": symbol,
                "count": batch_size,
                "end": end_param,
                "style": "ticks",
            }
            await ws.send(json.dumps(req))
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            if msg.get("error"):
                print(f"[history] Batch {batch_num}: API error {msg['error']}")
                break

            history = msg.get("history", {})
            times = history.get("times", [])
            prices = history.get("prices", [])
            if not times:
                print(f"[history] Batch {batch_num}: sem dados retornados")
                break

            batch = [{"epoch": int(t), "quote": float(p)} for t, p in zip(times, prices)]
            # Prepend (older data first)
            all_ticks = batch + all_ticks
            end_param = int(times[0]) - 1

            n_total = len(all_ticks)
            first_e = all_ticks[0]["epoch"]
            last_e = all_ticks[-1]["epoch"]
            print(
                f"[history] Batch {batch_num + 1}/{num_batches}: "
                f"+{len(batch)} ticks | total={n_total} | "
                f"span={first_e}-{last_e}"
            )

    if len(all_ticks) < warmup:
        print(f"[history] Ticks insuficientes ({len(all_ticks)} < {warmup}). Abortando.")
        return 0

    print(f"[history] Computando indicadores em {len(all_ticks)} ticks...")
    df = calculate_tick_indicators(all_ticks, config=config)
    quotes = [float(t["quote"]) for t in all_ticks]
    epochs = [int(t["epoch"]) for t in all_ticks]

    # Initialise PG table
    _pg_init(pg_dsn)

    rows: list[dict[str, Any]] = []
    total_written = 0

    for i in range(len(df) - 5):  # -5: need 5-tick lookahead
        last = df.iloc[i]
        # Skip warmup rows (indicators not yet computed)
        if last[_RF_FEATURES_ALL].isna().any():
            continue

        dir_1t = _rf_direction(quotes, i, 1)
        dir_3t = _rf_direction(quotes, i, 3)
        dir_5t = _rf_direction(quotes, i, 5)
        if dir_1t is None or dir_3t is None or dir_5t is None:
            break

        row: dict[str, Any] = {
            "entry_epoch": epochs[i],
            "entry_quote": quotes[i],
            "future_rf_direction_1t": dir_1t,
            "future_rf_direction_3t": dir_3t,
            "future_rf_direction_5t": dir_5t,
        }
        for feat in _RF_FEATURES_ALL:
            row[feat] = _metric(last.get(feat))
        rows.append(row)

        if len(rows) >= flush_every:
            written = _pg_write(pg_dsn, rows)
            total_written += written
            rows.clear()
            print(f"[history] {total_written} linhas salvas...")

    if rows:
        total_written += _pg_write(pg_dsn, rows)

    print(f"[history] Concluído: {total_written} linhas salvas em shadow_ticks_rf")
    return total_written


# ---------------------------------------------------------------------------
# Real-time mode: live tick subscription with 5-tick lookahead labeling
# ---------------------------------------------------------------------------

async def collect_realtime(
    ws_url: str,
    token: str,
    symbol: str,
    tick_count: int,
    pg_dsn: str,
    ticks_to_collect: int,
    flush_every: int,
) -> int:
    """Subscribe to live ticks and write labeled RF rows with 5-tick lookahead."""
    config = AccumulatorStrategyConfig()
    min_ticks = config.minimum_ticks
    max_buf = tick_count + 10

    buffer: deque[dict[str, Any]] = deque(maxlen=max_buf)
    all_ticks: list[dict[str, Any]] = []
    pending: deque[int] = deque()  # entry indices waiting for labels
    rows: list[dict[str, Any]] = []
    total_written = 0

    _pg_init(pg_dsn)

    async with websockets.connect(ws_url, ping_interval=None, open_timeout=15) as ws:
        await ws.send(json.dumps({"authorize": token}))
        auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        if auth.get("error"):
            raise RuntimeError(f"Auth error: {auth['error']}")
        loginid = str(auth["authorize"].get("loginid", ""))
        if not loginid.upper().startswith("VRTC"):
            raise RuntimeError(f"Conta nao-demo detectada: {loginid}. Use token demo.")
        print(f"[realtime] Autorizado: {loginid}")

        # Load historical warmup ticks
        await ws.send(json.dumps({
            "ticks_history": symbol,
            "count": tick_count,
            "end": "latest",
            "style": "ticks",
        }))
        hist_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        history = hist_msg.get("history", {})
        for epoch, quote in zip(history.get("times", []), history.get("prices", [])):
            t = {"epoch": int(epoch), "quote": float(quote)}
            buffer.append(t)
            all_ticks.append(t)
        print(f"[realtime] {len(all_ticks)} ticks de warmup carregados.")

        # Subscribe to live ticks
        await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))

        collected = 0
        while collected < ticks_to_collect:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            if msg.get("error"):
                raise RuntimeError(msg["error"])
            if msg.get("msg_type") != "tick":
                continue

            tick = msg["tick"]
            epoch = int(tick["epoch"])
            if buffer and int(buffer[-1]["epoch"]) == epoch:
                continue

            normalized = {"epoch": epoch, "quote": float(tick["quote"])}
            buffer.append(normalized)
            all_ticks.append(normalized)
            collected += 1

            tick_list = list(buffer)
            df = calculate_tick_indicators(tick_list, config=config)

            if len(df) >= min_ticks:
                last = df.iloc[-1]
                if not last[_RF_FEATURES_ALL].isna().any():
                    entry_idx = len(all_ticks) - 1
                    pending.append(entry_idx)

            # Resolve pending entries that now have 5-tick lookahead
            while pending:
                idx = pending[0]
                if len(all_ticks) <= idx + 5:
                    break
                pending.popleft()

                # Recompute indicators at that tick's window
                win_start = max(0, idx - max_buf + 1)
                win_ticks = all_ticks[win_start: idx + 1]
                df_win = calculate_tick_indicators(win_ticks, config=config)
                if df_win.empty or len(df_win) < min_ticks:
                    continue
                last_win = df_win.iloc[-1]
                if last_win[_RF_FEATURES_ALL].isna().any():
                    continue

                quotes = [float(t["quote"]) for t in all_ticks]
                dir_1t = _rf_direction(quotes, idx, 1)
                dir_3t = _rf_direction(quotes, idx, 3)
                dir_5t = _rf_direction(quotes, idx, 5)
                if None in (dir_1t, dir_3t, dir_5t):
                    continue

                row: dict[str, Any] = {
                    "entry_epoch": all_ticks[idx]["epoch"],
                    "entry_quote": all_ticks[idx]["quote"],
                    "future_rf_direction_1t": dir_1t,
                    "future_rf_direction_3t": dir_3t,
                    "future_rf_direction_5t": dir_5t,
                }
                for feat in _RF_FEATURES_ALL:
                    row[feat] = _metric(last_win.get(feat))
                rows.append(row)

            if len(rows) >= flush_every:
                written = _pg_write(pg_dsn, rows)
                total_written += written
                rows.clear()
                print(f"[realtime] {collected}/{ticks_to_collect} ticks | {total_written} linhas salvas")

    if rows:
        total_written += _pg_write(pg_dsn, rows)

    print(f"[realtime] Concluído: {total_written} linhas salvas em shadow_ticks_rf")
    return total_written


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Coleta shadow ticks para Rise/Fall com labels de direção.")
    parser.add_argument(
        "--mode",
        choices=["history", "realtime"],
        default="history",
        help="history: download histórico offline; realtime: coleta ao vivo",
    )
    # History mode
    parser.add_argument("--batches", type=int, default=10, help="Número de batches históricos (5000 ticks cada)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Ticks por batch (max 5000)")
    # Realtime mode
    parser.add_argument("--ticks", type=int, default=5000, help="Ticks ao vivo a coletar (realtime)")
    # Common
    parser.add_argument("--flush-every", type=int, default=200, help="Linhas por flush para o PostgreSQL")
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=os.getenv("PG_DSN", ""),
        help="PostgreSQL DSN. Também lido de PG_DSN no .env.",
    )
    parser.add_argument("--symbol", type=str, default=None, help="Símbolo Deriv (padrão: do .env)")
    args = parser.parse_args()

    if not args.pg_dsn:
        print("Erro: PG_DSN não definido. Use --pg-dsn ou defina PG_DSN no .env")
        raise SystemExit(1)
    if not _HAS_PSYCOPG2:
        print("Erro: psycopg2 não instalado. Execute: pip install psycopg2-binary")
        raise SystemExit(1)

    symbol = args.symbol or cfg.symbol

    if args.mode == "history":
        print(f"[shadow_collect_rf] Modo HISTÓRICO | símbolo={symbol} | batches={args.batches} × {args.batch_size} ticks")
        total = asyncio.run(collect_history(
            ws_url=cfg.ws_url,
            token=cfg.token,
            symbol=symbol,
            pg_dsn=args.pg_dsn,
            num_batches=args.batches,
            batch_size=min(args.batch_size, 5000),
            flush_every=args.flush_every,
        ))
    else:
        print(f"[shadow_collect_rf] Modo REALTIME | símbolo={symbol} | ticks={args.ticks}")
        total = asyncio.run(collect_realtime(
            ws_url=cfg.ws_url,
            token=cfg.token,
            symbol=symbol,
            tick_count=cfg.tick_count,
            pg_dsn=args.pg_dsn,
            ticks_to_collect=args.ticks,
            flush_every=args.flush_every,
        ))

    print(f"Total: {total} linhas em shadow_ticks_rf")


if __name__ == "__main__":
    main()
