from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
from collections import deque
from pathlib import Path
from typing import Any

import websockets

# PostgreSQL sink (optional) — instale psycopg2-binary para activar
try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

from config import load_config
from strategy import (
    AccumulatorStrategyConfig,
    accumulator_quant_filters_pass,
    calculate_tick_indicators,
    score_accumulator_row,
)


FIELDS = [
    "entry_epoch",
    "entry_quote",
    "score",
    "signal",
    "block_reason",
    "bb_width_percent",
    "tick_atr_percent",
    "recent_move_percent",
    "hurst_exponent",
    "tick_imbalance",
    "hawkes_intensity",
    "velocity_zscore",
    "acceleration_zscore",
    "pmi_distance_percent",
    "markov_p_up_given_up",
    "markov_p_down_given_down",
    "shannon_entropy",
    "kalman_residual_zscore",
    "future_result",
    "future_exit_epoch",
    "future_exit_quote",
    "future_held_ticks",
    "future_max_move_percent",
    # Regression targets for WFO/SHAP analysis
    "y1_max_drawdown_5ticks",  # max adverse move % in next 5 ticks
    "y2_seconds_to_3pct",      # seconds until price moves >=3%; -1 if never within max_hold
]


def _metric(row: Any, name: str) -> float | str:
    try:
        value = float(row.get(name))
    except (TypeError, ValueError):
        return ""
    if value != value:
        return ""
    return round(value, 8)


def _y1_max_drawdown_5ticks(
    ticks: list[dict[str, Any]],
    entry_index: int,
) -> float | None:
    """Return max adverse % move in the next 5 ticks after entry_index."""
    lookahead = 5
    if len(ticks) <= entry_index + lookahead:
        return None
    entry_quote = float(ticks[entry_index]["quote"])
    max_move = 0.0
    for i in range(entry_index + 1, entry_index + lookahead + 1):
        move = abs(float(ticks[i]["quote"]) - entry_quote) / entry_quote * 100
        max_move = max(max_move, move)
    return round(max_move, 8)


def _y2_seconds_to_3pct(
    ticks: list[dict[str, Any]],
    entry_index: int,
    barrier_percent: float,
    max_hold_ticks: int,
) -> float | None:
    """Return seconds until price moves >= barrier_percent from entry.

    Returns -1.0 if barrier is never touched within max_hold_ticks.
    Returns None if there are not enough ticks yet to resolve.
    """
    if len(ticks) <= entry_index + max_hold_ticks:
        return None
    entry_quote = float(ticks[entry_index]["quote"])
    entry_epoch = float(ticks[entry_index]["epoch"])
    for i in range(entry_index + 1, entry_index + max_hold_ticks + 1):
        move = abs(float(ticks[i]["quote"]) - entry_quote) / entry_quote * 100
        if move >= barrier_percent:
            return round(float(ticks[i]["epoch"]) - entry_epoch, 4)
    return -1.0


def _future_result(
    ticks: list[dict[str, Any]],
    entry_index: int,
    growth_rate: float,
    take_profit_percent: float,
    barrier_percent: float,
    max_hold_ticks: int,
) -> dict[str, Any] | None:
    if len(ticks) <= entry_index + max_hold_ticks:
        return None

    entry_quote = float(ticks[entry_index]["quote"])
    value = 1.0
    target_profit = take_profit_percent / 100
    max_move_percent = 0.0

    for index in range(entry_index + 1, entry_index + max_hold_ticks + 1):
        quote = float(ticks[index]["quote"])
        move_percent = abs((quote - entry_quote) / entry_quote * 100)
        max_move_percent = max(max_move_percent, move_percent)
        if move_percent >= barrier_percent:
            return {
                "future_result": "LOSS",
                "future_exit_epoch": int(ticks[index]["epoch"]),
                "future_exit_quote": quote,
                "future_held_ticks": index - entry_index,
                "future_max_move_percent": round(max_move_percent, 8),
            }

        value *= 1 + growth_rate
        if value - 1.0 >= target_profit:
            return {
                "future_result": "WIN",
                "future_exit_epoch": int(ticks[index]["epoch"]),
                "future_exit_quote": quote,
                "future_held_ticks": index - entry_index,
                "future_max_move_percent": round(max_move_percent, 8),
            }

    exit_index = entry_index + max_hold_ticks
    return {
        "future_result": "TIME",
        "future_exit_epoch": int(ticks[exit_index]["epoch"]),
        "future_exit_quote": float(ticks[exit_index]["quote"]),
        "future_held_ticks": max_hold_ticks,
        "future_max_move_percent": round(max_move_percent, 8),
    }


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in FIELDS} for row in rows)


# ---------------------------------------------------------------------------
# PostgreSQL sink (opcional) — ativa quando PG_DSN está definido no .env
# ---------------------------------------------------------------------------

_PG_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS shadow_ticks (
    id                        BIGSERIAL PRIMARY KEY,
    entry_epoch               BIGINT,
    entry_quote               DOUBLE PRECISION,
    score                     DOUBLE PRECISION,
    signal                    SMALLINT,
    block_reason              TEXT,
    bb_width_percent          DOUBLE PRECISION,
    tick_atr_percent          DOUBLE PRECISION,
    recent_move_percent       DOUBLE PRECISION,
    hurst_exponent            DOUBLE PRECISION,
    tick_imbalance            DOUBLE PRECISION,
    hawkes_intensity          DOUBLE PRECISION,
    velocity_zscore           DOUBLE PRECISION,
    acceleration_zscore       DOUBLE PRECISION,
    pmi_distance_percent      DOUBLE PRECISION,
    markov_p_up_given_up      DOUBLE PRECISION,
    markov_p_down_given_down  DOUBLE PRECISION,
    shannon_entropy           DOUBLE PRECISION,
    kalman_residual_zscore    DOUBLE PRECISION,
    future_result             TEXT,
    future_exit_epoch         BIGINT,
    future_exit_quote         DOUBLE PRECISION,
    future_held_ticks         INTEGER,
    future_max_move_percent   DOUBLE PRECISION,
    y1_max_drawdown_5ticks    DOUBLE PRECISION,
    y2_seconds_to_3pct        DOUBLE PRECISION,
    inserted_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shadow_ticks_entry_epoch_idx ON shadow_ticks (entry_epoch);
"""


def _pg_write_rows(pg_dsn: str, rows: list[dict[str, Any]]) -> None:
    """Insert rows into the shadow_ticks PostgreSQL table.

    The table is created automatically on first call.
    Silently skips if psycopg2 is not installed.
    """
    if not _HAS_PSYCOPG2 or not rows:
        return
    cols = [f for f in FIELDS]  # same order as CSV, no id/inserted_at
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = (
        f"INSERT INTO shadow_ticks ({', '.join(cols)}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(_PG_CREATE_TABLE)
                values = [
                    tuple(row.get(col, None) or None for col in cols)
                    for row in rows
                ]
                psycopg2.extras.execute_batch(cur, insert_sql, values, page_size=500)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        # PostgreSQL is optional — never crash the collector
        print(f"[shadow_collect] PG write error (non-fatal): {exc}")


async def collect_shadow_rows(output: Path, ticks_to_collect: int, flush_every: int, pg_dsn: str = "") -> int:
    bot_config = load_config()
    strategy_config = bot_config.accumulator_strategy_config
    buffer: deque[dict[str, Any]] = deque(maxlen=bot_config.tick_count + bot_config.accumulator_max_hold_ticks + 10)
    all_ticks: list[dict[str, Any]] = []
    pending: deque[dict[str, Any]] = deque()
    rows: list[dict[str, Any]] = []
    written = 0

    async with websockets.connect(bot_config.ws_url, ping_interval=None, open_timeout=10) as ws:
        await ws.send(json.dumps({"authorize": bot_config.token}))
        auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if auth.get("error"):
            raise RuntimeError(auth["error"])
        loginid = str(auth["authorize"].get("loginid", ""))
        if not loginid.upper().startswith("VRTC"):
            raise RuntimeError(f"Coleta bloqueada: token autorizou conta nao-demo {loginid}.")

        await ws.send(
            json.dumps(
                {
                    "ticks_history": bot_config.symbol,
                    "count": bot_config.tick_count,
                    "end": "latest",
                    "style": "ticks",
                }
            )
        )
        history_message = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        history = history_message.get("history", {})
        for epoch, quote in zip(history.get("times", []), history.get("prices", [])):
            normalized = {"epoch": int(epoch), "quote": float(quote)}
            buffer.append(normalized)
            all_ticks.append(normalized)

        await ws.send(json.dumps({"ticks": bot_config.symbol, "subscribe": 1}))
        collected = 0
        while collected < ticks_to_collect:
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            if message.get("error"):
                raise RuntimeError(message["error"])
            if message.get("msg_type") != "tick":
                continue

            tick = message["tick"]
            epoch = int(tick["epoch"])
            if buffer and int(buffer[-1]["epoch"]) == epoch:
                continue
            normalized = {"epoch": epoch, "quote": float(tick["quote"])}
            buffer.append(normalized)
            all_ticks.append(normalized)
            collected += 1

            tick_list = list(buffer)
            df = calculate_tick_indicators(tick_list, config=strategy_config)
            if len(df) >= strategy_config.minimum_ticks:
                last = df.iloc[-1]
                score = score_accumulator_row(last, strategy_config)
                quant_pass, reason = accumulator_quant_filters_pass(last, strategy_config)
                signal = score >= strategy_config.min_score and quant_pass
                pending.append(
                    {
                        "entry_index": len(all_ticks) - 1,
                        "row": {
                            "entry_epoch": epoch,
                            "entry_quote": float(tick["quote"]),
                            "score": score,
                            "signal": int(signal),
                            "block_reason": "ok" if signal else reason,
                            "bb_width_percent": _metric(last, "bb_width_percent"),
                            "tick_atr_percent": _metric(last, "tick_atr_percent"),
                            "recent_move_percent": _metric(last, "recent_move_percent"),
                            "hurst_exponent": _metric(last, "hurst_exponent"),
                            "tick_imbalance": _metric(last, "tick_imbalance"),
                            "hawkes_intensity": _metric(last, "hawkes_intensity"),
                            "velocity_zscore": _metric(last, "velocity_zscore"),
                            "acceleration_zscore": _metric(last, "acceleration_zscore"),
                            "pmi_distance_percent": _metric(last, "pmi_distance_percent"),
                            "markov_p_up_given_up": _metric(last, "markov_p_up_given_up"),
                            "markov_p_down_given_down": _metric(last, "markov_p_down_given_down"),
                            "shannon_entropy": _metric(last, "shannon_entropy"),
                            "kalman_residual_zscore": _metric(last, "kalman_residual_zscore"),
                        },
                    }
                )

            while pending:
                item = pending[0]
                idx = int(item["entry_index"])
                result = _future_result(
                    all_ticks,
                    idx,
                    bot_config.accumulator_growth_rate,
                    bot_config.accumulator_take_profit_percent,
                    barrier_percent=0.05,
                    max_hold_ticks=bot_config.accumulator_max_hold_ticks,
                )
                if result is None:
                    break
                y1 = _y1_max_drawdown_5ticks(all_ticks, idx)
                if y1 is None:
                    break
                y2 = _y2_seconds_to_3pct(
                    all_ticks, idx,
                    barrier_percent=3.0,
                    max_hold_ticks=bot_config.accumulator_max_hold_ticks,
                )
                if y2 is None:
                    break
                pending.popleft()
                rows.append({
                    **item["row"],
                    **result,
                    "y1_max_drawdown_5ticks": y1,
                    "y2_seconds_to_3pct": y2,
                })

            if len(rows) >= flush_every:
                _write_rows(output, rows)
                if pg_dsn:
                    _pg_write_rows(pg_dsn, rows)
                written += len(rows)
                rows.clear()

    _write_rows(output, rows)
    if pg_dsn:
        _pg_write_rows(pg_dsn, rows)
    return written + len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Coleta ticks demo e grava dataset shadow com metricas e resultado futuro.")
    parser.add_argument("--ticks", type=int, default=600)
    parser.add_argument("--flush-every", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("data/shadow_ticks.csv"))
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=os.getenv("PG_DSN", ""),
        help="PostgreSQL DSN (ex: postgresql://user:pass@localhost/pegasus_db). "
             "Também lido de PG_DSN no .env. Opcional.",
    )
    args = parser.parse_args()

    if args.pg_dsn and not _HAS_PSYCOPG2:
        print("⚠️  PG_DSN definido mas psycopg2 não está instalado. Instale: pip install psycopg2-binary")

    total = asyncio.run(collect_shadow_rows(args.output, args.ticks, args.flush_every, args.pg_dsn))
    print(f"{total} linhas shadow salvas em {args.output}")


if __name__ == "__main__":
    main()
