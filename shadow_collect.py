from __future__ import annotations

import argparse
import asyncio
import csv
import json
from collections import deque
from pathlib import Path
from typing import Any

import websockets

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
    "future_result",
    "future_exit_epoch",
    "future_exit_quote",
    "future_held_ticks",
    "future_max_move_percent",
]


def _metric(row: Any, name: str) -> float | str:
    try:
        value = float(row.get(name))
    except (TypeError, ValueError):
        return ""
    if value != value:
        return ""
    return round(value, 8)


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


async def collect_shadow_rows(output: Path, ticks_to_collect: int, flush_every: int) -> int:
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
                        },
                    }
                )

            while pending:
                item = pending[0]
                result = _future_result(
                    all_ticks,
                    int(item["entry_index"]),
                    bot_config.accumulator_growth_rate,
                    bot_config.accumulator_take_profit_percent,
                    barrier_percent=0.05,
                    max_hold_ticks=bot_config.accumulator_max_hold_ticks,
                )
                if result is None:
                    break
                pending.popleft()
                rows.append({**item["row"], **result})

            if len(rows) >= flush_every:
                _write_rows(output, rows)
                written += len(rows)
                rows.clear()

    _write_rows(output, rows)
    return written + len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Coleta ticks demo e grava dataset shadow com metricas e resultado futuro.")
    parser.add_argument("--ticks", type=int, default=600)
    parser.add_argument("--flush-every", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("data/shadow_ticks.csv"))
    args = parser.parse_args()

    total = asyncio.run(collect_shadow_rows(args.output, args.ticks, args.flush_every))
    print(f"{total} linhas shadow salvas em {args.output}")


if __name__ == "__main__":
    main()
