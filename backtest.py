from __future__ import annotations

import argparse
import csv
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from logger import logger
from strategy import AccumulatorStrategyConfig, accumulator_quant_filters_pass, calculate_tick_indicators, score_accumulator_row


def load_ticks(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "history" in data:
            history = data["history"]
            return [
                {"epoch": int(epoch), "quote": float(quote)}
                for epoch, quote in zip(history.get("times", []), history.get("prices", []))
            ]
        rows = data["ticks"] if isinstance(data, dict) and "ticks" in data else data
        return [{"epoch": int(row["epoch"]), "quote": float(row["quote"])} for row in rows]

    df = pd.read_csv(path)
    required = {"epoch", "quote"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Arquivo sem colunas obrigatorias para ticks: {sorted(missing)}")
    return [
        {"epoch": int(row["epoch"]), "quote": float(row["quote"])}
        for row in df[list(required)].to_dict("records")
    ]


def normalize_ticks(ticks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [{"epoch": int(tick["epoch"]), "quote": float(tick["quote"])} for tick in ticks],
        key=lambda tick: tick["epoch"],
    )


def parse_blocked_hours(value: str) -> tuple[int, ...]:
    if not value:
        return ()

    hours: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = [int(item.strip()) for item in part.split("-", 1)]
            hours.update(range(start, end + 1))
        else:
            hours.add(int(part))

    invalid = [hour for hour in hours if hour < 0 or hour > 23]
    if invalid:
        raise ValueError(f"Horas UTC invalidas: {invalid}")
    return tuple(sorted(hours))


def max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0] if equity_curve else 0.0
    worst = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            worst = max(worst, (peak - equity) / peak)
    return worst


def _clean_metric(value: Any) -> float | None:
    try:
        metric = float(value)
    except (TypeError, ValueError):
        return None
    if metric != metric:
        return None
    return metric


def simulate_accumulator_trade(
    ticks: list[dict[str, Any]],
    entry_index: int,
    stake: float,
    growth_rate: float,
    take_profit_percent: float,
    barrier_percent: float,
    max_hold_ticks: int,
) -> dict[str, Any]:
    entry = ticks[entry_index]
    entry_quote = float(entry["quote"])
    target_profit = stake * take_profit_percent / 100
    max_exit_index = min(entry_index + max_hold_ticks, len(ticks) - 1)
    value = stake
    max_move_percent = 0.0

    for exit_index in range(entry_index + 1, max_exit_index + 1):
        quote = float(ticks[exit_index]["quote"])
        move_percent = abs((quote - entry_quote) / entry_quote * 100)
        max_move_percent = max(max_move_percent, move_percent)

        if move_percent >= barrier_percent:
            return {
                "exit_index": exit_index,
                "exit_epoch": int(ticks[exit_index]["epoch"]),
                "exit_quote": quote,
                "held_ticks": exit_index - entry_index,
                "profit": -stake,
                "result": "LOSS",
                "exit_reason": "barrier",
                "max_adverse_move_percent": max_move_percent,
            }

        value *= 1 + growth_rate
        profit = round(value - stake, 2)
        if profit >= target_profit:
            return {
                "exit_index": exit_index,
                "exit_epoch": int(ticks[exit_index]["epoch"]),
                "exit_quote": quote,
                "held_ticks": exit_index - entry_index,
                "profit": profit,
                "result": "WIN",
                "exit_reason": "take_profit",
                "max_adverse_move_percent": max_move_percent,
            }

    exit_quote = float(ticks[max_exit_index]["quote"])
    profit = round(value - stake, 2)
    return {
        "exit_index": max_exit_index,
        "exit_epoch": int(ticks[max_exit_index]["epoch"]),
        "exit_quote": exit_quote,
        "held_ticks": max_exit_index - entry_index,
        "profit": profit,
        "result": "WIN" if profit > 0 else "LOSS",
        "exit_reason": "max_hold_ticks",
        "max_adverse_move_percent": max_move_percent,
    }


def run_accumulator_backtest(
    ticks: list[dict[str, Any]],
    initial_balance: float,
    stake: float,
    growth_rate: float,
    take_profit_percent: float,
    barrier_percent: float,
    max_hold_ticks: int,
    cooldown_ticks: int,
    strategy_config: AccumulatorStrategyConfig | None = None,
    blocked_utc_hours: tuple[int, ...] = (),
    indicator_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    strategy_config = strategy_config or AccumulatorStrategyConfig()
    normalized_ticks = normalize_ticks(ticks)
    df = indicator_frame if indicator_frame is not None else calculate_tick_indicators(normalized_ticks, config=strategy_config)

    if len(normalized_ticks) < strategy_config.minimum_ticks:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "winrate": 0.0,
            "ending_balance": round(initial_balance, 2),
            "net_profit": 0.0,
            "max_drawdown_pct": 0.0,
            "max_loss_streak": 0,
            "trades": [],
        }

    epochs = [int(tick["epoch"]) for tick in normalized_ticks]
    bb_widths = df["bb_width_percent"].to_numpy()
    tick_atrs = df["tick_atr_percent"].to_numpy()
    recent_moves = df["recent_move_percent"].to_numpy()

    balance = initial_balance
    equity_curve = [balance]
    trades: list[dict[str, Any]] = []
    loss_streak = 0
    max_loss_streak = 0
    i = strategy_config.minimum_ticks - 1

    while i < len(normalized_ticks) - 1:
        entry_epoch = epochs[i]
        if datetime.fromtimestamp(entry_epoch, UTC).hour in blocked_utc_hours:
            i += 1
            continue

        bb_width = _clean_metric(bb_widths[i])
        tick_atr = _clean_metric(tick_atrs[i])
        recent_move = _clean_metric(recent_moves[i])
        if bb_width is None or tick_atr is None or recent_move is None:
            i += 1
            continue

        row = df.iloc[i]
        score = score_accumulator_row(row, strategy_config)
        quant_pass, _ = accumulator_quant_filters_pass(row, strategy_config)
        if score < strategy_config.min_score or not quant_pass:
            i += 1
            continue

        simulated = simulate_accumulator_trade(
            ticks=normalized_ticks,
            entry_index=i,
            stake=stake,
            growth_rate=growth_rate,
            take_profit_percent=take_profit_percent,
            barrier_percent=barrier_percent,
            max_hold_ticks=max_hold_ticks,
        )
        profit = float(simulated["profit"])
        balance = round(balance + profit, 2)
        equity_curve.append(balance)

        if profit > 0:
            loss_streak = 0
        else:
            loss_streak += 1
            max_loss_streak = max(max_loss_streak, loss_streak)

        trades.append(
            {
                "entry_epoch": entry_epoch,
                "exit_epoch": simulated["exit_epoch"],
                "direction": "ACCU",
                "score": score,
                "stake": stake,
                "growth_rate": growth_rate,
                "take_profit_percent": take_profit_percent,
                "barrier_percent": barrier_percent,
                "max_hold_ticks": max_hold_ticks,
                "held_ticks": simulated["held_ticks"],
                "entry_quote": normalized_ticks[i]["quote"],
                "exit_quote": simulated["exit_quote"],
                "bb_width_percent": bb_width,
                "tick_atr_percent": tick_atr,
                "recent_move_percent": recent_move,
                "max_adverse_move_percent": simulated["max_adverse_move_percent"],
                "profit": profit,
                "result": simulated["result"],
                "exit_reason": simulated["exit_reason"],
                "balance": balance,
            }
        )
        i = int(simulated["exit_index"]) + cooldown_ticks + 1

    wins = sum(1 for trade in trades if trade["profit"] > 0)
    losses = len(trades) - wins
    winrate = (wins / len(trades) * 100) if trades else 0.0

    return {
        "total_trades": len(trades),
        "wins": wins,
        "losses": losses,
        "winrate": round(winrate, 2),
        "ending_balance": round(balance, 2),
        "net_profit": round(balance - initial_balance, 2),
        "max_drawdown_pct": round(max_drawdown(equity_curve) * 100, 2),
        "max_loss_streak": max_loss_streak,
        "trades": trades,
    }


def write_trades(path: Path, trades: list[dict[str, Any]]) -> None:
    if not trades:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(trades[0].keys()))
        writer.writeheader()
        writer.writerows(trades)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest de Accumulators 1s do Pegasus usando ticks.")
    parser.add_argument("--ticks", "--data", dest="ticks", required=True, type=Path, help="CSV/JSON com epoch,quote.")
    parser.add_argument("--blocked-utc-hours", default="", help="Ex: 0,1,22-23.")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--growth-rate", type=float, default=0.03)
    parser.add_argument("--take-profit-percent", type=float, default=3.0)
    parser.add_argument("--barrier-percent", type=float, default=0.05)
    parser.add_argument("--max-hold-ticks", type=int, default=8)
    parser.add_argument("--cooldown-ticks", type=int, default=3)
    parser.add_argument("--min-score", type=int, default=7)
    parser.add_argument("--bb-window", type=int, default=20)
    parser.add_argument("--bb-std-dev", type=float, default=2.0)
    parser.add_argument("--max-bb-width-percent", type=float, default=0.08)
    parser.add_argument("--atr-window", type=int, default=20)
    parser.add_argument("--max-tick-atr-percent", type=float, default=0.015)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--max-recent-move-percent", type=float, default=0.05)
    parser.add_argument("--output", type=Path, default=None, help="CSV opcional com cada trade simulado.")
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    ticks = load_ticks(args.ticks)
    strategy_config = AccumulatorStrategyConfig(
        min_score=args.min_score,
        bb_window=args.bb_window,
        bb_std_dev=args.bb_std_dev,
        max_bb_width_percent=args.max_bb_width_percent,
        atr_window=args.atr_window,
        max_tick_atr_percent=args.max_tick_atr_percent,
        recent_window=args.recent_window,
        max_recent_move_percent=args.max_recent_move_percent,
    )
    result = run_accumulator_backtest(
        ticks=ticks,
        initial_balance=args.initial_balance,
        stake=args.stake,
        growth_rate=args.growth_rate,
        take_profit_percent=args.take_profit_percent,
        barrier_percent=args.barrier_percent,
        max_hold_ticks=args.max_hold_ticks,
        cooldown_ticks=args.cooldown_ticks,
        strategy_config=strategy_config,
        blocked_utc_hours=parse_blocked_hours(args.blocked_utc_hours),
    )

    if args.output:
        write_trades(args.output, result["trades"])

    printable = {key: value for key, value in result.items() if key != "trades"}
    print(json.dumps(printable, indent=2))


if __name__ == "__main__":
    main()
