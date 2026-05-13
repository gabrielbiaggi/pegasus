from __future__ import annotations

import argparse
import csv
import json
import logging
from itertools import product
from pathlib import Path
from typing import Any

from backtest import load_candles, run_backtest
from logger import logger


def parse_int_range(value: str) -> list[int]:
    if "," in value:
        return [int(item.strip()) for item in value.split(",") if item.strip()]

    parts = [int(part) for part in value.split(":")]
    if len(parts) == 2:
        start, stop = parts
        step = 1
    elif len(parts) == 3:
        start, stop, step = parts
    else:
        raise ValueError(f"Range invalido: {value}")

    if step <= 0:
        raise ValueError("Step precisa ser maior que zero.")
    return list(range(start, stop + 1, step))


def run_grid(
    candles: list[dict[str, Any]],
    min_scores: list[int],
    duration_candles: list[int],
    cooldown_candles: list[int],
    initial_balance: float,
    stake: float,
    payout: float,
    min_trades: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for min_score, duration, cooldown in product(min_scores, duration_candles, cooldown_candles):
        result = run_backtest(
            candles=candles,
            min_score=min_score,
            initial_balance=initial_balance,
            stake=stake,
            duration_candles=duration,
            payout=payout,
            cooldown_candles=cooldown,
        )
        if result["total_trades"] < min_trades:
            continue

        rows.append(
            {
                "min_score": min_score,
                "duration_candles": duration,
                "cooldown_candles": cooldown,
                "total_trades": result["total_trades"],
                "wins": result["wins"],
                "losses": result["losses"],
                "winrate": result["winrate"],
                "ending_balance": result["ending_balance"],
                "net_profit": result["net_profit"],
                "max_drawdown_pct": result["max_drawdown_pct"],
                "max_loss_streak": result["max_loss_streak"],
            }
        )

    rows.sort(
        key=lambda row: (
            row["net_profit"],
            row["winrate"],
            -row["max_drawdown_pct"],
            -row["max_loss_streak"],
        ),
        reverse=True,
    )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Otimiza parametros do Pegasus usando backtest em grade.")
    parser.add_argument("--candles", required=True, type=Path, help="CSV/JSON com epoch,open,high,low,close.")
    parser.add_argument("--min-scores", default="5:8", help="Ex: 5:8, 4:10:2 ou 5,6,7.")
    parser.add_argument("--durations", default="3:8", help="Duracao em candles. Ex: 3:8 ou 3,5,10.")
    parser.add_argument("--cooldowns", default="0:3", help="Cooldown em candles. Ex: 0:3 ou 0,1,2.")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--payout", type=float, default=0.85)
    parser.add_argument("--min-trades", type=int, default=10)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--output", type=Path, default=Path("logs/optimization.csv"))
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    candles = load_candles(args.candles)
    rows = run_grid(
        candles=candles,
        min_scores=parse_int_range(args.min_scores),
        duration_candles=parse_int_range(args.durations),
        cooldown_candles=parse_int_range(args.cooldowns),
        initial_balance=args.initial_balance,
        stake=args.stake,
        payout=args.payout,
        min_trades=args.min_trades,
    )

    write_csv(args.output, rows)
    print(json.dumps(rows[: args.top], indent=2))


if __name__ == "__main__":
    main()
