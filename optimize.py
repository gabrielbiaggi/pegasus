from __future__ import annotations

import argparse
import csv
import json
import logging
from itertools import product
from pathlib import Path
from typing import Any

from backtest import load_ticks, normalize_ticks, parse_blocked_hours, run_accumulator_backtest
from logger import logger
from strategy import AccumulatorStrategyConfig, calculate_tick_indicators


def parse_int_range(value: str) -> list[int]:
    if "," in value:
        return [int(item.strip()) for item in value.split(",") if item.strip()]

    parts = [int(part) for part in value.split(":")]
    if len(parts) == 1:
        return parts
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


def parse_float_values(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def run_grid(
    ticks: list[dict[str, Any]],
    min_scores: list[int],
    bb_width_percents: list[float],
    tick_atr_percents: list[float],
    recent_move_percents: list[float],
    max_hurst_exponents: list[float],
    max_pmi_distance_percents: list[float],
    max_hawkes_intensities: list[float],
    max_abs_tick_imbalances: list[int],
    take_profit_percents: list[float],
    max_hold_ticks_values: list[int],
    cooldown_ticks_values: list[int],
    growth_rate: float,
    barrier_percent: float,
    blocked_utc_hours: tuple[int, ...],
    initial_balance: float,
    stake: float,
    min_trades: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    normalized_ticks = normalize_ticks(ticks)
    indicator_frame = calculate_tick_indicators(normalized_ticks, config=AccumulatorStrategyConfig())

    for (
        min_score,
        bb_width_percent,
        tick_atr_percent,
        recent_move_percent,
        max_hurst_exponent,
        max_pmi_distance_percent,
        max_hawkes_intensity,
        max_abs_tick_imbalance,
        take_profit_percent,
        max_hold_ticks,
        cooldown_ticks,
    ) in product(
        min_scores,
        bb_width_percents,
        tick_atr_percents,
        recent_move_percents,
        max_hurst_exponents,
        max_pmi_distance_percents,
        max_hawkes_intensities,
        max_abs_tick_imbalances,
        take_profit_percents,
        max_hold_ticks_values,
        cooldown_ticks_values,
    ):
        strategy_config = AccumulatorStrategyConfig(
            min_score=min_score,
            max_bb_width_percent=bb_width_percent,
            max_tick_atr_percent=tick_atr_percent,
            max_recent_move_percent=recent_move_percent,
            max_hurst_exponent=max_hurst_exponent,
            max_pmi_distance_percent=max_pmi_distance_percent,
            max_hawkes_intensity=max_hawkes_intensity,
            max_abs_tick_imbalance=max_abs_tick_imbalance,
        )
        result = run_accumulator_backtest(
            ticks=normalized_ticks,
            initial_balance=initial_balance,
            stake=stake,
            growth_rate=growth_rate,
            take_profit_percent=take_profit_percent,
            barrier_percent=barrier_percent,
            max_hold_ticks=max_hold_ticks,
            cooldown_ticks=cooldown_ticks,
            strategy_config=strategy_config,
            blocked_utc_hours=blocked_utc_hours,
            indicator_frame=indicator_frame,
        )
        if result["total_trades"] < min_trades:
            continue

        rows.append(
            {
                "min_score": min_score,
                "max_bb_width_percent": bb_width_percent,
                "max_tick_atr_percent": tick_atr_percent,
                "max_recent_move_percent": recent_move_percent,
                "max_hurst_exponent": max_hurst_exponent,
                "max_pmi_distance_percent": max_pmi_distance_percent,
                "max_hawkes_intensity": max_hawkes_intensity,
                "max_abs_tick_imbalance": max_abs_tick_imbalance,
                "growth_rate": growth_rate,
                "take_profit_percent": take_profit_percent,
                "barrier_percent": barrier_percent,
                "max_hold_ticks": max_hold_ticks,
                "cooldown_ticks": cooldown_ticks,
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
            row["total_trades"],
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
    parser = argparse.ArgumentParser(description="Otimiza Pegasus Accumulators 1s usando backtest em ticks.")
    parser.add_argument("--ticks", "--data", dest="ticks", required=True, type=Path, help="CSV/JSON com epoch,quote.")
    parser.add_argument("--min-scores", default="7:10", help="Ex: 7:10, 6:10:2 ou 7,8,9.")
    parser.add_argument("--bb-width-percents", default="0.04,0.06,0.08,0.10")
    parser.add_argument("--tick-atr-percents", default="0.008,0.01,0.015,0.02")
    parser.add_argument("--recent-move-percents", default="0.02,0.03,0.05")
    parser.add_argument("--max-hurst-exponents", default="0.45,0.50,0.55,0.60")
    parser.add_argument("--max-pmi-distance-percents", default="0.005,0.01,0.02,0.05")
    parser.add_argument("--max-hawkes-intensities", default="0.2,0.5,1.0")
    parser.add_argument("--max-abs-tick-imbalances", default="2,3,4")
    parser.add_argument("--take-profit-percents", default="3,4,5")
    parser.add_argument("--max-hold-ticks", default="3:8")
    parser.add_argument("--cooldown-ticks", default="0:5")
    parser.add_argument("--growth-rate", type=float, default=0.03)
    parser.add_argument("--barrier-percent", type=float, default=0.05)
    parser.add_argument("--blocked-utc-hours", default="", help="Ex: 0,1,22-23.")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--min-trades", type=int, default=10)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--output", type=Path, default=Path("logs/accumulator_optimization.csv"))
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    ticks = load_ticks(args.ticks)
    rows = run_grid(
        ticks=ticks,
        min_scores=parse_int_range(args.min_scores),
        bb_width_percents=parse_float_values(args.bb_width_percents),
        tick_atr_percents=parse_float_values(args.tick_atr_percents),
        recent_move_percents=parse_float_values(args.recent_move_percents),
        max_hurst_exponents=parse_float_values(args.max_hurst_exponents),
        max_pmi_distance_percents=parse_float_values(args.max_pmi_distance_percents),
        max_hawkes_intensities=parse_float_values(args.max_hawkes_intensities),
        max_abs_tick_imbalances=parse_int_range(args.max_abs_tick_imbalances),
        take_profit_percents=parse_float_values(args.take_profit_percents),
        max_hold_ticks_values=parse_int_range(args.max_hold_ticks),
        cooldown_ticks_values=parse_int_range(args.cooldown_ticks),
        growth_rate=args.growth_rate,
        barrier_percent=args.barrier_percent,
        blocked_utc_hours=parse_blocked_hours(args.blocked_utc_hours),
        initial_balance=args.initial_balance,
        stake=args.stake,
        min_trades=args.min_trades,
    )

    write_csv(args.output, rows)
    print(json.dumps(rows[: args.top], indent=2))


if __name__ == "__main__":
    main()
