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
from strategy import StrategyConfig, calculate_indicators, generate_signal


def load_candles(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        candles = data["candles"] if isinstance(data, dict) and "candles" in data else data
        return list(candles)

    df = pd.read_csv(path)
    required = {"epoch", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Arquivo sem colunas obrigatorias: {sorted(missing)}")
    return df[list(required)].to_dict("records")


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


def run_backtest(
    candles: list[dict[str, Any]],
    min_score: int,
    initial_balance: float,
    stake: float,
    duration_candles: int,
    payout: float,
    cooldown_candles: int,
    strategy_config: StrategyConfig | None = None,
    blocked_utc_hours: tuple[int, ...] = (),
) -> dict[str, Any]:
    strategy_config = strategy_config or StrategyConfig(
        min_score=min_score,
        use_trend_filter=False,
        use_atr_filter=False,
    )
    balance = initial_balance
    equity_curve = [balance]
    trades: list[dict[str, Any]] = []
    loss_streak = 0
    max_loss_streak = 0
    i = strategy_config.minimum_candles
    df = calculate_indicators(candles, config=strategy_config)

    while i < len(candles) - duration_candles:
        entry_epoch = int(candles[i]["epoch"])
        if datetime.fromtimestamp(entry_epoch, UTC).hour in blocked_utc_hours:
            i += 1
            continue

        signal, score = generate_signal(df.iloc[: i + 1], min_score=min_score, config=strategy_config)

        if not signal:
            i += 1
            continue

        entry = float(candles[i]["close"])
        exit_price = float(candles[i + duration_candles]["close"])
        won = exit_price > entry if signal == "CALL" else exit_price < entry
        profit = round(stake * payout, 2) if won else -stake
        balance = round(balance + profit, 2)
        equity_curve.append(balance)

        if won:
            loss_streak = 0
        else:
            loss_streak += 1
            max_loss_streak = max(max_loss_streak, loss_streak)

        trades.append(
            {
                "entry_epoch": entry_epoch,
                "exit_epoch": int(candles[i + duration_candles]["epoch"]),
                "direction": signal,
                "score": score,
                "entry": entry,
                "exit": exit_price,
                "profit": profit,
                "result": "WIN" if won else "LOSS",
                "balance": balance,
            }
        )
        i += duration_candles + cooldown_candles

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
    parser = argparse.ArgumentParser(description="Backtest simples da estrategia do Pegasus.")
    parser.add_argument("--candles", required=True, type=Path, help="CSV/JSON com epoch,open,high,low,close.")
    parser.add_argument("--min-score", type=int, default=5)
    parser.add_argument("--use-trend-filter", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--trend-ema-window", type=int, default=200)
    parser.add_argument("--use-atr-filter", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--atr-window", type=int, default=14)
    parser.add_argument("--min-atr-percent", type=float, default=0.05)
    parser.add_argument("--rsi-extreme-weight", type=int, default=3)
    parser.add_argument("--rsi-soft-weight", type=int, default=1)
    parser.add_argument("--macd-cross-weight", type=int, default=3)
    parser.add_argument("--bollinger-touch-weight", type=int, default=2)
    parser.add_argument("--ema-cross-weight", type=int, default=2)
    parser.add_argument("--blocked-utc-hours", default="", help="Ex: 0,1,22-23.")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--duration-candles", type=int, default=5)
    parser.add_argument("--payout", type=float, default=0.85)
    parser.add_argument("--cooldown-candles", type=int, default=1)
    parser.add_argument("--output", type=Path, default=None, help="CSV opcional com cada trade simulado.")
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    candles = load_candles(args.candles)
    strategy_config = StrategyConfig(
        min_score=args.min_score,
        use_trend_filter=args.use_trend_filter,
        trend_ema_window=args.trend_ema_window,
        use_atr_filter=args.use_atr_filter,
        atr_window=args.atr_window,
        min_atr_percent=args.min_atr_percent,
        rsi_extreme_weight=args.rsi_extreme_weight,
        rsi_soft_weight=args.rsi_soft_weight,
        macd_cross_weight=args.macd_cross_weight,
        bollinger_touch_weight=args.bollinger_touch_weight,
        ema_cross_weight=args.ema_cross_weight,
    )
    result = run_backtest(
        candles=candles,
        min_score=args.min_score,
        initial_balance=args.initial_balance,
        stake=args.stake,
        duration_candles=args.duration_candles,
        payout=args.payout,
        cooldown_candles=args.cooldown_candles,
        strategy_config=strategy_config,
        blocked_utc_hours=parse_blocked_hours(args.blocked_utc_hours),
    )

    if args.output:
        write_trades(args.output, result["trades"])

    printable = {key: value for key, value in result.items() if key != "trades"}
    print(json.dumps(printable, indent=2))


if __name__ == "__main__":
    main()
