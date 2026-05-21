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
from strategy import AccumulatorStrategyConfig, accumulator_quant_filters_pass, calculate_tick_indicators, generate_calm_accu_signal, score_accumulator_row


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


# ---------------------------------------------------------------------------
# Dynamic stake helpers (mirrors risk_manager.py logic for backtest)
# ---------------------------------------------------------------------------

# Proxy: convert signal score → estimated P(LOSS) for stake multiplier.
# These approximate what the EnsembleScorer would predict for each quality level.
_SCORE_TO_PLOSS: dict[int, float] = {10: 0.08, 9: 0.12, 8: 0.18, 7: 0.22}


def _stake_multiplier(p_loss: float) -> float:
    """Return stake multiplier based on estimated P(LOSS), matching risk_manager."""
    if p_loss < 0.05:
        return 4.0
    if p_loss < 0.10:
        return 3.0
    if p_loss < 0.15:
        return 2.0
    if p_loss < 0.20:
        return 1.5
    if p_loss < 0.25:
        return 1.25
    return 1.0


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
    slippage_ticks: int = 0,
    # --- Opção B: dynamic stake based on balance × pct × multiplier ---
    dynamic_stake: bool = False,
    base_pct: float = 0.02,
    max_stake_abs: float = 500.0,
    max_stake_pct_cap: float = 0.10,
    # --- Opção C: Soros compounding (adds prior WIN profit to next stake) ---
    use_soros_compound: bool = False,
    soros_max_steps: int = 3,
    soros_profit_factor: float = 1.0,
    # --- Opção D: Martingale (recupera perdas anteriores + lucro base pelo payout real) ---
    use_martingale: bool = False,
    martingale_max_gales: int = 3,
    martingale_payout_rate: float = 0.15,
    # --- Skip advanced quant filters (useful for datasets with different distributions) ---
    skip_quant_filters: bool = False,
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
    soros_step = 0
    soros_profit = 0.0
    martingale_step = 0
    martingale_accumulated_loss = 0.0
    martingale_base_stake = 0.0
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
        if score < strategy_config.min_score or (not skip_quant_filters and not quant_pass):
            i += 1
            continue

        # Slippage: signal fires at tick i but execution starts at tick i+slippage_ticks
        execution_index = i + max(0, slippage_ticks)
        if execution_index >= len(normalized_ticks) - 1:
            i += 1
            continue
        _pre_soros_step = soros_step
        _pre_gale_step = martingale_step

        if dynamic_stake:
            base = max(balance * base_pct, stake)
            p_loss_est = _SCORE_TO_PLOSS.get(score, 0.22)
            trade_stake = base * _stake_multiplier(p_loss_est)
            if use_soros_compound and soros_step > 0 and soros_profit > 0:
                trade_stake = trade_stake + soros_profit
        else:
            trade_stake = stake

        if use_martingale and martingale_step > 0 and martingale_base_stake > 0:
            # Fórmula correta: recupera todas as perdas anteriores + lucro original
            # Cap na banca inteira (sem pct_cap)
            trade_stake = round(
                min(martingale_accumulated_loss / martingale_payout_rate + martingale_base_stake, balance),
                2,
            )
        elif dynamic_stake:
            pct_cap = balance * max_stake_pct_cap
            _caps = [max(trade_stake, stake), pct_cap]
            if max_stake_abs > 0:
                _caps.append(max_stake_abs)
            trade_stake = round(min(_caps), 2)

        simulated = simulate_accumulator_trade(
            ticks=normalized_ticks,
            entry_index=execution_index,
            stake=trade_stake,
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
            if use_martingale:
                martingale_step = 0
                martingale_accumulated_loss = 0.0
                martingale_base_stake = 0.0
            if dynamic_stake and use_soros_compound:
                # Mirrors risk_manager.py: replace profit (not accumulate), reset after max_steps wins
                if soros_step < soros_max_steps:
                    soros_step += 1
                    soros_profit = round(profit * soros_profit_factor, 2)
                else:
                    soros_step = 0
                    soros_profit = 0.0
        else:
            loss_streak += 1
            max_loss_streak = max(max_loss_streak, loss_streak)
            if use_martingale:
                if martingale_step == 0:
                    martingale_base_stake = trade_stake
                martingale_accumulated_loss += trade_stake  # LOSS = stake inteiro
                martingale_step = min(martingale_step + 1, martingale_max_gales)
            if dynamic_stake and use_soros_compound:
                soros_step = 0
                soros_profit = 0.0

        trades.append(
            {
                "entry_epoch": entry_epoch,
                "exit_epoch": simulated["exit_epoch"],
                "direction": "ACCU",
                "score": score,
                "soros_step": _pre_soros_step,
                "gale_step": _pre_gale_step,
                "stake": trade_stake,
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


def run_calm_accu_backtest(
    ticks: list[dict[str, Any]],
    initial_balance: float,
    stake: float,
    growth_rate: float,
    take_profit_percent: float,
    barrier_percent: float,
    max_hold_ticks: int,
    cooldown_ticks: int,
    calm_threshold: float = 7.3e-7,
    calm_lookback: int = 10,
    blocked_utc_hours: tuple[int, ...] = (),
    slippage_ticks: int = 0,
    # Soros compounding
    use_soros_compound: bool = False,
    soros_max_steps: int = 3,
    soros_profit_factor: float = 1.0,
    # Martingale
    use_martingale: bool = False,
    martingale_max_gales: int = 3,
    martingale_payout_rate: float = 0.05,
    # Fibonacci martingale
    martingale_mode: str = "classic",
) -> dict[str, Any]:
    """Backtest calm-entry ACCU strategy (BOOM1000).

    Uses rolling avg |return| < calm_threshold as entry filter instead of
    score-based indicators.
    """
    FIB_SEQUENCE = [1, 1, 2, 3, 5, 8, 13, 21]
    normalized_ticks = normalize_ticks(ticks)

    min_ticks = calm_lookback + 1
    if len(normalized_ticks) < min_ticks:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "winrate": 0.0,
            "ending_balance": round(initial_balance, 2), "net_profit": 0.0,
            "max_drawdown_pct": 0.0, "max_loss_streak": 0, "trades": [],
        }

    epochs = [int(t["epoch"]) for t in normalized_ticks]
    prices = [float(t["quote"]) for t in normalized_ticks]

    balance = initial_balance
    equity_curve = [balance]
    trades: list[dict[str, Any]] = []
    loss_streak = 0
    max_loss_streak = 0
    soros_step = 0
    soros_profit = 0.0
    martingale_step = 0
    martingale_accumulated_loss = 0.0
    martingale_base_stake = 0.0
    i = min_ticks

    while i < len(normalized_ticks) - 1:
        entry_epoch = epochs[i]
        if datetime.fromtimestamp(entry_epoch, UTC).hour in blocked_utc_hours:
            i += 1
            continue

        # Calm filter: rolling avg |return| over lookback ticks
        recent = prices[i - calm_lookback: i + 1]
        abs_returns = [abs(recent[j] / recent[j - 1] - 1) for j in range(1, len(recent))]
        avg_abs_ret = sum(abs_returns) / len(abs_returns)

        if avg_abs_ret >= calm_threshold:
            i += 1
            continue

        # Slippage
        execution_index = i + max(0, slippage_ticks)
        if execution_index >= len(normalized_ticks) - 1:
            i += 1
            continue

        _pre_soros_step = soros_step
        _pre_gale_step = martingale_step

        # Determine stake
        trade_stake = stake
        if use_martingale and martingale_step > 0 and martingale_base_stake > 0:
            if martingale_mode == "fibonacci":
                fib_idx = min(martingale_step, len(FIB_SEQUENCE) - 1)
                trade_stake = round(martingale_base_stake * FIB_SEQUENCE[fib_idx], 2)
            else:
                trade_stake = round(
                    min(martingale_accumulated_loss / martingale_payout_rate + martingale_base_stake, balance),
                    2,
                )
        if use_soros_compound and soros_step > 0 and soros_profit > 0 and martingale_step == 0:
            trade_stake = trade_stake + soros_profit

        trade_stake = min(trade_stake, balance)
        if trade_stake <= 0:
            break

        simulated = simulate_accumulator_trade(
            ticks=normalized_ticks,
            entry_index=execution_index,
            stake=trade_stake,
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
            if use_martingale:
                martingale_step = 0
                martingale_accumulated_loss = 0.0
                martingale_base_stake = 0.0
            if use_soros_compound:
                if soros_step < soros_max_steps:
                    soros_step += 1
                    soros_profit = round(profit * soros_profit_factor, 2)
                else:
                    soros_step = 0
                    soros_profit = 0.0
        else:
            loss_streak += 1
            max_loss_streak = max(max_loss_streak, loss_streak)
            if use_martingale:
                if martingale_step == 0:
                    martingale_base_stake = trade_stake
                martingale_accumulated_loss += trade_stake
                martingale_step = min(martingale_step + 1, martingale_max_gales)
            if use_soros_compound:
                soros_step = 0
                soros_profit = 0.0

        trades.append({
            "entry_epoch": entry_epoch,
            "exit_epoch": simulated["exit_epoch"],
            "direction": "CALM_ACCU",
            "avg_abs_ret": avg_abs_ret,
            "soros_step": _pre_soros_step,
            "gale_step": _pre_gale_step,
            "stake": trade_stake,
            "growth_rate": growth_rate,
            "take_profit_percent": take_profit_percent,
            "barrier_percent": barrier_percent,
            "max_hold_ticks": max_hold_ticks,
            "held_ticks": simulated["held_ticks"],
            "entry_quote": prices[i],
            "exit_quote": simulated["exit_quote"],
            "max_adverse_move_percent": simulated["max_adverse_move_percent"],
            "profit": profit,
            "result": simulated["result"],
            "exit_reason": simulated["exit_reason"],
            "balance": balance,
        })
        i = int(simulated["exit_index"]) + cooldown_ticks + 1

    wins = sum(1 for t in trades if t["profit"] > 0)
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
    parser.add_argument(
        "--slippage-ticks",
        type=int,
        default=0,
        help="Numero de ticks de atraso entre sinal (t) e execucao (t+N). Simula latencia da corretora.",
    )
    parser.add_argument("--min-score", type=int, default=7)
    parser.add_argument("--bb-window", type=int, default=20)
    parser.add_argument("--bb-std-dev", type=float, default=2.0)
    parser.add_argument("--max-bb-width-percent", type=float, default=0.08)
    parser.add_argument("--atr-window", type=int, default=20)
    parser.add_argument("--max-tick-atr-percent", type=float, default=0.015)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--max-recent-move-percent", type=float, default=0.05)
    parser.add_argument("--output", type=Path, default=None, help="CSV opcional com cada trade simulado.")
    # Dynamic stake (Opção B / C)
    parser.add_argument("--dynamic-stake", action="store_true", help="Usar stake dinâmico baseado em banca×pct×multiplicador.")
    parser.add_argument("--base-pct", type=float, default=0.02, help="Percentual da banca como base do stake (padrão 0.02=2%%).")
    parser.add_argument("--max-stake-abs", type=float, default=500.0, help="Cap absoluto de stake por trade.")
    parser.add_argument("--max-stake-pct-cap", type=float, default=0.10, help="Cap de stake como %% da banca (padrão 0.10=10%%).")
    parser.add_argument("--use-soros", action="store_true", help="Ativar Soros compounding (adiciona lucro acumulado ao próximo stake).")
    parser.add_argument("--soros-max-steps", type=int, default=3, help="Máximo de passos Soros antes de resetar.")
    parser.add_argument("--use-martingale", action="store_true", help="Ativar Martingale (recupera perdas + lucro pelo payout real).")
    parser.add_argument("--martingale-max-gales", type=int, default=3, help="Máximo de gales no Martingale.")
    parser.add_argument("--martingale-payout-rate", type=float, default=0.15, help="Taxa de payout do Accumulator (0.15 = 15%%).")
    # Comparison mode: runs Baseline vs B vs B+C automatically
    parser.add_argument(
        "--run-all-scenarios",
        action="store_true",
        help="Executa e compara 3 cenários: Baseline, Opção B (dynamic), Opção B+C (dynamic+Soros).",
    )
    parser.add_argument(
        "--skip-quant-filters",
        action="store_true",
        help="Desativa os filtros quantitativos avançados (Hurst, Hawkes, etc.). Útil para datasets com distribuições diferentes.",
    )
    # Calm ACCU mode
    parser.add_argument("--mode", choices=["accumulator", "calm_accu"], default="accumulator", help="Modo de backtest: accumulator (score-based) ou calm_accu (calm-entry BOOM1000).")
    parser.add_argument("--calm-threshold", type=float, default=7.3e-7, help="Calm ACCU: limiar de avg |return| para entrada.")
    parser.add_argument("--calm-lookback", type=int, default=10, help="Calm ACCU: janela de lookback em ticks.")
    parser.add_argument("--martingale-mode", default="classic", choices=["classic", "fibonacci"], help="Modo de martingale: classic ou fibonacci.")
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    ticks = load_ticks(args.ticks)

    # ---- Calm ACCU mode ----
    if args.mode == "calm_accu":
        calm_common: dict[str, Any] = dict(
            ticks=ticks,
            initial_balance=args.initial_balance,
            growth_rate=args.growth_rate,
            barrier_percent=args.barrier_percent,
            cooldown_ticks=args.cooldown_ticks,
            calm_threshold=args.calm_threshold,
            calm_lookback=args.calm_lookback,
            blocked_utc_hours=parse_blocked_hours(args.blocked_utc_hours),
            slippage_ticks=args.slippage_ticks,
            soros_max_steps=args.soros_max_steps,
            martingale_max_gales=args.martingale_max_gales,
            martingale_payout_rate=args.martingale_payout_rate,
        )

        if args.run_all_scenarios:
            scenarios = [
                dict(label="Baseline (fixo)", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, use_soros_compound=False, use_martingale=False, martingale_mode="classic"),
                dict(label="Soros", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, use_soros_compound=True, use_martingale=False, martingale_mode="classic"),
                dict(label="Martingale Classic", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, use_soros_compound=False, use_martingale=True, martingale_mode="classic"),
                dict(label="Soros+Fibo Martingale", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, use_soros_compound=True, use_martingale=True, martingale_mode="fibonacci"),
            ]
            rows = []
            for sc in scenarios:
                sc_label = sc.pop("label")
                r = run_calm_accu_backtest(**calm_common, **sc)
                rows.append({
                    "Cenário": sc_label,
                    "Trades": r["total_trades"],
                    "Wins": r["wins"],
                    "Losses": r["losses"],
                    "Winrate%": f"{r['winrate']:.1f}",
                    "PnL": f"{r['net_profit']:+.2f}",
                    "Banca Final": f"{r['ending_balance']:.2f}",
                    "ROI%": f"{r['net_profit'] / args.initial_balance * 100:+.2f}",
                    "Max DD%": f"{r['max_drawdown_pct']:.2f}",
                    "Max Loss Streak": r["max_loss_streak"],
                })
            if rows:
                keys = list(rows[0].keys())
                widths = [max(len(k), max(len(str(row[k])) for row in rows)) for k in keys]
                header = "  ".join(k.ljust(w) for k, w in zip(keys, widths))
                sep = "  ".join("-" * w for w in widths)
                print(header)
                print(sep)
                for row in rows:
                    print("  ".join(str(row[k]).ljust(w) for k, w in zip(keys, widths)))
            if args.output and rows:
                import csv as _csv
                with args.output.open("w", newline="", encoding="utf-8") as f:
                    w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                    w.writeheader()
                    w.writerows(rows)
                print(f"\nTabela salva em: {args.output}")
            return

        result = run_calm_accu_backtest(
            **calm_common,
            stake=args.stake,
            take_profit_percent=args.take_profit_percent,
            max_hold_ticks=args.max_hold_ticks,
            use_soros_compound=args.use_soros,
            use_martingale=args.use_martingale,
            martingale_mode=args.martingale_mode,
        )
        if args.output:
            write_trades(args.output, result["trades"])
        printable = {key: value for key, value in result.items() if key != "trades"}
        print(json.dumps(printable, indent=2))
        return
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

    common: dict[str, Any] = dict(
        ticks=ticks,
        initial_balance=args.initial_balance,
        growth_rate=args.growth_rate,
        barrier_percent=args.barrier_percent,
        cooldown_ticks=args.cooldown_ticks,
        strategy_config=strategy_config,
        blocked_utc_hours=parse_blocked_hours(args.blocked_utc_hours),
        slippage_ticks=args.slippage_ticks,
        base_pct=args.base_pct,
        max_stake_abs=args.max_stake_abs,
        max_stake_pct_cap=args.max_stake_pct_cap,
        soros_max_steps=args.soros_max_steps,
        martingale_max_gales=args.martingale_max_gales,
        martingale_payout_rate=args.martingale_payout_rate,
        skip_quant_filters=args.skip_quant_filters,
    )

    if args.run_all_scenarios:
        # Pre-compute indicators once and reuse across all 3 scenarios
        normalized = normalize_ticks(ticks)
        indicator_df = calculate_tick_indicators(normalized, config=strategy_config)
        common["indicator_frame"] = indicator_df

        scenarios = [
            # Baseline: fixed stake, TP 3% (reference — conservative)
            dict(label="Baseline (fixo, TP 3%)", stake=args.stake, take_profit_percent=3.0, max_hold_ticks=8, dynamic_stake=False, use_soros_compound=False, use_martingale=False),
            # Opção B: dynamic stake, configurable TP, no Soros
            dict(label=f"Opção B  (dynamic, TP {args.take_profit_percent:.0f}%)", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, dynamic_stake=True, use_soros_compound=False, use_martingale=False),
            # Opção C: dynamic stake + Soros
            dict(label=f"Opção C  (dynamic+Soros, TP {args.take_profit_percent:.0f}%)", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, dynamic_stake=True, use_soros_compound=True, use_martingale=False),
            # Opção D: dynamic + Soros + Martingale
            dict(label=f"Opção D  (dynamic+Soros+Martingale, TP {args.take_profit_percent:.0f}%)", stake=args.stake, take_profit_percent=args.take_profit_percent, max_hold_ticks=args.max_hold_ticks, dynamic_stake=True, use_soros_compound=True, use_martingale=True),
        ]

        rows = []
        for sc in scenarios:
            sc_label = sc.pop("label")
            r = run_accumulator_backtest(**common, **sc)
            rows.append({
                "Cenário": sc_label,
                "Trades": r["total_trades"],
                "Wins": r["wins"],
                "Losses": r["losses"],
                "Winrate%": f"{r['winrate']:.1f}",
                "PnL": f"{r['net_profit']:+.2f}",
                "Banca Final": f"{r['ending_balance']:.2f}",
                "ROI%": f"{r['net_profit'] / args.initial_balance * 100:+.2f}",
                "Max DD%": f"{r['max_drawdown_pct']:.2f}",
                "Max Loss Streak": r["max_loss_streak"],
            })

        # Print comparison table
        if rows:
            keys = list(rows[0].keys())
            widths = [max(len(k), max(len(str(row[k])) for row in rows)) for k in keys]
            header = "  ".join(k.ljust(w) for k, w in zip(keys, widths))
            sep = "  ".join("-" * w for w in widths)
            print(header)
            print(sep)
            for row in rows:
                print("  ".join(str(row[k]).ljust(w) for k, w in zip(keys, widths)))

        if args.output and rows:
            import csv as _csv
            with args.output.open("w", newline="", encoding="utf-8") as f:
                w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
            print(f"\nTabela salva em: {args.output}")
        return

    result = run_accumulator_backtest(
        **common,
        stake=args.stake,
        take_profit_percent=args.take_profit_percent,
        max_hold_ticks=args.max_hold_ticks,
        dynamic_stake=args.dynamic_stake,
        use_soros_compound=args.use_soros,
        use_martingale=args.use_martingale,
    )

    if args.output:
        write_trades(args.output, result["trades"])

    printable = {key: value for key, value in result.items() if key != "trades"}
    print(json.dumps(printable, indent=2))


if __name__ == "__main__":
    main()
