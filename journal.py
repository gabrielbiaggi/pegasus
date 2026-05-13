from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class TradeJournal:
    signal_fields = [
        "timestamp",
        "symbol",
        "contract_mode",
        "entry_epoch",
        "direction",
        "score",
        "stake",
        "dry_run",
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
    ]
    trade_fields = [
        "timestamp",
        "symbol",
        "contract_mode",
        "contract_id",
        "entry_epoch",
        "exit_epoch",
        "held_ticks",
        "direction",
        "score",
        "stake",
        "buy_price",
        "profit",
        "result",
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
    ]

    def __init__(self, directory: str = "logs"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _append(self, filename: str, fields: list[str], row: dict[str, Any]) -> None:
        path = self.directory / filename
        exists = path.exists()
        if exists and path.stat().st_size > 0:
            first_line = path.read_text(encoding="utf-8").splitlines()[0]
            if first_line != ",".join(fields):
                archive = path.with_name(f"{path.stem}.legacy-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}{path.suffix}")
                path.replace(archive)
                exists = False

        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            if not exists:
                writer.writeheader()
            writer.writerow({field: row.get(field, "") for field in fields})

    @staticmethod
    def _format_metric(metrics: dict[str, Any] | None, name: str) -> str:
        if not metrics or name not in metrics:
            return ""
        try:
            value = float(metrics[name])
        except (TypeError, ValueError):
            return ""
        if value != value:
            return ""
        return f"{value:.6f}"

    def log_signal(
        self,
        symbol: str,
        contract_mode: str,
        entry_epoch: int,
        direction: str,
        score: int,
        stake: float,
        dry_run: bool,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        self._append(
            "signals.csv",
            self.signal_fields,
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "contract_mode": contract_mode,
                "entry_epoch": entry_epoch,
                "direction": direction,
                "score": score,
                "stake": f"{stake:.2f}",
                "dry_run": dry_run,
                "bb_width_percent": self._format_metric(metrics, "bb_width_percent"),
                "tick_atr_percent": self._format_metric(metrics, "tick_atr_percent"),
                "recent_move_percent": self._format_metric(metrics, "recent_move_percent"),
                "hurst_exponent": self._format_metric(metrics, "hurst_exponent"),
                "tick_imbalance": self._format_metric(metrics, "tick_imbalance"),
                "hawkes_intensity": self._format_metric(metrics, "hawkes_intensity"),
                "velocity_zscore": self._format_metric(metrics, "velocity_zscore"),
                "acceleration_zscore": self._format_metric(metrics, "acceleration_zscore"),
                "pmi_distance_percent": self._format_metric(metrics, "pmi_distance_percent"),
                "markov_p_up_given_up": self._format_metric(metrics, "markov_p_up_given_up"),
                "markov_p_down_given_down": self._format_metric(metrics, "markov_p_down_given_down"),
                "shannon_entropy": self._format_metric(metrics, "shannon_entropy"),
                "kalman_residual_zscore": self._format_metric(metrics, "kalman_residual_zscore"),
            },
        )

    def log_trade(
        self,
        symbol: str,
        contract_mode: str,
        contract_id: int,
        entry_epoch: int,
        direction: str,
        score: int,
        stake: float,
        buy_price: float,
        profit: float,
        exit_epoch: int | None = None,
        held_ticks: int | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        result = "WIN" if profit > 0 else "LOSS"
        self._append(
            "trades.csv",
            self.trade_fields,
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "contract_mode": contract_mode,
                "contract_id": contract_id,
                "entry_epoch": entry_epoch,
                "exit_epoch": exit_epoch or "",
                "held_ticks": held_ticks if held_ticks is not None else "",
                "direction": direction,
                "score": score,
                "stake": f"{stake:.2f}",
                "buy_price": f"{buy_price:.2f}",
                "profit": f"{profit:.2f}",
                "result": result,
                "bb_width_percent": self._format_metric(metrics, "bb_width_percent"),
                "tick_atr_percent": self._format_metric(metrics, "tick_atr_percent"),
                "recent_move_percent": self._format_metric(metrics, "recent_move_percent"),
                "hurst_exponent": self._format_metric(metrics, "hurst_exponent"),
                "tick_imbalance": self._format_metric(metrics, "tick_imbalance"),
                "hawkes_intensity": self._format_metric(metrics, "hawkes_intensity"),
                "velocity_zscore": self._format_metric(metrics, "velocity_zscore"),
                "acceleration_zscore": self._format_metric(metrics, "acceleration_zscore"),
                "pmi_distance_percent": self._format_metric(metrics, "pmi_distance_percent"),
                "markov_p_up_given_up": self._format_metric(metrics, "markov_p_up_given_up"),
                "markov_p_down_given_down": self._format_metric(metrics, "markov_p_down_given_down"),
                "shannon_entropy": self._format_metric(metrics, "shannon_entropy"),
                "kalman_residual_zscore": self._format_metric(metrics, "kalman_residual_zscore"),
            },
        )
