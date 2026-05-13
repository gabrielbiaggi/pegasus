from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class TradeJournal:
    signal_fields = [
        "timestamp",
        "symbol",
        "candle_epoch",
        "direction",
        "score",
        "stake",
        "dry_run",
    ]
    trade_fields = [
        "timestamp",
        "symbol",
        "contract_id",
        "candle_epoch",
        "direction",
        "score",
        "stake",
        "buy_price",
        "profit",
        "result",
    ]

    def __init__(self, directory: str = "logs"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _append(self, filename: str, fields: list[str], row: dict[str, Any]) -> None:
        path = self.directory / filename
        exists = path.exists()
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            if not exists:
                writer.writeheader()
            writer.writerow({field: row.get(field, "") for field in fields})

    def log_signal(
        self,
        symbol: str,
        candle_epoch: int,
        direction: str,
        score: int,
        stake: float,
        dry_run: bool,
    ) -> None:
        self._append(
            "signals.csv",
            self.signal_fields,
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "candle_epoch": candle_epoch,
                "direction": direction,
                "score": score,
                "stake": f"{stake:.2f}",
                "dry_run": dry_run,
            },
        )

    def log_trade(
        self,
        symbol: str,
        contract_id: int,
        candle_epoch: int,
        direction: str,
        score: int,
        stake: float,
        buy_price: float,
        profit: float,
    ) -> None:
        result = "WIN" if profit > 0 else "LOSS"
        self._append(
            "trades.csv",
            self.trade_fields,
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "contract_id": contract_id,
                "candle_epoch": candle_epoch,
                "direction": direction,
                "score": score,
                "stake": f"{stake:.2f}",
                "buy_price": f"{buy_price:.2f}",
                "profit": f"{profit:.2f}",
                "result": result,
            },
        )
