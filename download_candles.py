from __future__ import annotations

import argparse
import asyncio
import csv
import json
from pathlib import Path
from typing import Any

import websockets


async def download(app_id: str, symbol: str, granularity: int, count: int) -> list[dict[str, Any]]:
    url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    request = {
        "ticks_history": symbol,
        "count": count,
        "end": "latest",
        "granularity": granularity,
        "style": "candles",
    }
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(request))
        response = json.loads(await ws.recv())

    if "error" in response:
        error = response["error"]
        raise RuntimeError(f"Erro Deriv {error.get('code')}: {error.get('message')}")

    return response.get("candles", [])


def write_csv(path: Path, candles: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["epoch", "open", "high", "low", "close"])
        writer.writeheader()
        for candle in candles:
            writer.writerow(
                {
                    "epoch": candle["epoch"],
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Baixa candles historicos da Deriv para backtest.")
    parser.add_argument("--app-id", default="1089")
    parser.add_argument("--symbol", default="R_100")
    parser.add_argument("--granularity", type=int, default=60)
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--output", type=Path, default=Path("data/candles_R_100.csv"))
    args = parser.parse_args()

    candles = asyncio.run(download(args.app_id, args.symbol, args.granularity, args.count))
    write_csv(args.output, candles)
    print(f"{len(candles)} candles salvos em {args.output}")


if __name__ == "__main__":
    main()
