from __future__ import annotations

import argparse
import asyncio
import csv
import json
from pathlib import Path
from typing import Any

import websockets


async def download_ticks(app_id: str, symbol: str, count: int) -> list[dict[str, Any]]:
    url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    request = {
        "ticks_history": symbol,
        "count": count,
        "end": "latest",
        "style": "ticks",
    }

    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(request))
        response = json.loads(await ws.recv())

    if "error" in response:
        error = response["error"]
        raise RuntimeError(f"Erro Deriv {error.get('code')}: {error.get('message')}")

    history = response.get("history", {})
    times = history.get("times", [])
    prices = history.get("prices", [])
    return [{"epoch": int(epoch), "quote": float(quote)} for epoch, quote in zip(times, prices)]


def write_ticks_csv(path: Path, ticks: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows({"epoch": tick["epoch"], "quote": tick["quote"]} for tick in ticks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Baixa ticks historicos da Deriv para Accumulators 1s.")
    parser.add_argument("--app-id", default="1089")
    parser.add_argument("--symbol", default="1HZ100V")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    output = args.output or Path(f"data/ticks_{args.symbol}.csv")
    ticks = asyncio.run(download_ticks(args.app_id, args.symbol, args.count))
    write_ticks_csv(output, ticks)
    print(f"{len(ticks)} ticks salvos em {output}")


if __name__ == "__main__":
    main()
