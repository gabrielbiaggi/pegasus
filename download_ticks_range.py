#!/usr/bin/env python3
"""Download BOOM1000 ticks for a specific date range from Deriv API."""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import websockets
from deriv_auth import get_auth

TOKEN = os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or ""
APP_ID = os.getenv("DERIV_APP_ID", "1089")


async def download_day(date_str: str, output_path: Path) -> int:
    """Download ticks for a single day. Returns tick count."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_epoch = int(dt.timestamp())
    end_epoch = int((dt + timedelta(days=1)).timestamp()) - 1

    auth = get_auth(APP_ID, "demo")
    ticks = []
    async with websockets.connect(auth.ws_url, ping_interval=30) as ws:
        if not auth.is_new_api:
            # Authorize
            await ws.send(json.dumps({"authorize": auth.legacy_token}))
            resp = json.loads(await ws.recv())
            if "error" in resp:
                raise ValueError(f"Auth failed: {resp['error']['message']}")

        # Fetch ticks in chunks (Deriv max ~5000 per request)
        start = start_epoch
        while start < end_epoch:
            await ws.send(
                json.dumps(
                    {
                        "ticks_history": "BOOM1000",
                        "start": start,
                        "end": min(start + 3600, end_epoch),  # 1 hour chunks
                        "style": "ticks",
                        "count": 5000,
                    }
                )
            )
            resp = json.loads(await ws.recv())
            if "error" in resp:
                break
            hist = resp.get("history", {})
            prices = hist.get("prices", [])
            times = hist.get("times", [])
            for t, p in zip(times, prices):
                ticks.append(f"{t},{p}")
            if not times:
                break
            start = max(times) + 1 if times else end_epoch + 1

    if ticks:
        output_path.write_text("epoch,quote\n" + "\n".join(ticks))
    return len(ticks)


async def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} START_DATE END_DATE")
        print(f"Example: {sys.argv[0]} 2026-05-21 2026-05-24")
        sys.exit(1)

    start_dt = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_dt = datetime.strptime(sys.argv[2], "%Y-%m-%d")
    current = start_dt
    data_dir = Path("data")

    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        out_path = data_dir / f"ticks_BOOM1000_{date_str}.csv"
        if out_path.exists() and out_path.stat().st_size > 1000:
            print(f"  {date_str}: já existe ({out_path.stat().st_size // 1024}KB)")
        else:
            print(f"  {date_str}: baixando...", end=" ", flush=True)
            try:
                n = await download_day(date_str, out_path)
                print(f"{n} ticks")
            except Exception as e:
                print(f"ERRO: {e}")
        current += timedelta(days=1)


if __name__ == "__main__":
    asyncio.run(main())
