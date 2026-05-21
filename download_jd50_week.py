"""Download 1 week of JD50 tick data from Deriv API in batches."""
from __future__ import annotations

import asyncio
import csv
import json
import time
from pathlib import Path
from typing import Any

import websockets

APP_ID = "1089"
SYMBOL = "JD50"
BATCH_SIZE = 5000
OUTPUT = Path("data/ticks_JD50_1week.csv")

# 7 days of ticks — JD50 runs ~1 tick/sec, 24/7 except weekends
# We request ticks in backward batches from "latest"


async def download_batch(
    app_id: str,
    symbol: str,
    count: int,
    end: int | str = "latest",
) -> list[dict[str, Any]]:
    """Download a single batch of ticks ending at `end`."""
    url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    request = {
        "ticks_history": symbol,
        "count": count,
        "end": end if isinstance(end, str) else str(end),
        "style": "ticks",
    }
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(request))
        response = json.loads(await ws.recv())

    if "error" in response:
        error = response["error"]
        raise RuntimeError(f"Deriv error {error.get('code')}: {error.get('message')}")

    history = response.get("history", {})
    times = history.get("times", [])
    prices = history.get("prices", [])
    return [{"epoch": int(e), "quote": float(q)} for e, q in zip(times, prices)]


async def download_week():
    """Download ~7 days of tick data in backward batches."""
    now = int(time.time())
    # 7 days = 604800 seconds; JD50 ticks ~1/sec ≈ 600k ticks
    # But weekends have no ticks, so effectively ~5 trading days
    target_start = now - 7 * 86400  # 7 days ago

    all_ticks: list[dict[str, Any]] = []
    end_epoch: int | str = "latest"
    batch_num = 0

    print(f"Downloading {SYMBOL} ticks from 7 days ago to now...")
    print(f"Target start epoch: {target_start} ({time.strftime('%Y-%m-%d %H:%M', time.gmtime(target_start))})")

    while True:
        batch_num += 1
        print(f"  Batch {batch_num}: fetching {BATCH_SIZE} ticks ending at {end_epoch}...", end=" ", flush=True)

        try:
            ticks = await download_batch(APP_ID, SYMBOL, BATCH_SIZE, end_epoch)
        except Exception as e:
            print(f"ERROR: {e}")
            break

        if not ticks:
            print("empty response, done.")
            break

        print(f"got {len(ticks)} ticks ({time.strftime('%Y-%m-%d %H:%M', time.gmtime(ticks[0]['epoch']))} → {time.strftime('%Y-%m-%d %H:%M', time.gmtime(ticks[-1]['epoch']))})")

        all_ticks = ticks + all_ticks  # prepend (older first)

        # Check if we've gone back far enough
        earliest = ticks[0]["epoch"]
        if earliest <= target_start:
            print(f"  Reached target start date.")
            break

        if len(ticks) < BATCH_SIZE:
            print(f"  Got fewer ticks than requested, no more data.")
            break

        # Next batch ends 1 second before the earliest tick we got
        end_epoch = earliest - 1

        # Small delay to avoid rate limiting
        await asyncio.sleep(0.5)

    # Deduplicate and sort by epoch
    seen = set()
    unique_ticks = []
    for t in all_ticks:
        key = (t["epoch"], t["quote"])
        if key not in seen:
            seen.add(key)
            unique_ticks.append(t)
    unique_ticks.sort(key=lambda x: x["epoch"])

    # Trim to target start
    unique_ticks = [t for t in unique_ticks if t["epoch"] >= target_start]

    print(f"\nTotal unique ticks: {len(unique_ticks)}")
    if unique_ticks:
        print(f"Range: {time.strftime('%Y-%m-%d %H:%M', time.gmtime(unique_ticks[0]['epoch']))} → {time.strftime('%Y-%m-%d %H:%M', time.gmtime(unique_ticks[-1]['epoch']))}")
        # Count ticks per day
        from collections import Counter
        day_counts = Counter(time.strftime('%Y-%m-%d', time.gmtime(t['epoch'])) for t in unique_ticks)
        print("\nTicks per day:")
        for day, cnt in sorted(day_counts.items()):
            print(f"  {day}: {cnt:,} ticks")

    # Write CSV
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows(unique_ticks)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(download_week())
