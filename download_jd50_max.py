"""Download maximum JD50 tick history from Deriv API (up to 14 days)."""
from __future__ import annotations

import asyncio
import csv
import json
import time
from pathlib import Path

import websockets

APP_ID = "1089"
SYMBOL = "JD50"
BATCH_SIZE = 5000
OUTPUT = Path("data/ticks_JD50_max.csv")


async def download_batch(start: int, end: int, retries: int = 3) -> list[dict]:
    url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    req = {
        "ticks_history": SYMBOL,
        "count": BATCH_SIZE,
        "start": str(start),
        "end": str(end),
        "style": "ticks",
    }
    for attempt in range(retries):
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(req))
                resp = json.loads(await ws.recv())

            if "error" in resp:
                return []

            hist = resp.get("history", {})
            times = hist.get("times", [])
            prices = hist.get("prices", [])
            return [{"epoch": int(e), "quote": float(q)} for e, q in zip(times, prices)]
        except Exception as e:
            if attempt < retries - 1:
                print(f"    retry {attempt+1}/{retries} after error: {e}")
                await asyncio.sleep(2 * (attempt + 1))
            else:
                print(f"    FAILED after {retries} attempts: {e}")
                return []


def _save(ticks: list[dict]) -> None:
    """Save ticks to CSV (quick checkpoint)."""
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows(ticks)


async def download_max():
    now = int(time.time())
    # Start from 14 days ago (API limit)
    start_epoch = now - 14 * 86400
    all_ticks: list[dict] = []
    cursor = start_epoch
    batch_num = 0

    print(f"Downloading {SYMBOL} ticks: {time.strftime('%Y-%m-%d %H:%M', time.gmtime(start_epoch))} → now")
    print(f"This will take several minutes...\n")

    # Resume from existing data if available
    if OUTPUT.exists():
        with OUTPUT.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_ticks.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})
        if all_ticks:
            cursor = all_ticks[-1]["epoch"] + 1
            print(f"  Resuming from {len(all_ticks):,} existing ticks (cursor={time.strftime('%m-%d %H:%M', time.gmtime(cursor))})")

    while cursor < now:
        batch_num += 1
        end = min(cursor + 7200, now)  # 2-hour chunks
        ticks = await download_batch(cursor, end)

        if ticks:
            t0 = time.strftime('%m-%d %H:%M', time.gmtime(ticks[0]['epoch']))
            t1 = time.strftime('%m-%d %H:%M', time.gmtime(ticks[-1]['epoch']))
            all_ticks.extend(ticks)
            actual_end = ticks[-1]["epoch"]
            print(f"  Batch {batch_num:3d}: {len(ticks):5d} ticks | {t0} → {t1} | total: {len(all_ticks):,}")

            if len(ticks) < BATCH_SIZE:
                cursor = end + 1
            else:
                cursor = actual_end + 1
        else:
            cursor = end + 1

        await asyncio.sleep(0.3)

        # Save progress every 50 batches
        if batch_num % 50 == 0:
            _save(all_ticks)
            print(f"  [checkpoint saved: {len(all_ticks):,} ticks]")

    _save(all_ticks)

    # Final dedup + sort
    seen = set()
    unique = []
    for t in all_ticks:
        key = (t["epoch"], t["quote"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    unique.sort(key=lambda x: x["epoch"])

    print(f"\n{'='*60}")
    print(f"Total unique ticks: {len(unique):,}")
    if unique:
        print(f"Range: {time.strftime('%Y-%m-%d %H:%M', time.gmtime(unique[0]['epoch']))} → {time.strftime('%Y-%m-%d %H:%M', time.gmtime(unique[-1]['epoch']))}")
        days = (unique[-1]["epoch"] - unique[0]["epoch"]) / 86400
        print(f"Duration: {days:.1f} days")

        from collections import Counter
        day_counts = Counter(time.strftime('%Y-%m-%d', time.gmtime(t['epoch'])) for t in unique)
        print("\nTicks per day:")
        for day, cnt in sorted(day_counts.items()):
            print(f"  {day}: {cnt:>7,} ticks")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows(unique)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(download_max())
