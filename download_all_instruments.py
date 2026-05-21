"""Download max tick history (14 days) for ALL Deriv synthetic instruments.

Downloads in batches of 5000 ticks, saves to data/ticks_{SYMBOL}_max.csv.
Resumes from existing partial downloads.
"""
from __future__ import annotations

import asyncio
import csv
import json
import sys
import time
from pathlib import Path

import websockets

APP_ID = "1089"
BATCH_SIZE = 5000
DATA_DIR = Path("data")

# ── ALL synthetic instruments available on Deriv ──────────────────────
INSTRUMENTS = {
    # Volatility Indices (continuous, 1-tick-per-second)
    "1HZ10V":   "Volatility 10 (1s)",
    "1HZ25V":   "Volatility 25 (1s)",
    "1HZ50V":   "Volatility 50 (1s)",
    "1HZ75V":   "Volatility 75 (1s)",
    "1HZ100V":  "Volatility 100 (1s)",
    "R_10":     "Volatility 10 Index",
    "R_25":     "Volatility 25 Index",
    "R_50":     "Volatility 50 Index",
    "R_75":     "Volatility 75 Index",
    "R_100":    "Volatility 100 Index",
    # Volatility Indices (200ms tick)
    "1HZ150V":  "Volatility 150 (1s)",
    "1HZ200V":  "Volatility 200 (1s)",
    "1HZ250V":  "Volatility 250 (1s)",
    "1HZ300V":  "Volatility 300 (1s)",
    # Jump Indices
    "JD10":     "Jump 10 Index",
    "JD25":     "Jump 25 Index",
    "JD50":     "Jump 50 Index",
    "JD75":     "Jump 75 Index",
    "JD100":    "Jump 100 Index",
    # Crash/Boom
    "BOOM300N":  "Boom 300 Index",
    "BOOM500":   "Boom 500 Index",
    "BOOM1000":  "Boom 1000 Index",
    "CRASH300N": "Crash 300 Index",
    "CRASH500":  "Crash 500 Index",
    "CRASH1000": "Crash 1000 Index",
    # Step Index
    "stpRNG":   "Step Index",
    # Range Break
    "RDBULL":   "Range Break 100",
    "RDBEAR":   "Range Break 200",
    # DEX indices
    "DEX600DN":  "DEX 600 Down",
    "DEX600UP":  "DEX 600 Up",
    "DEX900DN":  "DEX 900 Down",
    "DEX900UP":  "DEX 900 Up",
    "DEX1500DN": "DEX 1500 Down",
    "DEX1500UP": "DEX 1500 Up",
    # Drift Switch
    "DSI10":    "Drift Switch 10",
    "DSI20":    "Drift Switch 20",
    "DSI30":    "Drift Switch 30",
}


def save_ticks(path: Path, ticks: list[dict]) -> None:
    """Save ticks to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows(ticks)


def load_existing(path: Path) -> list[dict]:
    """Load existing ticks from CSV if file exists."""
    if not path.exists():
        return []
    ticks = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticks.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})
    return ticks


async def download_symbol(symbol: str, label: str) -> int:
    """Download max ticks for a single symbol using persistent WebSocket.

    Uses 2-hour chunks on a single WebSocket connection (fast, no rate limit).
    Properly reconnects if the WebSocket drops.
    """
    output = DATA_DIR / f"ticks_{symbol}_max.csv"

    # Check if already complete (>500K ticks = probably full 14 days)
    existing = load_existing(output)
    if len(existing) >= 500_000:
        print(f"  ✅ {symbol:12s} ({label}) — already have {len(existing):,} ticks, skipping")
        return len(existing)

    now = int(time.time())
    start_epoch = now - 14 * 86400  # 14 days back

    all_ticks: list[dict] = []
    cursor = start_epoch

    # If resuming, check if existing data starts near start_epoch (not from broken download)
    if existing:
        first_epoch = existing[0]["epoch"]
        # Only resume if first tick is within 1 day of start_epoch
        if abs(first_epoch - start_epoch) < 86400:
            all_ticks = existing.copy()
            cursor = all_ticks[-1]["epoch"] + 1
            print(f"  🔄 {symbol:12s} ({label}) — resuming from {len(existing):,} ticks...")
        else:
            print(f"  📥 {symbol:12s} ({label}) — downloading fresh (old data was corrupt)...")
    else:
        print(f"  📥 {symbol:12s} ({label}) — downloading...")

    batch_num = 0
    url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    ws = None

    async def get_ws():
        """Get or create WebSocket connection."""
        nonlocal ws
        if ws is not None:
            return ws
        ws = await websockets.connect(url, ping_interval=20, ping_timeout=10)
        return ws

    async def close_ws():
        """Close WebSocket connection."""
        nonlocal ws
        if ws:
            try:
                await ws.close()
            except Exception:
                pass
            ws = None

    try:
        while cursor < now:
            batch_num += 1
            end = min(cursor + 7200, now)  # 2-hour chunks

            req = {
                "ticks_history": symbol,
                "count": BATCH_SIZE,
                "start": str(cursor),
                "end": str(end),
                "style": "ticks",
            }

            resp = None
            for attempt in range(3):
                try:
                    conn = await get_ws()
                    await conn.send(json.dumps(req))
                    resp = json.loads(await conn.recv())
                    break
                except Exception:
                    await close_ws()
                    if attempt < 2:
                        await asyncio.sleep(1 * (attempt + 1))

            if resp is None:
                cursor = end + 1
                continue

            if "error" in resp:
                err = resp["error"]
                if err.get("code") == "InvalidSymbol":
                    print(f"  ❌ {symbol:12s} — invalid symbol")
                    return 0
                cursor = end + 1
                continue

            hist = resp.get("history", {})
            times = hist.get("times", [])
            prices = hist.get("prices", [])

            if times:
                batch = [{"epoch": int(e), "quote": float(q)} for e, q in zip(times, prices)]
                all_ticks.extend(batch)

                if len(batch) < BATCH_SIZE:
                    cursor = end + 1
                else:
                    cursor = batch[-1]["epoch"] + 1
            else:
                cursor = end + 1

            # Progress every 20 batches
            if batch_num % 20 == 0:
                print(f"       {len(all_ticks):,} ticks so far... (batch {batch_num})")

            # Checkpoint every 100 batches
            if batch_num % 100 == 0 and all_ticks:
                save_ticks(output, all_ticks)

            await asyncio.sleep(0.05)  # Minimal delay on persistent connection

    except Exception as e:
        print(f"  ⚠ {symbol:12s} — connection error: {e}")
    finally:
        await close_ws()

    if not all_ticks:
        print(f"  ❌ {symbol:12s} — no data available")
        return 0

    # Dedup + sort
    seen = set()
    unique = []
    for t in all_ticks:
        key = (t["epoch"], t["quote"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    unique.sort(key=lambda x: x["epoch"])

    save_ticks(output, unique)
    days = (unique[-1]["epoch"] - unique[0]["epoch"]) / 86400
    print(f"  ✅ {symbol:12s} — {len(unique):,} ticks ({days:.1f} days) [{batch_num} batches]")

    return len(unique)


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Allow filtering specific instruments from command line
    if len(sys.argv) > 1:
        selected = sys.argv[1:]
        instruments = {k: v for k, v in INSTRUMENTS.items() if k in selected}
    else:
        instruments = INSTRUMENTS

    print(f"{'='*70}")
    print(f"  DOWNLOADING MAX TICKS FOR {len(instruments)} INSTRUMENTS")
    print(f"  (14 days history, API limit)")
    print(f"{'='*70}\n")

    results = {}
    sem = asyncio.Semaphore(5)  # Max 5 concurrent downloads

    async def download_with_sem(symbol, label):
        async with sem:
            return symbol, await download_symbol(symbol, label)

    tasks = [download_with_sem(s, l) for s, l in instruments.items()]
    for coro in asyncio.as_completed(tasks):
        symbol, count = await coro
        results[symbol] = count

    # Summary
    print(f"\n{'='*70}")
    print(f"  DOWNLOAD SUMMARY")
    print(f"{'='*70}")
    for symbol, count in sorted(results.items(), key=lambda x: -x[1]):
        status = "✅" if count > 0 else "❌"
        label = INSTRUMENTS.get(symbol, "")
        print(f"  {status} {symbol:12s} {label:25s} {count:>10,} ticks")

    total = sum(results.values())
    available = sum(1 for c in results.values() if c > 0)
    print(f"\n  Total: {total:,} ticks across {available}/{len(results)} instruments")


if __name__ == "__main__":
    asyncio.run(main())
