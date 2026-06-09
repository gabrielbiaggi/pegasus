#!/usr/bin/env python3
import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import websockets

WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
START = date(2026, 1, 1)
END = date(2026, 1, 31)
DATA_DIR = Path("data")
OUTPUT_PATH = Path("logs/digits_january_audit.json")
PIP = {
    "1HZ10V": 2,
    "1HZ25V": 2,
    "1HZ50V": 2,
    "1HZ75V": 2,
    "1HZ100V": 2,
    "R_75": 2,
    "R_100": 2,
    "JD10": 2,
    "JD100": 2,
}


def iter_days():
    current = START
    while current <= END:
        yield current
        current += timedelta(days=1)


async def download_day(ws, symbol: str, day: date, out_path: Path) -> int:
    start_dt = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
    start_epoch = int(start_dt.timestamp())
    end_epoch = start_epoch + 86400 - 1
    ticks: list[str] = []
    cursor = start_epoch
    while cursor < end_epoch:
        chunk_end = min(cursor + 3600, end_epoch)
        await ws.send(
            json.dumps(
                {
                    "ticks_history": symbol,
                    "start": cursor,
                    "end": chunk_end,
                    "style": "ticks",
                    "count": 5000,
                }
            )
        )
        resp = json.loads(await ws.recv())
        if "error" in resp:
            raise RuntimeError(json.dumps(resp["error"]))
        hist = resp.get("history", {})
        prices = hist.get("prices", [])
        times = hist.get("times", [])
        for tick_ts, price in zip(times, prices):
            ticks.append(f"{tick_ts},{price}")
        if not times:
            break
        cursor = max(times) + 1
        await asyncio.sleep(0.12)
    if len(ticks) > 10:
        out_path.write_text("epoch,quote\n" + "\n".join(ticks), encoding="utf-8")
        return len(ticks)
    return 0


def load_existing() -> dict:
    if OUTPUT_PATH.exists():
        try:
            return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def write_existing(payload: dict) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def analyze_symbol(symbol: str) -> dict:
    counts = [0] * 10
    total = 0
    days = 0
    for path in sorted(DATA_DIR.glob(f"ticks_{symbol}_2026-01-*.csv")):
        if path.stat().st_size <= 5000:
            continue
        days += 1
        df = pd.read_csv(path, usecols=["quote"])
        vals = ((df["quote"].astype(float) * (10 ** PIP[symbol])).round().astype(int) % 10)
        vc = vals.value_counts()
        total += len(vals)
        for digit in range(10):
            counts[digit] += int(vc.get(digit, 0))

    rows: list[tuple[str, float]] = []
    odd = sum(counts[d] for d in (1, 3, 5, 7, 9)) / total * 100
    even = 100 - odd
    rows.append(("DIGITODD", round(odd, 4)))
    rows.append(("DIGITEVEN", round(even, 4)))
    for digit in range(10):
        rows.append((f"DIGITDIFF {digit}", round((1 - counts[digit] / total) * 100, 4)))
        rows.append((f"DIGITMATCH {digit}", round((counts[digit] / total) * 100, 4)))
    for barrier in range(10):
        over = sum(counts[x] for x in range(barrier + 1, 10)) / total * 100 if barrier < 9 else 0.0
        under = sum(counts[x] for x in range(0, barrier)) / total * 100 if barrier > 0 else 0.0
        rows.append((f"DIGITOVER {barrier}", round(over, 4)))
        rows.append((f"DIGITUNDER {barrier}", round(under, 4)))
    rows.sort(key=lambda item: item[1], reverse=True)
    return {
        "symbol": symbol,
        "days": days,
        "complete_month": days == 31,
        "ticks": total,
        "top_contracts": rows[:20],
        "best_contract": rows[0][0],
        "best_wr": rows[0][1],
    }


async def ensure_symbol(symbol: str, state: dict) -> dict:
    missing = []
    for day in iter_days():
        out = DATA_DIR / f"ticks_{symbol}_{day.isoformat()}.csv"
        if not (out.exists() and out.stat().st_size > 5000):
            missing.append((day, out))
    state.setdefault("downloads", {})[symbol] = {"missing_days": len(missing), "completed": 31 - len(missing)}
    write_existing(state)
    if missing:
        async with websockets.connect(WS_URL, ping_interval=30, open_timeout=15, max_size=None) as ws:
            for day, out in missing:
                for attempt in range(1, 9):
                    try:
                        ticks = await download_day(ws, symbol, day, out)
                        state["downloads"][symbol]["last_day"] = day.isoformat()
                        state["downloads"][symbol]["last_ticks"] = ticks
                        state["downloads"][symbol]["completed"] += 1
                        write_existing(state)
                        await asyncio.sleep(2.0)
                        break
                    except Exception as exc:
                        state["downloads"][symbol]["last_error"] = str(exc)
                        state["downloads"][symbol]["last_attempt"] = attempt
                        write_existing(state)
                        await asyncio.sleep(10 * attempt)
                else:
                    state["downloads"][symbol]["failed_day"] = day.isoformat()
                    write_existing(state)
                    return state
    state.setdefault("analysis", {})[symbol] = analyze_symbol(symbol)
    write_existing(state)
    return state


async def main(symbols: list[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = load_existing()
    state["started_at"] = datetime.now(timezone.utc).isoformat()
    state["symbols"] = symbols
    write_existing(state)
    for symbol in symbols:
        state = await ensure_symbol(symbol, state)
    ranked = sorted(
        state.get("analysis", {}).values(),
        key=lambda item: (item.get("complete_month", False), item.get("best_wr", 0.0)),
        reverse=True,
    )
    state["ranked"] = ranked
    state["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_existing(state)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("symbols", nargs="*", default=list(PIP))
    args = parser.parse_args()
    asyncio.run(main(args.symbols))
