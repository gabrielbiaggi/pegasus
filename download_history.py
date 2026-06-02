#!/usr/bin/env python3
"""
download_history.py — Baixa ticks históricos da Deriv por data (dia a dia).

Uso:
    python download_history.py --start 2026-01-01 --end 2026-04-30
    python download_history.py --start 2026-01-01 --end 2026-04-30 --skip-existing

A Deriv API permite buscar ticks históricos usando `start`/`end` por janela de tempo.
Cada dia tem ~86400 ticks. Salvamos cada dia como ticks_BOOM1000_YYYY-MM-DD.csv.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import websockets
from dotenv import load_dotenv

load_dotenv()

SYMBOL   = "BOOM1000"
APP_ID   = os.getenv("DERIV_APP_ID", "1089")
TOKEN    = os.getenv("DERIV_TOKEN", "")
WS_URL   = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
DATA_DIR = Path("data")

MAX_TICKS_PER_CALL = 5000  # Limite da Deriv API por chamada
RECONNECT_DELAY    = 3      # segundos entre reconexões

async def _fetch_ticks_range(ws, symbol: str, start_epoch: int, end_epoch: int) -> list[dict]:
    """Busca ticks num intervalo de tempo específico via websocket já conectado."""
    request = {
        "ticks_history": symbol,
        "start": start_epoch,
        "end": end_epoch,
        "count": MAX_TICKS_PER_CALL,
        "style": "ticks",
        "adjust_start_time": 1,
    }
    if TOKEN:
        request["passthrough"] = {"token": TOKEN}

    await ws.send(json.dumps(request))
    resp = json.loads(await ws.recv())

    if "error" in resp:
        err = resp["error"]
        raise RuntimeError(f"Deriv API error [{err.get('code')}]: {err.get('message')}")

    history = resp.get("history", {})
    times  = history.get("times", [])
    prices = history.get("prices", [])
    return [{"epoch": int(t), "quote": float(p)} for t, p in zip(times, prices)]


async def download_day(symbol: str, day: date) -> list[dict]:
    """
    Baixa TODOS os ticks de um dia específico, fazendo múltiplas chamadas se necessário.
    A Deriv limita a ~5000 ticks por chamada, então dividimos o dia em janelas.
    """
    # Um dia tem 86400 segundos = ~86400 ticks (BOOM1000 é ~1 tick/sec)
    day_start = int(date(day.year, day.month, day.day).strftime("%s") if hasattr(date, 'strftime') else
                    time.mktime(day.timetuple()))
    # Calcula epoch UTC corretamente
    import datetime as _dt
    dt_start = _dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=_dt.timezone.utc)
    dt_end   = _dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_dt.timezone.utc)
    day_start_epoch = int(dt_start.timestamp())
    day_end_epoch   = int(dt_end.timestamp())

    all_ticks: list[dict] = []
    seen_epochs: set[int] = set()

    # Divide o dia em janelas de ~4h para não ultrapassar o limite
    window_size = 4 * 3600  # 4 horas em segundos
    windows = []
    t = day_start_epoch
    while t < day_end_epoch:
        windows.append((t, min(t + window_size - 1, day_end_epoch)))
        t += window_size

    retries = 3
    for attempt in range(retries):
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=30) as ws:
                for win_start, win_end in windows:
                    ticks = await _fetch_ticks_range(ws, symbol, win_start, win_end)
                    for tick in ticks:
                        if tick["epoch"] not in seen_epochs:
                            seen_epochs.add(tick["epoch"])
                            all_ticks.append(tick)
                    await asyncio.sleep(0.2)  # rate limiting gentil
            break
        except Exception as e:
            if attempt < retries - 1:
                print(f"    [RETRY {attempt+1}] {e} — aguardando {RECONNECT_DELAY}s...")
                await asyncio.sleep(RECONNECT_DELAY)
            else:
                raise

    all_ticks.sort(key=lambda x: x["epoch"])
    return all_ticks


def save_day_csv(day: date, ticks: list[dict], data_dir: Path) -> Path:
    """Salva os ticks de um dia como CSV."""
    path = data_dir / f"ticks_{SYMBOL}_{day.isoformat()}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["epoch", "quote"])
        writer.writeheader()
        writer.writerows(ticks)
    return path


async def download_range(start: date, end: date, skip_existing: bool = True) -> None:
    """Baixa ticks dia a dia num intervalo de datas."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    current = start
    total_days = (end - start).days + 1
    done = 0

    print(f"📥 Baixando {SYMBOL} de {start} a {end} ({total_days} dias)...")
    print(f"   APP_ID={APP_ID} | Token={'SIM' if TOKEN else 'NÃO'}")
    print()

    while current <= end:
        path = DATA_DIR / f"ticks_{SYMBOL}_{current.isoformat()}.csv"

        if skip_existing and path.exists() and path.stat().st_size > 1000:
            existing_lines = sum(1 for _ in path.open()) - 1  # -1 header
            print(f"  ✅ {current} — já existe ({existing_lines:,} ticks), pulando")
            current += timedelta(days=1)
            done += 1
            continue

        # Pula fins de semana (mercados Deriv 24/7 mas verifica)
        print(f"  ⬇️  {current} [{done+1}/{total_days}] baixando...", end=" ", flush=True)
        t0 = time.time()

        try:
            ticks = await download_day(SYMBOL, current)

            if len(ticks) < 100:
                print(f"⚠️  apenas {len(ticks)} ticks — mercado fechado ou sem dados, pulando")
            else:
                saved_path = save_day_csv(current, ticks, DATA_DIR)
                elapsed = time.time() - t0
                print(f"✅ {len(ticks):,} ticks em {elapsed:.1f}s → {saved_path.name}")

        except Exception as e:
            print(f"❌ Erro: {e}")
            # Aguarda antes de continuar para não sobrecarregar
            await asyncio.sleep(5)

        current += timedelta(days=1)
        done += 1
        # Rate limiting entre dias
        await asyncio.sleep(0.5)

    print(f"\n🏁 Concluído! {done}/{total_days} dias processados.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Baixa ticks históricos do BOOM1000 da Deriv por data (dia a dia)."
    )
    parser.add_argument(
        "--start", type=date.fromisoformat, default="2026-01-01",
        help="Data de início (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=date.fromisoformat, default="2026-04-30",
        help="Data de fim (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="Pula dias que já têm CSV salvo (padrão: True)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Força re-download mesmo se já existir"
    )
    parser.add_argument(
        "--symbol", default=SYMBOL,
        help=f"Símbolo Deriv (padrão: {SYMBOL})"
    )

    args = parser.parse_args()
    skip = args.skip_existing and not args.force

    global SYMBOL
    SYMBOL = args.symbol

    asyncio.run(download_range(args.start, args.end, skip_existing=skip))


if __name__ == "__main__":
    main()
