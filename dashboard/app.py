import os
import re
import subprocess
import time as _time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Pegasus Dashboard")

BASE = Path("/opt/pegasus")
TRADES_CSV = BASE / "logs" / "trades.csv"
TRADES_LOG = BASE / "logs" / "trades.log"
ENV_FILE = BASE / ".env"
SCREEN_BOT = "pegasus"
VENV_PYTHON = str(BASE / ".venv/bin/python")


def _bot_running() -> bool:
    r = subprocess.run(["pgrep", "-f", "python.*bot.py"], capture_output=True)
    return r.returncode == 0


# Cache: balance changes only at login, so refresh at most every 60s
_balance_cache: tuple[float, str] = (0.0, "—")


def _search_log_backward(pattern: bytes, chunk: int = 65536) -> str:
    """Scan log backward in chunks; return first line containing pattern from EOF."""
    if not TRADES_LOG.exists():
        return ""
    try:
        with TRADES_LOG.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            buf = b""
            pos = size
            while pos > 0:
                read = min(chunk, pos)
                pos -= read
                f.seek(pos)
                data = f.read(read) + buf
                idx = data.rfind(pattern)
                if idx != -1:
                    eol = data.find(b"\n", idx)
                    line = data[idx: eol if eol != -1 else len(data)]
                    return line.decode("utf-8", errors="replace")
                # Keep tail bytes in case pattern spans a chunk boundary
                buf = data[: len(pattern) - 1]
    except Exception:
        pass
    return ""


def _last_balance() -> str:
    global _balance_cache
    now = _time.monotonic()
    # saldo= only appears at login — cache for 60 s to avoid repeated full scans
    if now - _balance_cache[0] < 60.0 and _balance_cache[1] != "—":
        return _balance_cache[1]
    line = _search_log_backward(b"saldo=")
    if not line:
        return "—"
    m = re.search(r"saldo=([\d.]+)", line)
    val = m.group(1) if m else "—"
    if val != "—":
        _balance_cache = (now, val)
    return val


def _last_p_loss() -> float | None:
    line = _search_log_backward(b"P(LOSS)=")
    if not line:
        return None
    try:
        m = re.search(r"P\(LOSS\)=([\d.]+)", line)
        return float(m.group(1)) if m else None
    except Exception:
        return None


def _read_log_tail(n_bytes: int = 65536) -> str:
    """Read the last n_bytes of trades.log for the live-log endpoint."""
    if not TRADES_LOG.exists():
        return ""
    try:
        with TRADES_LOG.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - n_bytes))
            return f.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _today_df() -> pd.DataFrame:
    if not TRADES_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(TRADES_CSV, parse_dates=["timestamp"])
    today = datetime.now(timezone.utc).date()
    return df[df["timestamp"].dt.date == today]


def _get_env(key: str) -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith(key + "="):
            return line.split("=", 1)[1].strip()
    return None


def _set_env(key: str, value: str) -> None:
    text = ENV_FILE.read_text() if ENV_FILE.exists() else ""
    lines = text.splitlines()
    new_lines = []
    found = False
    for line in lines:
        if line.startswith(key + "="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}")
    ENV_FILE.write_text("\n".join(new_lines) + "\n")


def _restart_bot() -> None:
    subprocess.run(["screen", "-S", SCREEN_BOT, "-X", "quit"], capture_output=True)
    import time; time.sleep(1)
    cmd = (
        f"cd {BASE} && {VENV_PYTHON} bot.py 2>&1 | tee -a logs/trades.log"
    )
    subprocess.run(["screen", "-dmS", SCREEN_BOT, "bash", "-c", cmd])


@app.get("/api/status")
def api_status(response: Response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    df = _today_df()
    wins = int((df["result"] == "WIN").sum()) if not df.empty else 0
    losses = int((df["result"] == "LOSS").sum()) if not df.empty else 0
    total = wins + losses
    pnl = round(float(df["profit"].sum()), 4) if not df.empty else 0.0
    last_ts = df["timestamp"].max().isoformat() if not df.empty else None
    return {
        "running": _bot_running(),
        "balance": _last_balance(),
        "wins": wins,
        "losses": losses,
        "total": total,
        "winrate": round(wins / total * 100, 1) if total else 0.0,
        "pnl": pnl,
        "p_loss": _last_p_loss(),
        "last_trade_ts": last_ts,
        "block_weekends": _get_env("BLOCK_WEEKENDS") == "true",
        "use_ensemble": _get_env("USE_ENSEMBLE") == "true",
        "ensemble_min_prob": _get_env("ENSEMBLE_MIN_PROB") or "0.294",
        "stake": _get_env("STAKE") or "1.00",
    }


@app.post("/api/bot/start")
def bot_start():
    if _bot_running():
        return {"ok": True, "msg": "Bot já estava rodando."}
    _restart_bot()
    return {"ok": True, "msg": "Bot iniciado."}


@app.post("/api/bot/stop")
def bot_stop():
    subprocess.run(["screen", "-S", SCREEN_BOT, "-X", "quit"], capture_output=True)
    subprocess.run(["pkill", "-f", "python.*bot.py"], capture_output=True)
    return {"ok": True, "msg": "Bot parado."}


@app.post("/api/bot/restart")
def bot_restart():
    _restart_bot()
    return {"ok": True, "msg": "Bot reiniciado."}


class EnvUpdate(BaseModel):
    key: str
    value: str

ALLOWED_KEYS = {
    "BLOCK_WEEKENDS", "USE_ENSEMBLE", "ENSEMBLE_MIN_PROB",
    "MAX_LOSS_PER_DAY", "MAX_PROFIT_PER_DAY", "STAKE",
    "MAX_TICK_LATENCY_MS", "LOG_LEVEL",
}

@app.post("/api/env")
def update_env(body: EnvUpdate):
    if body.key not in ALLOWED_KEYS:
        raise HTTPException(status_code=400, detail=f"Chave não permitida: {body.key}")
    _set_env(body.key, body.value)
    return {"ok": True, "key": body.key, "value": body.value}


@app.get("/api/trades")
def api_trades():
    if not TRADES_CSV.exists():
        return []
    df = pd.read_csv(TRADES_CSV)
    if "timestamp" in df.columns:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        df = df[df["timestamp"].astype(str).str.startswith(today_str)]
    if df.empty:
        return []
    cols = ["timestamp", "result", "profit", "score", "stake", "held_ticks"]
    available = [c for c in cols if c in df.columns]
    return df[available].tail(50).iloc[::-1].fillna("").to_dict(orient="records")


@app.get("/api/logs")
def api_logs():
    text = _read_log_tail(65536)  # last 64KB is plenty for 100 log lines
    if not text:
        return {"lines": []}
    lines = text.splitlines()
    # Drop first (possibly partial) line from the middle of the tail read
    if len(lines) > 1:
        lines = lines[1:]
    return {"lines": lines[-100:]}


@app.get("/", response_class=HTMLResponse)
@app.head("/")
def root():
    content = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(
        content=content,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )
