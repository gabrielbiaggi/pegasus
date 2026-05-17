import os
import re
import subprocess
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


def _last_balance() -> str:
    if not TRADES_LOG.exists():
        return "—"
    try:
        text = TRADES_LOG.read_text(encoding="utf-8", errors="replace")
        m = re.findall(r"saldo=([\d.]+)", text)
        return m[-1] if m else "—"
    except Exception:
        return "—"


def _last_p_loss() -> float | None:
    if not TRADES_LOG.exists():
        return None
    try:
        text = TRADES_LOG.read_text(encoding="utf-8", errors="replace")
        m = re.findall(r"P\(LOSS\)=([\d.]+)", text)
        return float(m[-1]) if m else None
    except Exception:
        return None


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
    return {
        "running": _bot_running(),
        "balance": _last_balance(),
        "wins": wins,
        "losses": losses,
        "total": total,
        "winrate": round(wins / total * 100, 1) if total else 0.0,
        "pnl": pnl,
        "p_loss": _last_p_loss(),
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
    cols = ["timestamp", "result", "profit", "score", "stake", "held_ticks"]
    available = [c for c in cols if c in df.columns]
    return df[available].tail(50).iloc[::-1].fillna("").to_dict(orient="records")


@app.get("/api/logs")
def api_logs():
    if not TRADES_LOG.exists():
        return {"lines": []}
    lines = TRADES_LOG.read_text(encoding="utf-8", errors="replace").splitlines()
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
