import asyncio
import json
import os
import re
import subprocess
import time as _time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import sys

# Adiciona o diretório pai (raiz do projeto) ao path para importar deriv_auth
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deriv_auth import check_token_expiry

app = FastAPI(title="Pegasus Dashboard")

BASE = Path("/opt/pegasus")
TRADES_CSV = BASE / "logs" / "trades.csv"
TRADES_LOG = BASE / "logs" / "trades.log"
BALANCE_JSON = BASE / "logs" / "balance.json"
ENV_FILE = BASE / ".env"
SCREEN_BOT = "pegasus"
VENV_PYTHON = str(BASE / ".venv/bin/python")


_bot_running_cache: tuple[float, bool] = (0.0, False)


def _bot_running() -> bool:
    global _bot_running_cache
    now = _time.monotonic()
    if now - _bot_running_cache[0] < 5.0:
        return _bot_running_cache[1]
    r = subprocess.run(["pgrep", "-f", "python.*bot.py"], capture_output=True)
    result = r.returncode == 0
    _bot_running_cache = (now, result)
    return result


# Cache: balance changes only at login, so refresh at most every 60s
_balance_cache: tuple[float, str] = (0.0, "—")
# Fast balance cache: backed by logs/balance.json written by bot on every balance_after event
_balance_fast_cache: tuple[float, str] = (0.0, "—")


def _read_balance_fast() -> str:
    """Read balance from logs/balance.json (written by bot on every buy/sell/balance event).
    Cache TTL = 0.5s. Falls back to log scanning if file absent."""
    global _balance_fast_cache
    now = _time.monotonic()
    if now - _balance_fast_cache[0] < 0.5 and _balance_fast_cache[1] != "—":
        return _balance_fast_cache[1]
    try:
        data = json.loads(BALANCE_JSON.read_text())
        val = str(round(float(data["balance"]), 2))
        _balance_fast_cache = (now, val)
        return val
    except Exception:
        pass
    # fallback to slow log scan
    return _last_balance()


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
                    line = data[idx : eol if eol != -1 else len(data)]
                    return line.decode("utf-8", errors="replace")
                # Keep tail bytes in case pattern spans a chunk boundary
                buf = data[: len(pattern) - 1]
    except Exception:
        pass
    return ""


def _last_balance() -> str:
    global _balance_cache
    now = _time.monotonic()
    if now - _balance_cache[0] < 10.0 and _balance_cache[1] != "—":
        return _balance_cache[1]
    # saldo_estimado= is logged on every WIN/LOSS (most up-to-date)
    line = _search_log_backward(b"saldo_estimado=")
    if line:
        m = re.search(r"saldo_estimado=([\d.]+)", line)
        val = m.group(1) if m else "—"
    else:
        # Fallback: saldo= only appears at login
        line = _search_log_backward(b"saldo=")
        if not line:
            return "—"
        m = re.search(r"saldo=([\d.]+)", line)
        val = m.group(1) if m else "—"
    if val != "—":
        _balance_cache = (now, val)
    return val


def _read_risk_state() -> dict:
    path = BASE / "logs" / "risk_state.json"
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}


def _pg_dsn_str() -> str:
    return _get_env("PG_DSN") or "postgresql://pegasus:pegasus@localhost/pegasus_db"


def _last_jump_signal() -> dict:
    """Parse bot log for the latest JumpMom signal info."""
    line = _search_log_backward(b"JumpMom ")
    if not line:
        return {}
    m = re.search(
        r"JumpMom (\w+):\s*\S*?(\d+)\s*\S*?(\d+)\s*\(conf=(\d+)%,\s*(\d+)/(\d+)\s*votes\)",
        line,
    )
    if not m:
        m = re.search(r"JumpMom (\w+): up=(\d+) dn=(\d+) conf=([\d.]+)", line)
        if not m:
            return {}
        return {
            "type": "jump",
            "direction": m.group(1),
            "votes_up": int(m.group(2)),
            "votes_down": int(m.group(3)),
            "confidence": float(m.group(4)),
        }
    return {
        "type": "jump",
        "direction": m.group(1),
        "votes_up": int(m.group(2)),
        "votes_down": int(m.group(3)),
        "confidence": float(m.group(4)),
        "votes_for": int(m.group(5)),
        "votes_total": int(m.group(6)),
    }


def _last_calm_accu_signal() -> dict:
    """Parse bot log for the latest Calm ACCU signal info."""
    # Prefer CALM ACCU ENTRY (has score/H/cusum/P(LOSS)) over Setup line (has stake/mode only).
    # Read both and merge: ENTRY for indicators, Setup for stake/mode.
    line_entry = _search_log_backward(b"CALM ACCU ENTRY")
    line_setup = _search_log_backward(b"Setup CALM ACCU")
    line = line_entry or line_setup
    if not line:
        return {}

    # Parse from the richer line (ENTRY has indicators; Setup has stake/mode).
    # Merge both when available so we get all fields.
    # "CALM ACCU ENTRY: score=29/37 | H=0.482 | cusum=5.39 | P(LOSS)=0.0047"
    # "Setup CALM ACCU detectado: score=29 stake=6.52 modo=SOROS 1/3"
    score, score_max, p_loss, hurst, cusum, stake, mode = (
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )

    for src in [l for l in [line_entry, line_setup] if l]:
        if score is None:
            m = re.search(r"score=(\d+)(?:/(\d+))?", src)
            if m:
                score = int(m.group(1))
                score_max = int(m.group(2)) if m.group(2) else None
        if p_loss is None:
            m = re.search(r"P\(LOSS\)=([\d.e+-]+)", src)
            if m:
                p_loss = float(m.group(1))
        if hurst is None:
            m = re.search(r"H=([\d.]+)", src)
            if m:
                hurst = float(m.group(1))
        if cusum is None:
            m = re.search(r"cusum=([\d.]+)", src)
            if m:
                cusum = float(m.group(1))
        if stake is None:
            m = re.search(r"stake=([\d.]+)", src)
            if m:
                stake = float(m.group(1))
        if mode is None:
            m = re.search(r"modo=(\S+)", src)
            if m:
                mode = m.group(1)

    if score is None:
        return {}

    return {
        "type": "calm_accu",
        "direction": "ACCU",
        "score": score,
        "score_max": score_max or 37,
        "p_loss": round(p_loss, 6) if p_loss is not None else None,
        "hurst": round(hurst, 3) if hurst is not None else None,
        "cusum": round(cusum, 2) if cusum is not None else None,
        "stake": stake,
        "mode": mode,
        "ensemble_min_prob": float(_get_env("ENSEMBLE_MIN_PROB") or "0.30"),
        # confidence = 1 - P(LOSS), scaled 0-100 for the bar
        "confidence": round((1.0 - p_loss) * 100, 1) if p_loss is not None else None,
    }


def _last_signal() -> dict:
    """Return the latest signal for the active contract mode."""
    mode = _get_env("CONTRACT_MODE") or "accumulator"
    if mode in ("calm_accu", "accumulator"):
        return _last_calm_accu_signal()
    return _last_jump_signal()


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


_pg_trades_cache: dict = {"ts": 0.0, "today": "", "df": None}


def _today_df() -> pd.DataFrame:
    """Read today's trades from PostgreSQL or local CSV fallback (no session restart filter)."""
    global _pg_trades_cache
    now = _time.monotonic()
    
    # 1. Calcule o início do dia local do usuário em UTC
    try:
        tz_offset = int(_get_env("USER_TZ_OFFSET") or "-3")
    except Exception:
        tz_offset = -3
        
    from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone
    utc_now = _datetime.now(_timezone.utc)
    local_now = utc_now + _timedelta(hours=tz_offset)
    local_today = local_now.date()
    local_midnight = _datetime(local_today.year, local_today.month, local_today.day)
    start_utc = local_midnight - _timedelta(hours=tz_offset)
    
    # Utiliza o cache se tiver sido atualizado há menos de 1 segundo
    cache_key = local_today.isoformat()
    if (
        now - _pg_trades_cache["ts"] < 1.0
        and _pg_trades_cache.get("today") == cache_key
        and _pg_trades_cache["df"] is not None
    ):
        return _pg_trades_cache["df"]

    df = pd.DataFrame()
    pg_ok = False
    
    # 2. Tenta ler do PostgreSQL
    try:
        conn = psycopg2.connect(_pg_dsn_str())
        df = pd.read_sql(
            "SELECT * FROM trades WHERE timestamp >= %s ORDER BY timestamp",
            conn,
            params=(start_utc,),
        )
        conn.close()
        pg_ok = True
    except Exception as exc:
        pass

    # 3. Tenta ler do CSV local se o DB falhar ou retornar vazio
    if df.empty:
        csv_file = BASE / "logs" / "trades.csv"
        if csv_file.exists():
            try:
                df_csv = pd.read_csv(csv_file)
                if not df_csv.empty and "timestamp" in df_csv.columns:
                    df_csv["timestamp"] = pd.to_datetime(df_csv["timestamp"]).dt.tz_localize(None)
                    # Filtra operações do dia de hoje local
                    cutoff = pd.Timestamp(start_utc).tz_localize(None)
                    df_filtered = df_csv[df_csv["timestamp"] >= cutoff]
                    if not df_filtered.empty:
                        df = df_filtered.copy()
            except Exception as e:
                pass

    _pg_trades_cache = {"ts": now, "today": cache_key, "df": df}
    return df


_env_cache: dict = {"mtime": -1.0, "data": {}}


def _load_env() -> dict[str, str]:
    global _env_cache
    if not ENV_FILE.exists():
        return {}
    try:
        mtime = ENV_FILE.stat().st_mtime
        if mtime == _env_cache["mtime"]:
            return _env_cache["data"]
        data: dict[str, str] = {}
        for line in ENV_FILE.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()
        _env_cache = {"mtime": mtime, "data": data}
        return data
    except Exception:
        return {}


def _get_env(key: str) -> str | None:
    return _load_env().get(key)


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
    _env_cache["mtime"] = -1.0  # invalidate cache


def _restart_bot() -> None:
    subprocess.run(["screen", "-S", SCREEN_BOT, "-X", "quit"], capture_output=True)
    import time

    time.sleep(1)
    # Limpa sessões mortas antes de criar nova
    subprocess.run(["screen", "-wipe"], capture_output=True)
    cmd = f"cd {BASE} && PYTHONUNBUFFERED=1 {VENV_PYTHON} -u bot.py 2>&1 | tee -a logs/bot.log"
    subprocess.run(["screen", "-dmS", SCREEN_BOT, "bash", "-c", cmd])


def _compute_max_loss_day(risk_state: dict) -> float:
    """Return max daily loss threshold — prefers STOP_LOSS_PCT (% of balance) over fixed $."""
    pct = float(_get_env("STOP_LOSS_PCT") or "0")
    if pct > 0:
        bal_str = _read_balance_fast()
        try:
            bal = float(bal_str)
        except (ValueError, TypeError):
            bal = 10000
        net = float(risk_state.get("daily_net_profit", 0.0))
        start_bal = bal - net
        return round(start_bal * pct / 100.0, 2)
    return float(_get_env("MAX_LOSS_PER_DAY") or "500")


def _compute_risk_blocked(risk_state: dict) -> bool:
    # Check if loss_block_override is active — user unblocked via dashboard.
    if risk_state.get("loss_block_override", False):
        return False
    return float(risk_state.get("daily_net_profit", 0.0)) <= -_compute_max_loss_day(
        risk_state
    )


def _get_payout_rate() -> float:
    """Payout rate for Martingale formula. Uses MARTINGALE_PAYOUT_RATE or TP%/100."""
    v = _get_env("MARTINGALE_PAYOUT_RATE")
    if v:
        return max(0.001, float(v))
    tp = _get_env("ACCUMULATOR_TAKE_PROFIT_PERCENT")
    if tp:
        return max(0.001, float(tp) / 100.0)
    return 0.15


def _initial_balance() -> float:
    """Starting capital for P&L-total calculation. Reads INITIAL_BALANCE env (default 10000)."""
    v = _get_env("INITIAL_BALANCE")
    return float(v) if v else 10000.0


def _compute_next_gale_stake(risk_state: dict) -> float:
    """Recovery stake for the next gale trade based on current accumulated loss."""
    base = float(risk_state.get("martingale_base_stake", 0.0))
    accum = float(risk_state.get("martingale_accumulated_loss", 0.0))
    step = int(risk_state.get("martingale_step", 0))
    if step == 0 or base == 0:
        return 0.0
    payout = _get_payout_rate()
    return round(accum / payout + base, 2)


def _compute_gale_effective_multiplier(risk_state: dict) -> float:
    """Effective stake multiplier = next_gale_stake / base_stake."""
    base = float(risk_state.get("martingale_base_stake", 0.0))
    step = int(risk_state.get("martingale_step", 0))
    if step == 0:
        # Show theoretical first-gale multiplier
        payout = _get_payout_rate()
        return round(1.0 / payout + 1.0, 2)
    if base == 0.0:
        return 1.0
    next_stake = _compute_next_gale_stake(risk_state)
    if next_stake == 0.0:
        return 1.0
    return round(next_stake / base, 2)


@app.get("/api/status")
def api_status(response: Response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    try:
        token_info = check_token_expiry()
    except Exception as e:
        token_info = {"status": "error", "message": str(e), "days_remaining": -1}
    df = _today_df()
    wins = int((df["result"] == "WIN").sum()) if not df.empty else 0
    losses = int((df["result"] == "LOSS").sum()) if not df.empty else 0
    total = wins + losses
    last_ts = df["timestamp"].max().isoformat() if not df.empty else None
    risk_state = _read_risk_state()
    # P&L = balance - session start balance (from risk_state.json)
    pnl = round(float(risk_state.get("daily_net_profit", 0.0)), 2)
    bal_str = _read_balance_fast()
    try:
        bal_float = float(bal_str)
    except (ValueError, TypeError):
        bal_float = 0.0
    ini_bal = float(risk_state.get("start_of_day_balance", 0)) or _initial_balance()
    pnl_total = round(bal_float - ini_bal, 2) if bal_float > 0 else None
    signal = _last_signal()  # mode-aware: calm_accu or jump_rise_fall
    return {
        "running": _bot_running(),
        "balance": _read_balance_fast(),
        "symbol": _get_env("SYMBOL") or "1HZ100V",
        "deriv_app_id": _get_env("DERIV_APP_ID") or "1089",
        "contract_mode": _get_env("CONTRACT_MODE") or "accumulator",
        "wins": wins,
        "losses": losses,
        "total": total,
        "winrate": round(wins / total * 100, 1) if total else 0.0,
        "pnl": pnl,
        "last_trade_ts": last_ts,
        "block_weekends": _get_env("BLOCK_WEEKENDS") == "true",
        "block_hours_enabled": (_get_env("BLOCK_HOURS_ENABLED") or "true")
        .strip()
        .lower()
        != "false",
        "blocked_utc_hours": _get_env("BLOCKED_UTC_HOURS") or "5,6,7,8,9",
        "stake": _get_env("STAKE") or "50.00",
        "use_soros": _get_env("USE_SOROS") == "true",
        "soros_max_steps": _get_env("SOROS_MAX_STEPS") or "3",
        "soros_profit_factor": _get_env("SOROS_PROFIT_FACTOR") or "1.0",
        "max_stake": _get_env("MAX_STAKE") or "1000",
        "risk_blocked": _compute_risk_blocked(risk_state),
        "max_loss_per_day": _compute_max_loss_day(risk_state),
        "max_profit_per_day": float(_get_env("MAX_PROFIT_PER_DAY") or "0"),
        "daily_loss": round(risk_state.get("daily_loss", 0.0), 2),
        "daily_net_profit": round(float(risk_state.get("daily_net_profit", 0.0)), 2),
        "soros_step": int(risk_state.get("soros_step", 0)),
        "soros_profit": round(float(risk_state.get("soros_profit", 0.0)), 2),
        "martingale_step": int(risk_state.get("martingale_step", 0)),
        "consecutive_losses": int(risk_state.get("consecutive_losses", 0)),
        "use_martingale": _get_env("USE_MARTINGALE") == "true",
        "martingale_max_gales": int(_get_env("MARTINGALE_MAX_GALES") or "6"),
        "martingale_mode": _get_env("MARTINGALE_MODE") or "classic",
        "martingale_payout_rate": _get_payout_rate(),
        "martingale_accumulated_loss": round(
            float(risk_state.get("martingale_accumulated_loss", 0.0)), 2
        ),
        "martingale_base_stake": round(
            float(risk_state.get("martingale_base_stake", 0.0)), 2
        ),
        "martingale_effective_multiplier": _compute_gale_effective_multiplier(
            risk_state
        ),
        "next_gale_stake": _compute_next_gale_stake(risk_state),
        "pnl_total": pnl_total,
        "initial_balance": ini_bal,
        "signal": signal,
        "stop_loss_pct": float(_get_env("STOP_LOSS_PCT") or "0"),
        "stop_gain_pct": float(_get_env("STOP_GAIN_PCT") or "0"),
        "stake_pct": round(float(_get_env("DYNAMIC_STAKE_BASE_PCT") or "0") * 100, 2),
        "stake_value": float(_get_env("STAKE") or "0"),
        "stop_loss_value": round(
            bal_float * float(_get_env("STOP_LOSS_PCT") or "0") / 100, 2
        )
        if float(_get_env("STOP_LOSS_PCT") or "0") > 0
        else float(_get_env("MAX_LOSS_PER_DAY") or "0"),
        "stop_gain_value": round(
            bal_float * float(_get_env("STOP_GAIN_PCT") or "0") / 100, 2
        )
        if float(_get_env("STOP_GAIN_PCT") or "0") > 0
        else float(_get_env("MAX_PROFIT_PER_DAY") or "0"),
        "account_mode": _get_env("ACCOUNT_MODE") or "demo",
        "calm_accu_threshold": _get_env("CALM_ACCU_THRESHOLD") or "7.3e-7",
        "calm_accu_lookback": _get_env("CALM_ACCU_LOOKBACK") or "10",
        "ensemble_min_prob": float(_get_env("ENSEMBLE_MIN_PROB") or "0.30"),
        "accumulator_min_hurst_exponent": float(_get_env("ACCUMULATOR_MIN_HURST_EXPONENT") or "0.45"),
        "calm_accu_max_entry_cusum": float(_get_env("CALM_ACCU_MAX_ENTRY_CUSUM") or "5.0"),
        "token_info": token_info,
    }


@app.get("/api/balance")
def api_balance(response: Response):
    """Lightweight endpoint — returns current balance only. Polled every 500ms by dashboard."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    bal = _read_balance_fast()
    try:
        bal_float = float(bal)
    except (ValueError, TypeError):
        bal_float = 0.0
    risk_state = _read_risk_state()
    ini_bal = float(risk_state.get("start_of_day_balance", 50.0)) or _initial_balance()
    daily_net_profit = round(float(risk_state.get("daily_net_profit", 0.0)), 2)
    virtual_balance = round(ini_bal + daily_net_profit, 2)
    pnl_total = round(bal_float - ini_bal, 2) if bal_float > 0 else None
    return {
        "balance": bal,
        "pnl_total": pnl_total,
        "initial_balance": ini_bal,
        "virtual_balance": virtual_balance,
        "daily_net_profit": daily_net_profit
    }


@app.post("/api/fetch-balance")
async def fetch_balance_from_deriv():
    """Connect to Deriv API with current DERIV_TOKEN, authorize, fetch live balance.
    Writes to balance.json so dashboard updates immediately."""
    import websockets

    token = _get_env("DERIV_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="DERIV_TOKEN não configurado")
    app_id = _get_env("DERIV_APP_ID") or "1089"
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    try:
        async with websockets.connect(uri) as conn:
            await conn.send(json.dumps({"authorize": token}))
            resp = json.loads(await conn.recv())
            if "error" in resp:
                raise HTTPException(
                    status_code=400, detail=resp["error"].get("message", "Auth failed")
                )
            auth = resp.get("authorize", {})
            bal = auth.get("balance", 0)
            # Persist to balance.json
            BALANCE_JSON.write_text(json.dumps({"balance": bal}))
            # Invalidate fast cache
            global _balance_fast_cache
            _balance_fast_cache = (0.0, "—")
            return {
                "balance": bal,
                "currency": auth.get("currency", "USD"),
                "account_type": auth.get("is_virtual") and "demo" or "real",
                "loginid": auth.get("loginid", ""),
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deriv API error: {e}")


@app.get("/api/sessions")
def api_sessions(response: Response):
    """Return per-day stats + monthly summary from PostgreSQL."""
    response.headers["Cache-Control"] = "no-store"
    try:
        conn = psycopg2.connect(_pg_dsn_str())
        df = pd.read_sql("SELECT * FROM trades ORDER BY timestamp", conn)
        conn.close()
    except Exception:
        return {"days": [], "monthly": {}}
    if df.empty:
        return {"days": [], "monthly": {}}
    df["date"] = df["timestamp"].dt.date.astype(str)
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0.0)
    days = []
    for date, grp in sorted(df.groupby("date"), reverse=True):
        wins = int((grp["result"] == "WIN").sum())
        losses = int((grp["result"] == "LOSS").sum())
        total = wins + losses
        pnl = round(float(grp["profit"].sum()), 2)
        days.append(
            {
                "date": date,
                "trades": total,
                "wins": wins,
                "losses": losses,
                "winrate": round(wins / total * 100, 1) if total else 0.0,
                "pnl": pnl,
            }
        )
    # Monthly summary (current month)
    now = datetime.now(timezone.utc)
    month_str = now.strftime("%Y-%m")
    mdf = df[df["date"].str.startswith(month_str)]
    m_wins = int((mdf["result"] == "WIN").sum())
    m_losses = int((mdf["result"] == "LOSS").sum())
    m_total = m_wins + m_losses
    m_pnl = round(float(mdf["profit"].sum()), 2)
    best_day = max(days, key=lambda d: d["pnl"]) if days else {}
    worst_day = min(days, key=lambda d: d["pnl"]) if days else {}
    monthly = {
        "month": month_str,
        "trades": m_total,
        "wins": m_wins,
        "losses": m_losses,
        "winrate": round(m_wins / m_total * 100, 1) if m_total else 0.0,
        "pnl": m_pnl,
        "active_days": len([d for d in days if d["date"].startswith(month_str)]),
        "best_day": best_day,
        "worst_day": worst_day,
    }
    return {"days": days, "monthly": monthly}


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


@app.post("/api/bot/unblock")
def bot_unblock():
    """Ignore the daily monetary loss limit — bot continues from current state."""
    risk_path = BASE / "logs" / "risk_state.json"
    if risk_path.exists():
        try:
            state = json.loads(risk_path.read_text())
            state["loss_block_override"] = True
            risk_path.write_text(json.dumps(state, indent=2))
        except Exception as exc:
            return {"ok": False, "msg": f"Erro ao atualizar estado: {exc}"}
    else:
        risk_path.parent.mkdir(parents=True, exist_ok=True)
        risk_path.write_text(json.dumps({"loss_block_override": True}, indent=2))
    _restart_bot()
    return {"ok": True, "msg": "Bot desbloqueado. Limite de perda ignorado."}


@app.post("/api/reset")
def api_reset(scope: str = "day", response: Response = None):
    """Reset trade history and risk state.
    scope='day'   → remove today's rows from trades.csv + reset risk_state
    scope='month' → remove current month's rows from trades.csv + reset risk_state
    """
    import shutil
    from datetime import date as _date

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    month = now.strftime("%Y-%m")

    # --- backup & filter trades.csv ---
    if TRADES_CSV.exists():
        bak = TRADES_CSV.with_suffix(f".csv.bak-reset-{now.strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(TRADES_CSV, bak)
        try:
            df = pd.read_csv(TRADES_CSV, parse_dates=["timestamp"])
            df["_date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
            if scope == "day":
                df = df[df["_date"] != today]
            elif scope == "month":
                df = df[~df["_date"].str.startswith(month)]
            df.drop(columns=["_date"]).to_csv(TRADES_CSV, index=False)
        except Exception:
            # fallback: just keep header
            header = TRADES_CSV.read_text().split("\n")[0]
            TRADES_CSV.write_text(header + "\n")

    # --- reset risk_state ---
    risk_path = BASE / "logs" / "risk_state.json"
    risk_state = {
        "day": today,
        "daily_loss": 0.0,
        "daily_net_profit": 0.0,
        "daily_peak_profit": 0.0,
        "daily_trailing_active": False,
        "trades_today": 0,
        "wins": 0,
        "losses": 0,
        "consecutive_losses": 0,
        "max_loss_streak_today": 0,
        "soros_step": 0,
        "soros_profit": 0.0,
        "martingale_step": 0,
        "martingale_accumulated_loss": 0.0,
        "martingale_base_stake": 0.0,
    }
    risk_path.write_text(json.dumps(risk_state, indent=2))

    # --- delete from PostgreSQL trades table ---
    try:
        conn = psycopg2.connect(_pg_dsn_str())
        cur = conn.cursor()
        if scope == "day":
            cur.execute("DELETE FROM trades WHERE timestamp::date = %s", (today,))
        elif scope == "month":
            cur.execute(
                "DELETE FROM trades WHERE to_char(timestamp, 'YYYY-MM') = %s", (month,)
            )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        deleted = 0

    # --- also clear signals from PostgreSQL ---
    try:
        conn = psycopg2.connect(_pg_dsn_str())
        cur = conn.cursor()
        if scope == "day":
            cur.execute("DELETE FROM signals WHERE timestamp::date = %s", (today,))
        elif scope == "month":
            cur.execute(
                "DELETE FROM signals WHERE to_char(timestamp, 'YYYY-MM') = %s", (month,)
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    # --- invalidate in-memory caches ---
    global _pg_trades_cache
    _pg_trades_cache = {"ts": 0, "today": "", "df": None}

    # --- archive & truncate trades.log ---
    if TRADES_LOG.exists() and TRADES_LOG.stat().st_size > 0:
        log_bak = TRADES_LOG.with_suffix(
            f".log.bak-reset-{now.strftime('%Y%m%d_%H%M%S')}"
        )
        shutil.copy2(TRADES_LOG, log_bak)
        TRADES_LOG.write_bytes(b"")

    # --- restart bot so it picks up fresh state ---
    _restart_bot()

    return {
        "ok": True,
        "scope": scope,
        "msg": f"Histórico '{scope}' resetado ({deleted} trades removidos do DB). Bot reiniciado.",
    }


class EnvUpdate(BaseModel):
    key: str
    value: str


ALLOWED_KEYS = {
    "BLOCK_WEEKENDS",
    "BLOCK_HOURS_ENABLED",
    "BLOCKED_UTC_HOURS",
    "STAKE",
    "ACCOUNT_MODE",
    "MAX_LOSS_PER_DAY",
    "MAX_PROFIT_PER_DAY",
    "MAX_LOSS_DAY_PCT",
    "STOP_LOSS_PCT",
    "STOP_GAIN_PCT",
    "DYNAMIC_STAKE_BASE_PCT",
    "MAX_TICK_LATENCY_MS",
    "LOG_LEVEL",
    "USE_SOROS",
    "SOROS_MAX_STEPS",
    "SOROS_PROFIT_FACTOR",
    "MAX_STAKE",
    "USE_MARTINGALE",
    "MARTINGALE_MAX_GALES",
    "MARTINGALE_PAYOUT_RATE",
    "MARTINGALE_MODE",
    "INITIAL_BALANCE",
    "CONTRACT_MODE",
    "CALM_ACCU_THRESHOLD",
    "CALM_ACCU_LOOKBACK",
}


@app.post("/api/env")
def update_env(body: EnvUpdate):
    if body.key not in ALLOWED_KEYS:
        raise HTTPException(status_code=400, detail=f"Chave não permitida: {body.key}")
    _set_env(body.key, body.value)
    # Swap tokens when account mode changes
    if body.key == "ACCOUNT_MODE":
        if body.value == "real":
            real_token = _get_env("DERIV_REAL_TOKEN") or ""
            if not real_token:
                raise HTTPException(
                    status_code=400, detail="DERIV_REAL_TOKEN não configurado no .env"
                )
            _set_env("DERIV_TOKEN", real_token)
            _set_env("ALLOW_REAL_TRADING", "true")
        else:
            demo_token = _get_env("DERIV_DEMO_TOKEN") or ""
            if not demo_token:
                raise HTTPException(
                    status_code=400, detail="DERIV_DEMO_TOKEN não configurado no .env"
                )
            _set_env("DERIV_TOKEN", demo_token)
            _set_env("ALLOW_REAL_TRADING", "false")
        # Auto-restart bot so it connects with the new token
        if _bot_running():
            _restart_bot()
    return {"ok": True, "key": body.key, "value": body.value}


@app.get("/api/trades")
def api_trades():
    df = _today_df()
    if df.empty:
        return []
    cols = [
        "timestamp",
        "direction",
        "result",
        "soros_step",
        "gale_step",
        "profit",
        "score",
        "stake",
        "held_ticks",
    ]
    available = [c for c in cols if c in df.columns]
    records = df[available].iloc[::-1].to_dict(orient="records")
    for r in records:
        if "timestamp" in r and hasattr(r["timestamp"], "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
        for k, v in list(r.items()):
            if v is None or (isinstance(v, float) and v != v):
                r[k] = ""
    return records


@app.get("/api/logs")
def api_logs():
    text = _read_log_tail(524288)  # last 512KB — unlimited scrollable in dashboard
    if not text:
        return {"lines": []}
    lines = text.splitlines()
    # Drop first (possibly partial) line from the middle of the tail read
    if len(lines) > 1:
        lines = lines[1:]
    return {"lines": lines}


def _compute_regime(ind: dict) -> dict:
    """Calcula status do regime de mercado para o dashboard.

    Retorna: status (ok|wait|block), cusum, hurst, shannon, kalman, p_loss, regime, etc.
    """
    cusum = float(ind.get("cusum_score") or 0.0)
    hurst = float(ind.get("hurst_exponent") or 0.5)
    shannon = float(ind.get("shannon_entropy") or 0.0)
    kalman = float(ind.get("kalman_residual_zscore") or 0.0)
    avg_ret = float(ind.get("avg_ret") or 0.0)
    p_loss = float(ind.get("p_loss") if ind.get("p_loss") is not None else 1.0)
    
    max_cusum = float(_get_env("CALM_ACCU_MAX_ENTRY_CUSUM") or "5.0")
    min_hurst = float(_get_env("ACCUMULATOR_MIN_HURST_EXPONENT") or "0.45")

    blocked_by = []
    if max_cusum > 0 and cusum > max_cusum:
        blocked_by.append(f"CUSUM={cusum:.2f} > limite={max_cusum:.1f}")
    if min_hurst > 0 and hurst < min_hurst:
        blocked_by.append(f"Hurst={hurst:.3f} < limite={min_hurst:.2f}")

    # Identifica os regimes de calmaria de forma idêntica ao bot
    is_absolute_calm = False
    is_medium_calm = False
    
    _pass_a_xgb = (p_loss < 0.22)
    if (
        avg_ret < 1.0e-6
        and cusum < 2.5
        and hurst > 0.48
        and shannon > 0.85
        and abs(kalman) < 1.5
        and _pass_a_xgb
    ):
        is_absolute_calm = True
        
    _pass_b_plus_xgb = (p_loss < 0.26)
    if (
        avg_ret < 2.2e-6
        and cusum < 4.0
        and hurst > 0.45
        and _pass_b_plus_xgb
    ):
        is_medium_calm = True

    # Rótulos de regime dinâmicos com base nos parâmetros atuais do .env
    regime_tp = float(_get_env("ACCUMULATOR_TAKE_PROFIT_PERCENT") or "30.0")
    regime_hold = int(_get_env("ACCUMULATOR_MAX_HOLD_TICKS") or "9")
    regime_b_plus_tp = float(_get_env("PCS_REGIME_B_PLUS_TP") or "9.0")
    regime_b_plus_hold = int(_get_env("PCS_REGIME_B_PLUS_HOLD") or "3")
    regime_b_minus_tp = float(_get_env("PCS_REGIME_B_MINUS_TP") or "3.0")
    regime_b_minus_hold = int(_get_env("PCS_REGIME_B_MINUS_HOLD") or "1")

    if blocked_by:
        status = "wait"
        regime_label = "Bloqueado / Aguardando Calmaria"
        regime_color = "var(--red)"
    else:
        status = "ok"
        if is_absolute_calm:
            regime_label = f"🔥 Regime A: Sniper Pro ({regime_tp:.1f}% TP, {regime_hold} Ticks, Soros ATIVO)"
            regime_color = "#10b981"  # emerald green
        elif is_medium_calm:
            regime_label = f"🌾 Regime B+: Medium Harvester ({regime_b_plus_tp:.1f}% TP, {regime_b_plus_hold} Ticks, Soros OFF)"
            regime_color = "#3b82f6"  # bright blue
        else:
            regime_label = f"🛡️ Regime B-: Defensive ({regime_b_minus_tp:.1f}% TP, {regime_b_minus_hold} Ticks, Soros OFF)"
            regime_color = "#eab308"  # amber/yellow

    return {
        "status": status,
        "cusum": round(cusum, 2),
        "hurst": round(hurst, 3),
        "shannon": round(shannon, 3),
        "kalman": round(kalman, 3),
        "avg_ret": avg_ret,
        "p_loss": p_loss,
        "max_cusum": max_cusum,
        "min_hurst": min_hurst,
        "blocked_by": blocked_by,
        "label": regime_label,
        "color": regime_color
    }


@app.get("/api/regime")
def api_regime(response: Response):
    """Endpoint leve só para regime do mercado. Polled a cada 1s pelo dashboard."""
    response.headers["Cache-Control"] = "no-store, no-cache"
    live_path = BASE / "logs" / "live_indicators.json"
    try:
        if live_path.exists():
            data = json.loads(live_path.read_text())
            if data:
                return _compute_regime(data)
    except Exception:
        pass
    return {
        "status": "unknown",
        "label": "Sem dados",
        "cusum": 0,
        "hurst": 0,
        "blocked_by": [],
    }


@app.get("/api/indicators")
def api_indicators(response: Response):
    """Return latest signal indicators — prefer live file from bot, fallback to DB."""
    response.headers["Cache-Control"] = "no-store"
    result = {"signal": _last_jump_signal()}

    # 1) Try live_indicators.json (updated every tick by the bot)
    live_path = BASE / "logs" / "live_indicators.json"
    try:
        if live_path.exists():
            data = json.loads(live_path.read_text())
            if data:
                result["latest_signal_indicators"] = data
                result["regime"] = _compute_regime(data)
                return result
    except Exception:
        pass

    # 2) Fallback: last signal row from PostgreSQL
    try:
        conn = psycopg2.connect(_pg_dsn_str())
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, direction, score,
                   bb_width_percent, tick_atr_percent, recent_move_percent,
                   hurst_exponent, tick_imbalance, hawkes_intensity,
                   velocity_zscore, acceleration_zscore, pmi_distance_percent,
                   markov_p_up_given_up, markov_p_down_given_down,
                   shannon_entropy, kalman_residual_zscore,
                   bayesian_prob_up, renyi_entropy, fisher_information,
                   wavelet_energy_ratio, cusum_score, tail_dependence, mi_flow
            FROM signals ORDER BY id DESC LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            data = {}
            for c, v in zip(cols, row):
                if hasattr(v, "isoformat"):
                    data[c] = v.isoformat()
                elif isinstance(v, float) and v != v:
                    data[c] = None
                else:
                    data[c] = v
            result["latest_signal_indicators"] = data
        cur.close()
        conn.close()
    except Exception:
        pass
    return result


@app.get("/api/backtest")
def api_backtest(response: Response):
    """Retorna o último backtest real (Monte Carlo removido — só dados reais)."""
    response.headers["Cache-Control"] = "no-store"
    return backtest_status(response)


# ── Backtest real: run / status / stop ─────────────────────────────────────

_backtest_proc: "subprocess.Popen | None" = None


@app.post("/api/backtest/run")
def backtest_run(
    start: str = "2026-05-06",
    end: str | None = None,
    start_balance: float = 50.0,
    compounding: bool = True,
    response: Response = None,
):
    """Inicia backtest em background. Retorna imediatamente."""
    global _backtest_proc
    if response:
        response.headers["Cache-Control"] = "no-store"
    if _backtest_proc and _backtest_proc.poll() is None:
        return {"status": "already_running", "msg": "Backtest ja em andamento"}

    from datetime import date as _date

    if end is None:
        end = _date.today().isoformat()

    out_file = str(BASE / "logs" / "backtest_live.json")
    # Escreve estado inicial
    import json as _json

    with open(out_file, "w") as f:
        _json.dump(
            {
                "status": "starting",
                "start_date": start,
                "end_date": end,
                "start_balance": start_balance,
                "results": [],
                "current_day": start,
            },
            f,
        )

    cmd = [
        str(BASE / ".venv" / "bin" / "python"),
        str(BASE / "backtest_engine.py"),
        start,
        end,
        str(start_balance),
        out_file,
    ]
    
    import os
    env = os.environ.copy()
    env["BACKTEST_COMPOUNDING"] = "true" if compounding else "false"
    
    _backtest_proc = subprocess.Popen(cmd, cwd=str(BASE), env=env)
    return {"status": "started", "start": start, "end": end, "output": out_file}


@app.get("/api/backtest/status")
def backtest_status(response: Response):
    """Retorna o estado atual do backtest (polling a cada 2s)."""
    if response:
        response.headers["Cache-Control"] = "no-store"
    out_file = BASE / "logs" / "backtest_live.json"
    try:
        if out_file.exists():
            data = json.loads(out_file.read_text())
            global _backtest_proc
            if _backtest_proc and _backtest_proc.poll() is not None:
                if data.get("status") == "running":
                    data["status"] = "done"
                    out_file.write_text(json.dumps(data))
            return data
    except Exception:
        pass
    return {"status": "idle", "results": []}


@app.post("/api/backtest/stop")
def backtest_stop():
    global _backtest_proc
    if _backtest_proc:
        _backtest_proc.terminate()
        _backtest_proc = None
    return {"status": "stopped"}


# ── Optimizer Live Status ───────────────────────────────────────────────────

_OPT_LOG_PATH = BASE / ".." / ".." / ".." / ".gemini" / "antigravity-ide" / "brain" / "b252b907-47a5-4f8c-8466-4a339d25a4b5" / ".system_generated" / "tasks" / "task-4123.log"
_OPT_LOG_ALT = Path("/home/bill/.gemini/antigravity-ide/brain/b252b907-47a5-4f8c-8466-4a339d25a4b5/.system_generated/tasks/task-4123.log")


def _optimizer_running() -> bool:
    if Path("/opt/pegasus").exists():
        r = subprocess.run(["systemctl", "is-active", "pegasus-optimizer.service"], capture_output=True)
        if r.returncode == 0:
            return True
    pid_file = BASE / "logs" / "optimizer_v2.pid"
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return True
        except Exception:
            pass
    r = subprocess.run(["pgrep", "-f", "python.*optimize_loop.py"], capture_output=True)
    return r.returncode == 0


def _read_stress_config() -> bool:
    path = BASE / "logs" / "stress_config.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text())
        return bool(data.get("ultra_stress", False))
    except Exception:
        return False


def _read_optimizer_workers(logs_dir: Path, now: float | None = None) -> list[dict]:
    """Read live optimizer worker progress files, preferring current monthly workers."""
    now = now if now is not None else _time.time()
    workers: list[dict] = []

    paths = sorted(
        logs_dir.glob("backtest_worker_*.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )

    seen: set[str] = set()
    for path in paths:
        worker_id = path.stem.replace("backtest_worker_", "", 1)
        if worker_id in seen:
            continue
        seen.add(worker_id)
        try:
            stat = path.stat()
            if stat.st_size > 65536:
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            mtime = stat.st_mtime
            curr_idx = int(data.get("current_day_index", 0) or 0)
            total_days = int(data.get("total_days", 0) or 0)
            elapsed = float(data.get("elapsed_s", 0.0) or 0.0)
            progress = round((curr_idx / total_days) * 100, 1) if total_days > 0 else 0.0
            est_remaining = None
            if curr_idx > 0 and total_days > curr_idx:
                est_remaining = round((total_days - curr_idx) * (elapsed / curr_idx), 1)
            stale = (now - mtime) > 180
            month = data.get("current_month") or ""
            status = "Finalizado" if total_days and curr_idx >= total_days else "Simulando..."
            if stale and status != "Finalizado":
                status = "Stale"
            workers.append({
                "worker_id": worker_id,
                "status": status,
                "progress_pct": progress,
                "est_remaining_s": est_remaining,
                "current_day_index": curr_idx,
                "total_days": total_days,
                "current_day": data.get("current_day"),
                "current_month": month,
                "source_file": path.name,
                "last_update_ago_s": int(now - mtime),
                "stale": stale,
            })
        except Exception:
            continue

    def sort_key(worker: dict) -> tuple[int, int, str]:
        worker_id = str(worker.get("worker_id", ""))
        is_active = not worker.get("stale") and worker.get("status") != "Finalizado"
        return (0 if is_active else 1, 0 if worker_id.startswith("par_") else 1, worker_id)

    return sorted(workers, key=sort_key)


def _merge_optimizer_candidates(saved_candidates: list[dict], workers: list[dict]) -> list[dict]:
    """Merge persisted optimizer candidates with live worker telemetry."""
    merged: list[dict] = []
    seen: set[str] = set()

    for worker in workers:
        worker_id = str(worker.get("worker_id") or "")
        if not worker_id:
            continue
        saved = next(
            (
                candidate
                for candidate in saved_candidates
                if str(candidate.get("worker_id") or "") == worker_id
            ),
            {},
        )
        merged.append({**saved, **worker})
        seen.add(worker_id)

    for candidate in saved_candidates:
        worker_id = str(candidate.get("worker_id") or "")
        if worker_id and worker_id in seen:
            continue
        merged.append(candidate)

    return merged


def _write_stress_config(enabled: bool) -> None:
    path = BASE / "logs" / "stress_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps({"ultra_stress": enabled}, indent=2))
    except Exception as e:
        print(f"Error writing stress config: {e}")


def _start_optimizer() -> bool:
    if _optimizer_running():
        return True
    if Path("/opt/pegasus").exists():
        try:
            subprocess.run(["sudo", "systemctl", "start", "pegasus-optimizer.service"], check=True)
            return True
        except Exception:
            pass
    cmd = [VENV_PYTHON, str(BASE / "optimize_loop.py")]
    try:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        log_file = open(BASE / "logs" / "optimizer_v2.log", "a")
        p = subprocess.Popen(
            cmd,
            cwd=str(BASE),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
        (BASE / "logs" / "optimizer_v2.pid").write_text(str(p.pid))
        return True
    except Exception as e:
        print(f"Error starting optimizer: {e}")
        return False


def _stop_optimizer() -> bool:
    if Path("/opt/pegasus").exists():
        try:
            subprocess.run(["sudo", "systemctl", "stop", "pegasus-optimizer.service"], check=True)
            subprocess.run(["pkill", "-f", "python.*optimize_loop.py"], capture_output=True)
            return True
        except Exception:
            pass
    pid_file = BASE / "logs" / "optimizer_v2.pid"
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 15)
            _time.sleep(1)
            try:
                os.kill(pid, 9)
            except Exception:
                pass
        except Exception:
            pass
    subprocess.run(["pkill", "-f", "python.*optimize_loop.py"], capture_output=True)
    return True


@app.get("/api/optimizer/status")
def optimizer_status(response: Response):
    """Retorna o status ao vivo do loop de otimização de parâmetros."""
    if response:
        response.headers["Cache-Control"] = "no-store"

    # We can read the current deployed iteration from the environment!
    deployed_it = _get_env("OPTIMIZER_CHAMPION_ITERATION")
    deployed_iteration_val = int(deployed_it) if deployed_it else None

    # Tenta ler o arquivo de estado direto (gerado pelo optimizer)
    state_file = BASE / "logs" / "optimizer_state.json"
    if state_file.exists():
        try:
            import time as _t
            data = json.loads(state_file.read_text(encoding="utf-8"))
            mtime = state_file.stat().st_mtime
            # Considerado rodando se processo ativo
            data["running"] = _optimizer_running()
            data["last_update_ago_s"] = int(_t.time() - mtime)
            data["ultra_stress"] = _read_stress_config()
            data["deployed_iteration"] = deployed_iteration_val
            workers = _read_optimizer_workers(BASE / "logs", now=_t.time())
            data["optimizer_workers"] = workers

            # Enriquecimento com progresso de workers em tempo real
            evaluating_candidates = data.get("evaluating_candidates", [])
            workers_by_id = {
                str(worker.get("worker_id")): worker
                for worker in workers
                if worker.get("worker_id")
            }
            if not evaluating_candidates and workers:
                evaluating_candidates = [
                    {
                        **worker,
                        "status": (
                            f"Simulando {worker.get('current_month', '')}".strip()
                            if worker.get("status") == "Simulando..."
                            else worker.get("status")
                        ),
                    }
                    for worker in workers
                ]
                data["evaluating_candidates"] = evaluating_candidates
            elif evaluating_candidates:
                evaluating_candidates = _merge_optimizer_candidates(evaluating_candidates, workers)
                data["evaluating_candidates"] = evaluating_candidates
            for idx, candidate in enumerate(evaluating_candidates):
                status = str(candidate.get("status", ""))
                worker_id = str(candidate.get("worker_id") or "")
                live_worker = workers_by_id.get(worker_id)
                if live_worker:
                    candidate.update(live_worker)
                    continue
                if status.startswith("Simulando"):
                    worker_file = (
                        BASE / "logs" / f"backtest_worker_{worker_id}.json"
                        if worker_id
                        else BASE / "logs" / f"backtest_worker_w{idx}.json"
                    )
                    if worker_file.exists():
                        try:
                            if data["running"]:
                                worker_data = json.loads(worker_file.read_text(encoding="utf-8"))
                                curr_idx = worker_data.get("current_day_index", 0)
                                total_d = worker_data.get("total_days", 0)
                                elapsed = worker_data.get("elapsed_s", 0)
                                if curr_idx > 0 and total_d > 0:
                                    pct = (curr_idx / total_d) * 100
                                    est_rem = (total_d - curr_idx) * (elapsed / curr_idx)
                                    candidate["progress_pct"] = round(pct, 1)
                                    candidate["est_remaining_s"] = round(est_rem, 1)
                                    candidate["current_day_index"] = curr_idx
                                    candidate["total_days"] = total_d
                                    curr_month = worker_data.get("current_month", "")
                                    candidate["current_month"] = curr_month
                                    month_suffix = f" {curr_month}" if curr_month else ""
                                    candidate["status"] = f"Simulando {month_suffix} ({curr_idx}/{total_d})"
                        except Exception:
                            pass
            return data
        except Exception as exc:
            pass

    # Fallback: retorna idle
    return {
        "running": _optimizer_running(),
        "iterations": [],
        "best": None,
        "baseline": None,
        "current_iteration": 0,
        "last_update_ago_s": 9999,
        "ultra_stress": _read_stress_config(),
        "deployed_iteration": deployed_iteration_val,
        "optimizer_workers": _read_optimizer_workers(BASE / "logs"),
    }


@app.post("/api/optimizer/start")
def optimizer_start():
    ok = _start_optimizer()
    return {"ok": ok, "msg": "Optimizer iniciado." if ok else "Falha ao iniciar."}


@app.post("/api/optimizer/stop")
def optimizer_stop():
    ok = _stop_optimizer()
    return {"ok": ok, "msg": "Optimizer parado." if ok else "Falha ao parar."}


@app.post("/api/optimizer/restart")
def optimizer_restart():
    _stop_optimizer()
    _time.sleep(2)
    ok = _start_optimizer()
    return {"ok": ok, "msg": "Optimizer reiniciado." if ok else "Falha ao reiniciar."}


@app.post("/api/optimizer/toggle_stress")
def optimizer_toggle_stress(enabled: bool):
    _write_stress_config(enabled)
    return {"ok": True, "ultra_stress": enabled}


@app.get("/api/history30d")
def api_history30d(balance: float = 10000.0):
    """Return last 30 days of real trade history + forward projections."""
    from datetime import timedelta

    pg_dsn = _pg_dsn_str()
    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        cur.execute(
            "SELECT timestamp, direction, result, profit, stake, gale_step"
            " FROM trades WHERE timestamp >= NOW() - interval '30 days'"
            " ORDER BY timestamp ASC"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        return {"error": f"DB error: {exc}"}

    if not rows:
        return {"error": "Sem trades nos últimos 30 dias."}

    # Group by day
    from collections import defaultdict

    daily: dict[str, dict] = defaultdict(
        lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "stakes": []}
    )
    for r in rows:
        ts = r[0]
        day = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
        d = daily[day]
        d["trades"] += 1
        profit = float(r[3] or 0)
        d["pnl"] += profit
        if r[2] == "WIN":
            d["wins"] += 1
        else:
            d["losses"] += 1
        d["stakes"].append(float(r[4] or 0))

    # Sort days
    sorted_days = sorted(daily.keys())
    days_data = []
    cumulative_pnl = 0.0
    equity_curve = []
    for day in sorted_days:
        d = daily[day]
        cumulative_pnl += d["pnl"]
        wr = round(d["wins"] / d["trades"] * 100, 1) if d["trades"] > 0 else 0
        avg_stake = sum(d["stakes"]) / len(d["stakes"]) if d["stakes"] else 0
        days_data.append(
            {
                "date": day,
                "trades": d["trades"],
                "wins": d["wins"],
                "losses": d["losses"],
                "pnl": round(d["pnl"], 2),
                "cumulative_pnl": round(cumulative_pnl, 2),
                "winrate": wr,
                "avg_stake": round(avg_stake, 2),
            }
        )
        equity_curve.append(round(cumulative_pnl, 2))

    # Overall stats
    total_trades = len(rows)
    total_wins = sum(1 for r in rows if r[2] == "WIN")
    total_pnl = sum(float(r[3] or 0) for r in rows)
    active_days = len(sorted_days)
    avg_trades_day = round(total_trades / active_days, 1) if active_days > 0 else 0
    avg_pnl_day = round(total_pnl / active_days, 2) if active_days > 0 else 0
    overall_wr = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
    best_day = max(days_data, key=lambda x: x["pnl"]) if days_data else None
    worst_day = min(days_data, key=lambda x: x["pnl"]) if days_data else None

    # Average stake across all trades
    all_stakes = [float(r[4] or 0) for r in rows]
    avg_stake_global = sum(all_stakes) / len(all_stakes) if all_stakes else 1.0

    # Calculate return per dollar staked (pnl_per_unit_staked)
    total_staked = sum(all_stakes)
    pnl_per_dollar_staked = total_pnl / total_staked if total_staked > 0 else 0

    # For projections: scale avg_pnl_day proportionally to chosen balance
    # If user's balance is different from what was actually used, scale linearly
    # Use stake as % of balance (from real data): avg_stake / avg_balance_during_period
    # Simplification: use pnl_per_dollar_staked * trades_per_day * projected_stake
    PAYOUT = float(_get_env("PAYOUT") or _get_env("MARTINGALE_PAYOUT_RATE") or "0.953")
    STAKE_PCT = float(_get_env("DYNAMIC_STAKE_BASE_PCT") or "0")
    if STAKE_PCT > 0:
        projected_stake = balance * STAKE_PCT
    else:
        projected_stake = float(_get_env("STAKE") or "1.00")

    # Projected daily PnL with chosen balance/stake
    projected_daily_pnl = pnl_per_dollar_staked * projected_stake * avg_trades_day
    daily_return_pct = (projected_daily_pnl / balance * 100) if balance > 0 else 0

    # Forward projections (LINEAR — more realistic for fixed/semi-fixed stake)
    projections = {}
    for proj_days in [7, 15, 30, 60, 90]:
        projected_pnl = round(projected_daily_pnl * proj_days, 2)
        projected_balance = round(balance + projected_pnl, 2)
        projected_roi = round(projected_pnl / balance * 100, 1) if balance > 0 else 0
        projections[str(proj_days)] = {
            "days": proj_days,
            "balance": projected_balance,
            "pnl": projected_pnl,
            "roi": projected_roi,
        }

    # Worst drawdown in the 30D
    peak = 0.0
    max_dd = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    return {
        "days": days_data,
        "equity_curve": equity_curve,
        "stats": {
            "total_trades": total_trades,
            "total_wins": total_wins,
            "total_losses": total_trades - total_wins,
            "total_pnl": round(total_pnl, 2),
            "winrate": overall_wr,
            "active_days": active_days,
            "avg_trades_day": avg_trades_day,
            "avg_pnl_day": avg_pnl_day,
            "avg_stake": round(avg_stake_global, 2),
            "daily_return_pct": round(daily_return_pct, 3),
            "projected_daily_pnl": round(projected_daily_pnl, 2),
            "pnl_per_dollar_staked": round(pnl_per_dollar_staked, 4),
            "max_drawdown": round(max_dd, 2),
            "best_day": {"date": best_day["date"], "pnl": best_day["pnl"]}
            if best_day
            else None,
            "worst_day": {"date": worst_day["date"], "pnl": worst_day["pnl"]}
            if worst_day
            else None,
        },
        "projections": projections,
        "balance": balance,
    }


@app.get("/", response_class=HTMLResponse)
@app.head("/")
def root():
    content = (Path(__file__).parent / "static" / "index.html").read_text(
        encoding="utf-8"
    )
    return HTMLResponse(
        content=content,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )
