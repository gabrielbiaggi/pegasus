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

app = FastAPI(title="Pegasus Dashboard")

BASE = Path("/opt/pegasus")
TRADES_CSV = BASE / "logs" / "trades.csv"
TRADES_LOG = BASE / "logs" / "trades.log"
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


_csv_cache: dict = {"mtime": -1.0, "date": "", "df": None}


def _today_df() -> pd.DataFrame:
    global _csv_cache
    if not TRADES_CSV.exists():
        return pd.DataFrame()
    try:
        mtime = TRADES_CSV.stat().st_mtime
        today = datetime.now(timezone.utc).date().isoformat()
        if mtime == _csv_cache["mtime"] and _csv_cache["date"] == today and _csv_cache["df"] is not None:
            return _csv_cache["df"]
        df = pd.read_csv(TRADES_CSV, parse_dates=["timestamp"])
        filtered = df[df["timestamp"].dt.date.astype(str) == today].copy()
        _csv_cache = {"mtime": mtime, "date": today, "df": filtered}
        return filtered
    except Exception:
        return pd.DataFrame()


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
    import time; time.sleep(1)
    cmd = (
        f"cd {BASE} && {VENV_PYTHON} bot.py 2>&1 | tee -a logs/trades.log"
    )
    subprocess.run(["screen", "-dmS", SCREEN_BOT, "bash", "-c", cmd])


def _compute_max_loss_day(risk_state: dict) -> float:
    """Return max daily loss threshold — prefer the value saved by the bot in risk_state.json,
    fall back to env-based computation (pct × balance or fixed)."""
    if "max_loss_day" in risk_state:
        return float(risk_state["max_loss_day"])
    loss_pct = float(_get_env("MAX_LOSS_DAY_PCT") or "0.0")
    if loss_pct > 0:
        # Use the last known balance to estimate; risk_state may not have it
        return round(float(_get_env("BALANCE_HINT") or "10000") * loss_pct, 2)
    return float(_get_env("MAX_LOSS_PER_DAY") or "200")


def _compute_risk_blocked(risk_state: dict) -> bool:
    return float(risk_state.get("daily_net_profit", 0.0)) <= -_compute_max_loss_day(risk_state)


def _get_payout_rate() -> float:
    """Payout rate for Martingale formula. Uses MARTINGALE_PAYOUT_RATE or TP%/100."""
    v = _get_env("MARTINGALE_PAYOUT_RATE")
    if v:
        return max(0.001, float(v))
    tp = _get_env("ACCUMULATOR_TAKE_PROFIT_PERCENT")
    if tp:
        return max(0.001, float(tp) / 100.0)
    return 0.15


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
    df = _today_df()
    wins = int((df["result"] == "WIN").sum()) if not df.empty else 0
    losses = int((df["result"] == "LOSS").sum()) if not df.empty else 0
    total = wins + losses
    pnl = round(float(df["profit"].sum()), 2) if not df.empty else 0.0
    last_ts = df["timestamp"].max().isoformat() if not df.empty else None
    risk_state = _read_risk_state()
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
        "use_soros": _get_env("USE_SOROS") == "true",
        "soros_max_steps": _get_env("SOROS_MAX_STEPS") or "3",
        "soros_profit_factor": _get_env("SOROS_PROFIT_FACTOR") or "1.0",
        "use_dynamic_stake": _get_env("DYNAMIC_STAKE") != "false",
        "dynamic_stake_base_pct": _get_env("DYNAMIC_STAKE_BASE_PCT") or "0.02",
        "max_stake": _get_env("MAX_STAKE") or "500.00",
        "max_stake_pct": _get_env("MAX_STAKE_PERCENT") or "0.10",
        "take_profit_pct": _get_env("ACCUMULATOR_TAKE_PROFIT_PERCENT") or "9.0",
        "risk_blocked": _compute_risk_blocked(risk_state),
        "max_loss_per_day": _compute_max_loss_day(risk_state),
        "daily_loss": round(risk_state.get("daily_loss", 0.0), 2),
        "daily_net_profit": round(float(risk_state.get("daily_net_profit", 0.0)), 2),
        "soros_step": int(risk_state.get("soros_step", 0)),
        "soros_profit": round(float(risk_state.get("soros_profit", 0.0)), 2),
        "martingale_step": int(risk_state.get("martingale_step", 0)),
        "consecutive_losses": int(risk_state.get("consecutive_losses", 0)),
        "use_martingale": _get_env("USE_MARTINGALE") == "true",
        "martingale_max_gales": int(_get_env("MARTINGALE_MAX_GALES") or "3"),
        "martingale_payout_rate": _get_payout_rate(),
        "martingale_accumulated_loss": round(float(risk_state.get("martingale_accumulated_loss", 0.0)), 2),
        "martingale_base_stake": round(float(risk_state.get("martingale_base_stake", 0.0)), 2),
        "martingale_effective_multiplier": _compute_gale_effective_multiplier(risk_state),
        "next_gale_stake": _compute_next_gale_stake(risk_state),
    }


@app.get("/api/sessions")
def api_sessions(response: Response):
    """Return per-day stats + monthly summary from trades.csv."""
    response.headers["Cache-Control"] = "no-store"
    if not TRADES_CSV.exists():
        return {"days": [], "monthly": {}}
    try:
        df = pd.read_csv(TRADES_CSV, parse_dates=["timestamp"])
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
        days.append({
            "date": date,
            "trades": total,
            "wins": wins,
            "losses": losses,
            "winrate": round(wins / total * 100, 1) if total else 0.0,
            "pnl": pnl,
        })
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


@app.post("/api/reset")
def api_reset(scope: str = "day", response: Response = None):
    """Reset trade history and risk state.
    scope='day'   → remove today's rows from trades.csv + reset risk_state
    scope='month' → remove current month's rows from trades.csv + reset risk_state
    """
    from datetime import date as _date
    import shutil

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

    # --- archive & truncate trades.log ---
    if TRADES_LOG.exists() and TRADES_LOG.stat().st_size > 0:
        log_bak = TRADES_LOG.with_suffix(f".log.bak-reset-{now.strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(TRADES_LOG, log_bak)
        TRADES_LOG.write_bytes(b"")

    # --- restart bot so it picks up fresh state ---
    _restart_bot()

    return {"ok": True, "scope": scope, "msg": f"Histórico '{scope}' resetado. Bot reiniciado."}



class EnvUpdate(BaseModel):
    key: str
    value: str

ALLOWED_KEYS = {
    "BLOCK_WEEKENDS", "USE_ENSEMBLE", "ENSEMBLE_MIN_PROB",
    "MAX_LOSS_PER_DAY", "MAX_PROFIT_PER_DAY", "STAKE",
    "MAX_TICK_LATENCY_MS", "LOG_LEVEL",
    "USE_SOROS", "SOROS_MAX_STEPS", "SOROS_PROFIT_FACTOR",
    "DYNAMIC_STAKE", "DYNAMIC_STAKE_BASE_PCT",
    "MAX_STAKE", "MAX_STAKE_PERCENT",
    "ACCUMULATOR_TAKE_PROFIT_PERCENT", "ACCUMULATOR_MAX_HOLD_TICKS",
    "USE_MARTINGALE", "MARTINGALE_MAX_GALES", "MARTINGALE_MULTIPLIER", "MARTINGALE_PAYOUT_RATE",
}

@app.post("/api/env")
def update_env(body: EnvUpdate):
    if body.key not in ALLOWED_KEYS:
        raise HTTPException(status_code=400, detail=f"Chave não permitida: {body.key}")
    _set_env(body.key, body.value)
    return {"ok": True, "key": body.key, "value": body.value}


_backtest_cache: dict = {"ts": 0.0, "result": None}
_BACKTEST_TTL = 60  # re-run at most every 60s


def _run_backtest_simulation(rows: list, initial_balance: float = 10000.0) -> dict:
    """Simulate accumulator trading on signal rows from shadow_ticks.

    Fixes vs previous version:
    - Cooldown: after each trade exits, skip rows within exit_epoch + COOLDOWN_TICKS
      (mirrors the real bot's cooldown between trades)
    - Soros: replaces profit each step (not accumulates), resets after SOROS_MAX_STEPS
      wins (mirrors risk_manager.py update() logic exactly)
    """
    INITIAL = initial_balance
    STAKE_FIXED = float(_get_env("STAKE") or "10.00")
    BASE_PCT = float(_get_env("DYNAMIC_STAKE_BASE_PCT") or "0.02")
    MAX_PCT_CAP = float(_get_env("MAX_STAKE_PERCENT") or "0.10")
    MAX_STAKE_ABS = float(_get_env("MAX_STAKE") or "0")   # 0 = no fixed cap, use pct only
    MIN_STAKE_ABS = float(_get_env("MIN_STAKE") or "0.35")
    SOROS_MAX = int(_get_env("SOROS_MAX_STEPS") or "3")
    SOROS_FACTOR = float(_get_env("SOROS_PROFIT_FACTOR") or "1.0")
    COOLDOWN = int(_get_env("ACCUMULATOR_COOLDOWN_TICKS") or "3")
    _loss_pct = float(_get_env("MAX_LOSS_DAY_PCT") or "0.0")
    MAX_LOSS_DAY = round(INITIAL * _loss_pct, 2) if _loss_pct > 0 else float(_get_env("MAX_LOSS_PER_DAY") or "200.0")

    def _mult(score: float) -> float:
        s = float(score or 0)
        if s >= 9:
            return 2.0
        if s >= 7:
            return 1.5
        return 1.0

    def _profit(stake: float, held: int) -> float:
        t = max(int(held or 2), 1)
        return round(stake * ((1.03 ** t) - 1), 2)

    if not rows:
        return {"error": "no data"}

    first_epoch = rows[0][0]
    last_epoch = rows[-1][0]
    total = len(rows)
    wins_raw = sum(1 for r in rows if r[3] == "WIN")

    def _simulate(*, max_epoch: int = 0, day_cap: int = 0, collect_targets: bool = False) -> dict:
        """Run a full simulation pass with cooldown, correct Soros, and optional caps."""
        bal = INITIAL
        ss = 0      # soros step (1..SOROS_MAX)
        sp = 0.0    # soros profit — last win's profit only (mirrors risk_manager.py)
        peak = INITIAL
        max_dd = 0.0
        n = 0
        wins = 0
        losses = 0
        total_won = 0.0       # sum of all WIN profits
        total_lost = 0.0      # sum of all LOSS stakes
        max_single_loss = 0.0 # worst single losing trade
        next_valid = 0  # skip rows with entry_epoch < next_valid (cooldown window)
        day_counts: dict[int, int] = {}
        day_net_pnls: dict[int, float] = {}  # net pnl per calendar-day bucket
        targets: dict = {p: None for p in [10, 20, 25, 30, 50, 100, 200]} if collect_targets else {}

        for (epoch, _q, score, result, exit_epoch, held) in rows:
            # Time-window limit
            if max_epoch and epoch - first_epoch > max_epoch:
                break
            # Cooldown: skip if still in cooldown after last trade's exit
            if epoch < next_valid:
                continue
            # Per-calendar-day trade cap (relative to dataset start, not wall clock)
            dk = (epoch - first_epoch) // 86400
            if day_cap:
                if day_counts.get(dk, 0) >= day_cap:
                    continue
                day_counts[dk] = day_counts.get(dk, 0) + 1

            # Daily stop: skip if net P&L for this day already hit -MAX_LOSS_DAY
            _day_net = day_net_pnls.get(dk, 0.0)
            if _day_net <= -MAX_LOSS_DAY:
                continue  # daily stop loss reached (net drawdown)
            remaining_budget = max(0.0, MAX_LOSS_DAY + _day_net)
            if remaining_budget < MIN_STAKE_ABS:
                continue  # remaining budget below minimum stake

            base = max(bal * BASE_PCT, STAKE_FIXED)
            soros_add = sp if 0 < ss <= SOROS_MAX else 0.0
            _caps = [base * _mult(score) + soros_add, bal * MAX_PCT_CAP, remaining_budget]
            if MAX_STAKE_ABS > 0:
                _caps.append(MAX_STAKE_ABS)
            stk = round(min(_caps), 2)
            if stk < MIN_STAKE_ABS:
                continue

            if result == "WIN":
                p = _profit(stk, held)
                bal = round(bal + p, 2)
                total_won += p
                # Soros: replace profit each step, reset after SOROS_MAX consecutive wins
                if ss < SOROS_MAX:
                    ss += 1
                    sp = round(p * SOROS_FACTOR, 2)
                else:
                    ss = 0
                    sp = 0.0
                wins += 1
                day_net_pnls[dk] = day_net_pnls.get(dk, 0.0) + p
            else:
                bal = round(bal - stk, 2)
                total_lost += stk
                max_single_loss = max(max_single_loss, stk)
                day_net_pnls[dk] = day_net_pnls.get(dk, 0.0) - stk
                ss = 0
                sp = 0.0
                losses += 1

            n += 1
            if bal > peak:
                peak = bal
            dd = (peak - bal) / peak * 100
            if dd > max_dd:
                max_dd = dd

            # Advance cooldown: next valid entry after trade exits + COOLDOWN ticks
            eff_exit = exit_epoch if exit_epoch else (epoch + (held or 0))
            next_valid = eff_exit + COOLDOWN

            if collect_targets:
                h = (epoch - first_epoch) / 3600
                for pct in list(targets.keys()):
                    if targets[pct] is None and bal >= INITIAL * (1 + pct / 100):
                        targets[pct] = {
                            "trade": n,
                            "hours": round(h, 2),
                            "balance": round(bal, 2),
                            "days_at_300": round(h / 24, 1),
                        }

        return {
            "trades": n,
            "wins": wins,
            "losses": losses,
            "total_won": round(total_won, 2),
            "total_lost": round(total_lost, 2),
            "max_single_loss": round(max_single_loss, 2),
            "balance": round(bal, 2),
            "max_dd": round(max_dd, 1),
            "roi": round((bal - INITIAL) / INITIAL * 100, 1),
            "targets": targets,
        }

    # Full simulation (no cap, with cooldown) — used only for global stats (winrate, max_dd)
    full = _simulate(collect_targets=False)

    # Cap 300 trades/day, time-limited, with cooldown and daily loss budget
    # Use 1-5 full days — 0.5/4.5 removed to avoid identical rows when signal rate fills cap in <12h
    cap_by_day: dict[str, dict] = {}
    for target_day in [1, 2, 3, 4, 5]:
        r = _simulate(max_epoch=int(target_day * 86400), day_cap=300)
        cap_by_day[str(target_day)] = {
            "trades": r["trades"], "wins": r["wins"], "losses": r["losses"],
            "balance": r["balance"], "roi": r["roi"],
            "total_won": r["total_won"], "total_lost": r["total_lost"],
        }

    # Bounded full simulation (300/day cap, entire dataset) — source for targets table,
    # consistent with cap_by_day above so metas align with the numbers the user sees there
    bounded = _simulate(collect_targets=True, day_cap=300)
    targets = bounded["targets"]

    # Sessions (no day cap, time-limited, with cooldown)
    sessions: dict[str, dict] = {}
    for label, seg in [("30min", 1800), ("1h", 3600), ("2h", 7200), ("4h", 14400)]:
        r = _simulate(max_epoch=seg)
        sessions[label] = {
            "trades": r["trades"], "wins": r["wins"], "losses": r["losses"],
            "balance": r["balance"], "roi": r["roi"],
            "total_won": r["total_won"], "total_lost": r["total_lost"],
            "max_single_loss": r["max_single_loss"],
        }

    avg_s = (last_epoch - first_epoch) / total if total > 1 else 8.0
    return {
        "total_signals": total,
        "effective_trades": full["trades"],
        "winrate": round(wins_raw / total * 100, 2),
        "effective_winrate": round(full["wins"] / full["trades"] * 100, 2) if full["trades"] else 0.0,
        "cooldown_ticks": COOLDOWN,
        "dataset_hours": round((last_epoch - first_epoch) / 3600, 1),
        "max_drawdown": full["max_dd"],
        "signal_rate_per_hour": round(3600 / avg_s),
        "avg_signal_interval_s": round(avg_s, 1),
        "cap_hours_per_day": round(300 * avg_s / 3600, 1),
        "targets": {str(k): v for k, v in targets.items() if v is not None},
        "cap_by_day": cap_by_day,
        "sessions": sessions,
        "initial_balance": INITIAL,
        "full_stats": {
            "wins": full["wins"],
            "losses": full["losses"],
            "total_won": full["total_won"],
            "total_lost": full["total_lost"],
            "max_single_loss": full["max_single_loss"],
            "avg_win": round(full["total_won"] / full["wins"], 2) if full["wins"] else 0.0,
            "avg_loss": round(full["total_lost"] / full["losses"], 2) if full["losses"] else 0.0,
        },
    }


@app.get("/api/backtest")
def api_backtest(refresh: bool = False, balance: float = 10000.0):
    global _backtest_cache
    now = _time.monotonic()
    cached = _backtest_cache.get("result")
    if (not refresh and cached and now - _backtest_cache["ts"] < _BACKTEST_TTL
            and cached.get("initial_balance", 10000.0) == balance):
        return cached
    pg_dsn = _get_env("PG_DSN") or "postgresql://pegasus:pegasus@localhost/pegasus_db"
    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        cur.execute(
            "SELECT entry_epoch, entry_quote, score, future_result, future_exit_epoch, future_held_ticks"
            " FROM shadow_ticks WHERE signal = 1 ORDER BY entry_epoch ASC"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB error: {exc}")
    result = _run_backtest_simulation(rows, initial_balance=balance)
    result["cached_at"] = datetime.now(timezone.utc).isoformat()
    _backtest_cache = {"ts": now, "result": result}
    return result


@app.get("/api/trades")
def api_trades():
    df = _today_df()
    if df.empty:
        return []
    cols = ["timestamp", "result", "soros_step", "gale_step", "profit", "score", "stake", "held_ticks"]
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
