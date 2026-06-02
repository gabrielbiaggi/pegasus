import json
import pathlib
import subprocess
import time
import datetime
import psycopg2

BASE_DIR = "/opt/pegasus"
risk_path = pathlib.Path(f"{BASE_DIR}/logs/risk_state.json")
balance_json_path = pathlib.Path(f"{BASE_DIR}/logs/balance.json")
trades_csv_path = pathlib.Path(f"{BASE_DIR}/logs/trades.csv")
signals_csv_path = pathlib.Path(f"{BASE_DIR}/logs/signals.csv")
trades_log_path = pathlib.Path(f"{BASE_DIR}/logs/trades.log")
env_path = pathlib.Path(f"{BASE_DIR}/.env")

def _pg_dsn_str():
    # Parse from .env on the server
    env_vars = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env_vars[k.strip()] = v.strip()
    
    return env_vars.get("PG_DSN") or "postgresql://pegasus:pegasus@localhost/pegasus_db"

def main():
    print("Stopping active bot and screen instances...")
    # Kill any python processes running bot.py or screen pegasus sessions
    subprocess.run(["pkill", "-9", "-f", "python.*bot.py"], capture_output=True)
    subprocess.run(["pkill", "-9", "-f", "python3.*bot.py"], capture_output=True)
    subprocess.run(["pkill", "-9", "-f", "SCREEN.*pegasus"], capture_output=True)
    subprocess.run(["screen", "-wipe"], capture_output=True)
    time.sleep(2)

    # 1. Reset risk_state.json to initial $50.0 balance reference
    print("Writing fresh initial $50.0 risk_state...")
    today_str = datetime.date.today().isoformat()
    risk_state = {
        "day": today_str,
        "start_of_day_balance": 50.0,
        "balance": 50.0,
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
    risk_path.parent.mkdir(parents=True, exist_ok=True)
    risk_path.write_text(json.dumps(risk_state, indent=2))
    print("risk_state.json reset completed.")

    # 2. Reset balance.json to show initial virtual balance of $50.0
    print("Writing fresh initial $50.0 balance.json...")
    balance_json_path.write_text(json.dumps({"balance": 50.0}, indent=2))
    print("balance.json reset completed.")

    # 3. Truncate files
    print("Truncating trades.csv, signals.csv, trades.log...")
    if trades_csv_path.exists():
        header = "timestamp,symbol,contract_type,barrier,action,stake,pnl,balance,win,duration_ticks,regime\n"
        trades_csv_path.write_text(header)
    if signals_csv_path.exists():
        header = "timestamp,symbol,avg,cusum,hurst,shannon,kalman,p_loss,signal\n"
        signals_csv_path.write_text(header)
    if trades_log_path.exists():
        trades_log_path.write_bytes(b"")

    # 4. Clear today's database entries so dashboard updates in real time
    dsn = _pg_dsn_str()
    print("Connecting to PostgreSQL and clearing today's trade & signal records...")
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        cur.execute("DELETE FROM trades WHERE timestamp::date = %s", (today_str,))
        trades_deleted = cur.rowcount
        print(f"Deleted {trades_deleted} trades from PostgreSQL.")
        
        cur.execute("DELETE FROM signals WHERE timestamp::date = %s", (today_str,))
        signals_deleted = cur.rowcount
        print(f"Deleted {signals_deleted} signals from PostgreSQL.")
        
        conn.commit()
        cur.close()
        conn.close()
        print("Database transaction committed successfully.")
    except Exception as e:
        print(f"[WARN] Database clear failed: {e}")

    # 5. Restart the bot
    print("Launching live bot in screen 'pegasus'...")
    subprocess.run([
        "screen", "-dmS", "pegasus", "bash", "-c",
        f"cd {BASE_DIR} && .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log"
    ])
    time.sleep(4)

    # Verify if running
    check = subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True)
    if check.returncode == 0:
        pids = check.stdout.decode().strip().split()
        print(f"[SUCCESS] Bot has been fully reset and initialized with $50.0 initial balance! PIDs: {pids}")
    else:
        print("[WARNING] Bot failed to start or PID not found. Check logs/trades.log.")

if __name__ == "__main__":
    main()
