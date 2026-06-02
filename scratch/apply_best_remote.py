import sqlite3
import json
import pathlib
import subprocess
import time

db_path = "/opt/pegasus/logs/results.db"
env_path = "/opt/pegasus/.env"
best_strategy_path = "/opt/pegasus/logs/best_strategy.json"

def main():
    if not pathlib.Path(db_path).exists():
        print(f"[ERROR] Database {db_path} does not exist!")
        return

    print("Connecting to results database...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Query the highest scoring record
    cursor.execute("SELECT iteration, score, pnl, avg_daily, params FROM optimizer_history ORDER BY score DESC LIMIT 1;")
    row = cursor.fetchone()
    if not row:
        print("[ERROR] No history rows found in optimizer_history!")
        conn.close()
        return

    print(f"Found Champion: Iteration #{row['iteration']} | Score: {row['score']:.2f} | PnL: ${row['pnl']:.2f} | Avg/Day: ${row['avg_daily']:.2f}/dia")
    
    try:
        params = json.loads(row['params'])
    except Exception as e:
        print(f"[ERROR] Could not parse params JSON: {e}")
        conn.close()
        return

    conn.close()

    # Save to logs/best_strategy.json
    print(f"Saving strategy parameters to {best_strategy_path}...")
    with open(best_strategy_path, "w", encoding="utf-8") as f:
        json.dump(params, f, indent=2, ensure_ascii=False)
    print("Strategy saved.")

    # Update .env file
    if not pathlib.Path(env_path).exists():
        print(f"[ERROR] .env file at {env_path} does not exist!")
        return

    print(f"Loading and updating {env_path}...")
    with open(env_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    # Convert env lines to dict or list of updated lines
    updated_lines = []
    updated_keys = set()
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in params:
                updated_lines.append(f"{k}={params[k]}")
                updated_keys.add(k)
                continue
        updated_lines.append(line)

    # Append any keys that weren't in the .env initially
    for k in params:
        if k not in updated_keys:
            updated_lines.append(f"{k}={params[k]}")

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("\n".join(updated_lines) + "\n")
    print(".env updated successfully.")

    # Restart the live bot
    print("Stopping any running bot instances...")
    # Kill any direct python bot.py processes
    subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True)
    # Quit screen session 'pegasus'
    subprocess.run(["screen", "-S", "pegasus", "-X", "quit"], capture_output=True)
    time.sleep(2)

    # Restart in detached screen
    print("Launching live bot in screen 'pegasus'...")
    subprocess.run([
        "screen", "-dmS", "pegasus", "bash", "-c",
        "cd /opt/pegasus && .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log"
    ])
    time.sleep(4)

    # Verify if running
    check = subprocess.run(["pgrep", "-f", "python bot.py"], capture_output=True)
    if check.returncode == 0:
        pids = check.stdout.decode().strip().split()
        print(f"[SUCCESS] Live bot is online and running under Champion parameters! PID(s): {pids}")
    else:
        print("[WARNING] Live bot process not found in system check. Please check logs/trades.log.")

if __name__ == "__main__":
    main()
