import sqlite3
import json
import sys
from pathlib import Path

def main():
    db_path = Path("/home/bill/dev/pegasus/logs/results.db")
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        sys.exit(1)
        
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Total iterations count
        cursor = conn.execute("SELECT COUNT(*) FROM optimizer_history")
        total_iters = cursor.fetchone()[0]
        print(f"Total iterations in DB: {total_iters}")
        
        # Best champion overall (highest score or highest average daily)
        print("\n--- TOP 5 CHAMPION RUNS (is_best = 1 or highest score) ---")
        cursor = conn.execute(
            "SELECT * FROM optimizer_history ORDER BY score DESC LIMIT 5"
        )
        rows = cursor.fetchall()
        for i, r in enumerate(rows):
            print(f"{i+1}. Iteration #{r['iteration']} | Avg Daily: ${r['avg_daily']:.2f}/day | "
                  f"Score: {r['score']:.2f} | Positive Days: {r['positive_days']}/31 | "
                  f"PnL: ${r['pnl']:.2f} | Sharpe: {r['sharpe']:.2f} | Drawdown: ${r['drawdown']:.2f}")
            params = json.loads(r['params'])
            print(f"   Params: {params}\n")
            
        print("\n--- TOP 5 BY AVG DAILY PROFIT (validated: positive_days >= 20, avg_daily <= 50.0) ---")
        cursor = conn.execute(
            "SELECT * FROM optimizer_history WHERE avg_daily <= 50.0 AND positive_days >= 20 ORDER BY avg_daily DESC LIMIT 5"
        )
        rows = cursor.fetchall()
        for i, r in enumerate(rows):
            print(f"{i+1}. Iteration #{r['iteration']} | Avg Daily: ${r['avg_daily']:.2f}/day | "
                  f"Score: {r['score']:.2f} | Positive Days: {r['positive_days']}/31 | "
                  f"PnL: ${r['pnl']:.2f} | Sharpe: {r['sharpe']:.2f}")
            params = json.loads(r['params'])
            print(f"   Params: {params}\n")
            
    except Exception as e:
        print(f"Error querying DB: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
