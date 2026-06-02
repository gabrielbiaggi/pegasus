import sqlite3
import json

db_path = "logs/results.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# Get row count and sample from optimizer_history
try:
    cursor.execute("SELECT COUNT(*) FROM optimizer_history;")
    count = cursor.fetchall()[0][0]
    print(f"Total rows in optimizer_history: {count}")

    # Fetch top 10 rows ordered by score or avg_daily
    cursor.execute("SELECT iteration, score, pnl, avg_daily, positive_days, consistency, params FROM optimizer_history ORDER BY score DESC LIMIT 10;")
    rows = cursor.fetchall()
    print("\nTOP 10 HISTORIC CHAMPIONS:")
    for r in rows:
        print(f"It #{r[0]} | Score: {r[1]:.2f} | PnL: ${r[2]:.2f} | Avg/Day: ${r[3]:.2f} | Pos Days: {r[4]} | Consist: {r[5]}%")
        # Try to parse params to see what strategy was used
        try:
            p = json.loads(r[6])
            print(f"  Params: TP={p.get('CALM_ACCU_THRESHOLD')} | STAKE={p.get('STAKE')} | HURST={p.get('ACCUMULATOR_MIN_HURST_EXPONENT')} | BYPASS={p.get('PCS_XGB_BYPASS_LIMIT')}")
        except Exception as e:
            pass

    # Fetch bottom 10 rows
    cursor.execute("SELECT iteration, score, pnl, avg_daily, positive_days, consistency, params FROM optimizer_history ORDER BY score ASC LIMIT 5;")
    bottom_rows = cursor.fetchall()
    print("\nBOTTOM 5 WORST:")
    for r in bottom_rows:
        print(f"It #{r[0]} | Score: {r[1]:.2f} | PnL: ${r[2]:.2f} | Avg/Day: ${r[3]:.2f} | Pos Days: {r[4]} | Consist: {r[5]}%")

except Exception as e:
    print("Error:", e)

conn.close()
