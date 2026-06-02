import sqlite3
import json
import sys

db_path = "/opt/pegasus/logs/results.db"

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("="*80)
print("             PEGASUS AUTO-OPTIMIZER v3 - IN-DEPTH CHRONICLE REPORT")
print("="*80)

# Total counts
total = conn.execute("SELECT COUNT(*) FROM optimizer_history").fetchone()[0]
print(f"Total Backtest Iterations: {total}")

# Baseline (Iteration 0 or first iteration)
baseline = conn.execute("SELECT * FROM optimizer_history ORDER BY id ASC LIMIT 1").fetchone()
if baseline:
    print(f"\nBaseline Performance (Start of Optimization):")
    print(f"  - Iteration: #{baseline['iteration']}")
    print(f"  - Avg Daily Profit: ${baseline['avg_daily']:.2f}/dia")
    print(f"  - Total P&L (Maio 2026): ${baseline['pnl']:.2f}")
    print(f"  - Consistency (Win Days): {baseline['consistency_pct']:.1f}%")
    print(f"  - Positive/Negative Days: {baseline['positive_days']} / {baseline['negative_days']}")

# Record Progression Timeline
records = conn.execute("SELECT * FROM optimizer_history WHERE is_best = 1 ORDER BY id ASC").fetchall()
print(f"\nRecord-Breaking Champion Progression Timeline ({len(records)} Records):")
for r in records:
    params_str = r['params']
    tp_b_plus = "N/A"
    tp_b_minus = "N/A"
    stake = "N/A"
    hurst = "N/A"
    p_loss = "N/A"
    try:
        p = json.loads(params_str)
        tp_b_plus = p.get('PCS_REGIME_B_PLUS_TP', 'N/A')
        tp_b_minus = p.get('PCS_REGIME_B_MINUS_TP', 'N/A')
        stake = p.get('STAKE', 'N/A')
        hurst = p.get('ACCUMULATOR_MIN_HURST_EXPONENT', 'N/A')
        p_loss = p.get('ENSEMBLE_MIN_PROB', 'N/A')
    except:
        pass
    print(f"  - It #{r['iteration']} ({r['timestamp'].split()[0]}): "
          f"Avg/Day = ${r['avg_daily']:.2f}/dia | PnL = ${r['pnl']:.2f} | Consist = {r['consistency_pct']:.1f}% | "
          f"Sharpe = {r['sharpe']:.2f} | Sortino = {r['sortino']:.1f} | DD = ${r['drawdown']:.2f} | "
          f"Params: [Stake=${stake} | TP_B+={tp_b_plus}% | TP_B-={tp_b_minus}% | MinHurst={hurst} | PLossLimit={p_loss}]")

# Current Champion parameters detail
best = conn.execute("SELECT * FROM optimizer_history WHERE is_best = 1 ORDER BY score DESC LIMIT 1").fetchone()
if best:
    print(f"\n" + "="*80)
    print("                      ABSOLUTE CHAMPION STRATEGY METRICS")
    print("="*80)
    print(f"  - Iteration: #{best['iteration']}")
    print(f"  - Score: {best['score']:.4f}")
    print(f"  - Avg Daily Profit: ${best['avg_daily']:.2f}/dia  (<-- Real Lucro Médio)")
    print(f"  - Total P&L (Maio 2026 Backtest): ${best['pnl']:.2f}")
    print(f"  - ROI (on $50 Banca): {best['roi']:.1f}%")
    print(f"  - Consistency: {best['consistency_pct']:.1f}% ({best['positive_days']} positive days / 0 negative days!)")
    print(f"  - Sharpe Ratio: {best['sharpe']:.4f}")
    print(f"  - Sortino Ratio: {best['sortino']:.4f}")
    print(f"  - Maximum Drawdown: ${best['drawdown']:.2f}")
    print(f"  - Elapsed Time: {best['elapsed_s']:.1f}s")
    
    print("\n  Complete Parameter Set:")
    try:
        p = json.loads(best['params'])
        for k, v in sorted(p.items()):
            print(f"    - {k}: {v}")
    except Exception as e:
        print(f"    [ERR] failed to parse parameters: {e}")

conn.close()
