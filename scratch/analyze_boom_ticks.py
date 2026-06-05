import pandas as pd
import numpy as np

# Load ticks
df = pd.read_csv("data/ticks_BOOM1000_2026-05-30.csv")
prices = df["quote"].values

# Simulate a 5-tick PUT contract entered at every single tick
wins = 0
losses = 0
duration = 5

for i in range(len(prices) - duration - 1):
    entry_price = prices[i]
    exit_price = prices[i + duration]
    
    # A PUT contract wins if exit price is strictly less than entry price
    if exit_price < entry_price:
        wins += 1
    else:
        losses += 1

total = wins + losses
wr = (wins / total * 100) if total > 0 else 0
print(f"Total ticks: {len(prices)}")
print(f"Wins: {wins} | Losses: {losses} | Win Rate: {wr:.4f}%")
