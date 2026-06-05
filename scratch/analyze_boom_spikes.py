import pandas as pd
import numpy as np
import sys

def main():
    path = 'data/ticks_BOOM1000_max.csv'
    try:
        df = pd.read_csv(path)
        df['quote'] = pd.to_numeric(df['quote'], errors='coerce')
        df = df.dropna().reset_index(drop=True)
        print(f"Loaded {len(df)} ticks of BOOM1000.")
        
        # Calculate price change
        df['diff'] = df['quote'].diff()
        
        # 1. SPIKES ANALYSIS
        spike_threshold = 1.0
        df['is_spike'] = df['diff'] > spike_threshold
        spike_indices = df.index[df['is_spike']].tolist()
        print(f"\nTotal spikes detected: {len(spike_indices)}")
        
        # 2. DIGIT DISTRIBUTION ANALYSIS (Assuming 3 decimal places)
        # e.g., if quote = 14126.688, round(14126.688, 3) * 1000 = 14126688, last digit is 8.
        quotes_rounded = (df['quote'].round(3) * 1000).astype(int)
        last_digits = quotes_rounded % 10
        counts = last_digits.value_counts().sort_index()
        percentages = counts / len(df) * 100
        
        print("\nLast digit distribution (3 decimals):")
        for digit, pct in percentages.items():
            print(f"Digit {digit}: {pct:.4f}% (count: {counts[digit]})")
            
        # 3. DIGIT DISTRIBUTION (Assuming 4 decimal places, just in case)
        quotes_rounded_4 = (df['quote'].round(4) * 10000).astype(int)
        last_digits_4 = quotes_rounded_4 % 10
        counts_4 = last_digits_4.value_counts().sort_index()
        percentages_4 = counts_4 / len(df) * 100
        
        print("\nLast digit distribution (4 decimals):")
        for digit, pct in percentages_4.items():
            print(f"Digit {digit}: {pct:.4f}% (count: {counts_4[digit]})")
            
        # 4. DIGIT DISTRIBUTION (Assuming 2 decimal places)
        quotes_rounded_2 = (df['quote'].round(2) * 100).astype(int)
        last_digits_2 = quotes_rounded_2 % 10
        counts_2 = last_digits_2.value_counts().sort_index()
        percentages_2 = counts_2 / len(df) * 100
        
        print("\nLast digit distribution (2 decimals):")
        for digit, pct in percentages_2.items():
            print(f"Digit {digit}: {pct:.4f}% (count: {counts_2[digit]})")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
