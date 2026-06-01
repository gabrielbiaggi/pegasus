import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

data_dir = Path("/home/bill/dev/pegasus/data")
max_path = data_dir / "ticks_BOOM1000_max.csv"

if not max_path.exists():
    print(f"File {max_path} does not exist.")
    exit(1)

print("Loading ticks_BOOM1000_max.csv...")
df = pd.read_csv(max_path)
print(f"Loaded {len(df)} rows. Converting epochs to dates...")
df["_date"] = pd.to_datetime(df["epoch"], unit="s", utc=True).dt.date

grouped = df.groupby("_date")
print(f"Found {len(grouped)} unique dates in max.csv.")

for date_val, group in grouped:
    iso_date = date_val.isoformat()
    # Check if the date is in May 2026
    if iso_date.startswith("2026-05"):
        out_file = data_dir / f"ticks_BOOM1000_{iso_date}.csv"
        if not out_file.exists():
            print(f"Writing {iso_date} with {len(group)} rows to {out_file}...")
            group_clean = group.drop(columns=["_date"])
            group_clean.to_csv(out_file, index=False)
        else:
            print(f"File {out_file} already exists, skipping.")

print("Splitting complete.")
