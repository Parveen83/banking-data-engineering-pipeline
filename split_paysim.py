import pandas as pd

INPUT_FILE = "data/raw/transactions.csv"
OUTPUT_DIR = "data/input"

df = pd.read_csv(INPUT_FILE)

rows = len(df)
chunk_size = rows // 4

files = [
    ("transactions_jan.csv", df.iloc[:chunk_size]),
    ("transactions_feb.csv", df.iloc[chunk_size:2*chunk_size]),    
    ("transactions_mar.csv", df.iloc[2*chunk_size:3*chunk_size]),
    ("transactions_apr.csv", df.iloc[3*chunk_size:]),
]

import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

for filename, chunk in files:
    chunk.to_csv(f"{OUTPUT_DIR}/{filename}", index=False)
    print(f"Created {filename}: {len(chunk)} rows")

print("Done.")
