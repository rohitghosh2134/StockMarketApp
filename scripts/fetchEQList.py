import os
import requests
import pandas as pd
import csv
from io import StringIO

# === Config ===
folder_path = "data/stockList"
file_path = os.path.join(folder_path, "stockList_raw.csv")
url = "https://public.fyers.in/sym_details/NSE_CM.csv"

# === Column names ===
column_names = [
    "fytoken",
    "symbol_details",
    "exchange_instrument_type",
    "minimum_lot_size",
    "tick_size",
    "isin",
    "trading_session",
    "last_update_date",
    "expiry_date",
    "symbol_ticker",
    "exchange",
    "segment",
    "scrip_code",
    "underlying_symbol",
    "underlying_scrip_code",
    "strike_price",
    "option_type",
    "underlying_fytoken",
    "reserved_str_1",
    "reserved_int_2",
    "reserved_float_3"
]

# === Step 1: Make sure folder exists ===
os.makedirs(folder_path, exist_ok=True)

# === Step 2: Create main CSV with headers if not exists ===
if not os.path.exists(file_path):
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
    print("📄 Created CSV file with headers.")

# === Step 3: Download raw CSV data (no headers) ===
try:
    response = requests.get(url)
    response.raise_for_status()
    raw_csv_data = response.text
    print("✅ Downloaded raw CSV data from URL.")
except requests.RequestException as e:
    print(f"❌ Failed to download CSV: {e}")
    exit()

# === Step 4: Load existing data to get existing symbols ===
existing_df = pd.read_csv(file_path)
existing_symbols = set(existing_df['symbol_ticker'])

# === Step 5: Read downloaded data into DataFrame WITHOUT headers ===
new_data_df = pd.read_csv(StringIO(raw_csv_data), header=None, names=column_names)

# === Step 6: Filter new rows that are not in existing file ===
filtered_new_rows = new_data_df[~new_data_df['symbol_ticker'].isin(existing_symbols)]

# === Step 7: Append new rows line by line (without header) ===
if not filtered_new_rows.empty:
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        for _, row in filtered_new_rows.iterrows():
            writer.writerow(row.tolist())
    print(f"💾 Appended {len(filtered_new_rows)} new rows to {file_path}.")
else:
    print("ℹ️ No new rows to append. CSV file is up to date.")
