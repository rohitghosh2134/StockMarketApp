import os
import requests
import pandas as pd
import csv
from io import StringIO
import logging

# === Setup Logging ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "fetchEQList.log")

logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# === Config ===
folder_path = "data/stockList"
file_path = os.path.join(folder_path, "stockList_raw.csv")
url = "https://public.fyers.in/sym_details/NSE_CM.csv"

# === Column names ===
column_names = [
    "fytoken", "symbol_details", "exchange_instrument_type", "minimum_lot_size",
    "tick_size", "isin", "trading_session", "last_update_date", "expiry_date",
    "symbol_ticker", "exchange", "segment", "scrip_code", "underlying_symbol",
    "underlying_scrip_code", "strike_price", "option_type", "underlying_fytoken",
    "reserved_str_1", "reserved_int_2", "reserved_float_3"
]

def main():
    logging.info("==== Starting fetchEQList process ====")

    # Step 1: Make sure folder exists
    os.makedirs(folder_path, exist_ok=True)

    # Step 2: Create main CSV with headers if not exists
    if not os.path.exists(file_path):
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
        logging.info("Created stockList_raw.csv with headers.")
    else:
        logging.info("Using existing stockList_raw.csv.")

    # Step 3: Download raw CSV data
    try:
        response = requests.get(url)
        response.raise_for_status()
        raw_csv_data = response.text
        logging.info("Successfully downloaded raw CSV data from Fyers.")
    except requests.RequestException as e:
        logging.error(f"Failed to download CSV from Fyers: {e}")
        logging.info("==== fetchEQList process aborted ====")
        return

    # Step 4: Load existing data
    try:
        existing_df = pd.read_csv(file_path)
        existing_symbols = set(existing_df['symbol_ticker'])
        logging.info(f"Loaded {len(existing_symbols)} existing symbols.")
    except Exception as e:
        logging.error(f"Failed to load existing CSV: {e}")
        logging.info("==== fetchEQList process aborted ====")
        return

    # Step 5: Parse new data
    try:
        new_data_df = pd.read_csv(StringIO(raw_csv_data), header=None, names=column_names)
    except Exception as e:
        logging.error(f"Failed to parse downloaded CSV: {e}")
        logging.info("==== fetchEQList process aborted ====")
        return

    # Step 6: Filter and append
    filtered_new_rows = new_data_df[~new_data_df['symbol_ticker'].isin(existing_symbols)]
    if not filtered_new_rows.empty:
        try:
            with open(file_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for _, row in filtered_new_rows.iterrows():
                    writer.writerow(row.tolist())
            logging.info(f"Appended {len(filtered_new_rows)} new symbols to stockList_raw.csv.")
        except Exception as e:
            logging.error(f"Failed to append new symbols: {e}")
    else:
        logging.info("No new symbols to append. CSV is up to date.")

    logging.info("==== fetchEQList process completed ====")

if __name__ == "__main__":
    main()
