import os
import sys
import pandas as pd
from datetime import date
from time import sleep
from jugaad_data.nse import stock_df
import logging

# === File and directory paths ===
stock_list_path = os.path.join("data", "stockList", "stockList_clean.csv")
raw_data_dir = os.path.join("data", "rawData")
log_dir = os.path.join("logs")
log_file = os.path.join(log_dir, "getRawStock.log")

# === Ensure required directories exist ===
os.makedirs(os.path.dirname(stock_list_path), exist_ok=True)
os.makedirs(raw_data_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)

# === Setup Logging ===
logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def fetch_and_save_stock_data(symbol: str, start: date, end: date):
    try:
        logging.info(f"---- [{symbol}] Fetching data from {start} to {end} ----")
        new_df = stock_df(symbol=symbol, from_date=start, to_date=end, series="EQ")

        output_path = os.path.join(raw_data_dir, f"{symbol}.csv")

        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            existing_df['Date'] = pd.to_datetime(existing_df['Date'])
            new_df['Date'] = pd.to_datetime(new_df['Date'])

            existing_dates = set(existing_df['Date'])
            new_rows = new_df[~new_df['Date'].isin(existing_dates)]

            if not new_rows.empty:
                combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
                combined_df.sort_values(by='Date', inplace=True)
                combined_df.to_csv(output_path, index=False)
                logging.info(f"[{symbol}] Appended {len(new_rows)} new rows.")
            else:
                logging.info(f"[{symbol}] No new rows to append.")
        else:
            new_df.to_csv(output_path, index=False)
            logging.info(f"[{symbol}] Saved new data to {output_path}")

        return True
    except Exception as e:
        logging.error(f"[{symbol}] Error while fetching/saving: {e}")
        return False

def already_has_data(symbol: str, end: date):
    path = os.path.join(raw_data_dir, f"{symbol}.csv")
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_csv(path, parse_dates=["Date"])
        return not df[df["Date"] >= pd.Timestamp(end)].empty
    except Exception as e:
        logging.warning(f"[{symbol}] Failed to read CSV: {e}")
        return False

def main():
    logging.info("==== Starting stock data download process ====")

    try:
        df_symbols = pd.read_csv(stock_list_path)
        symbols = df_symbols.iloc[:, 0].dropna().unique().tolist()
        logging.info(f"Loaded {len(symbols)} symbols from {stock_list_path}")
    except Exception as e:
        logging.error(f"Failed to load symbol list: {e}")
        return

    start_from_symbol = sys.argv[1] if len(sys.argv) > 1 else None
    if start_from_symbol and start_from_symbol in symbols:
        symbols = symbols[symbols.index(start_from_symbol):]
        logging.info(f"Resuming from symbol: {start_from_symbol}")
    elif start_from_symbol:
        logging.warning(f"Symbol '{start_from_symbol}' not found. Starting from beginning.")

    for symbol in symbols:
        try:
            logging.info(f"== Starting symbol: {symbol} ==")
            for year in range(2000, 2026):
                start_date = date(year, 1, 1)
                end_date = date(year, 12, 31)
                if year == 2025:
                    end_date = date(2025, 6, 15)

                if already_has_data(symbol, end_date):
                    logging.info(f"[{symbol}] Already has data till {end_date}, skipping year {year}")
                    continue

                success = fetch_and_save_stock_data(symbol, start=start_date, end=end_date)
                if not success:
                    logging.warning(f"[{symbol}] Skipping year {year} due to fetch error.")
                    break  # Stop processing this symbol if even one year fails

                sleep(1)
        except Exception as e:
            logging.error(f"[{symbol}] Skipped due to unexpected symbol-level error: {e}")
            continue  # Move to next symbol

    logging.info("==== Completed stock data download process ====")

if __name__ == "__main__":
    main()
