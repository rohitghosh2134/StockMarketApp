import os
import pandas as pd
from datetime import date
from jugaad_data.nse import stock_df
import logging

# Set up logging
log_dir = os.path.join("logs")
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_dir, "getRawStock.log"),
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

# File and directory paths
stock_list_path = os.path.join("data", "stockList", "stockList_clean.csv")
raw_data_dir = os.path.join("data", "rawData")

# Ensure raw data directory exists
os.makedirs(raw_data_dir, exist_ok=True)

def fetch_and_save_stock_data(symbol: str, start: date, end: date):
    try:
        logging.info(f"Fetching data for {symbol}")
        df = stock_df(symbol=symbol, from_date=start, to_date=end, series="EQ")
        output_path = os.path.join(raw_data_dir, f"{symbol}.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"Saved data for {symbol} to {output_path}")
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")

def main():
    try:
        df_symbols = pd.read_csv(stock_list_path)
        symbols = df_symbols.iloc[:, 0].dropna().unique()
        start_date = date(2020, 1, 1)
        end_date = date(2020, 1, 30)

        logging.info(f"Starting download for {len(symbols)} symbols.")

        for symbol in symbols:
            fetch_and_save_stock_data(symbol.strip(), start=start_date, end=end_date)

        logging.info("Finished downloading all stock data.")

    except FileNotFoundError:
        logging.error(f"Stock list file not found at {stock_list_path}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
