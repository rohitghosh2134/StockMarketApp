import os
import pandas as pd
import logging

# === Setup Logging ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "cleanEQList.log")

logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# === Configurations ===
file_path = "data/stockList/stockList_raw.csv"
output_path = "data/stockList/stockList_clean.csv"

def main():
    logging.info("==== Starting symbol cleaning process ====")

    # Step 1: Load raw CSV
    try:
        df = pd.read_csv(file_path)
        print("📄 Loaded raw stock list successfully.")
        logging.info(f"Loaded file: {file_path} with {len(df)} rows.")
    except FileNotFoundError:
        msg = f"File not found: {file_path}"
        print(f"❌ {msg}")
        logging.error(msg)
        return
    except Exception as e:
        logging.exception(f"Unexpected error reading CSV: {e}")
        print("❌ Unexpected error reading the CSV file.")
        return

    # Step 2: Filter symbol_ticker pattern 'NSE : {stockName} - EQ'
    pattern = r'^NSE\s*:\s*.+\s*-\s*EQ$'
    try:
        filtered_df = df[df['symbol_ticker'].str.contains(pattern, regex=True, na=False)]
        print(f"✅ Total rows matching 'NSE : {{stockName}} - EQ': {len(filtered_df)}")
        logging.info(f"Filtered {len(filtered_df)} rows matching NSE EQ pattern.")
    except KeyError:
        msg = "'symbol_ticker' column not found in input CSV."
        print(f"❌ {msg}")
        logging.error(msg)
        return

    # Step 3: Keep only 'underlying_symbol' and sort alphabetically
    try:
        filtered_df = filtered_df[['underlying_symbol']].drop_duplicates().reset_index(drop=True)
        filtered_df = filtered_df.sort_values(by='underlying_symbol').reset_index(drop=True)
        logging.info(f"Extracted {len(filtered_df)} unique underlying symbols, sorted alphabetically.")
    except KeyError:
        msg = "'underlying_symbol' column not found in input CSV."
        print(f"❌ {msg}")
        logging.error(msg)
        return

    # Step 4: Save cleaned symbols
    try:
        filtered_df.to_csv(output_path, index=False)
        print(f"💾 Cleaned symbol list saved to: {output_path}")
        logging.info(f"Saved cleaned list to: {output_path}")
    except Exception as e:
        logging.exception(f"Error saving cleaned CSV: {e}")
        print("❌ Failed to save the cleaned CSV.")

    logging.info("==== Symbol cleaning process completed ====")

if __name__ == "__main__":
    main()
