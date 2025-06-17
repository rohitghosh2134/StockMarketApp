import os
import sys
import pandas as pd
import platform
import subprocess
import signal
from datetime import date
from time import sleep
from jugaad_data.nse import stock_df
import logging
import argparse

# === Configurable Date Range ===
START_YEAR, START_MONTH, START_DAY = 2000, 1, 1
END_YEAR, END_MONTH, END_DAY = 2025, 6, 15

START_DATE = date(START_YEAR, START_MONTH, START_DAY)
END_DATE = date(END_YEAR, END_MONTH, END_DAY)

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

log_viewer_process = None  # Global reference

def launch_log_viewer():
    global log_viewer_process
    system_platform = platform.system()

    try:
        if system_platform == "Linux":
            log_viewer_process = subprocess.Popen(
                ["gnome-terminal", "--", "bash", "-c", f"tail -f {log_file}"],
                preexec_fn=os.setsid
            )
        elif system_platform == "Darwin":
            log_viewer_process = subprocess.Popen(
                ["osascript", "-e", f'tell app "Terminal" to do script \"tail -f {log_file}\"']
            )
        elif system_platform == "Windows":
            log_viewer_process = subprocess.Popen(
                ["cmd.exe", "/c", f"start cmd /k tail -f {log_file}"],
                shell=True
            )
        logging.info("📟 Log viewer terminal launched.")
    except Exception as e:
        logging.warning(f"⚠️ Failed to launch log viewer: {e}")

def terminate_log_viewer():
    global log_viewer_process
    try:
        if log_viewer_process:
            os.killpg(os.getpgid(log_viewer_process.pid), signal.SIGTERM)
            logging.info("🛑 Log viewer terminated.")
    except Exception as e:
        logging.warning(f"⚠️ Failed to terminate log viewer: {e}")

def fetch_and_save_stock_data(symbol: str, start: date, end: date):
    try:
        logging.info(f"[{symbol}] Fetching data from {start} to {end}")
        df = stock_df(symbol=symbol, from_date=start, to_date=end, series="EQ")
        sleep(5)

        path = os.path.join(raw_data_dir, f"{symbol}.csv")
        df.columns = df.columns.str.lower()

        if os.path.exists(path):
            existing = pd.read_csv(path)
            existing.columns = existing.columns.str.lower()
            if "date" not in existing.columns:
                logging.warning(f"[{symbol}] Missing 'date' in existing file.")
                return False

            existing["date"] = pd.to_datetime(existing["date"])
            df["date"] = pd.to_datetime(df["date"])
            new_rows = df[~df["date"].isin(existing["date"])]

            if not new_rows.empty:
                combined = pd.concat([existing, new_rows], ignore_index=True).sort_values(by="date")
                combined.to_csv(path, index=False)
                logging.info(f"[{symbol}] Appended {len(new_rows)} new rows.")
            else:
                logging.info(f"[{symbol}] No new rows to append.")
        else:
            df.to_csv(path, index=False)
            logging.info(f"[{symbol}] Saved new file.")
        return True
    except Exception as e:
        logging.error(f"[{symbol}] Error: {e}")
        return False

def already_has_data(symbol: str, end: date):
    path = os.path.join(raw_data_dir, f"{symbol}.csv")
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()
        if "date" not in df.columns:
            logging.warning(f"[{symbol}] Missing 'date' column.")
            return False
        df["date"] = pd.to_datetime(df["date"])
        return not df[df["date"] >= pd.Timestamp(end)].empty
    except Exception as e:
        logging.warning(f"[{symbol}] Failed to read CSV: {e}")
        return False

def get_last_processed_symbol_from_log():
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
            for line in reversed(lines):
                if "Processing symbol:" in line:
                    return line.split("Processing symbol:")[-1].strip()
    except Exception as e:
        logging.warning(f"⚠️ Failed to read log file: {e}")
    return None

def main():
    launch_log_viewer()
    logging.info("==== Starting getRawStock ====")

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", action="store_true", help="Start from beginning")
    parser.add_argument("--resume", action="store_true", help="Resume from last processed symbol in log")
    args = parser.parse_args()

    if args.start and args.resume:
        logging.error("❌ Conflicting arguments: use either --start or --resume, not both.")
        terminate_log_viewer()
        return

    try:
        df = pd.read_csv(stock_list_path)
        symbols = df.iloc[:, 0].dropna().unique().tolist()
        logging.info(f"✅ Loaded {len(symbols)} symbols.")
    except Exception as e:
        logging.error(f"❌ Error loading symbol list: {e}")
        terminate_log_viewer()
        return

    if args.resume:
        last_symbol = get_last_processed_symbol_from_log()
        if last_symbol and last_symbol in symbols:
            start_index = symbols.index(last_symbol)
            symbols = symbols[start_index:]
            logging.info(f"⏯️ Resuming from last logged symbol: {last_symbol}")
        else:
            logging.warning(f"⚠️ Last symbol '{last_symbol}' not found in stock list. Starting fresh.")

    for symbol in symbols:
        logging.info(f"🔄 Processing symbol: {symbol}")

        # ✅ NEW: Check if symbol already has data up to END_DATE
        path = os.path.join(raw_data_dir, f"{symbol}.csv")
        if os.path.exists(path):
            try:
                df_existing = pd.read_csv(path)
                df_existing.columns = df_existing.columns.str.lower()
                if "date" in df_existing.columns:
                    df_existing["date"] = pd.to_datetime(df_existing["date"])
                    last_date = df_existing["date"].max().date()
                    if last_date >= END_DATE:
                        logging.info(f"[{symbol}] ⏭️ Skipping: already up-to-date till {last_date}")
                        continue
            except Exception as e:
                logging.warning(f"[{symbol}] ⚠️ Failed to check last date in file: {e}")

        failed = True
        for year in range(START_YEAR, END_YEAR + 1):
            start = date(year, 1, 1)
            end = END_DATE if year == END_YEAR else date(year, 12, 31)

            if already_has_data(symbol, end):
                logging.info(f"[{symbol}] ✅ Already up-to-date for {year}.")
                continue

            if fetch_and_save_stock_data(symbol, start, end):
                failed = False
            else:
                logging.warning(f"[{symbol}] ⚠️ Failed for year {year}")

        if failed:
            logging.warning(f"[{symbol}] ❌ Skipped due to failure in all years")

    logging.info("✅✅✅ Stock data fetch complete.")
    terminate_log_viewer()

if __name__ == "__main__":
    main()
