import os
import sys
import pandas as pd
import subprocess
import time

# === Constants ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "data", "clean")
LOG_FILE = os.path.join(BASE_DIR, "logs", "getCleanStock.log")

os.makedirs(CLEAN_DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log(message):
    with open(LOG_FILE, "a") as logf:
        logf.write(f"[INFO] {message}\n")
    print(f"[INFO] {message}")

def process_stock(symbol):
    input_file = os.path.join(RAW_DATA_PATH, f"{symbol}.csv")
    output_file = os.path.join(CLEAN_DATA_PATH, f"{symbol}.csv")

    if not os.path.exists(input_file):
        log(f"File not found: {input_file}")
        return

    try:
        df = pd.read_csv(input_file, parse_dates=['date'])

        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Keep only essential columns (you can customize this list)
        keep_cols = [
            'date', 'open', 'high', 'low', 'prev. close',
            'ltp', 'close', '52w h', '52w l', 'volume', 'value'
        ]
        df = df[keep_cols]

        df.to_csv(output_file, index=False)
        log(f"✅ Cleaned (columns only): {symbol}")
    except Exception as e:
        log(f"❌ Error processing {symbol}: {e}")

def open_log_terminal():
    try:
        return subprocess.Popen(['x-terminal-emulator', '-e', f'tail -f "{LOG_FILE}"'])
    except FileNotFoundError:
        # Fallback for GNOME, KDE, Xfce terminals
        for terminal in ['gnome-terminal', 'konsole', 'xfce4-terminal', 'xterm']:
            try:
                return subprocess.Popen([terminal, '-e', f'tail -f "{LOG_FILE}"'])
            except FileNotFoundError:
                continue
        print("[WARN] Could not open terminal for tailing logs.")
        return None

def main():
    log("=== Starting raw stock data column cleaning ===")
    terminal_proc = open_log_terminal()
    time.sleep(1)

    for file in os.listdir(RAW_DATA_PATH):
        if file.endswith(".csv"):
            symbol = os.path.splitext(file)[0]
            process_stock(symbol)

    log("=== Raw column cleaning complete ===")
    time.sleep(2)
    if terminal_proc:
        terminal_proc.terminate()

if __name__ == "__main__":
    main()
