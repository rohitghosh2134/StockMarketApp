import subprocess
import time
import os
import logging
from datetime import datetime

# === File and directory paths ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.abspath(os.path.join(BASE_DIR, "../scripts/getRawStock.py"))
LOG_FILE = os.path.join(BASE_DIR, "../logs/getRawStock.log")
WATCHDOG_LOG_FILE = os.path.join(BASE_DIR, "../logs/watchdog.log")
CHECK_INTERVAL = 60  # seconds
TIMEOUT = 5 * 60     # 5 minutes
DEFAULT_SYMBOL = None

# === Ensure required directories exist ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(os.path.dirname(SCRIPT_PATH), exist_ok=True)

# === Setup Watchdog Logging ===
logging.basicConfig(
    filename=WATCHDOG_LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_last_log_time():
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
        for line in reversed(lines):
            if "Processing symbol:" in line or "Fetching data from" in line:
                timestamp_str = line.split(" - ")[0]
                last_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                return last_time
    except Exception as e:
        logging.error(f"Failed to read last log time: {e}")
    return None

def get_last_symbol():
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
        for line in reversed(lines):
            if "Processing symbol:" in line:
                return line.strip().split("Processing symbol: ")[-1]
    except Exception as e:
        logging.error(f"Failed to get last symbol: {e}")
    return DEFAULT_SYMBOL

def run_script(symbol=None):
    if not os.path.isfile(SCRIPT_PATH):
        logging.error(f"Script not found at path: {SCRIPT_PATH}")
        return None
    args = ["python3", SCRIPT_PATH]
    if symbol:
        args.append(symbol)
    logging.info(f"Launching script with symbol: {symbol}")
    return subprocess.Popen(args)

def watchdog():
    logging.info("===== Watchdog started =====")
    last_run_time = None
    symbol = DEFAULT_SYMBOL
    process = run_script(symbol)

    while True:
        time.sleep(CHECK_INTERVAL)

        last_log_time = get_last_log_time()
        if last_log_time:
            last_run_time = last_log_time

        now = datetime.now()
        if last_run_time and (now - last_run_time).total_seconds() > TIMEOUT:
            logging.warning("No log activity in the last 5 minutes. Restarting script...")

            if process:
                process.terminate()
                process.wait()

            symbol = get_last_symbol()
            logging.info(f"Restarting script from symbol: {symbol}")

            process = run_script(symbol)
            last_run_time = datetime.now()

if __name__ == "__main__":
    try:
        watchdog()
    except KeyboardInterrupt:
        logging.info("Watchdog terminated manually.")
    except Exception as e:
        logging.error(f"Watchdog crashed with error: {e}")
