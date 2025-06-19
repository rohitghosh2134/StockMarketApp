#!/usr/bin/env python3
"""
Clean raw NSE stock‑quote CSVs so they’re ready for a ‘predict‑the‑daily‑high’ model.

• Reads every *.csv in data/rawData
• Keeps columns: date, open, high, low, close, volume
• Drops bad / missing rows, sorts by date
• Saves to data/clean/{symbol}.csv   (creates the folder if missing)
• Writes a detailed log to logs/getCleanStock.log
• Launches a live‑updating log‑viewer terminal and closes it on exit
"""

import os
import sys
import pandas as pd
import logging
import platform
import subprocess
import signal

# ── Directory setup ────────────────────────────────────────────────────────────
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR    = os.path.join(BASE_DIR, "data", "rawData")
CLEAN_DIR  = os.path.join(BASE_DIR, "data", "clean")
LOG_DIR    = os.path.join(BASE_DIR, "logs")
LOG_FILE   = os.path.join(LOG_DIR, "getCleanStock.log")

os.makedirs(RAW_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(LOG_DIR,   exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename   = LOG_FILE,
    filemode   = "a",
    level      = logging.INFO,
    format     = "%(asctime)s - %(levelname)s - %(message)s"
)

# ── Live log viewer helpers ────────────────────────────────────────────────────
log_viewer_proc = None

def launch_log_viewer() -> None:
    """Open a terminal that tails the log file."""
    global log_viewer_proc
    try:
        system = platform.system()
        if system == "Linux":
            log_viewer_proc = subprocess.Popen(
                ["gnome-terminal", "--", "bash", "-c", f"tail -f {LOG_FILE}"],
                preexec_fn=os.setsid          # so we can kill the whole process group
            )
        elif system == "Darwin":
            log_viewer_proc = subprocess.Popen(
                ["osascript", "-e", f'tell app "Terminal" to do script "tail -f {LOG_FILE}"']
            )
        elif system == "Windows":
            log_viewer_proc = subprocess.Popen(
                ["cmd.exe", "/c", f"start cmd /k powershell -Command \"Get-Content {LOG_FILE} -Wait\""],
                shell=True
            )
        logging.info("📟 Opened live log viewer terminal.")
    except Exception as e:
        logging.warning(f"⚠️  Could not launch log viewer: {e}")

def close_log_viewer() -> None:
    """Terminate the tail‑f terminal."""
    global log_viewer_proc
    try:
        if log_viewer_proc and log_viewer_proc.poll() is None:
            if platform.system() == "Linux":
                os.killpg(os.getpgid(log_viewer_proc.pid), signal.SIGTERM)
            else:
                log_viewer_proc.terminate()
        logging.info("🛑 Closed live log viewer terminal.")
    except Exception as e:
        logging.warning(f"⚠️  Could not close log viewer: {e}")

# ── Cleaning routine ───────────────────────────────────────────────────────────
REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]

def clean_one_file(path: str, symbol: str) -> None:
    logging.info(f"\n========== CLEANING {symbol} ==========")
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip().str.lower()

        # ensure all required columns exist
        if any(col not in df.columns for col in REQUIRED_COLS):
            logging.warning(f"[{symbol}] Missing required columns → skipped.")
            return

        # convert + drop bad rows
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.dropna(subset=REQUIRED_COLS, inplace=True)
        df.sort_values("date", inplace=True)

        cleaned = df[REQUIRED_COLS]
        save_path = os.path.join(CLEAN_DIR, f"{symbol}.csv")
        cleaned.to_csv(save_path, index=False)
        logging.info(f"[{symbol}] ✅ Cleaned → {save_path}")
    except Exception as err:
        logging.error(f"[{symbol}] ❌ Error: {err}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    launch_log_viewer()
    logging.info("\n==================== START CLEAN ====================")

    for file in sorted(os.listdir(RAW_DIR)):
        if file.lower().endswith(".csv"):
            symbol = file[:-4]  # strip .csv
            clean_one_file(os.path.join(RAW_DIR, file), symbol)

    logging.info("==================== CLEAN COMPLETE ====================\n")
    close_log_viewer()

if __name__ == "__main__":
    try:
        main()
    finally:
        # ensure log viewer is closed even if script crashes
        close_log_viewer()
