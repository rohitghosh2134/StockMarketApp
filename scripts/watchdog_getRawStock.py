# scripts/watchdog_getRawStock.py

import os
import sys
import time
import subprocess

# === Constants ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LOG_FILE = os.path.abspath(os.path.join(BASE_DIR,"logs", "getRawStock.log"))
SCRIPT_PATH = os.path.abspath(os.path.join(BASE_DIR,"scripts", "getRawStock.py"))
TIMEOUT_SECONDS = 60  # Restart if no log update within this time
AUTO_RESUME = True  # Set True to continue from last processed stock

def get_last_modified_time(path):
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return 0

def is_process_running(process):
    return process and process.poll() is None

def restart_process(process):
    if is_process_running(process):
        process.terminate()
        time.sleep(3)
    return launch_get_raw_stock()

def launch_get_raw_stock():
    flag = "--resume" if AUTO_RESUME else "--start"
    return subprocess.Popen([sys.executable, SCRIPT_PATH, flag])

def main():
    print("👀 Watchdog started. Monitoring log activity...")
    last_log_time = get_last_modified_time(LOG_FILE)

    process = launch_get_raw_stock()
    print(f"🚀 Started getRawStock.py (PID {process.pid})")

    try:
        while True:
            time.sleep(10)
            current_log_time = get_last_modified_time(LOG_FILE)

            if current_log_time > last_log_time:
                last_log_time = current_log_time
                continue

            idle_time = time.time() - last_log_time
            if idle_time >= TIMEOUT_SECONDS:
                print(f"⚠️  No log updates for {idle_time:.0f}s. Restarting...")
                process = restart_process(process)
                last_log_time = get_last_modified_time(LOG_FILE)
                print(f"🔁 Restarted getRawStock.py (PID {process.pid})")

    except KeyboardInterrupt:
        print("🛑 Watchdog stopped.")
        if is_process_running(process):
            process.terminate()

if __name__ == "__main__":
    main()
