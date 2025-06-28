import subprocess
import platform
from pathlib import Path
from datetime import datetime

# Paths
base_dir = Path(__file__).resolve().parent
scripts_dir = base_dir / "scripts"
log_dir = base_dir / "logs"
log_file = log_dir / "run_pipeline.log"

# Create log directory if it doesn't exist
log_dir.mkdir(exist_ok=True)

# List of script filenames in order
scripts = ["fetchEQList.py", "cleanEQList.py", "autoRawStock.py", "stockDataClean.py"]

def log_section(title):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write("\n" + "="*60 + "\n")
        f.write(f"{title} | {timestamp}\n")
        f.write("="*60 + "\n")

def run_script(script_name):
    script_path = scripts_dir / script_name
    log_section(f"STARTING {script_name}")
    result = subprocess.run(
        ["python", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    with open(log_file, "a") as f:
        f.write(result.stdout)
    log_section(f"FINISHED {script_name}")

def start_log_tail():
    system = platform.system()
    if system == "Linux" or system == "Darwin":
        subprocess.Popen(["x-terminal-emulator", "-e", f"tail -f {log_file}"])  # fallback for most distros
    elif system == "Windows":
        command = f'start cmd /k "powershell -Command Get-Content {log_file} -Wait"'
        subprocess.Popen(command, shell=True)

def main():
    print(f"Log file: {log_file}")
    start_log_tail()
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
