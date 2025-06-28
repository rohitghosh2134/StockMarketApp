import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# === Constants ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "preProcessed")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "preprocess.log")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# === Logger Setup ===
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === Columns to Keep ===
keep_cols = [
    'date', 'open', 'high', 'low', 'prev. close',
    'ltp', 'close', '52w h', '52w l', 'volume', 'value'
]

# === RSI Function ===
def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))

# === Preprocessing Function ===
def preprocess_df(df, symbol):
    try:
        df = df[keep_cols].copy()
        df.replace("-", None, inplace=True)
        df.dropna(inplace=True)
        for col in df.columns:
            if col != 'date':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)

        df['date'] = pd.to_datetime(df['date'])
        df.sort_values('date', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Price action features
        df['price_range'] = df['high'] - df['low']
        df['gap_up'] = df['open'] - df['prev. close']
        df['body'] = abs(df['close'] - df['open'])
        df['upper_shadow'] = df['high'] - df[['close', 'open']].max(axis=1)
        df['lower_shadow'] = df[['close', 'open']].min(axis=1) - df['low']

        # Lag features
        for lag in [1, 2, 3, 5]:
            df[f'close_lag_{lag}'] = df['close'].shift(lag)
            df[f'high_lag_{lag}'] = df['high'].shift(lag)
            df[f'low_lag_{lag}'] = df['low'].shift(lag)
            df[f'volume_lag_{lag}'] = df['volume'].shift(lag)

        # Rolling features
        for window in [3, 5, 10, 20]:
            df[f'close_ma_{window}'] = df['close'].rolling(window).mean()
            df[f'high_std_{window}'] = df['high'].rolling(window).std()
            df[f'volume_ma_{window}'] = df['volume'].rolling(window).mean()

        # RSI
        df['rsi'] = compute_rsi(df['close'])

        # Percent change features
        df['pct_change_close'] = df['close'].pct_change()
        df['pct_change_high'] = df['high'].pct_change()
        df['pct_change_volume'] = df['volume'].pct_change()

        # Interaction features
        df['close_to_high'] = df['close'] / (df['high'] + 1e-9)
        df['low_to_close'] = df['low'] / (df['close'] + 1e-9)
        df['volume_to_value'] = df['volume'] / (df['value'] + 1e-9)

        # Time-based features
        df['day_of_week'] = df['date'].dt.dayofweek
        df['month'] = df['date'].dt.month

        # Multi-step future high targets (for 20 business days)
        for step in range(1, 21):
            df[f'high_t_plus_{step}'] = df['high'].shift(-step)

        

        # Drop initial rows with NA (from rolling and lag)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

        logger.info(f"[{symbol}] ✅ Preprocessing complete. Final rows: {len(df)}")
        return df

    except Exception as e:
        logger.error(f"[{symbol}] ❌ Failed to preprocess: {str(e)}")
        return None

# === Process All Symbols ===
def process_all_stocks():
    logger.info("==== Starting Preprocessing for All Stocks ====")
    for file in os.listdir(CLEAN_DIR):
        if file.endswith(".csv"):
            symbol = file.replace(".csv", "")
            logger.info(f"--- Processing {symbol} ---")
            path = os.path.join(CLEAN_DIR, file)
            try:
                df = pd.read_csv(path)
                processed_df = preprocess_df(df, symbol)
                if processed_df is not None:
                    save_path = os.path.join(PROCESSED_DIR, f"{symbol}.csv")
                    processed_df.to_csv(save_path, index=False)
                    logger.info(f"[{symbol}] ✅ Saved to: {save_path}")
            except Exception as e:
                logger.error(f"[{symbol}] ❌ Error during file processing: {e}")
    logger.info("==== Completed All Preprocessing ====")

if __name__ == "__main__":
    process_all_stocks()

