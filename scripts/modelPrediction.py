import os
import pandas as pd
import numpy as np
import logging
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from datetime import timedelta

# === Constants ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "multi_step_forecast.log")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

def create_multi_step_targets(df, target_col='high', n_steps=5):
    for i in range(1, n_steps + 1):
        df[f'target_t+{i}'] = df[target_col].shift(-i)
    df.dropna(inplace=True)
    return df

def load_data(symbol):
    file_path = os.path.join(PROCESSED_DATA_PATH, f"{symbol}.csv")
    if not os.path.exists(file_path):
        logging.error(f"Data file for symbol '{symbol}' not found at {file_path}")
        return None
    df = pd.read_csv(file_path, parse_dates=['date'])
    df.sort_values('date', inplace=True)
    return df

def train_model(X_train, y_train):
    model = MultiOutputRegressor(RandomForestRegressor(n_estimators=100, random_state=42))
    model.fit(X_train, y_train)
    return model

def evaluate_model(y_true, y_pred):
    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    logging.info(f"Evaluation - MSE: {mse:.6f}, MAE: {mae:.6f}")

def predict_future(model, X):
    preds = model.predict(X)
    return preds

def calculate_n_steps_from_period(df, period_type, period_count, freq='B'):
    """
    Calculate n_steps based on period type and count.
    freq='B' is business days, you can change for weeks/months/years if needed.
    """
    if period_type == 'days':
        return period_count
    elif period_type == 'weeks':
        return period_count * 5  # approx 5 business days in a week
    elif period_type == 'months':
        return period_count * 21  # approx 21 business days in a month
    elif period_type == 'years':
        return period_count * 252  # approx 252 trading days per year
    else:
        logging.warning(f"Unknown period type '{period_type}', defaulting to days")
        return period_count

def main():
    symbol = input("Enter the stock symbol to forecast: ").strip()
    df = load_data(symbol)
    if df is None:
        print("Data file not found. Exiting.")
        return

    choice = input("Do you want to forecast by (1) period (days/weeks/months/years) or (2) up to a specific date? Enter 1 or 2: ").strip()
    
    if choice == '1':
        period_count = int(input("Enter the number of periods (e.g. 10): "))
        period_type = input("Enter the period type (days/weeks/months/years): ").strip().lower()
        n_steps = calculate_n_steps_from_period(df, period_type, period_count)
        logging.info(f"User chose to forecast {n_steps} steps ahead ({period_count} {period_type}) for symbol {symbol}")

        df = create_multi_step_targets(df, target_col='high', n_steps=n_steps)
    elif choice == '2':
        target_date_str = input("Enter the target date (YYYY-MM-DD): ").strip()
        try:
            target_date = pd.to_datetime(target_date_str)
        except Exception:
            print("Invalid date format. Exiting.")
            return
        
        last_date = df['date'].max()
        if target_date <= last_date:
            print(f"Target date {target_date.date()} is before or equal to last date in data {last_date.date()}, no prediction needed.")
            return
        
        # Calculate how many business days between last date and target_date
        # Use pandas bdate_range to count business days
        business_days = pd.bdate_range(start=last_date + pd.Timedelta(days=1), end=target_date)
        n_steps = len(business_days)
        logging.info(f"User chose to forecast up to date {target_date.date()} ({n_steps} business days ahead) for symbol {symbol}")

        df = create_multi_step_targets(df, target_col='high', n_steps=n_steps)
    else:
        print("Invalid choice. Exiting.")
        return

    feature_cols = [col for col in df.columns if col not in ['date'] and not col.startswith('target_t+')]
    target_cols = [f'target_t+{i}' for i in range(1, n_steps + 1)]

    X = df[feature_cols]
    y = df[target_cols]

    split_idx = int(len(df)*0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    logging.info(f"Training model with {len(X_train)} samples")
    model = train_model(X_train, y_train)

    logging.info(f"Predicting on test set of size {len(X_test)}")
    y_pred = predict_future(model, X_test)

    evaluate_model(y_test, y_pred)

    # Predict future values for last available data row
    last_features = X.iloc[-1].values.reshape(1, -1)
    future_preds = model.predict(last_features)

    logging.info(f"Future {n_steps}-step predictions for last date {df['date'].iloc[-1].date()}:")
    for i, val in enumerate(future_preds[0], 1):
        logging.info(f"  t+{i}: {val:.4f}")

if __name__ == "__main__":
    main()
