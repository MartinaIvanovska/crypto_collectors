# PIEVERSE-USD (PostgreSQL version)

# %% Requirements
# pip install numpy pandas scikit-learn tensorflow joblib psycopg2-binary

# %% imports & config
import os
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from math import sqrt
from joblib import dump, load
from concurrent.futures import ThreadPoolExecutor, as_completed

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error

from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping

import psycopg2
from psycopg2.extras import execute_values

# --------------------------------------------------
# PROJECT STRUCTURE
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(__file__, "../../.."))
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# --------------------------------------------------
# POSTGRES CONFIG (Docker)
# --------------------------------------------------
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB", "crypto"),
    "user": os.getenv("PG_USER", "crypto_user"),
    "password": os.getenv("PG_PASS", "crypto_pass"),
}

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


# --------------------------------------------------
# DEFAULTS
# --------------------------------------------------
DEFAULT_LOOKBACKS = [30]
DEFAULT_FORECAST_DAYS = 10
MAX_WORKERS = 5
MIN_SAMPLES = 200
HIST_PRED_DAYS = 30
FUTURE_PRED_DAYS = 30

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# --------------------------------------------------
# PATH HELPERS
# --------------------------------------------------
def model_path(symbol, lookback):
    return os.path.join(MODEL_DIR, f"{symbol}_lb{lookback}.h5")


def scaler_path(symbol, lookback):
    return os.path.join(MODEL_DIR, f"{symbol}_lb{lookback}_scaler.joblib")


# --------------------------------------------------
# DB INIT
# --------------------------------------------------
def ensure_predictions_table():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            predicted_close DOUBLE PRECISION,
            PRIMARY KEY (symbol, date)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


# --------------------------------------------------
# MODEL
# --------------------------------------------------
def build_lstm_model(lookback, features):
    model = Sequential([
        LSTM(128, return_sequences=True, input_shape=(lookback, features)),
        Dropout(0.2),
        LSTM(64),
        Dropout(0.1),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse")
    return model


# --------------------------------------------------
# DATA PREP
# --------------------------------------------------
def create_sequences(scaled_array, lookback, target_col_idx=3):
    X, y = [], []
    for i in range(lookback, len(scaled_array)):
        X.append(scaled_array[i - lookback:i])
        y.append(scaled_array[i, target_col_idx])
    return np.array(X), np.array(y)


# --------------------------------------------------
# METRICS
# --------------------------------------------------
def compute_metrics(y_true, y_pred):
    mse = mean_squared_error(y_true, y_pred)
    rmse = sqrt(mse)
    try:
        mape = mean_absolute_percentage_error(y_true, y_pred)
    except Exception:
        mape = np.mean(np.abs((y_true - y_pred) / y_true))
    r2 = r2_score(y_true, y_pred)
    return {"mse": mse, "rmse": rmse, "mape": mape, "r2": r2}


# --------------------------------------------------
# RECURSIVE FORECASTING FUNCTION
# --------------------------------------------------
def predict_future(symbol, lookback, forecast_days, model, scaler, df, cols):
    """
    Predict the future closing prices using recursive forecasting.
    This function generates future predictions one day at a time.

    :param symbol: Cryptocurrency symbol (e.g., 'BTC')
    :param lookback: The number of past days to consider for predicting future days
    :param forecast_days: The number of days to forecast into the future
    :param model: Trained LSTM model
    :param scaler: Scaler used for normalizing the data
    :param df: DataFrame with historical data
    :param cols: The column names used for training ('open', 'high', 'low', 'close', 'volume')

    :return: List of future predicted closing prices
    """
    # Prepare the latest available data for forecasting
    last_data = df[cols].tail(lookback).values.astype(float)  # Take the last 'lookback' rows
    scaled_last_data = scaler.transform(last_data)  # Scale the data to the same scale as the training data

    future_predictions = []

    # Generate predictions one day at a time (recursive forecasting)
    for i in range(forecast_days):
        # Create the input for the model (reshape to match the model input)
        X_input = scaled_last_data.reshape(1, lookback, len(cols))

        # Predict the next day using the model
        pred_scaled = model.predict(X_input, verbose=0)

        # Inverse transform the prediction to get the real value (for closing price)
        temp = np.zeros((1, len(cols)))
        temp[:, 3] = pred_scaled  # Column index 3 corresponds to 'close' in the columns
        pred_real = scaler.inverse_transform(temp)[:, 3][0]

        # Append the predicted value to the future predictions list
        future_predictions.append(pred_real)

        # Update the scaled_last_data by adding the new predicted value (for recursive forecasting)
        # Here, we assume the open, high, low, and volume for the new day are unknown, so we set them to zero
        new_data = np.array([[0, 0, 0, pred_real, 0]])  # Assuming open, high, low, and volume are unknown
        scaled_new_data = scaler.transform(new_data)  # Scale the new data
        scaled_last_data = np.concatenate((scaled_last_data[1:], scaled_new_data), axis=0)  # Update the input

    return future_predictions


# --------------------------------------------------
# MAIN PER-SYMBOL PIPELINE
# --------------------------------------------------
def process_coin(symbol, lookback=30, forecast_days=10, dry_run=False):
    conn = get_pg_conn()

    df = pd.read_sql("""
        SELECT date, open, high, low, close, volume
        FROM daily
        WHERE symbol = %s
        ORDER BY date ASC
    """, conn, params=(symbol,))

    if df.empty:
        conn.close()
        return

    df["date"] = pd.to_datetime(df["date"])
    if len(df) < lookback + 1:
        conn.close()
        return

    cols = ["open", "high", "low", "close", "volume"]
    feat_arr = df[cols].values.astype(float)

    sp = scaler_path(symbol, lookback)
    if os.path.exists(sp):
        scaler = load(sp)
        scaled = scaler.transform(feat_arr)
    else:
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(feat_arr)
        dump(scaler, sp)

    X, y_scaled = create_sequences(scaled, lookback)
    dates = df["date"].values[lookback:]

    split = int(len(X) * 0.7)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y_scaled[:split], y_scaled[split:]

    mp = model_path(symbol, lookback)
    if os.path.exists(mp):
        model = load_model(mp, compile=False)
        model.compile(optimizer="adam", loss="mse")
        epochs = 5
    else:
        model = build_lstm_model(lookback, X.shape[2])
        epochs = 50

    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=epochs,
        batch_size=32,
        callbacks=[EarlyStopping(patience=5, restore_best_weights=True)],
        verbose=0
    )

    model.save(mp)
    dump(scaler, sp)

    preds_all = model.predict(X, verbose=0).reshape(-1)
    temp = np.zeros((len(preds_all), len(cols)))
    temp[:, 3] = preds_all
    inv_preds_all = scaler.inverse_transform(temp)[:, 3]

    hist_rows = [
        (symbol, pd.to_datetime(d).date(), float(p))
        for d, p in zip(dates[-HIST_PRED_DAYS:], inv_preds_all[-HIST_PRED_DAYS:])
    ]

    # Recursive future prediction
    future_predictions = predict_future(symbol, lookback, forecast_days, model, scaler, df, cols)

    last_date = df["date"].max()
    future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=forecast_days)

    future_rows = [
        (symbol, d.date(), float(pred))
        for d, pred in zip(future_dates, future_predictions)
    ]

    insert_sql = """
        INSERT INTO predictions (symbol, date, predicted_close)
        VALUES %s
        ON CONFLICT (symbol, date)
        DO UPDATE SET predicted_close = EXCLUDED.predicted_close
    """

    cur = conn.cursor()
    execute_values(cur, insert_sql, hist_rows + future_rows)
    conn.commit()
    cur.close()
    conn.close()


# --------------------------------------------------
# SYMBOL HELPERS
# --------------------------------------------------
def get_all_symbols():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM daily")
    symbols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols


def get_unpredicted_symbols():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT d.symbol
        FROM daily d
        LEFT JOIN predictions p ON d.symbol = p.symbol
        WHERE p.symbol IS NULL
    """)
    syms = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return syms


# --------------------------------------------------
# PARALLEL RUNNER
# --------------------------------------------------
def run_pipeline(symbols):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_coin, s, 30) for s in symbols]
        for _ in as_completed(futures):
            pass


# --------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------
if __name__ == "__main__":

    ensure_predictions_table()

    logger.info("Fetching symbols from PostgreSQL...")
    symbols = get_unpredicted_symbols()

    logger.info(f"Symbols to process: {len(symbols)}")
    if not symbols:
        logger.warning("Nothing to do.")
        exit()
    run_pipeline(["BTC-USD","ETH-USD"])
    # run_pipeline(symbols)

    logger.info("LSTM prediction pipeline completed.")
