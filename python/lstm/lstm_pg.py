import os
import logging
from math import sqrt
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from joblib import dump, load
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error

import psycopg2
from psycopg2.extras import execute_values
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
import tensorflow as tf

gpus = tf.config.list_physical_devices('GPU')
if gpus:
    try:
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
        print(f"Using GPU(s): {gpus}")
    except RuntimeError as e:
        print(e)
else:
    print("No GPU found, using CPU")


# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(__file__, ".."))
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB", "crypto"),
    "user": os.getenv("PG_USER", "crypto_user"),
    "password": os.getenv("PG_PASS", "crypto_pass"),
}

DEFAULT_LOOKBACK = 30
DEFAULT_FORECAST_DAYS = 10
MAX_WORKERS = 12
HIST_PRED_DAYS = 30
MIN_SAMPLES = 60  # require at least this many samples to train/predict

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("pieverse")

# --------------------------------------------------
# PATH HELPERS
# --------------------------------------------------

def model_path(symbol, lookback):
    sanitized = symbol.replace("/", "-")
    return os.path.join(MODEL_DIR, f"{sanitized}_lb{lookback}.h5")


def scaler_path(symbol, lookback):
    sanitized = symbol.replace("/", "-")
    return os.path.join(MODEL_DIR, f"{sanitized}_lb{lookback}_close_scaler.joblib")

# --------------------------------------------------
# DB helpers
# --------------------------------------------------

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


def ensure_predictions_table():
    conn = get_pg_conn()
    try:
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
    finally:
        conn.close()

# --------------------------------------------------
# DATA PREP
# --------------------------------------------------

def create_sequences_1d(series, lookback):
    """Create X,y from a 1D numpy array (shape: [n_samples, 1] or [n_samples,]).
    Returns X shaped (n, lookback, 1) and y shaped (n, ).
    """
    arr = np.array(series).reshape(-1, 1).astype(float)
    X, y = [], []
    for i in range(lookback, len(arr)):
        X.append(arr[i - lookback:i, 0])
        y.append(arr[i, 0])
    X = np.array(X)
    y = np.array(y)
    return X.reshape((X.shape[0], X.shape[1], 1)), y

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
    r2 = r2_score(y_true, y_pred) if len(y_true) > 1 else float('nan')
    return {"mse": mse, "rmse": rmse, "mape": mape, "r2": r2}

# --------------------------------------------------
# MODEL
# --------------------------------------------------

def build_lstm_model(lookback):
    model = Sequential([
        LSTM(64, return_sequences=False, input_shape=(lookback, 1)),
        Dropout(0.15),
        Dense(32, activation='relu'),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse")
    return model

# --------------------------------------------------
# PREDICTION (stable recursive for 1D close)
# --------------------------------------------------

def predict_future_close_only(model, scaler, close_series, lookback, forecast_days):
    """Recursive forecasting for 1D close-only model.

    - close_series: array-like of raw (unscaled) close prices (full historical history)
    - scaler: fitted scaler for close values
    - model: trained keras model that expects shape (1, lookback, 1) and outputs scaled close
    """
    # Prepare last lookback closes
    last_seq = np.array(close_series[-lookback:]).reshape(-1, 1)
    last_scaled = scaler.transform(last_seq)[:, 0]

    future_preds = []
    seq = last_scaled.copy()

    for _ in range(forecast_days):
        X_input = seq.reshape(1, lookback, 1)
        pred_scaled = model.predict(X_input, verbose=0)[0, 0]
        # inverse transform
        pred_inv = scaler.inverse_transform(np.array([[pred_scaled]]))[0, 0]
        future_preds.append(float(pred_inv))
        # append scaled prediction and slide
        seq = np.append(seq[1:], pred_scaled)

    return future_preds

# --------------------------------------------------
# PROCESS PIPELINE (per-symbol)
# --------------------------------------------------

def process_coin(symbol, lookback=DEFAULT_LOOKBACK, forecast_days=DEFAULT_FORECAST_DAYS, dry_run=False):
    logger.info(f"Processing {symbol} (lookback={lookback}, forecast_days={forecast_days})")
    conn = get_pg_conn()
    try:
        df = pd.read_sql("""
            SELECT date, close
            FROM daily
            WHERE symbol = %s
            ORDER BY date ASC
        """, conn, params=(symbol,))

        if df.empty:
            logger.warning(f"No data for {symbol}")
            return

        df['date'] = pd.to_datetime(df['date'])

        if len(df) < max(lookback + 1, MIN_SAMPLES):
            logger.warning(f"Not enough data for {symbol}. Need >= {max(lookback + 1, MIN_SAMPLES)}, got {len(df)}")
            return

        closes = df['close'].values.astype(float)

        # scaler (close only)
        sp = scaler_path(symbol, lookback)
        if os.path.exists(sp):
            scaler = load(sp)
        else:
            scaler = MinMaxScaler()
            scaler.fit(closes.reshape(-1, 1))
            dump(scaler, sp)

        scaled = scaler.transform(closes.reshape(-1, 1))

        # create sequences
        X, y = create_sequences_1d(scaled, lookback)

        # train/val split
        split = int(len(X) * 0.8)
        X_train, X_val = X[:split], X[split:]
        y_train, y_val = y[:split], y[split:]

        mp = model_path(symbol, lookback)
        if os.path.exists(mp):
            model = load_model(mp, compile=False)
            model.compile(optimizer='adam', loss='mse')
            start_epochs = 10
        else:
            model = build_lstm_model(lookback)
            start_epochs = 200

        es = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True, verbose=1)
        model.fit(
            X_train, y_train,
            validation_data=(X_val, y_val),
            epochs=start_epochs,
            batch_size=32,
            callbacks=[es],
            verbose=1
        )

        model.save(mp)
        dump(scaler, sp)

        # in-sample predictions (inverse)
        preds_scaled = model.predict(X, verbose=0).reshape(-1, 1)
        inv_preds = scaler.inverse_transform(preds_scaled)[:, 0]

        # historic rows (last HIST_PRED_DAYS predictions mapped to their dates)
        dates = df['date'].values[lookback:]
        hist_rows = []
        for d, p in zip(dates[-HIST_PRED_DAYS:], inv_preds[-HIST_PRED_DAYS:]):
            hist_rows.append((symbol, pd.to_datetime(d).date(), float(p)))

        # future predictions (stable recursive)
        future_predictions = predict_future_close_only(model, scaler, closes, lookback, forecast_days)
        last_date = df['date'].max()
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=forecast_days)
        future_rows = [(symbol, d.date(), float(p)) for d, p in zip(future_dates, future_predictions)]

        if dry_run:
            logger.info(f"Dry run enabled. Would insert {len(hist_rows) + len(future_rows)} rows")
            return

        # write to DB with upsert
        insert_sql = """
            INSERT INTO predictions (symbol, date, predicted_close)
            VALUES %s
            ON CONFLICT (symbol, date)
            DO UPDATE SET predicted_close = EXCLUDED.predicted_close
        """
        cur = conn.cursor()
        try:
            execute_values(cur, insert_sql, hist_rows + future_rows)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.exception("Failed to write predictions to DB")
            raise
        finally:
            cur.close()

        # compute and log validation metrics using last part of in-sample predictions
        try:
            # align true closes with inv_preds (these are predictions for dates[lookback:])
            true_for_preds = df['close'].values[lookback:]
            metrics = compute_metrics(true_for_preds[-len(inv_preds):], inv_preds)
            logger.info(f"Metrics for {symbol}: {metrics}")
        except Exception:
            logger.exception("Failed to compute metrics")

    finally:
        conn.close()

# --------------------------------------------------
# SYMBOL HELPERS
# --------------------------------------------------

def get_all_symbols():
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM daily")
        symbols = [r[0] for r in cur.fetchall()]
        cur.close()
        return symbols
    finally:
        conn.close()


def get_unpredicted_symbols():
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT d.symbol
            FROM daily d
            LEFT JOIN predictions p ON d.symbol = p.symbol
            WHERE p.symbol IS NULL
        """)
        syms = [r[0] for r in cur.fetchall()]
        cur.close()
        return syms
    finally:
        conn.close()

# --------------------------------------------------
# PARALLEL RUNNER
# --------------------------------------------------

def run_pipeline(symbols, lookback=DEFAULT_LOOKBACK, forecast_days=DEFAULT_FORECAST_DAYS, dry_run=False):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_coin, s, lookback, forecast_days, dry_run) for s in symbols]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                logger.exception("Error in worker")

# --------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------
if __name__ == "__main__":
    ensure_predictions_table()

    # Example: process a small set
    symbols_to_run = ["BTC-USD", "ETH-USD"]

    all_symbols = get_all_symbols()

    logger.info("Starting pipeline")
    run_pipeline(all_symbols, lookback=DEFAULT_LOOKBACK, forecast_days=DEFAULT_FORECAST_DAYS)
    logger.info("Pipeline finished")
