#%% Requirements (run once)
# pip install numpy pandas scikit-learn tensorflow joblib

#%% imports & config
import os
import sqlite3
import logging
import numpy as np
import pandas as pd
import csv

from math import sqrt
from joblib import dump, load

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error

from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping

from concurrent.futures import ThreadPoolExecutor, as_completed

# base paths (adjust if needed)
BASE_DIR = os.path.dirname(os.path.abspath("das_project_springboot/../.."))
DB_PATH = os.path.join(BASE_DIR, "data", "crypto_daily.db")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# defaults
DEFAULT_LOOKBACKS = [30]         # you can add more like [10, 30, 60] to experiment
DEFAULT_FORECAST_DAYS = 10       # used for quick runs; actual DB writes use FUTURE_PRED_DAYS below
MAX_WORKERS = 3
MIN_SAMPLES = 200                # minimum rows required to attempt training (adjustable)
HIST_PRED_DAYS = 30              # write last 30 days of historical predictions
FUTURE_PRED_DAYS = 30            # write next 30 days of forecasts

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
#%% helpers for paths
MODEL_DIR
#%% helpers for paths
def model_path(symbol, lookback):
    return os.path.join(MODEL_DIR, f"{symbol}_lb{lookback}.h5")

def scaler_path(symbol, lookback):
    return os.path.join(MODEL_DIR, f"{symbol}_lb{lookback}_scaler.joblib")

#%% DB table ensure
def ensure_predictions_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            symbol TEXT,
            date TEXT,
            predicted_close REAL,
            PRIMARY KEY (symbol, date)
        );
    """)
    conn.commit()
    conn.close()

#%% model builder
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

#%% prepare sequences (X,y) - time ordered
def create_sequences(scaled_array, lookback, target_col_idx=3):
    """
    scaled_array: numpy array shape (n_rows, n_features)
    returns X (n_samples, lookback, n_features), y (n_samples,)
    y is the target_col_idx column at time t (following the window)
    """
    X, y = [], []
    for i in range(lookback, len(scaled_array)):
        X.append(scaled_array[i-lookback:i])
        y.append(scaled_array[i, target_col_idx])
    return np.array(X), np.array(y)

#%% evaluation metrics wrapper
def compute_metrics(y_true, y_pred):
    # y_true and y_pred are in original scale (not scaled)
    mse = mean_squared_error(y_true, y_pred)
    rmse = sqrt(mse)
    try:
        mape = mean_absolute_percentage_error(y_true, y_pred)
    except Exception:
        # fallback if sklearn version doesn't have mape
        mape = np.mean(np.abs((y_true - y_pred) / y_true))
    r2 = r2_score(y_true, y_pred)
    return {"mse": mse, "rmse": rmse, "mape": mape, "r2": r2}

#%% main per-symbol pipeline
def process_coin(symbol, lookback=30, forecast_days=10, dry_run=False, min_samples=MIN_SAMPLES):
    """
    Processes a single coin:
      - loads data from `daily`
      - scales and creates sequences
      - trains or fine-tunes LSTM
      - evaluates on last 30% (time-based split)
      - generates iterative forecasts for FUTURE_PRED_DAYS
      - writes only last HIST_PRED_DAYS historical preds + FUTURE_PRED_DAYS future preds into DB
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        df = pd.read_sql("""
            SELECT date, open, high, low, close, volume
            FROM daily
            WHERE symbol=?
            ORDER BY date ASC
        """, conn, params=(symbol,))

        if df.empty:
            logger.warning(f"{symbol}: no rows in daily table")
            conn.close()
            return

        df["date"] = pd.to_datetime(df["date"])
        n_rows = len(df)
        if n_rows < lookback + 1:
            logger.warning(f"{symbol}: not enough rows for lookback={lookback} (rows={n_rows})")
            conn.close()
            return

        # require some minimal data to train - otherwise warn
        if n_rows < min_samples:
            logger.warning(f"{symbol}: only {n_rows} rows (<{min_samples}), still will attempt but results may be poor")

        cols = ["open", "high", "low", "close", "volume"]
        feat_arr = df[cols].values.astype(float)

        # load or fit scaler
        sp = scaler_path(symbol, lookback)
        if os.path.exists(sp):
            scaler = load(sp)
            scaled = scaler.transform(feat_arr)
        else:
            scaler = MinMaxScaler()
            scaled = scaler.fit_transform(feat_arr)
            dump(scaler, sp)

        # create sequences
        X, y_scaled = create_sequences(scaled, lookback, target_col_idx=3)  # close is index 3 in cols
        dates = df["date"].values[lookback:]  # date corresponding to each y

        # time-based train/validation split (70/30)
        split_idx = int(len(X) * 0.7)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y_scaled[:split_idx], y_scaled[split_idx:]
        dates_train, dates_val = dates[:split_idx], dates[split_idx:]

        if dry_run:
            logger.info(f"[DRY RUN] {symbol} lb={lookback}: samples={len(X)}, train={len(X_train)}, val={len(X_val)}")
            conn.close()
            return

        # load or build model
        mp = model_path(symbol, lookback)
        if os.path.exists(mp):
            model = load_model(mp, compile=False)
            model.compile(optimizer="adam", loss="mse")
            initial_epochs = 5
            logger.info(f"{symbol}: loaded existing model {mp}, will fine-tune for {initial_epochs} epochs")
        else:
            model = build_lstm_model(lookback, X.shape[2])
            initial_epochs = 50
            logger.info(f"{symbol}: training new model (lookback={lookback}) for up to {initial_epochs} epochs")

        # fit model
        history = model.fit(
            X_train, y_train,
            validation_data=(X_val, y_val),
            epochs=initial_epochs,
            batch_size=32,
            callbacks=[EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True)],
            verbose=1
        )

        # save model & scaler
        model.save(mp)
        dump(scaler, sp)

        # --- Evaluate on validation set (inverse transform)
        preds_val_scaled = model.predict(X_val, verbose=0).reshape(-1)
        # inverse transform: we need to create an array of shape (n_samples, n_features)
        temp = np.zeros((len(preds_val_scaled), len(cols)))
        temp[:, 3] = preds_val_scaled  # put predicted scaled close in index 3
        inv_preds_val = scaler.inverse_transform(temp)[:, 3]

        # inverse true targets on validation
        temp_true = np.zeros((len(y_val), len(cols)))
        temp_true[:, 3] = y_val
        inv_y_val = scaler.inverse_transform(temp_true)[:, 3]

        metrics = compute_metrics(inv_y_val, inv_preds_val)
        logger.info(f"{symbol} lb={lookback} Validation metrics: RMSE={metrics['rmse']:.4f}, MAPE={metrics['mape']:.4f}, R2={metrics['r2']:.4f}")

        # --- predictions on all historical X (we'll use these to select the last HIST_PRED_DAYS)
        preds_all_scaled = model.predict(X, verbose=0).reshape(-1)
        temp_all = np.zeros((len(preds_all_scaled), len(cols)))
        temp_all[:, 3] = preds_all_scaled
        inv_preds_all = scaler.inverse_transform(temp_all)[:, 3]

        # --- iterative forecast for future days
        # Use FUTURE_PRED_DAYS to generate forecasts that we will write to DB
        num_future_steps = FUTURE_PRED_DAYS
        last_window = scaled[-lookback:].copy()  # shape (lookback, n_features)
        future_preds = []
        last_observed_row = scaled[-1].copy()

        for step in range(num_future_steps):
            x_input = last_window.reshape((1, last_window.shape[0], last_window.shape[1]))
            pred_scaled = model.predict(x_input, verbose=0).reshape(-1)[0]

            # build inverse for this predicted close
            tmp = np.zeros((1, len(cols)))
            tmp[0, 3] = pred_scaled
            inv_pred_close = scaler.inverse_transform(tmp)[0, 3]
            future_preds.append(inv_pred_close)

            # construct a new scaled row to append to the window for next iteration:
            # strategy: use predicted close for open/high/low/close scaled positions, keep volume = last_observed_row volume
            new_row = last_observed_row.copy()
            new_row[3] = pred_scaled  # close
            new_row[0] = pred_scaled  # open
            new_row[1] = pred_scaled  # high
            new_row[2] = pred_scaled  # low
            # keep volume as last observed
            last_window = np.vstack([last_window[1:], new_row])
            last_observed_row = new_row

        # create future dates starting the day after last date in df
        last_date = df["date"].max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=num_future_steps, freq="D")

        # ===========================
        # WRITE TO DB: only last HIST_PRED_DAYS + next FUTURE_PRED_DAYS
        # ===========================
        # delete previous predictions for this symbol to keep table clean
        conn.execute("DELETE FROM predictions WHERE symbol=?", (symbol,))

        # last HIST_PRED_DAYS historical predictions (if available)
        hist_count = min(HIST_PRED_DAYS, len(dates))
        hist_dates = dates[-hist_count:]
        hist_preds = inv_preds_all[-hist_count:]

        hist_rows = [
            (symbol, pd.to_datetime(d).strftime("%Y-%m-%d"), float(p))
            for d, p in zip(hist_dates, hist_preds)
        ]

        # future rows (FUTURE_PRED_DAYS)
        future_rows = [
            (symbol, d.strftime("%Y-%m-%d"), float(p))
            for d, p in zip(future_dates, future_preds)
        ]

        # insert both
        conn.executemany("""
            INSERT OR REPLACE INTO predictions (symbol, date, predicted_close)
            VALUES (?, ?, ?)
        """, hist_rows + future_rows)
        conn.commit()
        conn.close()

        logger.info(f"{symbol}: updated predictions (historical={len(hist_rows)} + future={len(future_rows)}). Validation RMSE={metrics['rmse']:.4f}, MAPE={metrics['mape']:.4f}, R2={metrics['r2']:.4f}")

        # return useful artifacts for immediate testing / display
        return {
            "symbol": symbol,
            "lookback": lookback,
            "model_path": mp,
            "scaler_path": sp,
            "val_metrics": metrics,
            "val_dates": dates_val,
            "val_true": inv_y_val,
            "val_pred": inv_preds_val,
            "future_dates": future_dates,
            "future_pred": future_preds
        }

    except Exception as e:
        logger.exception(f"{symbol}: failed -> {e}")
        try:
            conn.close()
        except:
            pass
        return None

#%% Parallel runner (keeps original behavior)
def run_pipeline(symbols, lookbacks=DEFAULT_LOOKBACKS, forecast_days=DEFAULT_FORECAST_DAYS, dry_run=False):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for s in symbols:
            for lb in lookbacks:
                futures.append(executor.submit(process_coin, s, lb, forecast_days, dry_run))
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                results.append(res)
    return results

#%% Example usage
if __name__ == "__main__":

    ensure_predictions_table()
    TEST_COINS = ["BTC-USD", "ETH-USD"]

    # dry run to see sample counts
    run_pipeline(TEST_COINS, lookbacks=[30], forecast_days=10, dry_run=True)

    # full run: train/validate & forecast (writes last 30 historical + next 30 future)
    outs = run_pipeline(TEST_COINS, lookbacks=[30], forecast_days=10, dry_run=False)

    # print summarized metrics
    for out in outs:
        if out:
            lm = out["val_metrics"]
            logger.info(f"RESULT: {out['symbol']} lb={out['lookback']} -> RMSE={lm['rmse']:.4f}, MAPE={lm['mape']:.4f}, R2={lm['r2']:.4f}")
            # show saved future forecasts (next 30)
            for d, p in zip(out["future_dates"], out["future_pred"]):
                logger.info(f"  forecast {d.strftime('%Y-%m-%d')}: {p:.2f}")
    # print(outs)

    # Write forecasts to CSV file
    if outs:
        csv_filename = f"forecasts.csv"

        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = ['symbol', 'lookback', 'forecast_date', 'forecasted_price']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for out in outs:
                if out:
                    symbol = out['symbol']
                    lookback = out['lookback']
                    future_dates = out['future_dates']
                    future_pred = out['future_pred']

                    # Write each forecast row
                    for date, price in zip(future_dates, future_pred):
                        writer.writerow({
                            'symbol': symbol,
                            'lookback': lookback,
                            'forecast_date': date.strftime('%Y-%m-%d'),
                            'forecasted_price': f"{price:.2f}"
                        })

        logger.info(f"Forecasts written to {csv_filename}")