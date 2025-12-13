import os
import sqlite3
import numpy as np
import pandas as pd
from joblib import load
from tensorflow.keras.models import load_model

# =====================
# CONFIG
# =====================
BASE_DIR = os.path.dirname(os.path.abspath("das_project_springboot/.."))
DB_PATH = os.path.join(BASE_DIR, "data", "crypto_daily.db")
MODEL_DIR = os.path.join(BASE_DIR, "models")

SYMBOL = "BTC-USD"
LOOKBACK = 30
FORECAST_DAYS = 30

MODEL_PATH = os.path.join(MODEL_DIR, f"{SYMBOL}_lb{LOOKBACK}.h5")
SCALER_PATH = os.path.join(MODEL_DIR, f"{SYMBOL}_lb{LOOKBACK}_scaler.joblib")

FEATURE_COLS = ["open", "high", "low", "close", "volume"]

# =====================
# LOAD MODEL & SCALER
# =====================
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Model not found: {MODEL_PATH}")

if not os.path.exists(SCALER_PATH):
    raise FileNotFoundError(f"Scaler not found: {SCALER_PATH}")

model = load_model(MODEL_PATH)
scaler = load(SCALER_PATH)

print("Model and scaler loaded successfully")

# =====================
# LOAD LATEST DATA
# =====================
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("""
    SELECT date, open, high, low, close, volume
    FROM daily
    WHERE symbol=?
    ORDER BY date ASC
""", conn, params=(SYMBOL,))
conn.close()

df["date"] = pd.to_datetime(df["date"])

if len(df) < LOOKBACK:
    raise ValueError(f"Not enough data. Need at least {LOOKBACK} rows.")

# take last LOOKBACK rows
last_df = df.iloc[-LOOKBACK:]
last_date = last_df["date"].max()

# =====================
# SCALE DATA
# =====================
scaled = scaler.transform(last_df[FEATURE_COLS].values)

# =====================
# ITERATIVE FORECAST
# =====================
last_window = scaled.copy()
last_row = scaled[-1].copy()
future_preds = []

for step in range(FORECAST_DAYS):
    x_input = last_window.reshape(1, LOOKBACK, len(FEATURE_COLS))
    pred_scaled = model.predict(x_input, verbose=0)[0][0]

    # inverse scale predicted close
    tmp = np.zeros((1, len(FEATURE_COLS)))
    tmp[0, 3] = pred_scaled
    pred_close = scaler.inverse_transform(tmp)[0, 3]

    future_preds.append(pred_close)

    # build next input row
    new_row = last_row.copy()
    new_row[0] = pred_scaled  # open
    new_row[1] = pred_scaled  # high
    new_row[2] = pred_scaled  # low
    new_row[3] = pred_scaled  # close
    # volume unchanged

    last_window = np.vstack([last_window[1:], new_row])
    last_row = new_row

# =====================
# BUILD RESULT DF
# =====================
future_dates = pd.date_range(
    start=last_date + pd.Timedelta(days=1),
    periods=FORECAST_DAYS,
    freq="D"
)

forecast_df = pd.DataFrame({
    "date": future_dates,
    "predicted_close": future_preds
})

print("\nNext 30-day forecast:")
print(forecast_df)

# optional: save to CSV
forecast_df.to_csv(f"{SYMBOL}_30day_forecast.csv", index=False)
