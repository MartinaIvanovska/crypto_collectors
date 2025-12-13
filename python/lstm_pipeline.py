# lstm_pipeline_parallel.py
import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from concurrent.futures import ProcessPoolExecutor
import os


LOOKBACK = 30  # days
DB_PATH = os.path.join("data", "crypto_daily.db")  # adjust if needed
NUM_WORKERS = 4  # number of parallel processes


conn_main = sqlite3.connect(DB_PATH)
coins_df = pd.read_sql("SELECT DISTINCT symbol FROM daily", conn_main)
coin_list = coins_df['symbol'].tolist()
conn_main.close()
print(f"Found {len(coin_list)} coins")


def build_lstm_model(lookback, features):
    model = Sequential()
    model.add(LSTM(50, return_sequences=True, input_shape=(lookback, features)))
    model.add(LSTM(50))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    return model

def process_coin(coin_symbol):
    try:
        # Each process opens its own DB connection
        conn = sqlite3.connect(DB_PATH)

        # Load coin data
        df = pd.read_sql(f"""
            SELECT date, open, high, low, close, volume
            FROM daily
            WHERE symbol='{coin_symbol}'
            ORDER BY date ASC
        """, conn)

        if len(df) < LOOKBACK + 1:
            raise ValueError("Not enough data for lookback")

        # Scale OHLCV
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(df[['open','high','low','close','volume']])

        # Create sequences
        X, y = [], []
        for i in range(LOOKBACK, len(scaled)):
            X.append(scaled[i-LOOKBACK:i])
            y.append(scaled[i,3])  # predict close
        X, y = np.array(X), np.array(y)

        # Train/test split
        split = int(len(X)*0.7)
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]
        dates_test = df['date'].tolist()[LOOKBACK+split:]

        # Train LSTM
        model = build_lstm_model(LOOKBACK, X_train.shape[2])
        model.fit(X_train, y_train, epochs=50, batch_size=32, verbose=0)

        # Predict
        preds = model.predict(X_test)

        # Inverse scale
        preds_reshaped = np.zeros((len(preds), 5))
        preds_reshaped[:,3] = preds[:,0]
        inv_preds = scaler.inverse_transform(preds_reshaped)[:,3]

        # Save predictions
        df_pred = pd.DataFrame({
            "symbol": [coin_symbol]*len(inv_preds),
            "date": dates_test,
            "predicted_close": inv_preds
        })
        df_pred.to_sql("predictions", conn, if_exists="append", index=False)
        conn.commit()
        conn.close()
        print(f" Finished {coin_symbol}")

    except Exception as e:
        print(f" Skipping {coin_symbol}: {e}")


if __name__ == "__main__":
    # Make sure predictions table exists
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            symbol TEXT,
            date TEXT,
            predicted_close REAL,
            PRIMARY KEY (coin_symbol, date)
        )
    """)
    conn.close()


    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        executor.map(process_coin, coin_list)
