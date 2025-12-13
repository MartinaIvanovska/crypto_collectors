import os
import sqlite3
import pandas as pd
import ta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "crypto_daily.db")

SOURCE_TABLE = "daily"
TARGET_TABLE = "technical_analysis"

def load_data():
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT symbol, date, open, high, low, close, volume, source_timestamp
         FROM {SOURCE_TABLE}
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])
    return df

def add_indicators(df):
    """
    add oscillators: rsi, macd, stoch, adx, cci
    """
    n = len(df)
    if n == 0:
        return df

    df["rsi"] = ta.momentum.rsi(df["close"], window = 14)
    df["macd"] = ta.trend.macd_diff(df["close"])
    df["stoch"] = ta.momentum.stoch(df["high"], df["low"], df["close"])

    if n >= 30:
        try:
            df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window = 14)
        except Exception:
            df["adx"] = pd.Series(index = df.index, dtype = "float64")
    else:
        df["adx"] = pd.Series(index = df.index, dtype = "float64")

    df["cci"] = ta.trend.cci(df["high"], df["low"], df["close"], window=20)

    if n >= 20:
        df["sma20"] = ta.trend.sma_indicator(df["close"], window = 20)
        df["ema20"] = ta.trend.ema_indicator(df["close"], window = 20)
        df["wma20"] = ta.trend.wma_indicator(df["close"], window = 20)

        bb = ta.volatility.BollingerBands(df["close"], window = 20, window_dev=2)
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()

        df["vol_sma20"] = ta.trend.sma_indicator(df["volume"], window = 20)
    else:
        df["sma20"] = pd.Series(index = df.index, dtype = "float64")
        df["ema20"] = pd.Series(index = df.index, dtype = "float64")
        df["wma20"] = pd.Series(index = df.index, dtype = "float64")
        df["bb_high"] = pd.Series(index = df.index, dtype = "float64")
        df["bb_low"] = pd.Series(index = df.index, dtype = "float64")
        df["vol_sma20"] = pd.Series(index = df.index, dtype = "float64")

    return df

def generate_signal(row):
    buy = (
        row.get("rsi", 50) < 30 and
        row.get("macd", 0) > 0 and
        row.get("close", 0) < row.get("bb_low", float("inf"))
    )

    sell = (
        row.get("rsi", 50) > 70 and
        row.get("macd", 0) < 0 and
        row.get("close", 0) < row.get("bb_high", 0)
    )

    if buy:
        return "BUY"
    if sell:
        return "SELL"
    return "HOLD"

def compute_for_all():
    df = load_data()

    for symbol, g in df.groupby("symbol"):
        g = g.copy()
        g.set_index("date", inplace=True)

        daily = add_indicators(g.copy())
        daily["signal"] = daily.apply(generate_signal, axis=1)

        print("\nSymbol:", symbol)
        print(daily[["close", "rsi", "macd", "bb_low", "bb_high", "signal"]].tail(5))


def main():
    print ("Loading data from:", DB_PATH)
    compute_for_all()
    print("Done.")

if __name__ == "__main__":
    main()

