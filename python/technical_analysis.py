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
    First version: add oscillators rsi, macd, stoch
    """
    if len(df) == 0:
        return df

    df["rsi"] = ta.momentum.rsi(df["close"], window = 14)
    df["macd"] = ta.trend.macd_diff(df["close"])
    df["stoch"] = ta.momentum.stoch(df["high"], df["low"], df["close"])
    return df


def compute_for_all():
    df = load_data()
    for symbol, g in df.groupby("symbol"):
        g = g.copy();
        g.set_index("date", inplace=True)

        daily = add_indicators(g.copy())
        print("\nSymbol:", symbol)
        print(daily[["close", "rsi", "macd", "stoch"]].tail(3))


def main():
    print ("Loading data from:", DB_PATH)
    compute_for_all()
    print("Done")

if __name__ == "__main__":
    main()

