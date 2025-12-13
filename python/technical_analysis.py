import os
import sqlite3
import pandas as pd

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

def main():
    print ("Loading data from:", DB_PATH)
    df = load_data()
    print(df.head())

if __name__ == "__main__":
    main()

