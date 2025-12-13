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

def resample_timeframe(df, rule, label):
    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "source_timestamp": "last",
    }
    r = df.resample(rule).apply(ohlc)
    r["timeframe"] = label
    return r

def compute_for_all():
    df = load_data()
    results = []

    for symbol, g in df.groupby("symbol"):
        g = g.copy()
        g.set_index("date", inplace=True)

        #daily
        daily = add_indicators(g.copy())
        daily["timeframe"] = "1D"
        daily["signal"] = daily.apply(generate_signal, axis=1)
        daily["symbol"] = symbol
        results.append(daily)

        #weekly
        weekly_raw = resample_timeframe(g, "W", "1W")
        weekly = add_indicators(weekly_raw.copy())
        weekly["signal"] = weekly.apply(generate_signal, axis=1)
        weekly["symbol"] = symbol
        results.append(weekly)

        #monthly
        monthly_raw = resample_timeframe(g, "ME", "1M")
        monthly = add_indicators(monthly_raw.copy())
        monthly["signal"] = monthly.apply(generate_signal, axis=1)
        monthly["symbol"] = symbol
        results.append(monthly)

    all_df = pd.concat(results)
    all_df.reset_index(inplace=True)
    print(all_df.head())
    return all_df

def save_to_db(all_df):
    conn = sqlite3.connect(DB_PATH)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        symbol TEXT,
        date TEXT,
        timeframe TEXT,
        rsi REAL,
        macd REAL,
        stoch REAL,
        adx REAL,
        cci REAL,
        sma20 REAL,
        ema20 REAL,
        wma20 REAL,
        bb_high REAL,
        bb_low REAL,
        vol_sma20 REAL,
        signal TEXT
    )
    """

    conn.execute(create_sql)
    conn.execute(f"DELETE FROM {TARGET_TABLE}")

    indicators_df = all_df[[
        "symbol", "date", "timeframe",
        "rsi", "macd", "stoch", "adx", "cci",
        "sma20", "ema20", "wma20", "bb_high", "bb_low", "vol_sma20",
        "signal",
    ]]

    indicators_df.to_sql(TARGET_TABLE, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def main():
    print ("Loading data from:", DB_PATH)
    all_df = compute_for_all()
    save_to_db(all_df)
    print("Done. Indicators saved to table:", TARGET_TABLE)

if __name__ == "__main__":
    main()

