import os
import sqlite3
import pandas as pd
import ta
from multiprocessing import Pool, cpu_count

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
    add moving averages: sma20, ema20, wma20, bb_high_b_low, vol_sma20
    """
    n = len(df)
    if n == 0:
        return df

    #5 oscillators
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

    #5 moving averages
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
    """
    Generates BUY/SELL/HOLD signal using technical indicators:
    RSI, MACD, STOCH, ADX, CCI, SMA20, EMA20, WMA20, BollingerBands, vol_sma20
    """

    def safe_get(val, default):
        return val if pd.notna(val) else default

    # Default values
    close = safe_get(row.get("close"), 0)
    rsi = safe_get(row.get("rsi"), 50)
    macd = safe_get(row.get("macd"), 0)
    stoch = safe_get(row.get("stoch"), 50)
    adx = safe_get(row.get("adx"), 0)
    cci = safe_get(row.get("cci"), 0)
    sma20 = safe_get(row.get("sma20"), close)
    ema20 = safe_get(row.get("ema20"), close)
    wma20 = safe_get(row.get("wma20"), close)
    bb_high = safe_get(row.get("bb_high"), close * 1.1)
    bb_low = safe_get(row.get("bb_low"), close * 0.9)
    vol_sma20 = safe_get(row.get("vol_sma20"), 0)
    volume = safe_get(row.get("volume"), 0)

    buy_score = 0
    sell_score = 0

    # ---Oscillators---
    #RSI
    if rsi < 30:
        buy_score += 2
    elif rsi < 40:
        buy_score += 1
    if rsi > 70:
        sell_score += 2
    elif rsi > 60:
        sell_score += 1

    #MACD
    if macd > 0:
        buy_score += 1
    if macd < 0:
        sell_score += 1

    #STOCH
    if stoch < 20:
        buy_score += 1
    if stoch > 80:
        sell_score += 1

    #ADX
    if adx > 20 and macd > 0:
        buy_score += 1
    if adx > 20 and macd < 0:
        sell_score += 1

    #CCI
    if cci < -100:
        buy_score += 1
    if cci > 100:
        sell_score += 1

    #---Moving averages---

    #Price vs Moving averages
    if close > sma20 and close > ema20 and close > wma20:
        buy_score += 1
    if close < sma20 and close < ema20 and close < wma20:
        sell_score += 1

    # Bollinger bands
    if close < bb_low:
        buy_score += 2
    elif close < sma20:
        buy_score += 1

    if close > bb_high:
        sell_score += 2
    elif close > sma20:
        sell_score += 1

    #Volume signal
    if volume > vol_sma20 and buy_score > 0:
        buy_score += 1
    if volume > vol_sma20 and sell_score > 0:
        sell_score += 1

    #---FINAL SIGNAL----
    if buy_score >=4 and buy_score >= sell_score + 2:
        return "BUY"
    elif sell_score >=4 and sell_score >= buy_score + 2:
        return "SELL"
    else:
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

def process_symbol(args):
    symbol, g = args

    g = g.set_index("date").sort_index()

    results = []

    # daily
    daily = add_indicators(g.copy())
    daily["timeframe"] = "1D"
    daily["signal"] = daily.apply(generate_signal, axis=1)
    daily["symbol"] = symbol
    results.append(daily)

    # weekly
    weekly_raw = resample_timeframe(g, "W", "1W")
    weekly = add_indicators(weekly_raw)
    weekly["signal"] = weekly.apply(generate_signal, axis=1)
    weekly["symbol"] = symbol
    results.append(weekly)

    # monthly
    monthly_raw = resample_timeframe(g, "ME", "1M")
    monthly = add_indicators(monthly_raw)
    monthly["signal"] = monthly.apply(generate_signal, axis=1)
    monthly["symbol"] = symbol
    results.append(monthly)

    return pd.concat(results)

def compute_for_all():
    """
    Batch analysis for all symbols and three timeframes
    Returns DataFrame with indicators and signals
    """
    df = load_data()

    #prepare list (symbol, group_df)  for multiprocessing
    groups = [(symbol, g) for symbol, g in df.groupby("symbol")]

    with Pool(processes=3) as pool:
        parts = pool.map(process_symbol, groups)

    all_df = pd.concat(parts)
    all_df.reset_index(inplace=True)
    print(all_df.head())
    return all_df

def save_to_db(all_df):
    conn = sqlite3.connect(DB_PATH)

    conn.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

    create_sql = f"""
    CREATE TABLE {TARGET_TABLE} (
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
        signal TEXT,
        PRIMARY KEY (symbol, date, timeframe)
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
    print("Saving to database...")
    save_to_db(all_df)
    print("Done. Indicators saved to table:", TARGET_TABLE)


if __name__ == "__main__":
    main()

