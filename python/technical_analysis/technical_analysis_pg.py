import pandas as pd
import numpy as np
import ta
from multiprocessing import Pool
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ==============================
# PostgreSQL configuration
# ==============================
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "crypto"
PG_USER = "crypto_user"
PG_PASSWORD = "crypto_pass"

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SOURCE_TABLE = "daily"
TARGET_TABLE = "technical_analysis"

HISTORY_LIMIT_DAYS = 3 * 365


# ==============================
# Data loading
# ==============================
def load_data():
    """Fetch OHLCV data from the database."""
    print(f"Fetching data from {SOURCE_TABLE}...")
    query = f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM {SOURCE_TABLE}
        WHERE date::date >= CURRENT_DATE - INTERVAL '{HISTORY_LIMIT_DAYS} days'
        ORDER BY symbol, date
    """
    df = pd.read_sql(query, engine)
    df["date"] = pd.to_datetime(df["date"])
    # Ensure unique index per symbol/date
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    return df


# ==============================
# Sanitize DataFrame for PostgreSQL
# ==============================
def sanitize_for_postgres(df):
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.where(pd.notna(df), None)
    return df


# ==============================
# Indicators
# ==============================
def add_indicators(df):
    """
    Calculate the specific indicators required for the signal logic.
    """
    n = len(df)
    if n == 0:
        return df

    # --- Oscillators ---
    # RSI (14)
    df["rsi"] = ta.momentum.rsi(df["close"], window=14)

    # MACD Difference (Histogram)
    # The signal logic uses 'macd > 0', which implies the Histogram/Difference
    df["macd"] = ta.trend.macd_diff(df["close"])

    # Stochastic %K
    df["stoch"] = ta.momentum.stoch(df["high"], df["low"], df["close"])

    # ADX (14)
    if n >= 30:
        try:
            df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
        except Exception:
            df["adx"] = pd.Series(index=df.index, dtype="float64")
    else:
        df["adx"] = pd.Series(index=df.index, dtype="float64")

    # CCI (20)
    df["cci"] = ta.trend.cci(df["high"], df["low"], df["close"], window=20)

    # --- Moving Averages & Bands ---
    if n >= 20:
        df["sma20"] = ta.trend.sma_indicator(df["close"], window=20)
        df["ema20"] = ta.trend.ema_indicator(df["close"], window=20)
        df["wma20"] = ta.trend.wma_indicator(df["close"], window=20)

        # Bollinger Bands (High/Low)
        bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()

        # Volume SMA
        df["vol_sma20"] = ta.trend.sma_indicator(df["volume"], window=20)
    else:
        # Fill with NaNs if not enough data
        cols = ["sma20", "ema20", "wma20", "bb_high", "bb_low", "vol_sma20"]
        for col in cols:
            df[col] = pd.Series(index=df.index, dtype="float64")

    return df


# ==============================
# Signal generator
# ==============================
def generate_signal(row):
    """
    Generates BUY, SELL, or HOLD based on a point system.
    """

    def safe(val, default):
        return val if pd.notna(val) else default

    # Extract values safely
    close = safe(row.get("close"), 0)
    rsi = safe(row.get("rsi"), 50)
    macd = safe(row.get("macd"), 0)
    stoch = safe(row.get("stoch"), 50)
    adx = safe(row.get("adx"), 0)
    cci = safe(row.get("cci"), 0)
    sma20 = safe(row.get("sma20"), close)
    ema20 = safe(row.get("ema20"), close)
    wma20 = safe(row.get("wma20"), close)
    bb_high = safe(row.get("bb_high"), close * 1.1)
    bb_low = safe(row.get("bb_low"), close * 0.9)
    vol_sma20 = safe(row.get("vol_sma20"), 0)
    volume = safe(row.get("volume"), 0)

    buy, sell = 0, 0

    # RSI
    if rsi < 30:
        buy += 2
    elif rsi < 40:
        buy += 1
    if rsi > 70:
        sell += 2
    elif rsi > 60:
        sell += 1

    # MACD (Histogram)
    buy += macd > 0
    sell += macd < 0

    # Stochastic
    buy += stoch < 20
    sell += stoch > 80

    # ADX
    buy += adx > 20 and macd > 0
    sell += adx > 20 and macd < 0

    # CCI
    buy += cci < -100
    sell += cci > 100

    # Moving averages
    buy += close > sma20 and close > ema20 and close > wma20
    sell += close < sma20 and close < ema20 and close < wma20

    # Bollinger bands
    # If price is below low band -> strong buy
    buy += 2 if close < bb_low else (1 if close < sma20 else 0)
    # If price is above high band -> strong sell
    sell += 2 if close > bb_high else (1 if close > sma20 else 0)

    # Volume confirmation
    if volume > vol_sma20:
        if buy > 0: buy += 1
        if sell > 0: sell += 1

    # Final Decision
    if buy >= 4 and buy >= sell + 2:
        return "BUY"
    if sell >= 4 and sell >= buy + 2:
        return "SELL"
    return "HOLD"


# ==============================
# Resampling
# ==============================
def resample_timeframe(df, rule, label):
    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    # Resample and drop NaNs
    r = df.resample(rule).agg(ohlc).dropna()
    r["timeframe"] = label
    return r


def process_symbol(args):
    """
    Process a single symbol for 1D, 1W, 1M.
    Returns the latest row for each timeframe.
    """
    symbol, g = args
    g = g.set_index("date").sort_index()
    results = []

    def process_tf_df(tf_df, tf_label):
        if len(tf_df) < 20:  # Skip if insufficient data
            return

        processed = add_indicators(tf_df.copy())

        # We only need the latest row for the report
        latest = processed.tail(1).copy()

        latest["signal"] = latest.apply(generate_signal, axis=1)
        latest["symbol"] = symbol
        latest["timeframe"] = tf_label

        # Flatten index
        latest = latest.reset_index().rename(columns={"index": "date", "Date": "date"})
        results.append(latest)

    # 1. Daily
    process_tf_df(g, "1D")

    # 2. Weekly
    weekly = resample_timeframe(g, "W", "1W")
    process_tf_df(weekly, "1W")

    # 3. Monthly
    monthly = resample_timeframe(g, "ME", "1M")
    process_tf_df(monthly, "1M")

    if not results:
        return pd.DataFrame()

    return pd.concat(results)


# ==============================
# Batch processing
# ==============================
def compute_for_all():
    df = load_data()
    if df.empty:
        return pd.DataFrame()

    groups = [(symbol, g) for symbol, g in df.groupby("symbol")]

    print(f"Processing {len(groups)} symbols...")

    with Pool(processes=4) as pool:
        parts = list(tqdm(pool.imap(process_symbol, groups), total=len(groups)))

    valid_parts = [p for p in parts if not p.empty]

    if not valid_parts:
        return pd.DataFrame()

    all_df = pd.concat(valid_parts, ignore_index=True)
    return all_df


# ==============================
# Save to PostgreSQL
# ==============================
def save_to_db(all_df):
    if all_df.empty:
        print("No data to save.")
        return

    # Columns strictly requested by user
    cols_to_save = [
        "symbol", "timeframe",
        "rsi", "macd", "stoch", "adx", "cci",
        "sma20", "ema20", "wma20",
        "bb_high", "bb_low", "vol_sma20",
        "signal"
    ]

    # Filter and sanitize
    final_df = all_df[cols_to_save].copy()
    final_df = sanitize_for_postgres(final_df)

    print(f"Saving {len(final_df)} rows to '{TARGET_TABLE}'...")

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))

        conn.execute(text(f"""
            CREATE TABLE {TARGET_TABLE} (
                symbol TEXT,
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
                PRIMARY KEY (symbol, timeframe)
            )
        """))

    final_df.to_sql(
        TARGET_TABLE,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print("Database update complete.")
    print(f"  - 1D: {len(final_df[final_df['timeframe'] == '1D'])}")
    print(f"  - 1W: {len(final_df[final_df['timeframe'] == '1W'])}")
    print(f"  - 1M: {len(final_df[final_df['timeframe'] == '1M'])}")


# ==============================
# Main
# ==============================
def main():
    print("Starting Technical Analysis Pipeline...")
    all_df = compute_for_all()
    save_to_db(all_df)
    print("Done.")


if __name__ == "__main__":
    main()