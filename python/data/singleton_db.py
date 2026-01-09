import os
import time
import json
import queue
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import csv
import io
import sys
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import pool as pg_pool

# --- CONFIGURATION (edit or override with env vars) ---
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "kriptoserver.postgres.database.azure.com"),  # Azure endpoint
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB", "crypto"),
    # Azure requires user@servername
    "user": os.getenv("PG_USER", "adminmartina"),
    # Use PG_PASSWORD or PG_PASS (ensure it's set in env)
    "password": os.getenv("PG_PASSWORD", "Andrejcar123!"),
    "sslmode": os.getenv("PG_SSLMODE", "require")  # Azure requires SSL
}
SUMMARY_CSV = os.getenv("SUMMARY_CSV", "completeness_report.csv")

WORKERS = int(os.getenv("WORKERS", 50))
BATCH_INSERT_SIZE = int(os.getenv("BATCH_INSERT_SIZE", 5000))  # tuned for large scale
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", 200000))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 15))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible)")

# Time window
END_DT = datetime.utcnow()
START_DT = END_DT - timedelta(days=10 * 365)
START_TS = int(START_DT.timestamp())
END_TS = int(END_DT.timestamp())

# SQL for main table (Postgres)
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS daily (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    source_timestamp BIGINT,
    PRIMARY KEY (symbol, date)
);
"""

# optional: index on source_timestamp for sorting by the source ts
CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_daily_source_ts ON daily (source_timestamp);
"""

# --- Thread local session (HTTP keep-alive) ---
thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS)
        thread_local.session.mount('https://', adapter)
        thread_local.session.mount('http://', adapter)
    return thread_local.session

# --- Database singleton via a ThreadedConnectionPool ---
class PooledConnection:
    """A thin proxy around a psycopg2 connection that returns the connection to the pool when closed.

    This lets existing code call conn.close() as usual and have the connection returned to the
    pool instead of being actually closed.
    """
    def __init__(self, conn, pool: pg_pool.ThreadedConnectionPool):
        self._conn = conn
        self._pool = pool

    def close(self):
        """Return connection to the pool. Rollback any open transaction first to avoid locks."""
        try:
            # safe rollback to ensure connection is clean
            try:
                self._conn.rollback()
            except Exception:
                pass
            self._pool.putconn(self._conn)
        except Exception:
            # Last-resort: try to close the physical connection
            try:
                self._conn.close()
            except Exception:
                pass

    def __getattr__(self, name):
        # Proxy attribute access to the underlying psycopg2 connection
        return getattr(self._conn, name)


class DBPoolSingleton:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, minconn: int, maxconn: int, db_config: dict):
        # create a threaded connection pool
        self._pool = pg_pool.ThreadedConnectionPool(minconn, maxconn, **db_config)

    @classmethod
    def initialize(cls, minconn: int = 1, maxconn: int = 10, db_config: Optional[dict] = None):
        """Initialize the singleton pool (idempotent). Must be called before calling get_pg_conn if you
        want a specific pool size. If not called explicitly, the first call to instance() will lazily
        create a pool with defaults.
        """
        if db_config is None:
            db_config = DB_CONFIG
        with cls._lock:
            if cls._instance is None:
                cls._instance = DBPoolSingleton(minconn, maxconn, db_config)
        return cls._instance

    @classmethod
    def instance(cls):
        with cls._lock:
            if cls._instance is None:
                # lazy initialize with reasonable defaults if not explicitly initialized
                cls._instance = DBPoolSingleton(minconn=1, maxconn=max(5, WORKERS // 2), db_config=DB_CONFIG)
            return cls._instance

    def getconn(self):
        raw = self._pool.getconn()
        return PooledConnection(raw, self._pool)

    def putconn(self, conn):
        # accept raw connection or proxy
        if isinstance(conn, PooledConnection):
            self._pool.putconn(conn._conn)
        else:
            self._pool.putconn(conn)

    def closeall(self):
        try:
            self._pool.closeall()
        except Exception:
            pass


# --- Postgres helpers now use the singleton pool ---
def get_pg_conn():
    """Get a connection from the singleton pool. Caller should call conn.close() to return it to pool."""
    return DBPoolSingleton.instance().getconn()


def init_db():
    """Ensure main table exists and index created."""
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(CREATE_SQL)
    cur.execute(CREATE_INDEX_SQL)
    conn.commit()
    cur.close()
    conn.close()


def get_all_last_dates() -> Dict[str, Optional[str]]:
    """
    Return {symbol: last_date_iso or None}
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(CREATE_SQL)
    conn.commit()

    cur.execute("SELECT symbol, MAX(date) FROM daily GROUP BY symbol")
    rows = cur.fetchall()

    result = {}
    for symbol, last_date in rows:
        if last_date is None:
            result[symbol] = None
        elif hasattr(last_date, "isoformat"):
            # date or datetime
            result[symbol] = last_date.isoformat()
        else:
            # already a string (or unexpected type)
            result[symbol] = str(last_date)

    cur.close()
    conn.close()
    return result


# --- Yahoo fetch/parsing (mostly unchanged) ---
def yahoo_chart_json(symbol, start_ts=START_TS, end_ts=END_TS, timeout=REQUEST_TIMEOUT):
    session = get_session()
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           f"?period1={start_ts}&period2={end_ts}&interval=1d&includePrePost=true"
           "&events=div%7Csplit%7Cearn&lang=en-US&region=US&source=cosaic")
    try:
        r = session.get(url, timeout=timeout)
    except Exception as e:
        return "error", f"request-exception: {e}"
    if r.status_code == 404:
        return "not_found", f"404 for {symbol}"
    if not r.ok:
        return "error", f"HTTP {r.status_code} for {symbol}"
    try:
        j = r.json()
    except Exception as e:
        return "error", f"json-decode-error: {e}"
    chart = j.get("chart", {})
    result = chart.get("result")
    if not result:
        return "not_found", chart.get("error") or "no-result"
    return "ok", j


def parse_quote_to_rows(symbol, chart_json):
    chart = chart_json["chart"]
    result = chart["result"][0]
    timestamps = result.get("timestamp", [])
    indicators = result.get("indicators", {}).get("quote", [])
    if not timestamps or not indicators:
        return []
    quote = indicators[0]
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    volumes = quote.get("volume", [])

    rows = []
    length = len(timestamps)
    for i in range(length):
        ts = timestamps[i]
        c = closes[i]
        if c is None: continue
        o = opens[i] if i < len(opens) else None
        h = highs[i] if i < len(highs) else None
        l = lows[i] if i < len(lows) else None
        v = volumes[i] if i < len(volumes) else None
        # date string for CSV/COPY convenience
        dt = datetime.utcfromtimestamp(int(ts)).date().isoformat()
        row = (symbol, dt,
               None if o is None else float(o),
               None if h is None else float(h),
               None if l is None else float(l),
               float(c),
               None if v is None else int(v),
               int(ts))
        rows.append(row)
    return rows


def worker_fetch(symbol, last_date_iso, out_queue: queue.Queue):
    """Worker: use last_date_iso to compute start_ts"""
    if last_date_iso:
        try:
            last_date = datetime.fromisoformat(last_date_iso).date()
            fetch_from_date = last_date + timedelta(days=1)
            start_ts = int(datetime.combine(fetch_from_date, datetime.min.time()).timestamp())
        except Exception:
            start_ts = START_TS
    else:
        start_ts = START_TS

    if start_ts >= END_TS:
        return {"symbol": symbol, "status": "up_to_date", "rows": 0}

    status, payload = yahoo_chart_json(symbol, start_ts=start_ts, end_ts=END_TS)
    if status == "not_found":
        status, payload = yahoo_chart_json(symbol, start_ts=start_ts, end_ts=END_TS)

    if status == "error":
        print(f"[{symbol}] fetch error: {payload}")
        return {"symbol": symbol, "status": "error", "rows": 0}
    if status == "not_found":
        print(f"[{symbol}] not found (after one retry): {payload}")
        return {"symbol": symbol, "status": "not_found", "rows": 0}

    try:
        rows = parse_quote_to_rows(symbol, payload)
    except Exception as e:
        print(f"[{symbol}] parse exception: {e}")
        rows = []
    if rows:
        out_queue.put(rows)
        return {"symbol": symbol, "status": "ok", "rows": len(rows)}
    else:
        return {"symbol": symbol, "status": "empty", "rows": 0}

# --- Writer thread: fast bulk using COPY into temp + INSERT ON CONFLICT DO NOTHING ---
def writer_thread_fn(in_queue: queue.Queue, stop_event: threading.Event, stats: dict):
    """
    Writer thread:
      - Accumulate batches
      - For each flush: create temp table, COPY CSV into temp, then INSERT INTO daily SELECT * FROM temp ON CONFLICT DO NOTHING
      - This approach is very fast for large volumes.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    # Ensure table exists
    cur.execute(CREATE_SQL)
    conn.commit()

    batch: List[Tuple] = []
    last_flush = time.time()

    def flush_batch_via_copy(rows_batch: List[Tuple]):
        if not rows_batch:
            return 0
        # Create temp table that will be dropped at end of transaction
        tmp_name = "tmp_daily_ingest"
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS {tmp} (
                symbol TEXT,
                date DATE,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume BIGINT,
                source_timestamp BIGINT
            ) ON COMMIT DROP;
        """.format(tmp=tmp_name))
        # Build CSV in memory
        csv_buf = io.StringIO()
        csv_writer = csv.writer(csv_buf)
        for r in rows_batch:
            # csv writer will handle quoting; date as yyyy-mm-dd is fine
            csv_writer.writerow(r)
        csv_buf.seek(0)
        try:
            # COPY into temp table
            cur.copy_expert(f"COPY {tmp_name} (symbol,date,open,high,low,close,volume,source_timestamp) FROM STDIN WITH CSV", csv_buf)
            # Insert into main table from temp table with ON CONFLICT DO NOTHING
            cur.execute(f"""
                INSERT INTO daily (symbol,date,open,high,low,close,volume,source_timestamp)
                SELECT symbol,date,open,high,low,close,volume,source_timestamp FROM {tmp_name}
                ON CONFLICT (symbol, date) DO NOTHING;
            """)
            conn.commit()
            return len(rows_batch)
        except Exception as e:
            # If COPY approach fails (rare), fall back to execute_values
            conn.rollback()
            try:
                insert_sql = """
                    INSERT INTO daily (symbol,date,open,high,low,close,volume,source_timestamp)
                    VALUES %s
                    ON CONFLICT (symbol, date) DO NOTHING
                """
                execute_values(cur, insert_sql, rows_batch, page_size=1000)
                conn.commit()
                return len(rows_batch)
            except Exception as e2:
                conn.rollback()
                print("Bulk insert failed (both COPY and execute_values). Error:", e2)
                return 0

    try:
        while not (stop_event.is_set() and in_queue.empty()):
            try:
                item = in_queue.get(timeout=0.5)
            except queue.Empty:
                # timed flush if items present
                if batch and (time.time() - last_flush) > 1.0:
                    count = flush_batch_via_copy(batch)
                    stats["inserted"] += count
                    batch.clear()
                    last_flush = time.time()
                continue

            rows = []
            if isinstance(item, dict):
                rows = item.get("rows", []) or []
            elif isinstance(item, list):
                rows = item

            if rows:
                batch.extend(rows)

            if len(batch) >= BATCH_INSERT_SIZE:
                count = flush_batch_via_copy(batch)
                stats["inserted"] += count
                batch.clear()
                last_flush = time.time()

            in_queue.task_done()

        # final flush
        if batch:
            count = flush_batch_via_copy(batch)
            stats["inserted"] += count
            batch.clear()

    finally:
        cur.close()
        conn.close()

# -----------------------
# Completeness summary
# -----------------------
def build_completeness_report(symbols: List[str], start_dt=START_DT, end_dt=END_DT, out_csv=SUMMARY_CSV):
    conn = get_pg_conn()
    cur = conn.cursor()
    start_date = start_dt.date()
    end_date = end_dt.date()
    expected_days = (end_date - start_date).days + 1

    sql_text = """
    SELECT symbol, COUNT(*), MIN(date), MAX(date)
    FROM daily
    GROUP BY symbol
    """
    cur.execute(sql_text)
    db_stats = {row[0]: {'count': row[1], 'min': (row[2].isoformat() if row[2] else None), 'max': (row[3].isoformat() if row[3] else None)} for row in cur.fetchall()}

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "fetched_days", "expected_days", "coverage_pct", "earliest_fetched", "latest_fetched"])
        for sym in symbols:
            stat = db_stats.get(sym, {'count': 0, 'min': None, 'max': None})
            fetched_days = stat['count']
            earliest = stat['min']
            coverage = (fetched_days / expected_days * 100) if expected_days else 0.0
            writer.writerow([sym, fetched_days, expected_days, f"{coverage:.2f}", earliest or "", stat['max'] or ""])
    cur.close()
    conn.close()

# -----------------------
# Scraping helpers (unchanged)
# -----------------------
def fetch_batch(start, batch_size=100, min_volume=0, min_cap=0):
    session = get_session()
    url = f"https://finance.yahoo.com/markets/crypto/all/?start={start}&count={batch_size}"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    rows = soup.select('tr[data-testid="data-table-v2-row"]')
    batch_symbols = []
    for row in rows:
        symbol_span = row.select_one("span.symbol")
        if not symbol_span: continue
        symbol = symbol_span.get_text(strip=True)
        volume_td = row.select_one('td[data-testid-cell="vol_24hr"]')
        if not volume_td: continue
        volume_str = volume_td.get_text(strip=True).replace(',', '')

        def parse_suffix(s):
            m = 1.0
            if 'T' in s:
                m = 1e12; s = s.replace('T', '')
            elif 'B' in s:
                m = 1e9; s = s.replace('B', '')
            elif 'M' in s:
                m = 1e6; s = s.replace('M', '')
            elif 'K' in s:
                m = 1e3; s = s.replace('K', '')
            return float(s) * m

        try:
            volume = parse_suffix(volume_str)
            market_td = row.select_one('td[data-testid-cell="intradaymarketcap"]')
            market_str = market_td.get_text(strip=True).replace(',', '') if market_td else "0"
            market_cap = parse_suffix(market_str)
            if volume > min_volume and market_cap > min_cap:
                batch_symbols.append(symbol)
        except ValueError:
            continue
    print(f"Fetched {len(batch_symbols)} symbols from start={start} (after filtering)")
    return batch_symbols


def scrape_crypto_symbols_parallel(total=1000, batch_size=100, max_workers=10, min_volume=1000000, min_cap=1000000):
    all_symbols = []
    starts = list(range(0, total, batch_size))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_start = {executor.submit(fetch_batch, start, batch_size, min_volume, min_cap): start for start in starts}
        for future in as_completed(future_to_start):
            batch_symbols = future.result()
            all_symbols.extend(batch_symbols)
    seen = set()
    unique = []
    for s in all_symbols:
        if s not in seen:
            unique.append(s)
            seen.add(s)
    return unique[:total]

# -----------------------
# Optional: create yearly partitions (call manually if desired)
# -----------------------
def create_yearly_partitions(start_year: int, end_year: int):
    """
    Create partition tables for each year [start_year..end_year].
    Usage: call once after creating the main table if you want partitioning.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    # requires main table to be partitioned by RANGE (date). This function assumes main table already partitioned.
    for y in range(start_year, end_year + 1):
        part_name = f"daily_p_{y}"
        start = f"{y}-01-01"
        end = f"{y+1}-01-01"
        try:
            cur.execute(sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF daily FOR VALUES FROM (%s) TO (%s);"
            ).format(sql.Identifier(part_name)), [start, end])
        except Exception as e:
            print(f"Could not create partition {part_name}: {e}")
    conn.commit()
    cur.close()
    conn.close()

# -----------------------
# Main function
# -----------------------
def main():
    t0 = time.time()
    print("Fetching top coin symbols from BeautifulSoup from Yahoo finance...")
    symbols = scrape_crypto_symbols_parallel()
    print(f"Symbols fetched: {len(symbols)} (sample {symbols[:10]})")
    print(f"Time window: {START_DT.date()} through {END_DT.date()} (~{(END_DT - START_DT).days} days)\n")

    # prepare DB and existing dates map
    print(f"Preparing Postgres DB with config: host={DB_CONFIG['host']} port={DB_CONFIG['port']} db={DB_CONFIG['dbname']}")

    # Initialize the DB pool singleton with a sensible maxconn based on WORKERS
    maxconn = max(5, WORKERS + 5)
    DBPoolSingleton.initialize(minconn=1, maxconn=maxconn, db_config=DB_CONFIG)

    init_db()
    existing_dates_map = get_all_last_dates()

    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    stats = {"inserted": 0, "fetched_rows_total": 0}

    # start writer thread
    writer_thread = threading.Thread(target=writer_thread_fn, args=(q, stop_event, stats), daemon=True)
    writer_thread.start()

    # Threaded fetch
    print(f"Starting fetch with {WORKERS} worker threads...")
    fetched_symbols = 0
    ok_count = empty_count = notfound_count = error_count = up_to_date_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(worker_fetch, sym, existing_dates_map.get(sym), q): sym
            for sym in symbols
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                print(f"[{sym}] exception from worker: {e}")
                result = {"symbol": sym, "status": "error", "rows": 0}

            fetched_symbols += 1
            status = result.get("status")
            rows = result.get("rows", 0)
            stats["fetched_rows_total"] += rows

            if status in ("ok", "ok_blocking"):
                ok_count += 1
            elif status == "empty":
                empty_count += 1
            elif status == "not_found":
                notfound_count += 1
            elif status == "up_to_date":
                up_to_date_count += 1
            else:
                error_count += 1

            if fetched_symbols % 25 == 0 or fetched_symbols == len(symbols):
                elapsed = time.time() - t0
                print(f"[{fetched_symbols}/{len(symbols)}] fetched. OK: {ok_count}, empty: {empty_count}, not_found: {notfound_count}, up_to_date: {up_to_date_count}, errors: {error_count} — rows queued: {stats['fetched_rows_total']} — elapsed {elapsed:.1f}s")

    # wait for queue to drain to writer
    print("Fetchers done, waiting for writer to finish inserting...")
    q.join()
    stop_event.set()
    writer_thread.join(timeout=120)
    total_inserted = stats["inserted"]

    elapsed_total = time.time() - t0
    print("\n--- Summary ---")
    print(f"Symbols processed: {len(symbols)}")
    print(f"Successful symbols (with rows): {ok_count}")
    print(f"Empty/no-data symbols: {empty_count}")
    print(f"Up-to-date (no missing days): {up_to_date_count}")
    print(f"Not found (after one retry): {notfound_count}")
    print(f"Errors: {error_count}")
    print(f"Total rows inserted into DB this run: {total_inserted}")
    print(f"Total elapsed time: {elapsed_total:.2f} seconds (~{elapsed_total / 60:.2f} minutes)")

    # Close pool connections cleanly
    try:
        DBPoolSingleton.instance().closeall()
    except Exception:
        pass

    # Build completeness CSV
    # print(f"Building completeness report into {SUMMARY_CSV} ...")
    # build_completeness_report(symbols)
    # print("Done. Inspect DB in JetBrains Database tool or open the CSV for coverage per symbol.")


if __name__ == "__main__":
    main()
