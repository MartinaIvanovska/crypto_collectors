import os
import time
import json
import sqlite3
import queue
import threading
from datetime import datetime, timedelta, date
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
import csv

# --- CONFIGURATION ---
DB_FILE = "crypto_daily.db"
SUMMARY_CSV = "completeness_report.csv"

WORKERS = 50
BATCH_INSERT_SIZE = 5000
QUEUE_MAXSIZE = 50000
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"

# Time window
END_DT = datetime.utcnow() # - timedelta(days=90)
START_DT = END_DT - timedelta(days=10 * 365)  # ~10 years
START_TS = int(START_DT.timestamp())
END_TS = int(END_DT.timestamp())

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS daily (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,            -- ISO yyyy-mm-dd
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    source_timestamp INTEGER,
    PRIMARY KEY (symbol, date)
);
"""

# --- OPTIMIZATION: Thread-Local Session Storage ---
# This allows us to reuse TCP connections per thread (Keep-Alive)
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        # Mount adapter for higher concurrency
        adapter = requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS)
        thread_local.session.mount('https://', adapter)
    return thread_local.session

# --- OPTIMIZATION: Pre-fetch DB state ---
def get_all_last_dates(db_path):
    """
    Returns a dictionary {symbol: last_date_iso} for ALL symbols in DB.
    Replaces 1000 individual DB queries with 1 query.
    """
    conn = sqlite3.connect(db_path)
    conn.execute(CREATE_SQL)
    cur = conn.cursor()
    try:
        cur.execute("SELECT symbol, MAX(date) FROM daily GROUP BY symbol")
        return {row[0]: row[1] for row in cur.fetchall()}
    except:
        return {}
    finally:
        conn.close()


def yahoo_chart_json(symbol, start_ts=START_TS, end_ts=END_TS, timeout=REQUEST_TIMEOUT):
    """
    GET Yahoo chart JSON using a persistent Session.
    """
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
    """
    Parse Yahoo chart JSON into a list of row tuples.
    """
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


def worker_fetch(symbol, last_date_iso, out_queue):
    """
    Worker: Uses passed-in 'last_date_iso' instead of querying DB.
    """
    if last_date_iso:
        try:
            last_date = datetime.fromisoformat(last_date_iso).date()
            fetch_from_date = last_date + timedelta(days=1)
            start_ts = int(datetime.combine(fetch_from_date, datetime.min.time()).timestamp())
        except Exception:
            start_ts = START_TS
    else:
        start_ts = START_TS

    # nothing to do if start_ts > END_TS
    if start_ts >= END_TS:
        return {"symbol": symbol, "status": "up_to_date", "rows": 0}

    # initial attempt
    status, payload = yahoo_chart_json(symbol, start_ts=start_ts, end_ts=END_TS)
    if status == "not_found":
        # one additional retry
        status, payload = yahoo_chart_json(symbol, start_ts=start_ts, end_ts=END_TS)

    if status == "error":
        print(f"[{symbol}] fetch error: {payload}")
        return {"symbol": symbol, "status": "error", "rows": 0}
    if status == "not_found":
        print(f"[{symbol}] not found (after one retry): {payload}")
        return {"symbol": symbol, "status": "not_found", "rows": 0}

    # parse
    try:
        rows = parse_quote_to_rows(symbol, payload)
    except Exception as e:
        print(f"[{symbol}] parse exception: {e}")
        rows = []

    # Push rows to queue
    if rows:
        out_queue.put(rows)
        return {"symbol": symbol, "status": "ok", "rows": len(rows)}
    else:
        return {"symbol": symbol, "status": "empty", "rows": 0}


def writer_thread_fn(db_path, in_queue, stop_event, stats):
    """
    Writer thread with SQLite PRAGMAs
    """
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    # Speed-oriented pragmas
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("PRAGMA cache_size = -64000;")
    conn.commit()
    cur.execute(CREATE_SQL)
    conn.commit()

    insert_sql = ("INSERT OR IGNORE INTO daily (symbol,date,open,high,low,close,volume,source_timestamp) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

    batch = []
    last_flush = time.time()

    try:
        while not (stop_event.is_set() and in_queue.empty()):
            try:
                item = in_queue.get(timeout=0.5)
            except queue.Empty:
                if batch and (time.time() - last_flush) > 1.0:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    stats["inserted"] += len(batch)
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
                cur.executemany(insert_sql, batch)
                conn.commit()
                stats["inserted"] += len(batch)
                batch.clear()
                last_flush = time.time()

            in_queue.task_done()

        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            stats["inserted"] += len(batch)
            batch.clear()

    finally:
        conn.close()


# -----------------------
# Completeness summary
# -----------------------
def build_completeness_report(db_path, symbols, start_dt=START_DT, end_dt=END_DT, out_csv=SUMMARY_CSV):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    start_date = start_dt.date()
    end_date = end_dt.date()
    expected_days = (end_date - start_date).days + 1

    # Fetch all stats in ONE query instead of looping 1000 times
    sql = """
    SELECT symbol, COUNT(*), MIN(date), MAX(date)
    FROM daily
    GROUP BY symbol
    """
    cur.execute(sql)
    db_stats = {row[0]: {'count': row[1], 'min': row[2], 'max': row[3]} for row in cur.fetchall()}

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["symbol", "fetched_days", "expected_days", "coverage_pct", "earliest_fetched", "sample_missing_dates"])

        for sym in symbols:
            stat = db_stats.get(sym, {'count': 0, 'min': None, 'max': None})
            fetched_days = stat['count']
            earliest = stat['min']
            coverage = (fetched_days / expected_days * 100) if expected_days else 0.0

            writer.writerow([sym, fetched_days, expected_days, f"{coverage:.2f}", earliest or ""])
    conn.close()


def fetch_batch(start, batch_size=100, min_volume=0, min_cap=0):
    """
    Fetches a batch of data from the Yahoo Website from a certain offset(start)
    """
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

    # Keeping original print output
    print(f"Fetched {len(batch_symbols)} symbols from start={start} (after filtering)")
    return batch_symbols


def scrape_crypto_symbols_parallel(total=1000, batch_size=100, max_workers=10, min_volume=1000000, min_cap=1000000):
    """
    Collect all the symbols through the filtered worker threads.
    """
    all_symbols = []
    starts = list(range(0, total, batch_size))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_start = {executor.submit(fetch_batch, start, batch_size, min_volume, min_cap): start for start in
                           starts}
        for future in as_completed(future_to_start):
            batch_symbols = future.result()
            all_symbols.extend(batch_symbols)

    # Simple dedup preserving order
    seen = set()
    unique = []
    for s in all_symbols:
        if s not in seen:
            unique.append(s)
            seen.add(s)
    return unique[:total]


def main():
    t0 = time.time()
    print("Fetching top coin symbols from BeautifullSoup from the offical Yahoo finance page...")
    symbols = scrape_crypto_symbols_parallel()
    print(f"Symbols fetched: {len(symbols)} (sample {symbols[:10]})")
    print(f"Time window: {START_DT.date()} through {END_DT.date()} (~{(END_DT - START_DT).days} days)\n")

    # prepare DB (ensure it exists and table created)
    print(f"Preparing SQLite DB at: {DB_FILE}")

    # OPTIMIZATION: Pre-fetch dates map here
    existing_dates_map = get_all_last_dates(DB_FILE)

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute(CREATE_SQL)
    conn.commit()
    conn.close()

    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    stats = {"inserted": 0, "fetched_rows_total": 0}

    # start writer thread
    writer_thread = threading.Thread(target=writer_thread_fn, args=(DB_FILE, q, stop_event, stats), daemon=True)
    writer_thread.start()

    # Threaded fetch
    print(f"Starting fetch with {WORKERS} worker threads...")
    fetched_symbols = 0
    ok_count = 0
    empty_count = 0
    notfound_count = 0
    error_count = 0
    up_to_date_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        # Pass the pre-fetched date from the map (existing_dates_map.get(sym))
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

            # light progress output
            if fetched_symbols % 25 == 0 or fetched_symbols == len(symbols):
                elapsed = time.time() - t0
                print(
                    f"[{fetched_symbols}/{len(symbols)}] fetched. OK: {ok_count}, empty: {empty_count}, not_found: {notfound_count}, up_to_date: {up_to_date_count}, errors: {error_count} — rows queued: {stats['fetched_rows_total']} — elapsed {elapsed:.1f}s")

    # wait for queue to drain to writer
    print("Fetchers done, waiting for writer to finish inserting...")
    q.join()  # wait until all queued items are processed by writer
    stop_event.set()
    writer_thread.join(timeout=60)
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

    # Completeness report
    # print(f"\nBuilding completeness report into {SUMMARY_CSV} ...")
    # build_completeness_report(DB_FILE, symbols)
    # print("Done. Inspect the DB directly (sqlite3) or the completeness CSV for coverage per symbol.")


if __name__ == "__main__":
    main()