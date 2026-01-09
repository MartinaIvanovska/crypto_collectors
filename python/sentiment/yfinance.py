import os
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin
from typing import List, Dict
from urllib.parse import quote_plus
from sqlalchemy.engine import URL

import requests
import pandas as pd
from bs4 import BeautifulSoup

from transformers import pipeline  # FinBERT

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Text, Float, DateTime, UniqueConstraint, func, text
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


CRYPTO_NEWS_URL = "https://finance.yahoo.com/topic/crypto/"


# ---------------------------
# Database helpers
# ---------------------------

def get_db_engine():
    PG_HOST = "kriptoserver.postgres.database.azure.com"
    PG_PORT = 5432
    PG_DB = "crypto"
    PG_USER = "adminmartina"  # exactly same as psycopg2
    PG_PASSWORD = "Andrejcar123!"
    PG_SSLMODE = "require"

    DATABASE_URL = URL.create(
        "postgresql+psycopg2",
        username=PG_USER,
        password=PG_PASSWORD,  # pass raw, don't quote_plus
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        query={"sslmode": PG_SSLMODE}
    )

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # Test the connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))  # ✅
        print(result.fetchone())
    return engine


def define_news_table(metadata: MetaData) -> Table:
    return Table(
        "crypto_news",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("title", Text, nullable=False),
        Column("description", Text),
        Column("published_at", DateTime),
        Column("url", Text, nullable=False),
        Column("image_url", Text),
        Column("text", Text),
        Column("sentiment", String(32)),
        Column("confidence", Float),
        Column("created_at", DateTime, server_default=func.now()),
        UniqueConstraint("url", name="uq_crypto_news_url"),
        extend_existing=True,
    )


def create_table_if_needed(engine):
    metadata = MetaData()
    table = define_news_table(metadata)
    metadata.create_all(engine)
    return table

def clear_crypto_news_table():
    """Clear all records from the crypto_news table."""
    engine = get_db_engine()
    table = create_table_if_needed(engine)
    conn = engine.connect()
    trans = conn.begin()
    try:
        deleted = conn.execute(table.delete())
        trans.commit()
        print(f"Cleared {deleted.rowcount} rows from the crypto_news table.")
    except SQLAlchemyError as e:
        trans.rollback()
        print("Error clearing the crypto_news table:", e)
    finally:
        conn.close()

def upsert_news_records(engine, table: Table, records: List[Dict]):
    if not records:
        print("No records to insert")
        return

    conn = engine.connect()
    trans = conn.begin()
    try:
        for rec in records:
            # Build an insert statement with ON CONFLICT (url) DO UPDATE
            stmt = insert(table).values(**rec)
            update_cols = {k: stmt.excluded[k] for k in ("title", "description", "published_at", "image_url", "text", "sentiment", "confidence")}
            stmt = stmt.on_conflict_do_update(index_elements=[table.c.url], set_=update_cols)
            conn.execute(stmt)
        trans.commit()
        print(f"Upserted {len(records)} records to the database")
    except Exception as e:
        trans.rollback()
        print("Error while upserting records:", e)
    finally:
        conn.close()


# ---------------------------
# Scraping + parsing
# ---------------------------

def get_webpage_content(url: str) -> str:
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = requests.get(url, headers=browser_headers, timeout=20)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return ""


def extract_publication_time(element):
    publishing_div = element.find("div", class_="publishing")
    if publishing_div:
        full_text = publishing_div.get_text(strip=True)
        time_pattern = r"(\d+)\s*(m|min|minute|h|hour|d|day|w|week|mo|month|y|year)s?\s*ago"
        match = re.search(time_pattern, full_text, re.IGNORECASE)

        if match:
            value = int(match.group(1))
            unit = match.group(2).lower()
            now = datetime.now()

            if unit in ["m", "min", "minute"]:
                publish_time = now - timedelta(minutes=value)
            elif unit in ["h", "hour"]:
                publish_time = now - timedelta(hours=value)
            elif unit in ["d", "day"]:
                publish_time = now - timedelta(days=value)
            elif unit in ["w", "week"]:
                publish_time = now - timedelta(weeks=value)
            elif unit in ["mo", "month"]:
                publish_time = now - timedelta(days=value * 30)
            elif unit in ["y", "year"]:
                publish_time = now - timedelta(days=value * 365)
            else:
                return None

            return publish_time

    return None


def parse_news_items(html_content: str) -> List[Dict]:
    soup = BeautifulSoup(html_content, "lxml")
    articles = []

    news_elements = soup.select("li.stream-item, li.story-item, article.js-stream-content")

    for item in news_elements:
        link_element = item.find("a", href=True)
        if not link_element:
            link_element = item.select_one("a[href]")
            if not link_element:
                continue

        article_link = link_element.get("href", "")
        article_title = link_element.get("title") or link_element.get_text(strip=True)

        # Extract image URL
        img_element = item.find("img")
        article_image_url = img_element["src"] if img_element and img_element.has_attr("src") else ""

        article_description = ""

        publish_time_dt = extract_publication_time(item)

        articles.append({
            "title": article_title,
            "description": article_description,
            "published_at": publish_time_dt,  # keep as datetime or None
            "url": urljoin(CRYPTO_NEWS_URL, article_link) if article_link else "",
            "image_url": article_image_url
        })

    return articles


def collect_crypto_news() -> pd.DataFrame:
    print(f"Retrieving cryptocurrency news from {CRYPTO_NEWS_URL}")

    page_html = get_webpage_content(CRYPTO_NEWS_URL)
    if not page_html:
        print("Failed to retrieve page content")
        return pd.DataFrame()

    news_data = parse_news_items(page_html)
    print(f"Successfully collected {len(news_data)} news articles")
    return pd.DataFrame(news_data)


# ---------------------------
# Sentiment analysis (FinBERT)
# ---------------------------

def analyze_sentiment_with_finbert(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    print("Initializing FinBERT sentiment analysis pipeline...")

    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert"
    )

    df["text"] = df["title"].astype(str)
    texts = df["text"].tolist()

    batch_size = 32
    all_results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch_results = sentiment_pipeline(batch, truncation=True)
        all_results.extend(batch_results)
        print(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} articles")

    df["sentiment"] = [r["label"].lower() for r in all_results]
    df["confidence"] = [r["score"] for r in all_results]

    return df


# ---------------------------
# Save to DB instead of CSV
# ---------------------------

def save_to_database(news_df: pd.DataFrame) -> None:
    if news_df.empty:
        print("No news articles were collected.")
        return

    engine = get_db_engine()
    table = create_table_if_needed(engine)

    # Prepare records for insertion
    records = []
    for _, row in news_df.iterrows():
        published_at = row.get("published_at")
        # published_at can be datetime or string; ensure it's a datetime or None
        if isinstance(published_at, str):
            try:
                published_at = datetime.strptime(published_at, "%Y-%m-%d %H:%M:%S")
            except Exception:
                published_at = None

        rec = {
            "title": row.get("title", ""),
            "description": row.get("description", ""),
            "published_at": published_at,
            "url": row.get("url", ""),
            "image_url": row.get("image_url", ""),
            "text": row.get("text", ""),
            "sentiment": row.get("sentiment", ""),
            "confidence": float(row.get("confidence") or 0.0),
        }
        records.append(rec)

    upsert_news_records(engine, table, records)

    # Display a short summary similar to the CSV flow
    print(f"\nData Statistics:")
    print(f"Total articles processed: {len(records)}")
    sentiment_counts = news_df["sentiment"].value_counts()
    for sentiment, count in sentiment_counts.items():
        percentage = (count / len(records)) * 100
        print(f"  {sentiment}: {count} articles ({percentage:.1f}%)")

    avg_confidence = news_df["confidence"].mean()
    print(f"Average confidence score: {avg_confidence:.3f}")

def drop_crypto_news_table():
    """Drop the crypto_news table if it exists."""
    engine = get_db_engine()
    metadata = MetaData()
    table = define_news_table(metadata)
    conn = engine.connect()
    trans = conn.begin()
    try:
        table.drop(engine, checkfirst=True)
        trans.commit()
        print("Dropped the crypto_news table.")
    except SQLAlchemyError as e:
        trans.rollback()
        print("Error dropping the crypto_news table:", e)
    finally:
        conn.close()

# ---------------------------
# Main
# ---------------------------

def main():
    start_time = time.time()
    drop_crypto_news_table()
    crypto_news_df = collect_crypto_news()

    if not crypto_news_df.empty:
        analyzed_df = analyze_sentiment_with_finbert(crypto_news_df)
        save_to_database(analyzed_df)

        elapsed_time = round(time.time() - start_time, 2)
        print(f"\nProcess completed in {elapsed_time} seconds")
    else:
        print("No data was collected. Check your internet connection or try again later.")


if __name__ == "__main__":
    clear_crypto_news_table()
    main()
