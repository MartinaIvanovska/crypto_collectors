import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urljoin
from typing import List, Dict, Optional
import time
import re
from transformers import pipeline  # For FinBERT

CRYPTO_NEWS_URL = "https://finance.yahoo.com/topic/crypto/"
OUTPUT_FILENAME = "crypto_news_finbert_sentiment_yfinance.csv"


def get_webpage_content(url: str) -> str:
    """Retrieve HTML content from a given URL."""
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
    """
    Extract and convert publication time from Yahoo Finance format.
    Expected format: "Source•Xm ago", "Source•Xh ago", "Source•Xd ago"
    Example: "Yahoo Finance•53m ago"
    """
    publishing_div = element.find("div", class_="publishing")
    if publishing_div:
        full_text = publishing_div.get_text(strip=True)
        time_pattern = r'(\d+)\s*(m|min|minute|h|hour|d|day|w|week|mo|month|y|year)s?\s*ago'
        match = re.search(time_pattern, full_text, re.IGNORECASE)

        if match:
            value = int(match.group(1))
            unit = match.group(2).lower()
            now = datetime.now()

            if unit in ['m', 'min', 'minute']:
                publish_time = now - timedelta(minutes=value)
            elif unit in ['h', 'hour']:
                publish_time = now - timedelta(hours=value)
            elif unit in ['d', 'day']:
                publish_time = now - timedelta(days=value)
            elif unit in ['w', 'week']:
                publish_time = now - timedelta(weeks=value)
            elif unit in ['mo', 'month']:
                publish_time = now - timedelta(days=value * 30)
            elif unit in ['y', 'year']:
                publish_time = now - timedelta(days=value * 365)
            else:
                return None

            return publish_time.strftime("%Y-%m-%d %H:%M:%S")

    return None


def parse_news_items(html_content: str) -> List[Dict]:
    """Extract news article information from HTML content."""
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

        # NOTE: Yahoo Finance doesn't provide article descriptions in the listing
        # We'll leave description empty or you could choose to scrape full articles
        article_description = ""

        publish_time = extract_publication_time(item)

        articles.append({
            "title": article_title,
            "description": article_description,  # Empty for Yahoo Finance
            "published_at": publish_time,
            "url": urljoin(CRYPTO_NEWS_URL, article_link) if article_link else ""
        })

    return articles


def collect_crypto_news() -> pd.DataFrame:
    """Main function to gather cryptocurrency news from Yahoo Finance."""
    print(f"Retrieving cryptocurrency news from {CRYPTO_NEWS_URL}")

    page_html = get_webpage_content(CRYPTO_NEWS_URL)
    if not page_html:
        print("Failed to retrieve page content")
        return pd.DataFrame()

    news_data = parse_news_items(page_html)

    print(f"Successfully collected {len(news_data)} news articles")
    return pd.DataFrame(news_data)


def analyze_sentiment_with_finbert(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze sentiment using FinBERT model.
    Returns DataFrame with added 'sentiment' and 'confidence' columns.
    """
    if df.empty:
        return df

    print("Initializing FinBERT sentiment analysis pipeline...")

    # Initialize the FinBERT pipeline
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert"
    )

    # Create the 'text' column by combining title and description
    # Since description is empty for Yahoo Finance, we just use title
    df["text"] = df["title"].astype(str)

    print(f"Analyzing sentiment for {len(df)} articles...")

    # Get sentiment predictions
    texts = df["text"].tolist()

    # Process in batches if you have many articles to avoid memory issues
    batch_size = 32
    all_results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch_results = sentiment_pipeline(batch, truncation=True)
        all_results.extend(batch_results)
        print(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} articles")

    # Extract sentiment and confidence scores
    df["sentiment"] = [r["label"].lower() for r in all_results]
    df["confidence"] = [r["score"] for r in all_results]

    return df


def save_and_display_results(news_df: pd.DataFrame) -> None:
    """Save the news data to CSV file and display results."""
    if not news_df.empty:
        # Reorder columns to match your requested format
        final_columns = ["title", "description", "published_at", "text", "sentiment", "confidence"]

        # Ensure all columns exist (description might be empty)
        for col in final_columns:
            if col not in news_df.columns:
                if col == "description":
                    news_df[col] = ""  # Add empty description column
                else:
                    print(f"Warning: Column '{col}' not found in DataFrame")

        # Select and reorder columns
        final_df = news_df[final_columns]

        # Save to CSV
        final_df.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8')
        print(f"Results saved to '{OUTPUT_FILENAME}'")

        # Display statistics
        print(f"\nData Statistics:")
        print(f"Total articles analyzed: {len(final_df)}")

        sentiment_counts = final_df["sentiment"].value_counts()
        for sentiment, count in sentiment_counts.items():
            percentage = (count / len(final_df)) * 100
            print(f"  {sentiment}: {count} articles ({percentage:.1f}%)")

        avg_confidence = final_df["confidence"].mean()
        print(f"Average confidence score: {avg_confidence:.3f}")

        # Display sample
        print("\nSample of analyzed articles:")
        print("=" * 100)
        for idx, row in final_df.head(3).iterrows():
            title_preview = row['title'][:80] + "..." if len(row['title']) > 80 else row['title']
            print(f"Title: {title_preview}")
            print(f"Sentiment: {row['sentiment']} (confidence: {row['confidence']:.3f})")
            print(f"Published: {row['published_at'] if pd.notna(row['published_at']) else 'Not available'}")
            print("-" * 100)
    else:
        print("No news articles were collected.")


def main():
    """Execute the scraping and sentiment analysis process."""
    start_time = time.time()

    # Step 1: Collect news from Yahoo Finance
    crypto_news_df = collect_crypto_news()

    if not crypto_news_df.empty:
        # Step 2: Analyze sentiment using FinBERT
        analyzed_df = analyze_sentiment_with_finbert(crypto_news_df)

        # Step 3: Save and display results
        save_and_display_results(analyzed_df)

        elapsed_time = round(time.time() - start_time, 2)
        print(f"\nProcess completed in {elapsed_time} seconds")

        # Show file info
        import os
        if os.path.exists(OUTPUT_FILENAME):
            file_size = os.path.getsize(OUTPUT_FILENAME) / 1024
            print(f"File size: {file_size:.2f} KB")
    else:
        print("No data was collected. Check your internet connection or try again later.")


if __name__ == "__main__":
    main()