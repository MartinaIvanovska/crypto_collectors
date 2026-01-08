from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, DateTime, UniqueConstraint, text, \
    Float
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func
import time
import os
from datetime import datetime
import uuid
import pandas as pd
from transformers import pipeline


def get_db_engine():
    """Create database engine"""
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", 5432))
    db = os.environ.get("PG_DB", "crypto")
    user = os.environ.get("PG_USER", "crypto_user")
    password = os.environ.get("PG_PASSWORD", "crypto_pass")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True)
    return engine


def create_symbol_news_table(engine):
    """Create symbol_news table if it doesn't exist"""
    metadata = MetaData()

    symbol_news = Table(
        'symbol_news',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('uuid', String(36), default=lambda: str(uuid.uuid4()), unique=True),
        Column('symbol', String(20), nullable=False, index=True),
        Column('title', Text, nullable=False),
        Column('url', Text, nullable=False),
        Column('image', Text),
        Column('sentiment', String(20)),  # positive, negative, neutral
        Column('confidence', Float),  # Confidence score from FinBERT
        Column('scraped_at', DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column('created_at', DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column('source', String(100), default='yahoo_finance'),
        UniqueConstraint('symbol', 'url', name='uix_symbol_url'),  # Prevent duplicate articles
        UniqueConstraint('uuid', name='uix_uuid')
    )

    # Create table if it doesn't exist
    metadata.create_all(engine)
    return symbol_news


def analyze_sentiment_with_finbert(articles_data):
    """Analyze sentiment of news titles using FinBERT"""
    if not articles_data:
        return articles_data

    print("Initializing FinBERT sentiment analysis pipeline...")

    try:
        # Initialize the sentiment analysis pipeline
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            device=0 # Use CPU (-1). Change to 0 or higher for GPU if available
        )

        # Create DataFrame from articles
        df = pd.DataFrame(articles_data)
        df["text"] = df["title"].astype(str)
        texts = df["text"].tolist()

        batch_size = 32
        all_results = []

        print(f"Analyzing sentiment for {len(texts)} articles...")

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_results = sentiment_pipeline(batch, truncation=True)
            all_results.extend(batch_results)
            print(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} articles")

        # Add sentiment and confidence to articles
        for i, (article, result) in enumerate(zip(articles_data, all_results)):
            article['sentiment'] = result["label"].lower()
            article['confidence'] = float(result["score"])

        print(f"Sentiment analysis complete. Results: "
              f"Positive: {sum(1 for a in articles_data if a.get('sentiment') == 'positive')}, "
              f"Negative: {sum(1 for a in articles_data if a.get('sentiment') == 'negative')}, "
              f"Neutral: {sum(1 for a in articles_data if a.get('sentiment') == 'neutral')}")

    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        # Fallback: assign neutral sentiment if analysis fails
        for article in articles_data:
            article['sentiment'] = 'neutral'
            article['confidence'] = 0.5

    return articles_data


def scrape_yahoo_finance_news(symbol):
    """Scrape news for a specific symbol from Yahoo Finance"""
    # Configure Chrome for maximum speed
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # BLOCK UNNECESSARY RESOURCES (Biggest speed gain)
    chrome_options.add_experimental_option(
        "prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
            "profile.default_content_setting_values.javascript": 1,  # Keep JS enabled for dynamic content
        }
    )

    # Initialize
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    articles_data = []
    try:
        # Navigate to the symbol's news page
        url = f'https://finance.yahoo.com/quote/{symbol}/news/'
        driver.get(url)

        # Wait for news container with timeout
        wait = WebDriverWait(driver, 15)

        # Wait for the news stream container - more specific selector
        news_container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul[class*='stream'], div[data-testid='news-stream']"))
        )

        # Additional wait for content to load
        time.sleep(2)

        # Find all news items using specific selectors from the HTML structure
        news_selectors = [
            "li.stream-item",  # Primary selector from HTML
            "li[class*='stream-item']",
            "section[data-testid='storyitem']",
            "div[data-test-locator='mrt-node-card']",
            "article[role='article']"
        ]

        for selector in news_selectors:
            news_items = driver.find_elements(By.CSS_SELECTOR, selector)
            if len(news_items) > 0:
                print(f"Found {len(news_items)} items with selector: {selector}")
                break

        if not news_items:
            # Fallback to any list items in the news container
            news_items = driver.find_elements(By.CSS_SELECTOR, "ul li, ol li")
            print(f"Fallback: Found {len(news_items)} list items")

        for item in news_items[:12]:  # Limit to first 12 items
            try:
                # Extract title - using specific selectors from HTML
                title = ""
                title_selectors = [
                    "h3.clamp",  # Most specific from HTML
                    "h3[class*='title']",
                    "a[class*='titles'] h3",
                    "h3",
                    "a[data-test*='headline']"
                ]

                for title_selector in title_selectors:
                    try:
                        title_elem = item.find_element(By.CSS_SELECTOR, title_selector)
                        title = title_elem.text.strip()
                        if title and len(title) > 10:  # Ensure it's a real title
                            break
                    except:
                        continue

                if not title:
                    # Try to get title from any link with title attribute
                    try:
                        title_link = item.find_element(By.CSS_SELECTOR, "a[title]")
                        title = title_link.get_attribute('title').strip()
                    except:
                        pass

                if not title or len(title) < 10:
                    print(f"Skipping item, title too short: '{title}'")
                    continue

                # Extract URL - using specific selectors from HTML
                link = ""
                link_selectors = [
                    "a[class*='titles']",  # Title link from HTML
                    "a.subtle-link[href*='/news/']",
                    "a[href*='/news/']",
                    "a[href*='article']",
                    "a[title]"  # Link with title attribute
                ]

                for link_selector in link_selectors:
                    try:
                        link_elem = item.find_element(By.CSS_SELECTOR, link_selector)
                        href = link_elem.get_attribute('href')
                        if href and ('/news/' in href or 'article' in href or 'yahoo.com' in href):
                            link = href
                            break
                    except:
                        continue

                if not link:
                    # Try any link in the item
                    try:
                        link_elem = item.find_element(By.TAG_NAME, "a")
                        href = link_elem.get_attribute('href')
                        if href:
                            link = href
                    except:
                        pass

                # Make sure link is absolute
                if link and link.startswith('/'):
                    link = f'https://finance.yahoo.com{link}'
                elif not link:
                    print(f"No link found for title: {title[:50]}...")
                    continue

                # Extract image URL - using specific selectors from HTML
                img_url = ""
                img_selectors = [
                    "a[class*='thumb'] img",  # Image in thumbnail link from HTML
                    "img[src*='yimg.com']",
                    "img[src*='s.yimg.com']",
                    "img"
                ]

                for img_selector in img_selectors:
                    try:
                        img_elem = item.find_element(By.CSS_SELECTOR, img_selector)
                        img_url = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
                        if img_url:
                            break
                    except:
                        continue

                # Clean up the URL if it's a Yahoo image URL with parameters
                if img_url and 's.yimg.com' in img_url:
                    # Extract the base URL before query parameters
                    img_url = img_url.split('?')[0] if '?' in img_url else img_url

                articles_data.append({
                    'symbol': symbol,
                    'title': title,
                    'url': link,
                    'image': img_url
                })

                print(f"  Extracted: {title[:60]}...")

            except Exception as e:
                print(f"Error extracting article: {e}")
                continue

    except Exception as e:
        print(f"Error scraping {symbol}: {e}")
    finally:
        driver.quit()

    return articles_data


def save_articles_to_db(engine, symbol_news_table, articles):
    """Save scraped articles to database"""
    if not articles:
        return 0

    saved_count = 0
    try:
        with engine.connect() as conn:
            for article in articles:
                try:
                    # Use PostgreSQL-specific insert with ON CONFLICT
                    stmt = insert(symbol_news_table).values(
                        symbol=article['symbol'],
                        title=article['title'],
                        url=article['url'],
                        image=article['image'],
                        sentiment=article.get('sentiment', 'neutral'),
                        confidence=article.get('confidence', 0.5)
                    )

                    # Add ON CONFLICT DO NOTHING clause
                    stmt = stmt.on_conflict_do_nothing(
                        constraint='uix_symbol_url'
                    )

                    result = conn.execute(stmt)
                    conn.commit()

                    if result.rowcount > 0:
                        saved_count += 1

                except Exception as e:
                    print(f"Error saving article '{article['title'][:50]}...': {e}")
                    conn.rollback()
                    continue

    except Exception as e:
        print(f"Database error: {e}")

    return saved_count


def get_symbols_to_scrape(engine):
    """Get symbols from database or use default list"""
    try:
        with engine.connect() as conn:
            # Try to get symbols from daily table
            result = conn.execute(text("SELECT DISTINCT symbol FROM daily"))
            symbols = [row[0] for row in result]

            if symbols:
                return symbols
            else:
                # Fallback to popular crypto symbols
                return [
                    'BTC-USD', 'ETH-USD', 'BNB-USD', 'XRP-USD', 'SOL-USD',
                    'ADA-USD', 'DOGE-USD', 'DOT-USD', 'MATIC-USD', 'AVAX-USD'
                ]
    except Exception as e:
        print(f"Error getting symbols from database: {e}")
        # If table doesn't exist or error, use default list
        return [
            'BTC-USD', 'ETH-USD', 'BNB-USD', 'XRP-USD', 'SOL-USD',
            'ADA-USD', 'DOGE-USD', 'DOT-USD', 'MATIC-USD', 'AVAX-USD'
        ]


def main():
    """Main function to scrape and save news"""
    print("Starting Yahoo Finance news scraper...")

    # Initialize database
    print("Connecting to database...")
    engine = get_db_engine()
    symbol_news_table = create_symbol_news_table(engine)

    # Get symbols to scrape
    symbols = get_symbols_to_scrape(engine)
    print(f"Found {len(symbols)} symbols to scrape: {symbols}")

    # For testing, use just a couple of symbols
    symbols = ["BTC-USD", "ETH-USD"]

    all_articles = []
    total_saved = 0

    # First, scrape all articles
    for symbol in symbols:
        print(f"\nScraping news for {symbol}...")
        articles = scrape_yahoo_finance_news(symbol)
        print(f"  Found {len(articles)} articles")
        all_articles.extend(articles)

        # Add delay between requests to be respectful to the server
        time.sleep(1)

    # Analyze sentiment for all articles at once
    if all_articles:
        print(f"\nAnalyzing sentiment for {len(all_articles)} articles...")
        analyzed_articles = analyze_sentiment_with_finbert(all_articles)

        # Save analyzed articles to database
        print("\nSaving analyzed articles to database...")
        saved = save_articles_to_db(engine, symbol_news_table, analyzed_articles)
        total_saved = saved
        print(f"  Saved {saved} new articles to database")

    print(f"\nScraping completed! Total {total_saved} new articles saved to symbol_news table.")

    # Print summary with sentiment breakdown
    try:
        with engine.connect() as conn:
            # Use text() for raw SQL queries
            result = conn.execute(text("SELECT COUNT(*) FROM symbol_news"))
            total_count = result.scalar()
            print(f"Total articles in database: {total_count}")

            result = conn.execute(text("""
                SELECT symbol, COUNT(*) as article_count 
                FROM symbol_news 
                GROUP BY symbol 
                ORDER BY article_count DESC
            """))
            print("\nArticles per symbol:")
            for row in result:
                print(f"  {row[0]}: {row[1]} articles")

            # Print sentiment distribution
            result = conn.execute(text("""
                SELECT sentiment, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
                FROM symbol_news 
                GROUP BY sentiment
                ORDER BY count DESC
            """))
            print("\nSentiment distribution:")
            for row in result:
                print(f"  {row[0]}: {row[1]} articles ({row[2]}%)")

    except Exception as e:
        print(f"Error getting summary: {e}")


if __name__ == "__main__":
    main()