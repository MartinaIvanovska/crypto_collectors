import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from sqlalchemy.engine import URL

def get_db_engine():
    PG_HOST = os.environ.get("PG_HOST", "localhost")
    PG_PORT = int(os.environ.get("PG_PORT", 5432))
    PG_DB = os.environ.get("PG_DB", "crypto")
    PG_USER = os.environ.get("PG_USER", "crypto_user")
    PG_PASSWORD = os.environ.get("PG_PASSWORD", "crypto_pass")
    PG_SSLMODE = os.environ.get("PG_SSLMODE", "require")  # Azure requires sslmode=require

    # --- Encode password to handle special characters ---
    PG_PASSWORD_ESCAPED = quote_plus(PG_PASSWORD)

    # --- Create SQLAlchemy URL safely ---
    url = URL.create(
        "postgresql+psycopg2",
        username=PG_USER,
        password=PG_PASSWORD_ESCAPED,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        query={"sslmode": PG_SSLMODE}
    )
    engine = create_engine(url, pool_pre_ping=True)
    return engine



def get_sentiment_sum(symbol: str) -> int:
    """
    Returns sentiment sum for a given symbol
    positive = +1
    negative = -1
    neutral  = 0
    """

    engine = get_db_engine()

    query = text("""
        SELECT sentiment
        FROM symbol_news
        WHERE symbol = :symbol
    """)

    sentiment_map = {
        "positive": 1,
        "negative": -1,
        "neutral": 0
    }

    total_score = 0

    with engine.connect() as conn:
        result = conn.execute(query, {"symbol": symbol}).mappings()

        for row in result:
            sentiment = row['sentiment'].lower()
            # print(sentiment)
            total_score += sentiment_map.get(sentiment, 0)

    return total_score


# Example usage
if __name__ == "__main__":
    coin = "ETH-USD"
    score = get_sentiment_sum(coin)
    print(f"Sentiment score for {coin}: {score}")
