import os
from sqlalchemy import create_engine, text

def get_db_engine():
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", 5432))
    db = os.environ.get("PG_DB", "crypto")
    user = os.environ.get("PG_USER", "crypto_user")
    password = os.environ.get("PG_PASSWORD", "crypto_pass")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
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
