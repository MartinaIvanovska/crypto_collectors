import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from sqlalchemy.engine import URL

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
