# python/main.py

# -------------------------
# Imports
# -------------------------
from on_chain.onchain_dashboard import get_all_metrics
from sentiment.sentiment_sum import compute_sentiment_sum

# -------------------------
# Combined wrapper
# -------------------------
def gather_all_data(symbol: str, csv_path: str, keyword: str):
    """
    Returns all metrics and sentiment sum as separate variables.
    """
    # Get on-chain metrics
    metrics = get_all_metrics(symbol)

    # Get sentiment sum
    sentiment_sum = compute_sentiment_sum(csv_path, keyword)

    # Assign to individual variables
    asset = metrics.get("Asset")
    active_addresses = metrics.get("Active Addresses")
    transactions = metrics.get("Transactions")
    hash_rate = metrics.get("Hash Rate")
    mvrv = metrics.get("MVRV")
    tvl = metrics.get("TVL")
    nvt = metrics.get("NVT")
    coingecko_id = metrics.get("CoinGecko ID")

    # Return both dict and individual variables as a tuple
    return metrics, (
        asset,
        active_addresses,
        transactions,
        hash_rate,
        mvrv,
        tvl,
        nvt,
        coingecko_id,
        sentiment_sum
    )

# -------------------------
# Main function
# -------------------------
def main(sym = "BTC",
         pateka="sentiment/crypto_news_finbert_sentiment_whale_news.csv",
         zbor="Bitcoin"):
    # Example inputs
    symbol = sym
    # csv_path = "sentiment/crypto_news_finbert_sentiment_whale_news.csv"
    # csv_path = "sentiment/crypto_news_finbert_sentiment.csv"
    csv_path = pateka
    keyword = zbor

    # Gather all data
    metrics_dict, metrics_vars = gather_all_data(symbol, csv_path, keyword)

    # Unpack variables
    (
        asset,
        active_addresses,
        transactions,
        hash_rate,
        mvrv,
        tvl,
        nvt,
        coingecko_id,
        sentiment_sum
    ) = metrics_vars

    # Print results
    print("\n--- On-chain & Sentiment Data ---")
    print("Asset:", asset)
    print("Active Addresses:", active_addresses)
    print("Transactions:", transactions)
    print("Hash Rate:", hash_rate)
    print("MVRV:", mvrv)
    print("TVL:", tvl)
    print("NVT:", nvt)
    print("CoinGecko ID:", coingecko_id)
    print("Sentiment Sum:", sentiment_sum)


if __name__ == "__main__":
    main()
    main("ETH",
         "sentiment/crypto_news_finbert_sentiment.csv",
         "Ethereum")
