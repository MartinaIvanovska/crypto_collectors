# python/main.py

# -------------------------
# Imports
# -------------------------
from on_chain.onchain_dashboard import get_all_metrics
from sentiment.symbol_sentiment import get_sentiment_sum  # <-- new DB backed function

# -------------------------
# Scaling ranges and weights (unchanged)
# -------------------------
SCALING_RANGES = {
    "active_addresses": (100_000, 1_500_000),
    "transactions": (100_000, 2_000_000),
    "hashrate": (50_000_000_000, 700_000_000_000),
    "tvl": (1_000_000_000, 100_000_000_000),
    "nvt": (10, 150),  # inverted
    "mvrv": (0.7, 4.5),  # inverted
}

WEIGHTS = {
    "active_addresses": 0.10,
    "transactions": 0.10,
    "hashrate": 0.30,
    "tvl": 0.05,
    "nvt": 0.25,
    "mvrv": 0.20,
}


def minmax_scale(value, min_val, max_val, invert=False):
    """
    Scales a value to [0,1] using min-max normalization.
    Optionally inverts the result (useful for valuation metrics).
    """
    if value is None:
        return 0.5  # neutral fallback

    # Clip to bounds
    value = max(min(value, max_val), min_val)

    scaled = (value - min_val) / (max_val - min_val)

    if invert:
        scaled = 1.0 - scaled

    return round(scaled, 4)


def gather_all_data(symbol: str):
    """
    Returns on-chain metrics and sentiment_sum (from DB) as a tuple:
      (metrics_dict, (asset, active_addresses, transactions, ... , sentiment_sum))
    """
    # Get on-chain metrics
    metrics = get_all_metrics(symbol)

    # Get sentiment sum from DB-backed function
    try:
        sentiment_sum = int(get_sentiment_sum(symbol) or 0)
    except Exception as e:
        print(f"Warning: error obtaining sentiment for {symbol}: {e}")
        sentiment_sum = 0

    # Assign to individual variables
    asset = metrics.get("Asset")
    active_addresses = metrics.get("Active Addresses")
    transactions = metrics.get("Transactions")
    hash_rate = metrics.get("Hash Rate")
    mvrv = metrics.get("MVRV")
    tvl = metrics.get("TVL")
    nvt = metrics.get("NVT")
    coingecko_id = metrics.get("CoinGecko ID")

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


def combination(metrics: dict, sentiment_sum: float):
    """
    MinMax-scaled on-chain + sentiment trading signal
    """
    addr_raw = metrics.get("Active Addresses")
    tx_raw = metrics.get("Transactions")
    hash_raw = metrics.get("Hash Rate")
    tvl_raw = metrics.get("TVL")
    nvt_raw = metrics.get("NVT")
    mvrv_raw = metrics.get("MVRV")

    addr = minmax_scale(addr_raw, *SCALING_RANGES["active_addresses"])
    tx = minmax_scale(tx_raw, *SCALING_RANGES["transactions"])
    hash_r = minmax_scale(hash_raw, *SCALING_RANGES["hashrate"])
    tvl = minmax_scale(tvl_raw, *SCALING_RANGES["tvl"])
    nvt = minmax_scale(nvt_raw, *SCALING_RANGES["nvt"], invert=True)
    mvrv = minmax_scale(mvrv_raw, *SCALING_RANGES["mvrv"], invert=True)

    onchain_score = (
        addr * WEIGHTS["active_addresses"] +
        tx * WEIGHTS["transactions"] +
        hash_r * WEIGHTS["hashrate"] +
        tvl * WEIGHTS["tvl"] +
        nvt * WEIGHTS["nvt"] +
        mvrv * WEIGHTS["mvrv"]
    )

    # Normalize sentiment sum into [-1, 1], same logic you had before
    sentiment_score = max(min(sentiment_sum / 10, 1.0), -1.0)
    print("Sentiment Sum:", sentiment_sum)
    print("Normalized Sentiment Score:", sentiment_score)

    final_score = 0.75 * onchain_score + 0.25 * sentiment_score

    signal = (
        "BUY" if final_score >= 0.75 else
        "NEUTRAL" if final_score >= 0.45 else
        "SELL"
    )

    return {
        "onchain_score": round(onchain_score, 4),
        "sentiment_score": round(sentiment_score, 4),
        "final_score": round(final_score, 4),
        "signal": signal,
        "scaled_metrics": {
            "active_addresses": addr,
            "transactions": tx,
            "hashrate": hash_r,
            "tvl": tvl,
            "nvt": nvt,
            "mvrv": mvrv,
        }
    }


# -------------------------
# Main function
# -------------------------
def main(sym="BTC"):
    """
    Main function that processes data for a given cryptocurrency.
    """
    symbol = sym.split("-")[0]

    # Gather all data (on-chain + DB sentiment)
    metrics_dict, metrics_vars = gather_all_data(symbol)

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

    prediction = combination(metrics_dict, sentiment_sum)

    print("\n" + "=" * 50)
    print("TRADING SIGNAL")
    print("=" * 50)
    print("On-chain Score:", prediction["onchain_score"])
    print("Sentiment Score:", prediction["sentiment_score"])
    print("Final Score:", prediction["final_score"])
    print("SIGNAL:", prediction["signal"])

    print("\n" + "=" * 50)
    print(f"DATA FOR {symbol}")
    print("=" * 50)
    print(f"Sentiment sum (raw): {sentiment_sum}")

    print("\n--- On-chain Metrics ---")
    print("Asset:", asset)
    print("Active Addresses:", active_addresses)
    print("Transactions:", transactions)
    print("Hash Rate:", hash_rate)
    print("MVRV:", mvrv)
    print("TVL:", tvl)
    print("NVT:", nvt)
    print("CoinGecko ID:", coingecko_id)

    # Return results for potential further processing
    return {
        "symbol": symbol,
        "signal": prediction["signal"],
        "final_score": prediction["final_score"],
        "sentiment_sum": sentiment_sum,
        "metrics": metrics_dict
    }


if __name__ == "__main__":
    # Run for multiple cryptocurrencies
    results = []

    # Bitcoin analysis
    print("\n" + "=" * 60)
    print("BITCOIN ANALYSIS")
    print("=" * 60)
    btc_result = main("BTC-USD")
    results.append(btc_result)

    # Ethereum analysis
    print("\n" + "=" * 60)
    print("ETHEREUM ANALYSIS")
    print("=" * 60)
    eth_result = main("ETH-USD")
    results.append(eth_result)

    # Summary report
    print("\n" + "=" * 60)
    print("SUMMARY REPORT")
    print("=" * 60)
    for result in results:
        print(
            f"{result['symbol']:5} | Signal: {result['signal']:7} | Score: {result['final_score']:.3f} | Sentiment: {result['sentiment_sum']:+d}"
        )
