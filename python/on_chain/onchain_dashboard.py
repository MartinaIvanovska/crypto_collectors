import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from tabulate import tabulate

# -----------------------------
# GLOBAL CONSTANTS / CACHE
# -----------------------------

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
LLAMA_CHAINS_URL = "https://api.llama.fi/chains"
WHALE_ALERT_URL = "https://whale-alert.io/alerts.json?range=last_30_days"

CHAIN_CACHE: Dict[str, Dict] = {}
# -----------------------------
# INITIAL DATA LOAD
# -----------------------------

def load_chain_data() -> None:
    """Fetch TVL + CoinGecko IDs once and cache them."""
    global CHAIN_CACHE
    chains = requests.get(LLAMA_CHAINS_URL, timeout=10).json()
    for c in chains:
        symbol = c.get("tokenSymbol")
        if symbol:
            CHAIN_CACHE[symbol.upper()] = {
                "tvl": c.get("tvl", 0),
                "coin_id": c.get("gecko_id", "")
            }

load_chain_data()

# -----------------------------
# BASIC LOOKUPS
# -----------------------------

def get_coin_id(symbol: str) -> str:
    return CHAIN_CACHE.get(symbol.upper(), {}).get("coin_id", "")

def get_tvl(symbol: str) -> float:
    return CHAIN_CACHE.get(symbol.upper(), {}).get("tvl", 0)

# -----------------------------
# COINGECKO METRICS
# -----------------------------

def get_nvt(symbol: str) -> float:
    """NVT = Market Cap / Volume"""
    coin_id = get_coin_id(symbol)
    if not coin_id:
        return 0

    try:
        r = requests.get(f"{COINGECKO_BASE}/coins/{coin_id}", timeout=10).json()
        md = r.get("market_data", {})
        market_cap = md.get("market_cap", {}).get("usd", 0)
        volume = md.get("total_volume", {}).get("usd", 0)
        return market_cap / volume if volume else 0
    except Exception as e:
        print(f"Error fetching NVT for {symbol}: {e}")
        return 0

# -----------------------------
# COINMETRICS (COMMUNITY API)
# -----------------------------

def fetch_coinmetrics(symbol: str, metrics: List[str], days: int = 7) -> Dict:
    """Fetch metrics from CoinMetrics community API."""
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    url = (
        f"https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?"
        f"assets={symbol.lower()}&metrics={','.join(metrics)}"
        f"&start_time={start.strftime('%Y-%m-%d')}&end_time={end.strftime('%Y-%m-%d')}"
    )

    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if "data" not in data or not data["data"]:
            return {m: 0 for m in metrics}
        latest = data["data"][-1]
        return {m: float(latest.get(m, 0) or 0) for m in metrics}
    except Exception as e:
        print(f"Error fetching CoinMetrics data for {symbol}: {e}")
        return {m: 0 for m in metrics}

# -----------------------------
# METRIC WRAPPERS
# -----------------------------

def get_address_count(symbol: str) -> float:
    return fetch_coinmetrics(symbol, ["AdrActCnt"]).get("AdrActCnt", 0)

def get_transaction_count(symbol: str) -> float:
    return fetch_coinmetrics(symbol, ["TxCnt"]).get("TxCnt", 0)

def get_hash_rate(symbol: str) -> float:
    return fetch_coinmetrics(symbol, ["HashRate"]).get("HashRate", 0)

def get_mvrv(symbol: str) -> float:
    return fetch_coinmetrics(symbol, ["CapMVRVCur"]).get("CapMVRVCur", 0)

# -----------------------------
# EXCHANGE FLOW CALCULATOR
# -----------------------------
EXCHANGE_KEYWORDS = [
    "coinbase", "binance", "okex", "kraken", "bitfinex",
    "bybit", "huobi", "htx", "bitstamp", "kucoin",
    "gate", "gemini", "aave", "transferred"
]

def is_exchange(text: str) -> bool:
    text = text.lower()
    return any(ex in text for ex in EXCHANGE_KEYWORDS)


def get_exchange_flows(symbol: str, limit: int = 100) -> Dict:
    data = get_whale_movements(limit=50000)

    inflow = 0.0
    outflow = 0.0

    for tx in data:
        text = tx.get("text", "").lower()
        amounts = tx.get("amounts", [])

        for a in amounts:
            if a.get("symbol", "").lower() != symbol.lower():
                continue

            value_usd = a.get("value_usd", 0) or 0

            # unknown → exchange = INFLOW
            if "to" in text and is_exchange(text) and "unknown wallet to" in text:
                inflow += value_usd

            # exchange → unknown = OUTFLOW
            elif "from" in text and is_exchange(text) and "to unknown wallet" in text:
                outflow += value_usd

    return {
        "exchange_inflow": inflow,
        "exchange_outflow": outflow,
        "net_flow": outflow - inflow
    }

# -----------------------------
# WHALE ALERTS
# -----------------------------

def get_whale_movements(limit: int = 5, whale_url = WHALE_ALERT_URL ) -> List[Dict]:
    try:
        data = requests.get(whale_url, timeout=10).json()[:limit]
        result = []
        for d in data:
            d.pop("emoticons")
            d.pop("link")
            # d.pop("color")
            result.append(d)
        return result
    except Exception as e:
        print(f"Error fetching whale movements: {e}")
        return []


# -----------------------------
# AGGREGATED METRICS
# -----------------------------

def get_all_metrics(symbol: str) -> Dict:
    if "-" in symbol:
        symbol = symbol.split("-")[0]
    metrics = fetch_coinmetrics(symbol, ["AdrActCnt", "TxCnt", "HashRate", "CapMVRVCur"])

    return {
        "Asset": symbol.upper(),
        "Active Addresses": metrics.get("AdrActCnt"),
        "Transactions": metrics.get("TxCnt"),
        "Hash Rate": metrics.get("HashRate"),
        "MVRV": metrics.get("CapMVRVCur"),
        "TVL": get_tvl(symbol),
        "NVT": get_nvt(symbol),
        "CoinGecko ID": get_coin_id(symbol)
    }

def exchange_flows(symbol: str) -> Dict:
    exchange_flows = get_exchange_flows(symbol)
    return {
        "Exchange Inflow (USD)": exchange_flows["exchange_inflow"],
        "Exchange Outflow (USD)": exchange_flows["exchange_outflow"],
        "Exchange Net Flow (USD)": exchange_flows["net_flow"]
    }


# -----------------------------
# DISPLAY UTILITIES
# -----------------------------

def display_metrics(symbol: str) -> None:
    data = get_all_metrics(symbol)
    print("\nON-CHAIN METRICS")
    print(tabulate(data.items(), headers=["Metric", "Value"], tablefmt="pretty"))

def display_whales(limit: int = 5000) -> None:
    whales = get_whale_movements(limit=limit)
    print("\nWHALE MOVEMENTS")
    print(tabulate(whales, headers="keys", tablefmt="pretty"))

# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    data = get_whale_movements(limit=20)
    # print(data)
    test_coins = ["BTC","ETH","DOGE","SOL","USDT"]
    all_metrics = []
    all_exchange_flows = []
    for c in test_coins:
        all_metrics.append(get_all_metrics(c))
        all_exchange_flows.append(exchange_flows(c))
        display_metrics(c)

    display_whales(5000)
    # print(all_metrics)
    # print(all_exchange_flows)