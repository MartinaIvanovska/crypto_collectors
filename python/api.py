import os
import sqlite3
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

# --- Import your modules ---
from technical_analysis import technical_analysis
from lstm import lstm_attempt
from on_chain.onchain_dashboard import get_all_metrics, get_whale_movements
from main import gather_all_data, combination  # Assuming main.py is renamed or imported as is

app = FastAPI(title="Crypto Analytics Microservice")

# --- Configuration ---
# Paths must be relative to the container structure
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Docker volume map will place data at /data
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "..", "data", "crypto_daily.db"))


# --- Pydantic Models for Response Documentation ---
class TechnicalData(BaseModel):
    symbol: str
    date: str
    signal: str
    rsi: float
    macd: float
    stoch: float
    details: Dict[str, float]


class ForecastData(BaseModel):
    symbol: str
    historical: List[Dict[str, Any]]  # Last 30 days
    forecast: List[Dict[str, Any]]  # Next 30 days


class SentimentOnChainData(BaseModel):
    symbol: str
    final_score: float
    signal: str
    sentiment_score: float
    onchain_score: float
    metrics: Dict[str, Any]
    whale_alerts: List[Dict[str, Any]]


# --- API Endpoints ---

@app.get("/health")
def health():
    return {"status": "up"}


@app.get("/api/technical/{symbol}", response_model=List[TechnicalData])
def get_technical_analysis(symbol: str):
    """
    Fetches the latest technical indicators and signals from the DB.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Fetch last 5 records to show trend
        query = f"""
            SELECT symbol, date, signal, rsi, macd, stoch, 
                   sma20, ema20, bb_high, bb_low 
            FROM {technical_analysis.TARGET_TABLE}
            WHERE symbol = ? AND timeframe = '1D'
            ORDER BY date DESC LIMIT 5
        """
        df = pd.read_sql_query(query, conn, params=(symbol,))
        conn.close()

        if df.empty:
            # Optional: trigger analysis if missing
            return []

        results = []
        for _, row in df.iterrows():
            results.append({
                "symbol": row['symbol'],
                "date": row['date'],
                "signal": row['signal'],
                "rsi": row['rsi'],
                "macd": row['macd'],
                "stoch": row['stoch'],
                "details": {
                    "sma20": row['sma20'],
                    "ema20": row['ema20'],
                    "bb_high": row['bb_high'],
                    "bb_low": row['bb_low']
                }
            })
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/forecast/{symbol}", response_model=ForecastData)
def get_forecast(symbol: str):
    """
    Returns LSTM historical context (last 30 days) and future predictions (next 30 days).
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # 1. Get Future Predictions
        future_sql = "SELECT date, predicted_close FROM predictions WHERE symbol = ? AND date > date('now') ORDER BY date ASC"
        future_df = pd.read_sql_query(future_sql, conn, params=(symbol,))

        # 2. Get Recent History (Context)
        hist_sql = "SELECT date, predicted_close FROM predictions WHERE symbol = ? AND date <= date('now') ORDER BY date DESC LIMIT 30"
        hist_df = pd.read_sql_query(hist_sql, conn, params=(symbol,))
        conn.close()

        return {
            "symbol": symbol,
            "historical": hist_df.iloc[::-1].to_dict(orient="records"),  # Reverse to chronological
            "forecast": future_df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment-onchain/{symbol}", response_model=SentimentOnChainData)
def get_sentiment_onchain(symbol: str, keyword: str = Query(..., description="e.g. Bitcoin")):
    """
    Real-time aggregation of On-Chain Metrics + Sentiment Analysis.
    """
    try:
        # Use existing logic from main.py
        # gather_all_data returns (metrics_dict, tuple_of_vars)
        metrics_dict, metrics_vars = gather_all_data(symbol, keyword)

        # Extract sentiment sum (last item in the tuple based on main.py)
        sentiment_sum = metrics_vars[-1]

        # Calculate scores
        result = combination(metrics_dict, sentiment_sum)

        # Fetch Whale Alerts (Real-time limit 5)
        whales = get_whale_movements(limit=5)

        return {
            "symbol": symbol,
            "final_score": result['final_score'],
            "signal": result['signal'],
            "onchain_score": result['onchain_score'],
            "sentiment_score": result['sentiment_score'],
            "metrics": result['scaled_metrics'],  # or metrics_dict for raw values
            "whale_alerts": whales
        }
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Analysis failed. Ensure DB is populated and APIs are reachable.")


@app.post("/admin/refresh-pipeline")
def run_pipeline_manually():
    """
    Triggers the heavy calculation tasks (Technical Analysis + LSTM Training).
    Call this via a Scheduler in Spring Boot or Cron.
    """
    try:
        # 1. Update Technical Analysis Table
        technical_analysis.main()

        # 2. Update LSTM Predictions
        # Warning: This is slow. In production, run this asynchronously (Celery/BackgroundTasks)
        lstm_attempt.run_pipeline(["BTC-USD", "ETH-USD"], lookbacks=[30], forecast_days=30)

        return {"status": "Pipeline execution started/completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)