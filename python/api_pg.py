import os
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, Column, String, Float, JSON, Integer, desc
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import OperationalError
from datetime import date

from fastapi.middleware.cors import CORSMiddleware

from technical_analysis.technical_analysis_pg import main as run_technical_analysis_pipeline
from lstm.lstm_pg import run_pipeline as run_lstm_pipeline
from on_chain.onchain_dashboard import get_all_metrics, get_whale_movements, exchange_flows
from main import gather_all_data, combination



app = FastAPI(title="Crypto Analytics Microservice")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuration for PostgreSQL ---
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", 5432))
PG_DB = os.environ.get("PG_DB", "crypto")
PG_USER = os.environ.get("PG_USER", "crypto_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "crypto_pass")

# SQLAlchemy connection string
DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    TECHNICAL_ANALYSIS_TABLE = "technical_analysis"
    PREDICTIONS_TABLE = "predictions"
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
except OperationalError as e:
    print(f"FATAL: Could not connect to PostgreSQL: {e}")
    engine = None


class Prediction(Base):
    __tablename__ = "predictions"

    symbol = Column(String, primary_key=True, nullable=False)
    date = Column(String, primary_key=True, nullable=False)
    predicted_close = Column(Float, nullable=False)



class TechnicalAnalysis(Base):
    __tablename__ = "technical_analysis"

    symbol = Column(String, primary_key=True, nullable=False)
    timeframe = Column(String, primary_key=True, nullable=False)  # Combined primary key
    rsi = Column(Float)
    macd = Column(Float)
    stoch = Column(Float)
    adx = Column(Float)
    cci = Column(Float)
    sma20 = Column(Float)
    ema20 = Column(Float)
    wma20 = Column(Float)
    bb_high = Column(Float)
    bb_low = Column(Float)
    vol_sma20 = Column(Float)
    signal = Column(String)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class SentimentOnChainData(BaseModel):
    symbol: str
    final_score: float
    signal: str
    sentiment_score: float
    onchain_score: float
    metrics: Dict[str, Any]
    whale_alerts: List[Dict[str, Any]]


class AllOnChainMetrics(BaseModel):
    asset: str = Field(alias="Asset")
    active_addresses: float = Field(alias="Active Addresses")
    transactions: float = Field(alias="Transactions")
    hash_rate: float = Field(alias="Hash Rate")
    mvrv: float = Field(alias="MVRV")
    tvl: float = Field(alias="TVL")
    nvt: float = Field(alias="NVT")
    coingecko_id: str = Field(alias="CoinGecko ID")
    exchange_inflow: float = Field(alias="Exchange Inflow (USD)")
    exchange_outflow: float = Field(alias="Exchange Outflow (USD)")
    exchange_net_flow: float = Field(alias="Exchange Net Flow (USD)")

    class Config:
        populate_by_name = True


class WhaleMovement(BaseModel):
    date: str
    hash: str
    symbol: str
    type: str
    amount: float
    amount_usd: float
    text: str
    amounts: List[Dict[str, Any]]


class WhaleReport(BaseModel):
    total_alerts: int
    alerts: List[WhaleMovement]


class PredictionData(BaseModel):
    symbol: str
    date: date
    predicted_close: float


class TechnicalAnalysisData(BaseModel):
    symbol: str
    timeframe: str
    rsi: Optional[float]
    macd: Optional[float]
    stoch: Optional[float]
    adx: Optional[float]
    cci: Optional[float]
    sma20: Optional[float]
    ema20: Optional[float]
    wma20: Optional[float]
    bb_high: Optional[float]
    bb_low: Optional[float]
    vol_sma20: Optional[float]
    signal: Optional[str]


# --- API Endpoints ---

@app.get("/health")
def health():
    return {"status": "up", "db_connected": engine is not None}

@app.get("/api/forecast/{symbol}", response_model=List[PredictionData])
def get_forecast(symbol: str, db: Session = Depends(get_db)):
    """
    Returns all predictions for a given symbol from the 'predictions' table.
    """
    symbol = symbol.upper().strip()
    try:
        results = db.query(Prediction).filter(Prediction.symbol == symbol).order_by(Prediction.date).all()
        if not results:
            raise HTTPException(status_code=404, detail=f"No predictions found for symbol: {symbol}")
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch predictions: {e}")


@app.get("/api/technical/{symbol}", response_model=List[TechnicalAnalysisData])
def get_technical_analysis(symbol: str, db: Session = Depends(get_db)):
    """
    Fetches the latest technical indicators and signals from the DB
    for 1D, 1W, and 1M timeframes.
    """
    symbol = symbol.upper().strip()
    try:
        results = db.query(TechnicalAnalysis).filter(TechnicalAnalysis.symbol == symbol).all()
        if not results:
            raise HTTPException(status_code=404, detail=f"No technical data found for symbol: {symbol}")
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch technical analysis: {e}")

@app.get("/api/sentiment-onchain/{symbol}", response_model=SentimentOnChainData)
def get_sentiment_onchain(symbol: str, keyword: str = Query(..., description="e.g. Bitcoin")):
    """
    Real-time aggregation of On-Chain Metrics + Sentiment Analysis.
    """
    try:
        metrics_dict, metrics_vars = gather_all_data(symbol, keyword)
        sentiment_sum = metrics_vars[-1]
        result = combination(metrics_dict, sentiment_sum)
        whales = get_whale_movements(limit=5)

        return {
            "symbol": symbol,
            "final_score": result['final_score'],
            "signal": result['signal'],
            "onchain_score": result['onchain_score'],
            "sentiment_score": result['sentiment_score'],
            "metrics": result['scaled_metrics'],
            "whale_alerts": whales
        }
    except Exception as e:
        print(f"Error in /api/sentiment-onchain: {e}")
        raise HTTPException(status_code=500,
                            detail="Analysis failed. Ensure DB is populated and external APIs are reachable.")


@app.post("/admin/refresh-pipeline")
def run_pipeline_manually():
    """
    Triggers the heavy calculation tasks (Technical Analysis + LSTM Training) using the new Postgres modules.
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Database service unavailable.")

    try:
        # 1. Update Technical Analysis Table
        run_technical_analysis_pipeline()

        # 2. Update LSTM Predictions
        run_lstm_pipeline(symbols=["BTC-USD", "ETH-USD"])

        return {"status": "Pipeline execution started/completed (PostgreSQL versions)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


@app.get("/api/onchain-metrics/{symbol}", response_model=AllOnChainMetrics)
def get_aggregated_onchain_metrics(symbol: str):
    try:
        metrics = get_all_metrics(symbol)
        flows = exchange_flows(symbol)
        all_data = {**metrics, **flows}
        return AllOnChainMetrics.model_validate(all_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch on-chain metrics for {symbol}: {e}")


@app.get("/api/whale-reports", response_model=List[Dict[str, Any]])
def get_latest_whale_movements(limit: int = Query(50, description="...")):
    whales_raw = get_whale_movements(limit=limit)
    return whales_raw


if __name__ == "__main__":
    # Optional: Create tables on startup if they don't exist
    # if engine:
    #     Base.metadata.create_all(bind=engine)

    uvicorn.run(app, host="0.0.0.0", port=8000)