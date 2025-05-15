import os
from fastapi import APIRouter, Depends, HTTPException,Request
from sqlalchemy.orm import Session
from app.schemas.historical_data import HistoricalDataRequest,HistoricalDataCreate
from app.services import broker_service, timescaledb_service
from shared_architecture.config.config_loader import get_env

from app.services import broker_service, symbol_service
import logging

from shared_architecture.db.models.broker import Broker
from shared_architecture.db.session import get_db
from typing import List
import datetime
import asyncio


router = APIRouter()

def get_broker(request: Request) -> str:
    """
    Retrieve broker name from environment variables.
    """
    return env_config("DB_NAME", default="")
async def check_api_rate_limit(db: Session, broker):
    """
    Checks if the API rate limit has been exceeded.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    if broker.minute_api_limit is not None:
        if broker.minute_api_requests >= broker.minute_api_limit and (now - broker.last_api_call_time) < datetime.timedelta(minutes=1):
            raise HTTPException(status_code=429, detail="Minute API limit exceeded")

    if broker.daily_api_limit is not None:
        if (now.date() - broker.last_api_call_time.date()).days >= 1:
            broker.minute_api_requests = 0

    broker.minute_api_requests += 1
    broker.last_api_call_time = now
    db.commit()


@router.post("/fetch/", response_model=List[HistoricalDataCreate])
async def fetch_historical_data(
    request: Request, 
    data_request: HistoricalDataRequest,
):
    """
    Endpoint to trigger fetching historical data from the broker.
    """
    try:
        # Retrieve Breeze instance from FastAPI state
        breeze_instance = request.app.state.broker_instance

        # Fetch historical data (converted DataFrame -> dict)
        fetched_data = breeze_instance.get_historical_data(data_request)

        from shared_architecture.utils.service_helpers import connection_manager
        db=connection_manager.get_timescaledb_session()
        # Batch insert data into TimescaleDB
        stored_data = await timescaledb_service.batch_upsert_historical_data(db,fetched_data)

        return fetched_data
    except HTTPException as e:
        raise e
    except Exception as e:
        log_exception(f"Error fetching historical data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    



@router.post("/", response_model=List[HistoricalDataRequest], status_code=200)
async def create_historical_data(
    data: List[HistoricalDataCreate],
    broker_name: str = Depends(get_broker),
):
    """
    Endpoint to receive and store historical data.
    """
    try:
        stored_data = []
        for item in data:
            stored_data.append(await timescaledb_service.upsert_historical_data(db, item))
        return stored_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{instrument_key}", response_model=List[HistoricalDataRequest])
async def get_historical_data(instrument_key: str, db: Session = Depends(get_db)):
    """
    Endpoint to retrieve historical data for a specific instrument.
    """
    data = await timescaledb_service.get_historical_data_by_instrument_key(db, instrument_key)
    if not data:
        raise HTTPException(status_code=404, detail="Historical data not found")
    return data


