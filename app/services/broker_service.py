import importlib
from typing import List, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session
from unittest.mock import MagicMock
from app.core.config import Settings
from app.schemas import historical_data as historical_data_schema, feed as feed_schema
from shared_architecture.db.models import tick_data as tick_data_model
from app.services import rabbitmq_service
from shared_architecture.db.models import broker as broker_model
from shared_architecture.utils.logging_utils import log_info, log_exception
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from shared_architecture.connections import get_timescaledb_session



broker_instance = None  # Global broker instance

async def get_broker_details(db: AsyncSession, settings: Settings) -> List[broker_model.Broker]: 
    """ Retrieves broker details from TimescaleDB based on broker_name and username. """
    
    SessionLocal = get_timescaledb_session()

    stmt = select(broker_model.Broker).where(
        broker_model.Broker.broker_name == settings.BROKER_NAME,
        broker_model.Broker.username == settings.USER_NAME
    )

    async with SessionLocal() as db1:
        result = await db1.execute(stmt)
        brokers = result.scalars().all()

        if not brokers:
            raise HTTPException(status_code=404, detail="Broker details not found")

        return brokers

async def get_broker_module(broker_name: str):
    try:
        if isinstance(broker_name, MagicMock):
            raise RuntimeError("Cannot import module from mock broker name.")
        module = importlib.import_module(f"app.brokers.{broker_name}")
        return module
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Broker module import failed: {e}")

async def initialize_broker(broker: broker_model.Broker):
    """
    Initializes the broker module and creates an instance.
    """
    global broker_instance
    try:
        broker_module = await get_broker_module(broker.broker_name)
        broker_instance = broker_module.Broker(broker)
        log_info(f"Broker {broker.broker_name} initialized.")
        return broker_instance
    except ImportError as e:
        log_exception(f"Failed to import broker module {broker.broker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Broker module import failed: {e}")
    except Exception as e:
        log_exception(f"Failed to initialize broker {broker.broker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Broker initialization failed: {e}")

async def shutdown_broker():
    """
    Shuts down the broker connection.
    """
    global broker_instance
    if broker_instance:
        try:
            await broker_instance.disconnect()
            log_info("Broker connection closed.")
        except Exception as e:
            log_exception(f"Failed to disconnect broker: {e}")

async def fetch_historical_data(
    broker: broker_model.Broker,
    data_request: historical_data_schema.HistoricalDataRequest
) -> List[Dict[str, Any]]:
    """
    Fetches historical data from the broker API.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    return await broker_instance.get_historical_data(data_request)

async def subscribe_to_symbol(
    broker: broker_model.Broker,
    instrument_key: str,
    broker_token: str,
    interval: str = "1second",
    get_market_depth: bool = False,
    get_exchange_quotes: bool = True
):
    """
    Subscribes to a symbol for real-time feeds.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    await broker_instance.subscribe(
        instrument_key, broker_token, interval, get_market_depth, get_exchange_quotes
    )

async def unsubscribe_from_symbol(
    broker: broker_model.Broker,
    instrument_key: str,
    broker_token: str,
    interval: str = "1second"
):
    """
    Unsubscribes from a symbol.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    await broker_instance.unsubscribe(instrument_key, broker_token, interval)

async def process_realtime_feed(
    db: Session,
    broker: broker_model.Broker,
    feed_data: Dict[str, Any]
):
    """
    Processes real-time feed data, stores in TimescaleDB and publishes to RabbitMQ.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    try:
        processed_feed_data = await broker_instance.process_feed(feed_data)
        feed = feed_schema.FeedCreate(**processed_feed_data)

        tick_data_record = tick_data_model.TickData(
            time=processed_feed_data.get("timestamp"),
            instrument_key=feed.instrument_key,
            interval="1T",
            open=feed.open,
            high=feed.high,
            low=feed.low,
            close=feed.close,
            volume=feed.volume,
            oi=processed_feed_data.get("OI"),
            expirydate=processed_feed_data.get("expiry_date"),
            option_type=processed_feed_data.get("right"),
            strikeprice=processed_feed_data.get("strike_price"),
            greeks_open_iv=None,
            greeks_open_delta=None,
            greeks_open_gamma=None,
            greeks_open_theta=None,
            greeks_open_rho=None,
            greeks_open_vega=None,
            greeks_high_iv=None,
            greeks_high_delta=None,
            greeks_high_gamma=None,
            greeks_high_theta=None,
            greeks_high_rho=None,
            greeks_high_vega=None,
            greeks_low_iv=None,
            greeks_low_delta=None,
            greeks_low_gamma=None,
            greeks_low_theta=None,
            greeks_low_rho=None,
            greeks_low_vega=None,
            greeks_close_iv=None,
            greeks_close_delta=None,
            greeks_close_gamma=None,
            greeks_close_theta=None,
            greeks_close_rho=None,
            greeks_close_vega=None,
        )

        db.add(tick_data_record)
        db.commit()
        db.refresh(tick_data_record)

        await rabbitmq_service.publish_tick_data(feed.dict())

    except Exception as e:
        log_exception(f"Error processing real-time feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
