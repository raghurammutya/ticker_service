import os
import importlib
import datetime
import logging
import psycopg2  # Third-party library, but often closely related to DB

from typing import List, Dict, Any,Callable

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from core.config import Settings
from schemas import historical_data as historical_data_schema, feed as feed_schema
from models import tick_data as tick_data_model
from services import rabbitmq_service
from shared_architecture.db.models import broker as broker_model

broker_instance = None  # Global variable to store the broker instance
async def get_broker_details(db: Session, settings: Settings) -> broker_model.Broker:
    """
    Retrieves broker details from TimescaleDB based on broker_name and username.
    """
#try:

    broker = db.query(broker_model.Broker).filter(
         broker_model.Broker.broker_name == settings.BROKER_NAME,
         broker_model.Broker.username == settings.USER_NAME
     ).all()  # Explicit schema in query
    if not broker:
         raise HTTPException(status_code=404, detail="Broker details not found")
    return broker
async def get_broker_module(broker_name: str):
    """
    Dynamically imports the broker-specific module.
    """
    try:
        module_name = f"brokers.{broker_name.lower()}"  # Assuming broker modules are in app/brokers/
        broker_module = importlib.import_module(module_name)
        return broker_module
    except ImportError as e:
        logging.error(f"Failed to import broker module {broker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Broker module import failed: {e}")

async def initialize_broker(broker: broker_model.Broker):
    """
    Initializes the broker module and creates an instance.
    """
    global broker_instance
    try:
        broker_module = await get_broker_module(broker.broker_name)
        broker_instance = broker_module.Broker(broker)
        logging.info(f"Broker {broker.broker_name} initialized.")
        return broker_instance
    except ImportError as e:
        logging.error(f"Failed to import broker module {broker.broker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Broker module import failed: {e}")
    except Exception as e:
        logging.error(f"Failed to initialize broker {broker.broker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Broker initialization failed: {e}")

async def shutdown_broker():
    """
    Shuts down the broker connection.
    """
    global broker_instance
    if broker_instance:
        try:
            await broker_instance.disconnect()
            logging.info("Broker connection closed.")
        except Exception as e:
            logging.error(f"Failed to disconnect broker: {e}")

async def fetch_historical_data(broker: broker_model.Broker, data_request: historical_data_schema.HistoricalDataRequest) -> List[Dict[str, Any]]:
    """
    Fetches historical data from the broker API.
    Returns a list of dictionaries.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    fetched_data = await broker_instance.get_historical_data(data_request)
    return fetched_data

async def subscribe_to_symbol(broker: broker_model.Broker, instrument_key: str, broker_token: str, interval: str = '1second', get_market_depth: bool = False, get_exchange_quotes: bool = True):
    """
    Subscribes to a symbol for real-time feeds.
    Now takes broker_token and interval as input.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    await broker_instance.subscribe(instrument_key, broker_token, interval, get_market_depth, get_exchange_quotes)

async def unsubscribe_from_symbol(broker: broker_model.Broker, instrument_key: str, broker_token: str, interval: str = '1second'):
    """
    Unsubscribes from a symbol.
    Now takes broker_token and interval as input.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    await broker_instance.unsubscribe(instrument_key, broker_token, interval)

async def process_realtime_feed(db: Session, broker: broker_model.Broker, feed_data: Dict[str, Any]):
    """
    Processes real-time feed data, stores it in TickData, and publishes to RabbitMQ.
    """
    global broker_instance
    if not broker_instance:
        raise HTTPException(status_code=500, detail="Broker not initialized")
    try:
        # Call broker module to process the feed data
        processed_feed_data = await broker_instance.process_feed(feed_data)

        # Convert the processed data to TickData model
        feed = feed_schema.FeedCreate(**processed_feed_data)

        # Map the feed data to the TickData model
        tick_data_record = tick_data_model.TickData(
            time=processed_feed_data.get("timestamp"),  # Use timestamp from processed data
            instrument_key=feed.instrument_key,
            interval="1T",  # Default. replace if needed.
            open=feed.open,
            high=feed.high,
            low=feed.low,
            close=feed.close,
            volume=feed.volume,
            oi=processed_feed_data.get("OI"),  # Map OI
            expirydate=processed_feed_data.get("expiry_date"),  # Map expiry_date
            option_type=processed_feed_data.get("right"),  # Map option_type
            strikeprice=processed_feed_data.get("strike_price"),  # Map strike_price
            greeks_open_iv=None,  # Not available in sample
            greeks_open_delta=None,  # Not available in sample
            greeks_open_gamma=None,  # Not available in sample
            greeks_open_theta=None,  # Not available in sample
            greeks_open_rho=None,  # Not available in sample
            greeks_open_vega=None,  # Not available in sample
            greeks_high_iv=None,  # Not available in sample
            greeks_high_delta=None,  # Not available in sample
            greeks_high_gamma=None,  # Not available in sample
            greeks_high_theta=None,  # Not available in sample
            greeks_high_rho=None,  # Not available in sample
            greeks_high_vega=None,  # Not available in sample
            greeks_low_iv=None,  # Not available in sample
            greeks_low_delta=None,  # Not available in sample
            greeks_low_gamma=None,  # Not available in sample
            greeks_low_theta=None,  # Not available in sample
            greeks_low_rho=None,  # Not available in sample
            greeks_low_vega=None,  # Not available in sample
            greeks_close_iv=None,  # Not available in sample
            greeks_close_delta=None,  # Not available in sample
            greeks_close_gamma=None,  # Not available in sample
            greeks_close_theta=None,  # Not available in sample
            greeks_close_rho=None,  # Not available in sample
            greeks_close_vega=None,  # Not available in sample
        )

        db.add(tick_data_record)
        db.commit()
        db.refresh(tick_data_record)

        # Publish to RabbitMQ
        # Ensure that feed.dict() is used here, not processed_feed_data
        await rabbitmq_service.publish_tick_data(feed.dict())

    except Exception as e:
        logging.error(f"Error processing real-time feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))