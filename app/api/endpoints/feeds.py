import asyncio
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from schemas import feed as feed_schema
from services import broker_service, rabbitmq_service, timescaledb_service
from shared_architecture.db import get_db
from shared_architecture.errors.custom_exceptions import ServiceUnavailableError
from typing import List, Dict, Any
from core.dependencies import get_app
import logging
# from shared_architecture.utils.service_helpers import connection_manager  # Old way
from core.dependencies import get_rabbitmq_connection  # New way

router = APIRouter()

@router.post("/feeds/", response_model=feed_schema.Feed, status_code=201)
async def create_feed(
    feed: feed_schema.FeedCreate,
    db: Session = Depends(get_db),
):
    """
    Endpoint to receive and process real-time feeds.
    """
    try:
        # 1. Validate the feed data (Pydantic schema)
        # 2. Store in TimescaleDB
        db_feed = await timescaledb_service.create_tick_data(db, feed)

        # 3. Publish to RabbitMQ
        await rabbitmq_service.publish_tick_data(feed.dict())  # Ensure it's a dict

        return db_feed
    except Exception as e:
        logging.error(f"Error processing feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except rabbitmq_service.RabbitMQConnectionError as e:  # Example
        raise ServiceUnavailableError(service_name="RabbitMQ", message=str(e))

@router.get("/feeds/", response_model=List[feed_schema.Feed])
async def get_feeds(db: Session = Depends(get_db)):
    """
    Endpoint to retrieve feeds (for debugging/testing).
    """
    feeds = await timescaledb_service.get_all_tick_data(db)
    return feeds

# Add this endpoint to simulate ticks

@router.post("/simulate_ticks/")
async def simulate_ticks(interval: float = 1.0):
    """
    Starts tick simulation via Breeze.
    """
    app = get_app()
    breeze = app.state.broker_instance  # Assuming broker_instance is a Breeze instance
    return await breeze.start_simulation(interval=interval)

@router.post("/stop_simulate_ticks/")
async def stop_simulate_ticks():
    """
    Stops tick simulation via Breeze.
    """
    app = get_app()
    breeze = app.state.broker_instance
    return await breeze.stop_simulation()