from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from app.services import broker_service 
from app.core.config import Settings

# ✅ Updated imports from new shared_architecture API
from shared_architecture.connections import (
    get_timescaledb_session,
    get_redis_connection,
    get_rabbitmq_connection,
    get_mongo_connection
)

# ✅ Dependency that yields a TimescaleDB session
def get_timescaledb_session_dep():
    """
    Yields a TimescaleDB session.
    """
    db: Session = get_timescaledb_session()
    try:
        yield db
    finally:
        db.close()


# ✅ Dependency that yields a Redis client (async)
async def get_redis_client():
    """
    Yields an async Redis client.
    """
    redis = await get_redis_connection()
    try:
        yield redis
    finally:
        if redis:
            await redis.close()


# ✅ Dependency that yields a RabbitMQ channel
def get_rabbitmq_connection():
    """
    Yields a RabbitMQ connection/channel.
    """
    rabbitmq = get_rabbitmq_connection()
    try:
        yield rabbitmq
    finally:
        rabbitmq.close()


# ✅ Dependency that yields a MongoDB client
def get_mongodb_client():
    """
    Yields a MongoDB client.
    """
    mongo = get_mongo_connection()
    try:
        yield mongo
    finally:
        mongo.close()


# ✅ FastAPI settings
def get_settings():
    return Settings()


# ✅ Broker instance from app.state
def get_broker_instance(app: FastAPI):
    if hasattr(app.state, "broker_instance"):
        return app.state.broker_instance
    raise HTTPException(status_code=500, detail="Broker instance not initialized in app.state")


# ✅ App reference
def get_app():
    from app.main import app  # Be cautious about circular imports if any
    return app
