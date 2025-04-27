from fastapi import FastAPI,Depends,HTTPException
from sqlalchemy.orm import Session
from shared_architecture.utils.service_helpers import connection_manager
from sqlalchemy.orm import Session
from services import broker_service 
from core.config import Settings
def get_timescaledb_session():
    """
    Yields a TimescaleDB session from the ConnectionManager.
    """
    try:
        db: Session = connection_manager.get_timescaledb_session()
        yield db
    finally:
        if db:
            db.close()

def get_redis_client():
    """
    Yields a Redis client from the ConnectionManager.
    """
    redis_client = connection_manager.get_redis_connection()
    try:
        yield redis_client
    finally:
        if redis_client:
            redis_client.close()

def get_rabbitmq_connection():
    """
    Yields a RabbitMQ connection from the ConnectionManager.
    """
    rabbitmq_conn = connection_manager.get_rabbitmq_connection()
    try:
        yield rabbitmq_conn
    finally:
        if rabbitmq_conn:
            rabbitmq_conn.close()

def get_mongodb_client():
    """
    Yields a MongoDB client from the ConnectionManager.
    """
    mongodb_client = connection_manager.get_mongodb_connection()
    try:
        yield mongodb_client
    finally:
        if mongodb_client:
            mongodb_client.close()



def get_settings():
    return Settings()

def get_broker_instance(app: FastAPI):
    """
    Dependency to provide the initialized broker instance.
    """
    if hasattr(app.state, "broker_instance"):
        return app.state.broker_instance
    else:
        raise HTTPException(status_code=500, detail="Broker instance not initialized in app.state")
    
def get_app():
    """
    Dependency to provide the FastAPI application instance.
    """
    from main import app  # Import app from main.py
    return app