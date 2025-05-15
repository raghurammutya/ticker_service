import os
import sys
import asyncio
from pathlib import Path

from fastapi import FastAPI, Request
from typing import Dict, Any

from shared_architecture.utils.logging_utils import log_info, log_error, log_warning, log_debug, log_exception
# === Service Path Setup ===
os.chdir(Path(__file__).resolve().parents[1])
print("Service Root:", os.getcwd())

# === Shared Architecture Imports ===
from shared_architecture.config.config_manager import ConfigManager
from shared_architecture.config.config_loader import get_env
from shared_architecture.utils.logging_utils import configure_logging, log_info, log_error
from shared_architecture.connections import get_redis_connection, connect_to_redis, get_timescaledb_session, get_rabbitmq_connection,test_timescaledb_connection
import pytz
from shared_architecture.db.migrations import apply_migrations
from sqlalchemy.types import TypeDecorator, DateTime

# === Microservice Imports ===
from app.services.market_data_manager import MarketDataManager, RedisBatchWriter
from app.services import broker_service, symbol_service
from app.core.config import Settings
from app.api.api_v1 import api_router

# === Logging Setup ===
logger = configure_logging(service_name="ticker_service")
log_info(f"BROKER_NAME in Environment: {get_env('BROKER_NAME', '')}")
log_info(f"USER_NAME in Environment: {get_env('USER_NAME', '')}")

# === FastAPI App Factory ===
def create_app(config: Dict[str, Any]) -> FastAPI:
    global settings
    settings = Settings(**config.config)
    app = FastAPI(
        title=settings.PROJECT_NAME,
        openapi_url=f"{settings.API_V1_STR}/openapi.json"
    )
    app.state.settings = settings
    return app

# === Config and App Initialization ===
service_name = "ticker_service"
config_manager = ConfigManager(service_name=service_name)
settings = Settings(**config_manager.config)
app = create_app(config_manager)
app.include_router(api_router, prefix=config_manager.config.get("API_V1_STR", "/api/v1"))

# === Shared Infrastructure Initialization ===
async def initialize_connections(app: FastAPI, config: Dict[str, Any]) -> Dict[str, Any]:
    try:
        log_info("Initializing shared infrastructure connections...")

        redis_conn = await connect_to_redis()
        tsdb_conn = get_timescaledb_session()
        
        await test_timescaledb_connection(tsdb_conn)
        connections = {
            #"redis": redis_conn,
            "timescaledb": tsdb_conn,
        }

        for name, conn in connections.items():
            if conn is None:
                raise RuntimeError(f"Failed to establish {name} connection")

        app.state.connections = connections
        #app.state.market_data_manager = MarketDataManager(RedisBatchWriter(redis=redis_conn))

        log_info("All infrastructure connections and market data manager initialized.")
        return connections

    except Exception as e:
        log_error(f"[InitConnections] Failed to initialize connections: {e}")
        raise

IST = pytz.timezone("Asia/Kolkata")

class ISTDateTime(TypeDecorator):
    impl = DateTime(timezone=True)

    def process_bind_param(self, value, dialect):
        """Convert datetime to IST before storing in DB."""
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
            return value.astimezone(IST)
        return value

    def process_result_value(self, value, dialect):
        """Convert datetime to IST when fetching from DB."""
        if value:
            return value.astimezone(IST)
        return value

# === Microservice-Specific Startup ===
async def initialize_microservice(app: FastAPI):
    from shared_architecture.db.migrations import apply_migrations
    from app.services import broker_service, symbol_service

    USE_MOCKS = os.getenv("USE_MOCKS", "false").lower() == "true"

    try:
        log_info("Initializing microservice-specific components...")

        if not USE_MOCKS:
            broker = await broker_service.get_broker_details(
                app.state.connections["timescaledb"], app.state.settings
            )

            # Ensure broker has data before accessing index
            if broker:
                app.state.broker_instance = await broker_service.initialize_broker(broker[0])  
            else:
                raise ValueError("No broker details found!")

        SessionLocal = app.state.connections["timescaledb"]
        async with SessionLocal() as session:
            await symbol_service.refresh_symbols(app)

        apply_migrations()
        log_info("Microservice-specific startup tasks completed.")
    except Exception as e:
        log_error(f"Microservice initialization failed: {e}")
        raise

# === Startup ===
@app.on_event("startup")
async def startup_event():
    try:
        await initialize_connections(app, config_manager.config)
        await initialize_microservice(app)

        # Initialize and attach MarketDataManager
        mdm = MarketDataManager()
        #await mdm.initialize()
        app.state.market_data_manager = mdm

        log_info("MarketDataManager initialized. Background tasks started.")
    except Exception as e:
        log_error(f"Startup failed: {e}")
        raise

# === Shutdown ===
@app.on_event("shutdown")
async def shutdown_event():
    app.state.broker_instance.disconnect()
    log_info("Application shutdown complete.")

# === Health Check ===
@app.get("/health")
async def health_check():
    broker_status = "unhealthy"
    try:
        broker_instance = app.state.broker_instance
        if broker_instance and broker_instance.broker_session:
            broker_status = "healthy"
    except Exception as e:
        log_error(f"Broker health check failed: {e}")

    redis_health = await app.state.market_data_manager.health_check()
    return {
        "redis_status": redis_health["status"],
        "batch_processor_status": redis_health["batch_processor_status"],
        "failure_queue_status": redis_health["failure_queue_status"],
        "broker_status": broker_status
    }

# === Request Logging Middleware ===
@app.middleware("http")
async def log_requests(request: Request, call_next):
    response = await call_next(request)
    return response

# === Main Runner ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
