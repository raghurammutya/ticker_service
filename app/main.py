import logging
import os
import sys
from fastapi import FastAPI, Request
from typing import Dict, Any
import asyncio
from shared_architecture.config.config_manager import ConfigManager
from sqlalchemy.orm import Session

print("BROKER_NAME in Environment:", os.getenv("BROKER_NAME"))
print("USER_NAME in Environment:", os.getenv("USER_NAME"))
# --- Project Setup ---
# Get the absolute path to the outermost service directory
# Get the absolute path of the current file
# current_file_path = os.path.abspath(__file__)

# # Get the directory containing the current file
# current_directory = os.path.dirname(current_file_path)

# # Move up one level to set the project root
# project_root = os.path.dirname(current_directory)

# print(sys.path)

# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
#     print(f"Added project root to sys.path: {project_root}")  # Debugging
os.chdir('/')
print("Current Directory:", os.getcwd())

# --- Connection Initialization ---
async def initialize_connections(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Initialize shared connections (e.g., TimescaleDB, Redis, RabbitMQ).

    Args:
        config (Dict[str, Any]): The shared configuration.

    Returns:
        Dict[str, Any]: A dictionary of initialized connections.
    """
    from shared_architecture.utils.service_helpers import initialize_service
    from services.redis_service import RedisService

    try:
        logging.info("Initializing shared connections...")
        await initialize_service(
            service_name=config.get("PROJECT_NAME", "ticker_service"),
            config=config
        )
        from shared_architecture.utils.service_helpers import connection_manager

        connections = {
            "timescaledb": connection_manager.get_timescaledb_session(),
            "redis": await connection_manager.get_redis_connection(),
            "rabbitmq": connection_manager.get_rabbitmq_connection(),
            # "mongodb": connection_manager.get_mongodb_connection(),
        }

        for name, conn in connections.items():
            if not conn:
                raise RuntimeError(f"Failed to establish {name} connection")

        app.state.connections = connections
        app.state.redis_service = RedisService(
            redis=app.state.connections.get("redis")  # Pass the Redis connection
        )
        logging.info("All shared connections successfully initialized.")
        return connections
    except Exception as e:
        logging.error(f"Failed to initialize connections: {e}")
        raise

# --- Microservice-Specific Startup ---
async def initialize_microservice(app: FastAPI):
    """
    Service-specific startup logic (e.g., brokers, migrations, symbols).

    Args:
        app (FastAPI): The FastAPI application instance.
        connections (Dict[str, Any]): Initialized shared connections.
        config (Dict[str, Any]): The microservice-specific configuration.
    """
    from shared_architecture.db.migrations import apply_migrations
    from services import broker_service,symbol_service
    from services import rabbitmq_service, timescaledb_service
    from services.tick_service import process_tick_queue
    try:
        logging.info("Initializing microservice-specific components...")
        broker = await broker_service.get_broker_details(
            app.state.connections["timescaledb"], settings
        )
        app.state.broker_instance = await broker_service.initialize_broker(broker[0])
        await app.state.broker_instance._initialize_connection()
        await symbol_service.refresh_symbols(app.state.connections["timescaledb"], app)

        logging.info("Microservice-specific components initialized successfully.")
        apply_migrations()
        logging.info("Microservice-specific startup tasks completed.")
    except Exception as e:
        logging.error(f"Microservice initialization failed: {e}")
        raise

# --- FastAPI Application Factory ---
def create_app(config: Dict[str, Any]) -> FastAPI:
    """
    Creates and configures the FastAPI application instance.

    Args:
        config (Dict[str, Any]): The service-specific configuration.

    Returns:
        FastAPI: The configured FastAPI application.
    """
    global settings
    from core.config import Settings  # Delayed import
    settings = Settings(**config.config)
    app = FastAPI(
        title=settings.PROJECT_NAME,
        openapi_url=f"{settings.API_V1_STR}/openapi.json"
    )
    app.state.settings = settings
    return app

# --- Application Instance ---
service_name = "ticker_service"
config_manager = ConfigManager(service_name=service_name)
from core.config import Settings
settings = Settings(**config_manager.config)
app = create_app(config_manager)

# Include API Routes
from api.api_v1 import api_router
api_prefix = config_manager.config.get("API_V1_STR", "/api/v1")
app.include_router(api_router, prefix=api_prefix)

# --- Startup Event ---
connections = None

@app.on_event("startup")
async def startup_event():
    """
    Handles application startup: shared connections and microservice-specific initialization.
    """
    try:
        global connections
        # Initialize shared connections
        connections = await initialize_connections(config_manager.config)
        # Initialize microservice-specific components
        await initialize_microservice(app)

        # Initialize RedisService
        redis_connection = app.state.connections.get("redis")
        if not redis_connection:
            raise RuntimeError("Redis connection is missing or not initialized.")
        from services.redis_service import RedisService
        app.state.redis_service = RedisService(redis=redis_connection)

        # Start background tasks for batch processing and failure queue handling
        asyncio.create_task(app.state.redis_service.run_batch_processor())
        asyncio.create_task(app.state.redis_service.process_failure_queue())

        logging.info("RedisService initialized and background tasks started.")

    except Exception as e:
        logging.error(f"Startup failed: {e}")
        raise

        logging.info("Application startup complete.")
    except Exception as e:
        logging.error(f"Application startup failed: {e}")
        raise

# --- Shutdown Event ---
@app.on_event("shutdown")
async def shutdown_event():
    """
    Handles application shutdown: closing shared connections.
    """
    from shared.utils.service_helpers import connection_manager
    app.state.broker_instance.disconnect()
    connection_manager.close_all()
    logging.info("Application shutdown complete.")

# --- Health Check ---
@app.get("/health")
async def health_check():
    """
    Combined health check endpoint for Redis, async jobs, and broker instance.

    Returns:
        dict: Health status including Redis, async jobs, and broker instance.
    """
    # Default broker status
    broker_status = "unhealthy"

    # Check broker instance health
    try:
        broker_instance = app.state.broker_instance
        if broker_instance and broker_instance.broker_session:  # Assuming the broker has an `is_connected()` method
            broker_status = "healthy"
    except Exception as e:
        logging.error(f"Broker health check failed: {e}")
        broker_status = "unhealthy"

    # Check Redis and async job statuses
    redis_health = await app.state.redis_service.health_check()

    # Return combined health status
    return {
        "redis_status": redis_health["status"],
        "batch_processor_status": redis_health["batch_processor_status"],
        "failure_queue_status": redis_health["failure_queue_status"],
        "broker_status": broker_status
    }

@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(Request)
    response = await call_next(request)
    return response

# --- Main Execution ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",  # Run from app/main.py
        host="0.0.0.0",
        port=8000,
        reload=True
    )