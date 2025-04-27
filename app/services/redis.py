import os
import logging
from redis.asyncio import Redis

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Redis Configuration
REDIS_HOST = os.getenv("SHARED_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("SHARED_REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("SHARED_REDIS_DB", 0))
REDIS_MAX_CONNECTIONS = int(os.getenv("SHARED_REDIS_MAX_CONNECTIONS", 10))

async def connect_to_redis():
    """
    Asynchronously connect to Redis and test the connection.

    Returns:
        redis.asyncio.Redis: Async Redis connection object if successful.
    """
    try:
        # Build Redis connection
        redis_client = Redis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            db=REDIS_DB,
            decode_responses=True,  # Return strings instead of bytes
            max_connections=REDIS_MAX_CONNECTIONS,
        )

        # Test the connection
        await redis_client.ping()
        logging.info("Redis async connection successful!")
        return redis_client

    except Exception as e:
        logging.error(f"Error connecting to Redis asynchronously: {e}")
        return None

async def close_redis_connection(redis_client):
    """
    Closes the Redis connection gracefully.

    Args:
        redis_client (redis.asyncio.Redis): The Redis connection to close.
    """
    if redis_client:
        try:
            await redis_client.close()
            logging.info("Redis async connection closed successfully.")
        except Exception as e:
            logging.error(f"Error closing Redis connection: {e}")