import asyncio
import json
import logging
import aio_pika
from core.config import Settings
from fastapi import FastAPI,Depends
from sqlalchemy.orm import Session
from core.dependencies import get_app
from typing import List,Dict,Any

async def publish_tick_data(feed_data: dict):
    """
    Publishes tick data to RabbitMQ with routing based on instrument_key.

    Args:
        feed_data (dict): The tick data dictionary.
    """
    try:
        app=get_app()
        connection: aio_pika.Connection = await aio_pika.connect_robust(app.state.settings.rabbitmq_url)  # Adjust URL
        # Establish an asynchronous connection to RabbitMQ
        #connection=app.state.connections
        #aio_pika.Connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)  # Adjust URL

        async with connection:
            # Create an asynchronous channel
            channel: aio_pika.Channel = await connection.channel()

            # Declare the exchange (ensure this matches your RabbitMQ setup)
            exchange_name = "tick_exchange"  # Replace with your exchange name
            exchange: aio_pika.Exchange = await channel.declare_exchange(
                exchange_name, aio_pika.ExchangeType.DIRECT
            )

            # Extract routing key (instrument_key)
            routing_key = feed_data.get("instrument_key")
            if not routing_key:
                logging.error("Missing instrument_key in feed data. Cannot route message.")
                return

            message = aio_pika.Message(
                body=json.dumps(feed_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT  # Ensure messages survive restarts
            )

            # Publish the message
            await exchange.publish(message, routing_key=routing_key)
            logging.info(f"Published tick data for {routing_key} to RabbitMQ")

    except aio_pika.exceptions.AMQPConnectionError as e:
        logging.error(f"RabbitMQ connection error: {e}")
        # Handle connection errors (e.g., retry, raise exception)
    except Exception as e:
        logging.error(f"Error publishing to RabbitMQ: {e}")
        # Handle other exceptions
        # Handle other exceptions
async def publish_tick_batch(tick_batch: List[Dict[str, Any]]):
    """
    Publishes a batch of tick data to RabbitMQ in bulk.
    """
    try:
        app = get_app()  # Retrieve the FastAPI app instance
        connection: aio_pika.Connection = await aio_pika.connect_robust(app.state.settings.rabbitmq_url)

        async with connection:
            # Create an asynchronous channel
            channel: aio_pika.Channel = await connection.channel()

            # Declare the exchange (ensure it matches your RabbitMQ setup)
            exchange_name = "tick_exchange"
            exchange: aio_pika.Exchange = await channel.declare_exchange(
                exchange_name, aio_pika.ExchangeType.DIRECT
            )

            # Prepare the messages for batch publishing
            messages = []
            for tick_data in tick_batch:
                routing_key = tick_data.get("instrument_key")
                if not routing_key:
                    logging.error("Missing instrument_key in tick data. Cannot route message.")
                    continue

                message = aio_pika.Message(
                    body=json.dumps(tick_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                )
                messages.append((message, routing_key))

            # Publish messages in bulk
            for message, routing_key in messages:
                await exchange.publish(message, routing_key=routing_key)

            logging.info(f"Published {len(messages)} tick data entries to RabbitMQ")

    except aio_pika.exceptions.AMQPConnectionError as e:
        logging.error(f"RabbitMQ connection error: {e}")
    except Exception as e:
        logging.error(f"Error publishing tick batch to RabbitMQ: {e}")
# async def process_tick_queue(db: Session, broker_instance):
#     """
#     Consumes tick data from the broker's queue, processes it, and stores it.
#     """
#     try:
#         while True:
#             feed_data = await broker_instance.tick_queue.get()
#             # Process the feed data (e.g., moneyness calculation)
#             # ... your processing logic ...

#             # Store in TimescaleDB (asyncpg)
#             await timescaledb_service.create_tick_data(db, feed_data)

#             # Publish to RabbitMQ (if needed)
#             await publish_tick_data(feed_data)

#             broker_instance.tick_queue.task_done()

#     except Exception as e:
#         logging.error(f"Error processing tick queue: {e}")


# async def start_tick_processing(app: FastAPI, db: Session, broker_instance):
#     """
#     Starts the tick data processing task.
#     """
#     asyncio.create_task(process_tick_queue(app.state.broker_instance, db))