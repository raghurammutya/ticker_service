import asyncio
import json
from typing import List, Dict, Any
import aio_pika
from shared_architecture.connections import get_rabbitmq_connection  # ✅ Updated import
from shared_architecture.utils.logging_utils import log_info, log_exception


async def publish_tick_data(feed_data: dict):
    """
    Publishes tick data to RabbitMQ with routing based on instrument_key.
    """
    try:
        # ✅ Get a pre-configured RabbitMQ channel
        channel: aio_pika.Channel = await get_rabbitmq_connection()

        # Declare the exchange
        exchange_name = "tick_exchange"
        exchange: aio_pika.Exchange = await channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.DIRECT
        )

        routing_key = feed_data.get("instrument_key")
        if not routing_key:
            log_exception("Missing instrument_key in feed data. Cannot route message.")
            return

        message = aio_pika.Message(
            body=json.dumps(feed_data).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )

        await exchange.publish(message, routing_key=routing_key)
        log_info(f"Published tick data for {routing_key} to RabbitMQ")

    except aio_pika.exceptions.AMQPConnectionError as e:
        log_exception(f"RabbitMQ connection error: {e}")
    except Exception as e:
        log_exception(f"Error publishing to RabbitMQ: {e}")


async def publish_tick_batch(tick_batch: List[Dict[str, Any]]):
    """
    Publishes a batch of tick data to RabbitMQ in bulk.
    """
    try:
        channel: aio_pika.Channel = await get_rabbitmq_channel()

        exchange_name = "tick_exchange"
        exchange: aio_pika.Exchange = await channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.DIRECT
        )

        messages = []
        for tick_data in tick_batch:
            routing_key = tick_data.get("instrument_key")
            if not routing_key:
                log_exception("Missing instrument_key in tick data. Cannot route message.")
                continue

            message = aio_pika.Message(
                body=json.dumps(tick_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            messages.append((message, routing_key))

        for message, routing_key in messages:
            await exchange.publish(message, routing_key=routing_key)

        log_info(f"Published {len(messages)} tick data entries to RabbitMQ")

    except aio_pika.exceptions.AMQPConnectionError as e:
        log_exception(f"RabbitMQ connection error: {e}")
    except Exception as e:
        log_exception(f"Error publishing tick batch to RabbitMQ: {e}")
