# tick_service.py
import asyncio
import logging
from typing import List, Dict, Any

from app.services import timescaledb_service, rabbitmq_service
from shared_architecture.db import get_db
from app.core.dependencies import get_app
from sqlalchemy.orm import Session
from fastapi import FastAPI

class TickProcessor:
    def __init__(self):
        self.tick_queue = asyncio.Queue()  # Shared queue for all brokers
    def start_processing(self, app: FastAPI):
        """
        Start the tick processing queue in a background task.
        """
        log_info("Starting tick processing...")
        asyncio.create_task(process_tick_queue(app))
        log_info("Started tick processing...")

    async def enqueue_tick(self, tick_data: Dict[str, Any]):
        """Adds standardized tick data to the queue."""
       
        await self.tick_queue.put(tick_data)
        print(f"Enqueued tick: {tick_data}")

async def process_tick_queue(app: FastAPI):
    """
    Consumes tick data from the broker's queue, processes it, and stores it.
    Processes ticks in batches or every second, whichever comes first.
    """
    try:
        log_info("process_tick_queue started...")

        batch_size = 50  # Number of ticks to process in one batch
        time_interval = 1.0  # Time in seconds to process available ticks

        while True:
            if not state.tick_processor.tick_queue.empty():

                print("Tick queue consumer is running...")
                start_time = asyncio.get_event_loop().time()  # Capture the start time
                batch = []
                while not state.tick_processor.tick_queue.empty():
                    tick_data = await state.tick_processor.tick_queue.get()
                    batch.append(tick_data)
                if batch:
                    log_info(f"Processing batch of size {len(batch)}")
                    await rabbitmq_service.publish_tick_batch(batch)

            else:
                logging.debug("Tick queue is empty, sleeping...")
                await asyncio.sleep(0.5)
                    #await rabbitmq_service.publish_tick_batch(batch)
            # Collect ticks for processing
            while len(batch) < batch_size and (asyncio.get_event_loop().time() - start_tie) < time_interval:
                if not state.tick_processor.tick_queue.empty():
                    tick_data = await state.tick_processor.tick_queue.get()
                    print(f"Retrieved tick: {tick_data}")

                    batch.append(tick_data)
                    logging.debug(f"Tick retrieved from queue: {tick_data}")
                else:
                    break

            # Process the batch if it has any data
            if batch:
                log_info(f"Processing batch of size {len(batch)}")
                await process_tick_batch(app, batch)

            # Ensure consistent intervals
            elapsed_time = asyncio.get_event_loop().time() - start_time
            if elapsed_time < time_interval:
                await asyncio.sleep(time_interval - elapsed_time)

    except Exception as e:
        log_exception(f"Error processing tick queue: {e}")

async def process_tick_batch(app: FastAPI, tick_batch: List[Dict[str, Any]]):
    """
    Handles batch processing of tick data.
    """
    try:
        # Database session setup
        # from services import timescaledb_service, rabbitmq_service
        # db_session = await get_db()

        # # Store tick data in TimescaleDB
        # await timescaledb_service.batch_insert_ticks(db_session, tick_batch)

        # Publish tick data to RabbitMQ
        await rabbitmq_service.publish_tick_batch(tick_batch)

        log_info(f"Successfully processed {len(tick_batch)} ticks.")
    except Exception as e:
        log_exception(f"Error processing tick batch: {e}")
