import logging
import json
import asyncio
from redis.asyncio import Redis


class RedisService:
    def __init__(self, redis, batch_size=10, batch_timeout=1.0, failure_queue="failure_queue"):
        self.redis = redis
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.buffer = []  # Buffer for batching
        self.buffer_lock = asyncio.Lock()  # Lock for synchronizing the buffer
        self.failure_queue = failure_queue
    async def add_tick(self, tick_data):
        """
        Adds a single tick to the buffer for batching.
        """
        async with self.buffer_lock:
            self.buffer.append(tick_data)
            if len(self.buffer) >= self.batch_size:
                await self._process_batch()

    async def _process_batch(self):
        """
        Processes the current buffer of ticks into Redis using pipelining.
        This is intended to be a private method.
        """
        async with self.buffer_lock:
            if not self.buffer:
                return  # No ticks to process

            tick_batch = self.buffer
            self.buffer = []  # Clear the buffer immediately

        try:
            async with self.redis.pipeline() as pipe:
                for tick_data in tick_batch:
                    tick_data_cleaned = {k: str(v) for k, v in tick_data.items()}
                    stream_name = f"tick_stream:{tick_data_cleaned['exchange']}@{tick_data_cleaned['stock_code']}"
                    pipe.xadd(stream_name, tick_data_cleaned)

                await pipe.execute()
                log_info(f"Pushed {len(tick_batch)} ticks to Redis.")
        except Exception as e:
            log_exception(f"Failed to push batch ticks to Redis: {e}")

    async def run_batch_processor(self):
        """
        Continuously flushes the buffer at regular intervals.
        This is the public method for batch processing.
        """
        while True:
            await asyncio.sleep(self.batch_timeout)
            await self._process_batch()  # Calls the private method

    async def process_failure_queue(self):
        """
        Retry failed ticks from the failure queue.
        """
        self.failure_queue_running = True
        try:
            while True:
                tick_data = await self.redis.lpop(self.failure_queue)
                if tick_data:
                    tick_data = json.loads(tick_data)
                    try:
                        await self.add_tick(tick_data)
                    except Exception as e:
                        log_exception(f"Retry failed for tick data: {tick_data}, Error: {e}")
                else:
                    log_info("Failure queue is empty. Sleeping...")
                    await asyncio.sleep(5)
        except Exception as e:
            log_exception(f"Failure queue processor encountered an error: {e}")
            self.failure_queue_running = False

    async def health_check(self):
        """
        Verifies Redis availability and the status of async jobs.

        Returns:
            dict: Health status of Redis and async jobs.
        """
        redis_status = "unhealthy"
        batch_processor_status = "unhealthy"
        failure_queue_status = "unhealthy"

        # Check Redis health
        try:
            if self.redis and await self.redis.ping():
                redis_status = "healthy"
        except Exception:
            redis_status = "unhealthy"

        # Check async job statuses
        batch_processor_status = "healthy" if self.batch_processor_running else "unhealthy"
        failure_queue_status = "healthy" if self.failure_queue_running else "unhealthy"

        # Return combined health status
        return {
            "redis_status": redis_status,
            "batch_processor_status": batch_processor_status,
            "failure_queue_status": failure_queue_status
        }