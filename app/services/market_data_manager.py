import asyncio
import hashlib
import json
import logging
from collections import defaultdict
from typing import Dict, List, Optional

from redis.asyncio.client import Redis
from shared_architecture.connections import get_redis_connection as get_redis
from shared_architecture.utils.logging_utils import log_info, log_warning,log_exception
from app.utils.instrument_key_helper import get_instrument_key


# Constants
NUM_SHARDS = 10
REDIS_MAXLEN = 10000

logger = logging.getLogger(__name__)

class RedisBatchWriter:
    def __init__(self, redis: Redis, batch_size: int = 100, flush_interval: float = 0.1):
        self.redis = redis
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = defaultdict(list)
        self.lock = asyncio.Lock()

    async def add(self, stream_key: str, data: dict):
        async with self.lock:
            self.buffer[stream_key].append(data)
            if len(self.buffer[stream_key]) >= self.batch_size:
                await self.flush_stream(stream_key)

    async def flush_stream(self, stream_key: str):
        if not self.buffer[stream_key]:
            return
        pipe = self.redis.pipeline()
        for item in self.buffer[stream_key]:
            pipe.xadd(stream_key, item, maxlen=REDIS_MAXLEN, approximate=True)
        await pipe.execute()
        self.buffer[stream_key] = []

    async def flush(self):
        async with self.lock:
            for stream_key in list(self.buffer):
                await self.flush_stream(stream_key)

    async def periodic_flusher(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    async def run_batch_processor(self):
        asyncio.create_task(self.periodic_flusher())

    async def process_failure_queue(self):
        logger.info("📦 Failure queue processor started...")
        while True:
            try:
                tick_data = await self.redis.lpop("tick:failures")
                if tick_data:
                    tick_data = json.loads(tick_data)
                    stream_key = tick_data.get("stream_key")
                    record = tick_data.get("record")
                    if stream_key and record:
                        await self.add(stream_key, record)
                    else:
                        logger.warning(f"Invalid data structure in failure queue: {tick_data}")
                else:
                    await asyncio.sleep(5)
            except Exception as e:
                logger.exception(f"Failure queue processing error: {e}")

class MarketDataManager:
    def __init__(self):
        self.redis_writer: Optional[RedisBatchWriter] = None

    async def initialize(self):
        try:
            redis = await get_redis()
            if redis is None:
                raise RuntimeError("Redis connection is None — check your Redis config or startup sequence.")

            self.redis_writer = RedisBatchWriter(redis)
            await self.redis_writer.run_batch_processor()
            asyncio.create_task(self.redis_writer.process_failure_queue())

            log_info("✅ RedisBatchWriter initialized and background tasks launched.")
        except Exception as e:
            log_exception(f"❌ Failed to initialize MarketDataManager: {e}")
            raise

    def _get_shard_key(self, instrument_key: str) -> str:
        shard = int(hashlib.sha256(instrument_key.encode()).hexdigest(), 16) % NUM_SHARDS
        return f"stream:shard:{shard}"

    async def process_breeze_ticks(self, ticks: List[dict]):
        if not self.redis_writer:
            raise RuntimeError("MarketDataManager not initialized")

        count = 0
        for tick in ticks:
            try:
                instrument_key = get_instrument_key(
                    exchange=tick["exchange_code"],
                    stock_code=tick["stock_code"],
                    product_type=tick["product_type"],
                    expiry_date=tick.get("expiry_date"),
                    option_type=tick.get("right"),
                    strike_price=tick.get("strike_price")
                )
                stream_key = self._get_shard_key(instrument_key)
                tick_record = {
                    "instrument_key": instrument_key,
                    "ltp": tick.get("last_traded_price"),
                    "timestamp": tick.get("last_trade_time") or tick.get("updated_at"),
                    "status": "U",
                    "source": "ticker_service"
                }
                await self.redis_writer.add(stream_key, tick_record)
                count += 1
            except Exception as e:
                log_warning(f"Skipping malformed tick: {tick} - {e}")

        if count > 0:
            log_info(f"✅ Queued {count} ticks to Redis stream shards.")
