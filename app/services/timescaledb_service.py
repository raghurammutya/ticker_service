import os

import psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import tuple_
from sqlalchemy import create_engine
from app.schemas.historical_data import HistoricalDataCreate
from shared_architecture.utils.logging_utils import log_info, log_error, log_warning
from sqlalchemy.ext.asyncio import AsyncSession

from shared_architecture.db.models.historical_data import HistoricalData
from sqlalchemy import exc
from typing import List
from sqlalchemy.orm import Session

CHUNK_SIZE = 500
class TimescaleDBConnection:
    def __init__(self, config: dict):
        """
        Initialize TimescaleDB connection.

        Args:
        config (dict): Configuration dictionary.
        """
        self.config = config
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        log_info(f"TimescaleDBConnection initialized with config: {self.config}")

    def _create_engine(self):
        """
        Creates the SQLAlchemy engine.
        """
        db_url = self._get_database_url()
        return create_engine(
        db_url,
        pool_size=int(self.config.get("pool_size", 10)), # Ensure int, provide default
        max_overflow=int(self.config.get("max_overflow", 5)), # Ensure int, provide default
        pool_timeout=int(self.config.get("pool_timeout", 30)), # Ensure int, provide default
        pool_recycle=int(self.config.get("pool_recycle", 1800)), # Ensure int, provide default
        )

    def _get_database_url(self) -> str:
        """
        Constructs the database URL from configuration.
        """
        return (
        f"postgresql://{self.config.get('postgres_user')}:"
        f"{self.config.get('postgres_password')}@"
        f"{self.config.get('postgres_host')}:"
        f"{self.config.get('postgres_port')}/"
        f"{self.config.get('postgres_db')}"
        )

    def get_session(self):
        """
        Provides a database session.
        """
        return self.SessionLocal()

async def batch_upsert_historical_data(
    self,
    session: AsyncSession,
    instrument_key: str,
    interval: str,
    data_list: list[dict],
    source: str = "ticker_service"
):
    """
    Efficiently insert/update OHLCV data in TimescaleDB with batching and conflict handling.
    """
    if not data_list:
        log_info(f"No data to insert for {instrument_key}:{interval}")
        return

    start_ts = datetime.utcnow()

    total_inserted = 0
    total_skipped = 0

    for chunk_start in range(0, len(data_list), CHUNK_SIZE):
        chunk = data_list[chunk_start:chunk_start + CHUNK_SIZE]

        # Normalize to dict
        chunk = [
            d.dict() if hasattr(d, "dict") else dict(d)
            for d in chunk
        ]

        # Ensure source is added
        for d in chunk:
            d["source"] = source

        # Build the raw SQL insert
        values_sql = ",".join(
            [
                f"""(
                    :time_{i}, :instrument_key_{i}, :interval_{i},
                    :open_{i}, :high_{i}, :low_{i}, :close_{i}, :volume_{i},
                    :open_interest_{i}, :greek_iv_{i}, :greek_delta_{i}, 
                    :greek_theta_{i}, :greek_gamma_{i}, :greek_vega_{i}, 
                    :greek_rho_{i}, :source_{i}
                )"""
                for i in range(len(chunk))
            ]
        )

        sql = text(f"""
        INSERT INTO historical_data (
            time, instrument_key, interval,
            open, high, low, close, volume,
            open_interest, greek_iv, greek_delta,
            greek_theta, greek_gamma, greek_vega,
            greek_rho, source
        ) VALUES {values_sql}
        ON CONFLICT (time, instrument_key, interval)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            greek_iv = EXCLUDED.greek_iv,
            greek_delta = EXCLUDED.greek_delta,
            greek_theta = EXCLUDED.greek_theta,
            greek_gamma = EXCLUDED.greek_gamma,
            greek_vega = EXCLUDED.greek_vega,
            greek_rho = EXCLUDED.greek_rho,
            source = EXCLUDED.source
        ;
        """)

        # Map parameters
        params = {}
        for i, d in enumerate(chunk):
            params.update({
                f"time_{i}": d.get("time"),
                f"instrument_key_{i}": instrument_key,
                f"interval_{i}": interval,
                f"open_{i}": d.get("open"),
                f"high_{i}": d.get("high"),
                f"low_{i}": d.get("low"),
                f"close_{i}": d.get("close"),
                f"volume_{i}": d.get("volume"),
                f"open_interest_{i}": d.get("open_interest"),
                f"greek_iv_{i}": d.get("greek_iv"),
                f"greek_delta_{i}": d.get("greek_delta"),
                f"greek_theta_{i}": d.get("greek_theta"),
                f"greek_gamma_{i}": d.get("greek_gamma"),
                f"greek_vega_{i}": d.get("greek_vega"),
                f"greek_rho_{i}": d.get("greek_rho"),
                f"source_{i}": d.get("source"),
            })

        await session.execute(sql, params)
        total_inserted += len(chunk)

    await session.commit()

    end_ts = datetime.utcnow()
    log_info(
        f"[Timescale] Inserted {total_inserted} rows for {instrument_key}:{interval} "
        f"in {round((end_ts - start_ts).total_seconds(), 2)}s"
    )