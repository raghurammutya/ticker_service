import os
import logging
import psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import tuple_
from sqlalchemy import create_engine
from schemas.historical_data import HistoricalDataCreate
from models.historical_data import HistoricalData
from sqlalchemy import exc
from typing import List
from sqlalchemy.orm import Session
logging.basicConfig(
level=logging.INFO,
format="%(asctime)s - %(levelname)s - %(message)s",
)

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
        logging.info(f"TimescaleDBConnection initialized with config: {self.config}")

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

async def batch_upsert_historical_data(db, data_list: List[HistoricalDataCreate]) -> List[HistoricalData]:
    """
    Batch upserts historical data into the historical_data table.
    Efficiently handles large data volumes using bulk insert/update.
    """
    try:
        # Extract timestamps, instrument keys, and intervals for existing record lookup
        lookup_keys = [(d['datetime'], d['instrument_key'], d['interval']) for d in data_list]

        # Fetch existing records efficiently


        existing_records = db.query(HistoricalData).filter(
            tuple_(HistoricalData.time, HistoricalData.instrument_key, HistoricalData.interval).in_(lookup_keys)
        ).all()


        # Convert existing records to a dict for fast lookup
        existing_map = {(rec.time, rec.instrument_key, rec.interval): rec for rec in existing_records}

        new_records = []
        update_records = []

        for data in data_list:
            # Ensure 'datetime' is mapped to 'time'
            data['time'] = data.pop('datetime') if 'datetime' in data else None
            
            if data['time'] is None:
                raise ValueError("Missing required 'datetime' field in data!")

            # Create the key tuple
            key = (data['time'], data['instrument_key'], data['interval'])

            if key in existing_map:
                # Update the record instead of inserting a new one
                update_records.append(data)
            else:
                # Add to new records for bulk insert
                new_records.append(data)

        # Perform bulk updates for existing records
        if update_records:
            db.bulk_update_mappings(HistoricalData, update_records)

        # Perform bulk inserts for new records
        if new_records:
            db.bulk_insert_mappings(HistoricalData, new_records)

        db.commit()

        return new_records + update_records  # Return stored data
    except exc.SQLAlchemyError as e:
        db.rollback()
        logging.error(f"Error batch upserting historical data: {e}")
        raise    # No TimescaleDBPool class anymore