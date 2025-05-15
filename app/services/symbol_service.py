import os
import time
import datetime
import asyncio
import logging
from io import StringIO
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from sqlalchemy import func, select, update, insert
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.core.config import Settings
from app.schemas import symbol as symbol_schema
from app.core.dependencies import get_broker_instance

from shared_architecture.db.models import symbol as symbol_model
from shared_architecture.utils.logging_utils import log_info, log_exception
from shared_architecture.connections import get_timescaledb_session

# Constants
BATCH_SIZE = 1000
SessionLocal = get_timescaledb_session()
MAX_CONCURRENT_BATCHES = min(10, os.cpu_count() or 4)
semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
 # Limit the number of concurrent DB sessions
async def get_symbol_by_instrument_key(instrument_key: str, db: AsyncSession) -> symbol_schema.Symbol:
    symbol = db.query(symbol_model.Symbol).filter(
        symbol_model.Symbol.instrument_key == instrument_key
    ).first()
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol


async def get_all_symbols(db: AsyncSession) -> List[symbol_schema.Symbol]:
    return db.query(symbol_model.Symbol).all()




from shared_architecture.connections.timescaledb import get_timescaledb_session


async def refresh_symbols(app: FastAPI):
    try:
        broker_instance = app.state.broker_instance
        today = datetime.date.today()

        async with SessionLocal() as db1:
            result = await db1.execute(select(func.max(symbol_model.Symbol.local_update_datetime)))
            last_updated = result.scalar_one_or_none()

        if last_updated and last_updated.date() == today:
            logging.info("Symbol data already refreshed today.")
            return
        async with SessionLocal() as db1:
            if await are_broker_tokens_updated(db1, broker_instance):
                logging.info("Broker tokens are up-to-date. Skipping refresh.")
                return
        
        logging.info("Broker tokens need update. Proceeding to update.")
        async with SessionLocal() as db1:
            await update_broker_tokens(db1, app)
        # Mark old symbols as inactive
        stmt = update(symbol_model.Symbol).values({
            symbol_model.Symbol.refresh_flag: True,
            symbol_model.Symbol.remarks: "Inactive"
        })
        async with SessionLocal() as db1:
            await db1.execute(stmt)
            await db1.commit()
        logging.info("Proceeding with full symbol refresh.")
        new_symbols_data: List[Dict[str, Any]] = await broker_instance.get_symbols()
        logging.info("Symbols refreshed successfully.")
        async with SessionLocal() as db1:
            await update_broker_tokens(db1, app)
    except Exception as e:
        logging.error(f"Error refreshing symbols: {e}")
        raise HTTPException(status_code=500, detail=str(e))
#   # Include logic to skip if updating?

#         # Perform bulk upsert using shared utility
#         await upsert_dataframe(
#             model=symbol_model.Symbol,
#             batch_dicts=new_symbols_data,
#             key_field=["instrument_key"],
#             session_factory=SessionLocal
#         )







async def _process_symbol_batch(db: AsyncSession, batch: List[symbol_schema.SymbolCreate]):
    """ Processes a batch of symbols for adding or updating. """
    now = datetime.datetime.now(datetime.timezone.utc)
    batch_dicts = [symbol.dict() for symbol in batch]

    # Fetch existing keys
    async with SessionLocal() as db1:
        result = await db1.execute(select(symbol_model.Symbol.instrument_key).filter(
            symbol_model.Symbol.instrument_key.in_([s["instrument_key"] for s in batch_dicts])
        ))
        existing_keys = {row[0] for row in result.fetchall()}

    inserts = []
    updates = []

    for symbol_data in batch_dicts:
        if symbol_data["instrument_key"] in existing_keys:
            updates.append(symbol_data)
        else:
            inserts.append(symbol_data)

    # Perform bulk inserts and updates
    async with SessionLocal() as db1:
        if inserts:
            await db1.execute(symbol_model.Symbol.__table__.insert(), inserts)
        if updates:
            await db1.execute(update(symbol_model.Symbol).where(
                symbol_model.Symbol.instrument_key.in_([s["instrument_key"] for s in updates])
            ).values({symbol_model.Symbol.localupdatedatetime: now}))
        await db1.commit()

    logging.info(f"Batch processing complete: {len(inserts)} inserts, {len(updates)} updates")

# async def refresh_symbols(db: AsyncSession, app: FastAPI):
#     try:
#         start_time = time.time()
#         log_info("🚀 Starting symbol refresh pipeline...")

#         broker_instance = app.state.broker_instance
#         today = datetime.date.today()

#         # Step 1: Check last updated timestamp using existing session
#         log_info("🔍 Checking last symbol update timestamp...")
#         async with SessionLocal() as db1:
#             result = await db1.execute(select(func.max(symbol_model.Symbol.localupdatedatetime)))
#         last_updated = result.scalar_one_or_none()

#         if last_updated and last_updated.date() == today:
#             log_info("🟢 Symbol data already refreshed today.")
#             if await are_broker_tokens_updated(db, broker_instance):
#                 log_info("✅ Broker tokens up-to-date. Skipping refresh.")
#                 return
#             else:
#                 log_info("🛠️ Updating broker tokens.")
#                 await update_broker_tokens(db, app)
#                 return

#         # Step 2: Fetch symbol data from broker
#         log_info("📥 Fetching latest symbols from broker...")
#         new_symbols_data: List[Dict[str, Any]] = await broker_instance.get_symbols()
#         log_info(f"✅ Fetched {len(new_symbols_data)} symbols in {round(time.time() - start_time, 2)}s")

#         # Step 3: Mark old symbols as inactive using existing db session
#         log_info("🧹 Marking old symbols as inactive...")
#         stmt = update(symbol_model.Symbol).values({
#             symbol_model.Symbol.refresh_flag: True,
#             symbol_model.Symbol.remarks: "Inactive"
#         })
#         async with SessionLocal() as db1:
#             await db1.execute(stmt)
#             await db1.commit()

#         # Step 4: Convert new symbol data to CSV format for COPY
#         log_info("📦 Preparing data for bulk insert via COPY...")
#         df = pd.DataFrame(new_symbols_data)
#         csv_buffer = StringIO()
#         df.to_csv(csv_buffer, index=False, header=False)
#         csv_buffer.seek(0)  # Reset buffer for reading

#         # Step 5: Bulk insert using COPY, avoiding hardcoded credentials
#         log_info("🚀 Bulk inserting symbols using COPY...")
#         import psycopg2

#         async with SessionLocal() as db1:
#             from sqlalchemy.engine.url import make_url

#             # Convert SQLAlchemy connection URL to a valid PostgreSQL DSN
#             sync_engine = SessionLocal().bind  
#             db_url = str(make_url(sync_engine.url)).replace("postgresql+asyncpg", "postgresql")

#             conn = psycopg2.connect(db_url)
#             cursor = conn.cursor()

#             cursor.copy_expert(
#                 """
#                 COPY symbols (instrument_key, localupdatedatetime, refresh_flag, remarks) 
#                 FROM STDIN WITH CSV
#                 """,
#                 csv_buffer
#             )

#             conn.commit()
#             cursor.close()
#             conn.close()




#         log_info(f"✅ Bulk insert completed in {round(time.time() - start_time, 2)}s.")

#         # Step 6: Handle primary key conflicts using ON CONFLICT DO UPDATE
#         log_info("🔄 Resolving primary key conflicts...")
#         upsert_stmt = insert(symbol_model.Symbol).values(new_symbols_data).on_conflict_do_update(
#             index_elements=["instrument_key"],
#             set_={
#                 "localupdatedatetime": datetime.datetime.now(datetime.timezone.utc),
#                 "refresh_flag": False,
#                 "remarks": "Upserted",
#             }
#         )
#         await db.execute(upsert_stmt)
#         await db.commit()

#         log_info("✅ Primary key conflicts resolved.")

#         # Step 7: Update Broker Tokens using existing session
#         log_info("🔑 Proceeding to update broker tokens...")
#         await update_broker_tokens(db, app)
#         log_info("✅ Broker tokens updated after symbol refresh.")

#     except Exception as e:
#         log_exception(f"❌ Error refreshing symbols: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# async def process_batch(db: AsyncSession, batch: List[symbol_schema.SymbolCreate], batch_id: int):
#     start = datetime.datetime.now(datetime.timezone.utc)
#     log_info(f"[Batch {batch_id}] ⏳ Started at {start.isoformat()} with {len(batch)} symbols.")
    
#     try:
#         batch_dicts = [symbol.dict() for symbol in batch]
#         insert_stmt = insert(symbol_model.Symbol).values(batch_dicts)
#         insert_stmt = insert_stmt.on_conflict_do_update(
#             index_elements=["instrument_key"],
#             set_={
#                 "localupdatedatetime": datetime.datetime.now(datetime.timezone.utc),
#                 "refresh_flag": True,
#                 "remarks": "Updated via UPSERT"
#             }
#         )
#         await db.execute(insert_stmt)
#         await db.commit()
#         log_info(f"[Batch {batch_id}] ✅ Completed in {round((datetime.datetime.now(datetime.timezone.utc) - start).total_seconds(), 2)}s")
#     except Exception as e:
#         await db.rollback()
#         log_exception(f"[Batch {batch_id}] ❌ Failed with error: {e}")
#     finally:
#         await db.close()



async def are_broker_tokens_updated(db: AsyncSession, broker_instance) -> bool:
    try:

        stmt = select(symbol_model.Symbol).where(symbol_model.Symbol.refresh_flag == False)
        result = await db.execute(stmt)
        symbols = result.scalars().all()

        if not symbols:
            return False

        for symbol in symbols:
            broker_token_column = f"{broker_instance.broker_name}_token"
            if getattr(symbol, broker_token_column, None) is None:
                return False
        return True
    except Exception as e:
        log_exception(f"Error checking broker tokens: {e}")
        return False



async def update_broker_tokens(db: AsyncSession, app: FastAPI):
    try:
        broker_instance = app.state.broker_instance
        if not broker_instance:
            raise Exception("Broker instance not initialized in app state")

        # Use SQLAlchemy async-compatible query
        stmt = select(symbol_model.Symbol).where(symbol_model.Symbol.refresh_flag == False)
        result = await db.execute(stmt)
        symbols = result.scalars().all()

        tasks = [_update_symbol_token(db, broker_instance, symbol) for symbol in symbols]
        await asyncio.gather(*tasks)
        await db.commit()

        log_info("Broker tokens updated successfully.")
    except Exception as e:
        log_exception(f"Error updating broker tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _update_symbol_token(db: AsyncSession, broker_instance, symbol: symbol_model.Symbol):
    broker_token = await broker_instance.get_symbol_token(symbol.instrument_key)
    if broker_token:
        token_col = f"{broker_instance.broker_name}_Token"
        setattr(symbol, token_col, broker_token)
        symbol.localupdatedatetime = datetime.datetime.now(datetime.timezone.utc)


async def get_broker_token(instrument_key: str, broker_name: str, db: AsyncSession) -> str:
    symbol = await get_symbol_by_instrument_key(instrument_key, db)
    column_name = f"{broker_name}_Token"
    if not hasattr(symbol, column_name):
        raise ValueError(f"Column '{column_name}' does not exist in Symbol record.")
    token = getattr(symbol, column_name, None)
    if not token or token.strip() == "":
        raise HTTPException(status_code=204, detail=f"Token in '{column_name}' is not populated.")
    return token
