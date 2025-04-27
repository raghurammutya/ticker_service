import asyncio
import datetime
import logging
from typing import List, Dict, Any

from fastapi import HTTPException,FastAPI,Depends
from sqlalchemy import func
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from core.config import Settings
from models import symbol as symbol_model
from schemas import symbol as symbol_schema
from services import broker_service
from core.dependencies import get_broker_instance
from shared_architecture.utils.service_helpers import connection_manager
async def get_symbol_by_instrument_key(instrument_key: str) -> symbol_schema.Symbol:
    """
    Retrieves a symbol by its instrument key.
    """
    #from shared_architecture.utils.service_helpers import connection_manager
    db=app.state.connections['timescaledb']
    symbol = db.query(symbol_model.Symbol).filter(symbol_model.Symbol.instrument_key == instrument_key).first()
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol

async def get_all_symbols(db: Session) -> List[symbol_schema.Symbol]:
    """
    Retrieves all symbols.
    """
    symbols = db.query(symbol_model.Symbol).all()
    return symbols

async def refresh_symbols(
    db: Session,
    app: FastAPI  # Add app as a parameter
):
    """
    Refreshes the symbols table with batch processing and parallel execution.

    Args:
        db (Session): Database session.
        broker_instance: The initialized broker instance.
    """
    try:
        broker_instance=app.state.broker_instance
        # Check if refresh has already been done today
        today = datetime.date.today()
        last_updated = db.query(func.max(symbol_model.Symbol.Local_Update_Datetime)).scalar()
        if last_updated and last_updated.date() == today:
            logging.info("Symbol data already refreshed today.")

            # Check if broker-specific tokens are updated
            if await are_broker_tokens_updated(db, broker_instance):
                logging.info("Broker tokens are also up-to-date. Skipping refresh.")
                return  # Skip the entire refresh process
            
            logging.info("Broker tokens need update. Proceeding to update tokens.")
            await update_broker_tokens(db, app)
            return
        logging.info("Symbol data needs refresh. Proceeding with full refresh.")
        new_symbols_data: List[Dict[str, Any]] = await broker_instance.get_symbols()
        # Mark all existing symbols as inactive
        db.query(symbol_model.Symbol).update({symbol_model.Symbol.Refresh_Flag: True, symbol_model.Symbol.Remarks: "Inactive"})
        db.commit()
        batch_size = 1000  # Adjust batch size as needed
        new_symbols: List[symbol_schema.SymbolCreate] = [symbol_schema.SymbolCreate(**data) for data in new_symbols_data] # Convert raw data to SymbolCreate
        for i in range(0, len(new_symbols), batch_size):
            batch = new_symbols[i:i + batch_size]
            await _process_symbol_batch(db, batch)
        db.commit()
        logging.info("Symbols refreshed successfully.")

        # Update broker specific tokens
        await update_broker_tokens(db, app)

    except Exception as e:
        logging.error(f"Error refreshing symbols: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def are_broker_tokens_updated(db: Session, broker_instance) -> bool:
    """
    Checks if broker-specific tokens are already updated for all active symbols.
    """
    try:
        symbols = db.query(symbol_model.Symbol).filter(symbol_model.Symbol.refresh_flag == False).all()
        for symbol in symbols:
            broker_token_column = f"{broker_instance.broker_name}_token"
            if getattr(symbol, broker_token_column, None) is None:
                return False  # Found a symbol with a missing broker token
        return True  # All tokens are present
    except Exception as e:
        logging.error(f"Error checking broker tokens: {e}")
        return False
async def _process_symbol_batch(db: Session, batch: List[symbol_schema.SymbolCreate]):
    """
    Processes a batch of symbols for adding or updating.
    """
    # Convert batch to dictionaries
    now = datetime.datetime.now(datetime.timezone.utc)

    batch_dicts = [symbol.dict() for symbol in batch]
    # Separate insert and update operations
    existing_keys = {s.instrument_key for s in db.query(symbol_model.Symbol.instrument_key).filter(
        symbol_model.Symbol.instrument_key.in_([s["instrument_key"] for s in batch_dicts])
    )}
    
    inserts = []
    updates = []
    for symbol_data in batch_dicts:
        symbol_data["Local_Update_Datetime"] = now  # Timestamp for update
        if symbol_data["instrument_key"] in existing_keys:
            updates.append(symbol_data)  # Updating existing records
        else:
            symbol_data["first_added_datetime"] = now  # Timestamp for new insertions
            inserts.append(symbol_data)  # New records
    # Perform bulk inserts and updates efficiently
    if inserts:
        db.bulk_insert_mappings(symbol_model.Symbol, inserts)
    if updates:
        db.bulk_update_mappings(symbol_model.Symbol, updates)

    db.commit()
    logging.info(f"Batch processing complete: {len(inserts)} inserts, {len(updates)} updates")

    # for new_symbol_data in batch:
    #     new_symbol = symbol_model.Symbol(**new_symbol_data.dict())  # Convert to dictionary
    #     existing_symbol = db.query(symbol_model.Symbol).filter(symbol_model.Symbol.instrument_key == new_symbol.instrument_key).first()

    #     # Handle local_update_datetime explicitly
    #     if new_symbol_data.Local_Update_Datetime is None or pd.isna(new_symbol_data.Local_Update_Datetime):
    #         new_symbol.local_update_datetime = None
    #     else:
    #         new_symbol.Local_Update_Datetime = new_symbol_data.Local_Update_Datetime

    #     if existing_symbol:
    #         # Update existing symbol
    #         for attr, value in new_symbol_data.dict().items():
    #             setattr(existing_symbol, attr, value)
    #         existing_symbol.Refresh_Flag = False
    #         existing_symbol.remarks = None
    #         existing_symbol.Local_Update_Datetime = datetime.datetime.now(datetime.timezone.utc)
    #     else:
    #         # Add new symbol
    #         new_symbol.Refresh_Flag = False
    #         new_symbol.remarks = None
    #         new_symbol.first_added_datetime = datetime.datetime.now(datetime.timezone.utc)  # Set on insert
    #         new_symbol.Local_Update_Datetime = datetime.datetime.now(datetime.timezone.utc)
    #         db.add(new_symbol)
    # db.commit()
    # logging.info("Batch processing complete")
async def update_broker_tokens(
    db: Session,
    app: FastAPI,  # Add app as a dependency
):
    """
    Updates broker-specific tokens for each symbol using parallel processing.

    Args:
        db (Session): Database session.
        app (FastAPI): The FastAPI application instance.
    """
    try:
        broker_instance = state.broker_instance  # Get the broker instance from state
        if not broker_instance:
            raise Exception("Broker instance not initialized in state")

        symbols = db.query(symbol_model.Symbol).filter(symbol_model.Symbol.Refresh_Flag == False).all()

        tasks = [
            _update_symbol_token(db, broker_instance, symbol)
            for symbol in symbols
        ]
        await asyncio.gather(*tasks)

        db.commit()
        logging.info("Broker tokens updated successfully.")
    except Exception as e:
        logging.error(f"Error updating broker tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _update_symbol_token(
    db: Session,
    broker_instance,  # Get the broker instance
    symbol: symbol_model.Symbol,
):
    """
    Updates the broker token for a single symbol.

    Args:
        db (Session): Database session.
        broker_instance: The initialized broker instance (e.g., Breeze or Kite).
        symbol (symbol_model.Symbol): The symbol to update.
    """
    broker_token = await broker_instance.get_symbol_token(symbol.instrument_key)
    if broker_token:
        broker_token_column = f"{broker_instance.broker_name}_Token"
        setattr(symbol, broker_token_column, broker_token)
        symbol.localupdatedatetime = datetime.datetime.now(datetime.timezone.utc)

async def get_broker_token(instrument_key: str, broker_name: str) -> str:
    """
    Retrieves the broker-specific token for a given instrument key and broker name.
    """
    symbol = await get_symbol_by_instrument_key(instrument_key)
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    column_name = f"{broker_name}_Token"
    if not hasattr(symbol, column_name):
        raise ValueError(f"Column '{column_name}' does not exist in Symbol record.")

    # Retrieve the column value
    broker_token = getattr(symbol, column_name, None)

    # Condition A: Column exists but not populated (None or empty string)
    if broker_token is None or broker_token.strip() == "":
        raise HTTPException(status_code=204, detail=f"Column '{column_name}' exists but is not populated.")

    # Condition C: Column exists and has a value
    return broker_token
