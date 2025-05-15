from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas import symbol as symbol_schema
from app.services import broker_service, symbol_service
from shared_architecture.db import get_db
from typing import List

router = APIRouter()

@router.get("/symbols/{instrument_key}", response_model=symbol_schema.Symbol)
async def get_symbol(instrument_key: str, db: Session = Depends(get_db)):
    """
    Endpoint to retrieve symbol details.
    """
    symbol = await symbol_service.get_symbol_by_instrument_key(db, instrument_key)
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol

@router.get("/symbols/", response_model=List[symbol_schema.Symbol])
async def get_all_symbols(db: Session = Depends(get_db)):
    """
    Endpoint to retrieve all symbols.
    """
    symbols = await symbol_service.get_all_symbols(db)
    return symbols

@router.post("/symbols/refresh/", status_code=200)
async def refresh_symbols(db: Session = Depends(get_db)):
    """
    Endpoint to trigger symbol refresh.
    """
    try:
        await symbol_service.refresh_symbols(db)
        return {"message": "Symbol refresh started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))