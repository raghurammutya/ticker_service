from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

class HistoricalDataRequest(BaseModel):
    instrument_key: str
    from_date: str
    to_date: str
    interval: str
    sort: Optional[str] = 'desc'  # Optional field with default value

class HistoricalDataCreate(BaseModel):
    instrument_key: str
    time: datetime
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: Optional[int] = None
    expirydate: Optional[date] = None
    option_type: Optional[str] = None
    strikeprice: Optional[float] = None
    greeks_open_iv: Optional[float] = None
    greeks_open_delta: Optional[float] = None
    greeks_open_gamma: Optional[float] = None
    greeks_open_theta: Optional[float] = None
    greeks_open_rho: Optional[float] = None
    greeks_open_vega: Optional[float] = None
    greeks_high_iv: Optional[float] = None
    greeks_high_delta: Optional[float] = None
    greeks_high_gamma: Optional[float] = None
    greeks_high_theta: Optional[float] = None
    greeks_high_rho: Optional[float] = None
    greeks_high_vega: Optional[float] = None
    greeks_low_iv: Optional[float] = None
    greeks_low_delta: Optional[float] = None
    greeks_low_gamma: Optional[float] = None
    greeks_low_theta: Optional[float] = None
    greeks_low_rho: Optional[float] = None
    greeks_low_vega: Optional[float] = None
    greeks_close_iv: Optional[float] = None
    greeks_close_delta: Optional[float] = None
    greeks_close_gamma: Optional[float] = None
    greeks_close_theta: Optional[float] = None
    greeks_close_rho: Optional[float] = None
    greeks_close_vega: Optional[float] = None

class HistoricalData(HistoricalDataCreate):
    id: int

    class Config:
        orm_mode = True