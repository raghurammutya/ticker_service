from pydantic import BaseModel
from typing import Optional

class FeedBase(BaseModel):
    instrument_key: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: Optional[int] = None
    expirydate: Optional[str] = None
    option_type: Optional[str] = None
    strikeprice: Optional[float] = None

class FeedCreate(FeedBase):
    pass

class Feed(FeedBase):
    time: str

    class Config:
        orm_mode = True