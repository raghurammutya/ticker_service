from pydantic import BaseModel

class SubscriptionCreate(BaseModel):
    instrument_key: str

class Subscription(SubscriptionCreate):
    # No extra fields needed for now
    pass