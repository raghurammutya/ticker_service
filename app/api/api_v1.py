from fastapi import APIRouter
import os
print("Current Working Directory:", os.getcwd())

from api.endpoints import feeds, historical_data, symbols, subscriptions

api_router = APIRouter()

api_router.include_router(feeds.router, prefix="/feeds", tags=["feeds"])
api_router.include_router(historical_data.router, prefix="/historical_data", tags=["historical_data"])
api_router.include_router(symbols.router, prefix="/symbols", tags=["symbols"])
api_router.include_router(subscriptions.router, prefix="/subscriptions", tags=["subscriptions"])
print("Routers registered in api_v1.py:", api_router.routes)