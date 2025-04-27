from fastapi import APIRouter, Depends, HTTPException, Request
import logging
from sqlalchemy.orm import Session
from schemas import subscription as subscription_schema
from services import broker_service, symbol_service
from pydantic import BaseModel, Field
from shared_architecture.db.models.broker import Broker
from shared_architecture.db.session import get_db
# Pydantic model to ensure arbitrary types are allowed

# Router declaration
router = APIRouter(tags=["subscriptions"])

import os


def get_broker(request: Request) -> str:
    """
    Retrieve broker name from environment variables.
    """
    return os.getenv("BROKER_NAME", "")  # Use a default if not set

@router.get("/test-subscriptions", status_code=200)
async def test_subscriptions():
    """Test route for subscriptions"""
    return {"message": "Test subscriptions route works"}

@router.post("/", status_code=200)
async def subscribe_to_symbol(
    request: Request,
    subscription: subscription_schema.SubscriptionCreate,
    broker_name: str = Depends(get_broker), 
    interval: str = '1second',
    get_market_depth: bool = False,
    get_exchange_quotes: bool = True
):
    """
    Endpoint to subscribe to a symbol for real-time feeds.
    """
    try:
        # Retrieve broker-specific token
        # broker_token = await symbol_service.get_broker_token(
        #     subscription.instrument_key, broker_name
        # )

        breeze_instance = request.app.state._state['broker_instance']


        breeze_instance.subscribe(instrument_key=subscription.instrument_key,
                                interval = '1second',
                                get_market_depth = False,
                                get_exchange_quotes = True)

        # Additional logic to handle subscription

        return {"message": f"Subscription successful for broker: {broker_name}"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error subscribing to symbol: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/unsubscribe/", status_code=200)
async def unsubscribe_from_symbol(
    request: Request,
    subscription: subscription_schema.SubscriptionCreate,
    db: Session = Depends(get_db),
    broker_name: str = Depends(get_broker), 
):
    """
    Endpoint to unsubscribe from a symbol.
    """
    try:
        # Retrieve broker-specific token
        broker_token = await symbol_service.get_broker_token(
            subscription.instrument_key, broker_name
        )
        
        breeze_instance = request.app.state._state['broker_instance']

        # Unsubscribe logic
        breeze_instance.unsubscribe(broker_token=broker_token)
        return {"message": "Unsubscription successful"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error unsubscribing from symbol: {e}")
        raise HTTPException(status_code=500, detail=str(e))