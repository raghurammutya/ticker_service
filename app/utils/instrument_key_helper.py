# app/utils/instrument_key_helper.py

def get_instrument_key(
    exchange: str,
    stock_code: str,
    product_type: str,
    expiry_date: str = None,
    option_type: str = None,
    strike_price: str = None
) -> str:
    """
    Generate a unique instrument key for equities, futures, and options.
    Special rule: If option_type == 'xx', treat as futures (ignore option_type/strike_price).
    """
    product_type = (product_type or "").strip().lower()
    option_type = (option_type or "").strip().lower()
    exchange = (exchange or "").strip().upper()
    stock_code = (stock_code or "").strip().upper()

    if product_type == "equities":
        return f"{exchange}@{stock_code}@equities"

    elif product_type == "futures":
        if not expiry_date:
            raise ValueError("expiry_date is required for futures")
        return f"{exchange}@{stock_code}@futures@{expiry_date}"

    elif product_type == "options":
        if option_type == "xx":
            # Treat like a future: drop option_type and strike_price
            if not expiry_date:
                raise ValueError("expiry_date is required for options with option_type='xx'")
            return f"{exchange}@{stock_code}@futures@{expiry_date}"

        # Normal options
        if not all([expiry_date, option_type, strike_price]):
            raise ValueError("expiry_date, option_type, and strike_price are required for options")
        return f"{exchange}@{stock_code}@options@{expiry_date}@{option_type}@{strike_price}"

    else:
        raise ValueError(f"Unsupported product type: {product_type}")
