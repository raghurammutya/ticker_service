from typing import Dict, Any, Optional,Type,Union
import pandas as pd
from datetime import datetime,date
def safe_convert(value: Any, target_type: Type, default: Optional[Any] = None):
    """
    Safely converts a value to the target type, handling None and potential conversion errors.
    """
    if value is None:
        return default
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default
    except Exception as e:
        print(f"Unexpected error during conversion: {e}")
        return default
    
def safe_convert_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """
    Safely converts a value to an integer, handling None and potential errors.

    Args:
        value: The value to convert.
        default: The value to return if conversion fails or if value is None.

    Returns:
        The converted integer, or the default if conversion fails or value is None.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
    except Exception as e:
        print(f"Unexpected error converting to int: {e}")  # Log unexpected errors
        return default
def safe_convert_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Safely converts a value to a float, handling None and potential errors.

    Args:
        value: The value to convert.
        default: The value to return if conversion fails or if value is None.

    Returns:
        The converted float, or the default if conversion fails or value is None.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
    except Exception as e:
        print(f"Unexpected error converting to float: {e}")  # Log unexpected errors
        return default
def safe_convert_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    """
    Safely converts a value to a boolean, handling None and various representations.

    Args:
        value: The value to convert.
        default: The value to return if conversion fails or if value is None.

    Returns:
        The converted boolean, or the default if conversion fails or value is None.
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)  # 0 -> False, non-zero -> True
    elif isinstance(value, str):
        if value.lower() in ("true", "1", "yes"):
            return True
        elif value.lower() in ("false", "0", "no"):
            return False
        else:
            return default  # Return default for invalid strings
    else:
        try:
            return bool(value)  # General boolean conversion
        except (ValueError, TypeError):
            return default
        except Exception as e:
            print(f"Unexpected error converting to bool: {e}")
            return default
def safe_parse_datetime(date_input: Union[str, datetime, date, pd.Timestamp]) -> Optional[datetime]:
    """
    Safely parses a string or datetime-like object into a datetime object.
    Handles pd.NaT.
    """
    if date_input is None or pd.isna(date_input):
        return None
    if isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, date):
        return datetime(date_input.year, date_input.month, date_input.day)
    if isinstance(date_input, str):
        formats = [
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d-%b-%Y',
            '%d-%m-%Y %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y/%m/%d',
            '%Y%m%d',
            '%d%m%Y'
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_input, fmt)
            except ValueError:
                pass  # Try the next format
    return None

# def create_standardized_feed_dict(raw_feed_data: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Standardizes a tick data message into a consistent dictionary format.

#     Args:
#         raw_feed_data (Dict[str, Any]): The raw tick data dictionary from the broker.

#     Returns:
#         Dict[str, Any]: A dictionary with standardized tick data fields.
#     """

#     standardized_feed = {
#         "instrument_key": raw_feed_data.get("instrument_key"),
#         "symbol": raw_feed_data.get("symbol"),
#         "open": safe_convert_float(raw_feed_data.get("open")),
#         "high": safe_convert_float(raw_feed_data.get("high")),
#         "low": safe_convert_float(raw_feed_data.get("low")),
#         "close": safe_convert_float(raw_feed_data.get("close")),
#         "volume": safe_convert_int(raw_feed_data.get("volume")),
#         "ltt": safe_parse_datetime(raw_feed_data.get("ltt")),
#         "oi": safe_convert_int(raw_feed_data.get("OI")),
#         "change": safe_convert_float(raw_feed_data.get("change")),
#         "bPrice": safe_convert_float(raw_feed_data.get("bPrice")),
#         "bQty": safe_convert_int(raw_feed_data.get("bQty")),
#         "sPrice": safe_convert_float(raw_feed_data.get("sPrice")),
#         "sQty": safe_convert_int(raw_feed_data.get("sQty")),
#         "ltq": safe_convert_int(raw_feed_data.get("ltq")),
#         "avgPrice": safe_convert_float(raw_feed_data.get("avgPrice")),
#         "quotes": raw_feed_data.get("quotes"),
#         "CHNGOI": safe_convert_int(raw_feed_data.get("CHNGOI")),
#         "ttq": safe_convert_int(raw_feed_data.get("ttq")),
#         "totalBuyQt": safe_convert_int(raw_feed_data.get("totalBuyQt")),
#         "totalSellQ": safe_convert_int(raw_feed_data.get("totalSellQ")),
#         "ttv": raw_feed_data.get("ttv"),
#         "trend": raw_feed_data.get("trend"),
#         "lowerCktLm": safe_convert_float(raw_feed_data.get("lowerCktLm")),
#         "upperCktLm": safe_convert_float(raw_feed_data.get("upperCktLm")),
#         "exchange": raw_feed_data.get("exchange"),
#         "stock_name": raw_feed_data.get("stock_name"),
#         "product_type": raw_feed_data.get("product_type"),
#         "expiry_date": safe_parse_datetime(raw_feed_data.get("expiry_date")),
#         "strike_price": safe_convert_float(raw_feed_data.get("strike_price")),
#         "right": raw_feed_data.get("right")
#     }

#     return standardized_feed