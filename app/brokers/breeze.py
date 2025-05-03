import os
import zipfile
import pandas as pd
import numpy as np
import logging
import threading
from datetime import datetime, timedelta,date
from typing import List,Union, Dict, Any,Optional,Type,Callable
from schemas import symbol as symbol_schema
from models import symbol as symbol_model
from schemas import historical_data as historical_data_schema
from services import rabbitmq_service
from services.redis_service import RedisService

from models import tick_data as tick_data_model
from sqlalchemy.orm import Session
import aiohttp
import pytz
from zoneinfo import ZoneInfo
from core.config import Settings
from sqlalchemy import inspect
import asyncio
from concurrent.futures import ThreadPoolExecutor
from breeze_connect import BreezeConnect
from utils.data_utils import safe_parse_datetime,safe_convert,safe_convert_bool,safe_convert_int,safe_convert_float  # Import the library
from dateutil import parser
import json
import time as t
from core.dependencies import get_app
class Broker:
    def __init__(self, broker_record):
        """
        Initializes the Breeze broker connection.
        """
        self.broker_record = broker_record
        self.api_key = broker_record.api_key
        self.api_secret = broker_record.api_secret
        self.symbol_url = broker_record.symbol_url  # Ensure this is in settings
        self.broker = None  # BreezeConnect instance
        self.broker_session = None
        self.executor = ThreadPoolExecutor(max_workers=5)  # Adjust as needed
        self.working_directory = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),  # app directory
            "tmp"
        )
        self.simulation_task = None
        self.app=get_app()
        self.redis_service =self.app.state.connections['redis']
        self.tick_queue: asyncio.Queue = asyncio.Queue()
        os.makedirs(self.working_directory, exist_ok=True) 
        self.instrument_keys = [
            "NSE@NIFTY@options@31-12-2024@call@19800",
            "NSE@NIFTY@futures@31-12-2024",
            "BSE@RELIANCE@equities"
        ]  # Sample instrument keys
        self.tick_count = 0
        
        
    async def start_simulation(self, interval: float = 1.0):
        """
        Starts simulating tick data at the specified interval.

        Args:
            interval (float): Interval in seconds between each tick.
        """
        try:
            if self.simulation_task and not self.simulation_task.done():
                logging.warning("Simulation is already running.")
                return {"message": "Simulation is already running."}

            async def simulate_ticks():
                while True:
                    for instrument_key in self.instrument_keys:
                        tick_data = self._generate_mock_tick(instrument_key)
                        await self._handle_tick_data(tick_data)  # Handle the tick data
                        logging.info(f"Mock: Sent tick for {instrument_key}")
                        await asyncio.sleep(interval)  # Wait for the interval

            logging.info("Starting tick simulation...")
            self.simulation_task = asyncio.create_task(simulate_ticks())
            return {"message": "Tick simulation started."}

        except Exception as e:
            logging.error(f"Error in start_simulation: {e}")
            raise

    async def stop_simulation(self):
        """
        Stops the ongoing tick simulation.
        """
        try:
            if self.simulation_task and not self.simulation_task.done():
                self.simulation_task.cancel()
                try:
                    await self.simulation_task  # Wait for cancellation
                except asyncio.CancelledError:
                    logging.info("Simulation task canceled successfully.")
                self.simulation_task = None  # Reset the task
                return {"message": "Tick simulation stopped."}
            else:
                logging.warning("No active simulation to stop.")
                return {"message": "No active simulation to stop."}

        except Exception as e:
            logging.error(f"Error in stop_simulation: {e}")
            raise



    async def _initialize_connection(self):
        """
        Establishes the connection with the Breeze API.
        """
        try:
            # Check if session is valid
            if self.broker_record.session_key_date != datetime.now().date():
                raise Exception("Session token is expired")

            # Initialize BreezeConnect and generate session
            self.broker = BreezeConnect(api_key=self.api_key)
            self.broker.generate_session(
                api_secret=self.api_secret,
                session_token=self.broker_record.session_token
            )
            self.broker_session = True  # Setting to True as it is used for checking.
            logging.info("Breeze: Connection initialized successfully.")
            self.broker.ws_connect()
            self.broker.on_ticks = self._handle_tick_data_sync
            logging.info(f"Breeze: Ready to receive real time feeds")
        except Exception as e:
            logging.error(f"Breeze: Failed to initialize connection: {e}")
            raise
    def get_model_column_names(self,model_class):
        """
        Retrieves a list of column names from a SQLAlchemy model.

        Args:
            model_class: The SQLAlchemy model class (e.g., MyModel).

        Returns:
            A list of column names (strings).
        """

        inspector = inspect(model_class)
        column_names = [column.name for column in inspector.columns]
        return column_names
    async def disconnect(self):
        """
        Closes the connection with the Breeze API.
        """
        try:
            if self.broker:
                self.broker.ws_disconnect()  # If BreezeConnect has a logout method
            self.broker = None
            self.broker_session = None
            logging.info("Breeze: Connection closed.")
        except Exception as e:
            logging.error(f"Breeze: Failed to disconnect: {e}")

    async def get_symbols(self) -> List[symbol_schema.SymbolCreate]:
        """
        Retrieves and processes symbol data from Breeze.
        """
        try:
            symbol_target_dir = self.working_directory
            if not self._is_downloaded_today(os.path.join(symbol_target_dir, os.path.basename(self.symbol_url))):
                downloaded_file = await self._download_file(self.symbol_url, symbol_target_dir)
                if downloaded_file:
                    await self._extract_zip(downloaded_file, symbol_target_dir)
                    logging.info("Breeze: Downloaded and extracted symbol files.")

            # Process files in parallel
            files_to_process = ["FONSEScripMaster.txt", "FOBSEScripMaster.txt", "NSEScripMaster.txt", "BSEScripMaster.txt"]
            file_paths = [os.path.join(symbol_target_dir, file) for file in files_to_process]
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(self.executor, self._process_data_file, file_path) for file_path in file_paths]
            results = await asyncio.gather(*tasks)
            symbols = []
            for result in results:
                symbols.extend(result)

            return symbols
            #symbols = [symbol_schema.SymbolCreate(**row) for row in all_symbols_data.to_dict(orient='records')]

            #return symbols

        except Exception as e:
            logging.error(f"Breeze: Error getting symbols: {e}")
            raise

    def _process_data_file(self, filepath) -> pd.DataFrame:
        """
        Processes a single symbol data file.
        """
        try:
            def generate_instrument_key(row):
                """Generates instrument_key based on the Product_Type and other columns."""
                if row['Product_Type'] == 'futures':
                    return f"{row['Exchange']}@{row['Stock_Code']}@{row['Product_Type']}@{row['Expiry_Date']}"
                elif row['Product_Type'] == 'options':
                    return f"{row['Exchange']}@{row['Stock_Code']}@{row['Product_Type']}@{row['Expiry_Date']}@{row['Option_Type']}@{row['Strike_Price']}"
                elif row['Product_Type'] == 'equities':
                    return f"{row['Exchange']}@{row['Stock_Code']}@{row['Product_Type']}"

            # Apply the function once across the DataFrame

            logging.info(f"Breeze: Starting process of {filepath}")
            db_columns = [
                "instrument_key", "Exchange", "Stock_Code", "Product_Type", "Expiry_Date",
                "Option_Type", "Strike_Price", "Stock_Token", "Instrument_Name", "Series",
                "Ca_Level", "Permitted_To_Trade", "Issue_Capital", "Warning_Qty", "Freeze_Qty",
                "Credit_Rating", "Normal_Market_Status", "Odd_Lot_Market_Status", "Spot_Market_Status",
                "Auction_Market_Status", "Normal_Market_Eligibility", "Odd_Lot_Market_Eligibility",
                "Spot_Market_Eligibility", "Auction_Market_Eligibility", "Scrip_Id", "Issue_Rate",
                "Issue_Start_Date", "Interest_Payment_Date", "Issue_Maturity_Date", "Margin_Percentage",
                "Minimum_Lot_Qty", "Lot_Size", "Tick_Size", "Company_Name", "Listing_Date",
                "Expulsion_Date", "Readmission_Date", "Record_Date", "Low_Price_Range",
                "High_Price_Range", "Security_Expiry_Date", "No_Delivery_Start_Date",
                "No_Delivery_End_Date", "Mf", "Aon", "Participant_In_Market_Index",
                "Book_Cls_Start_Date", "Book_Cls_End_Date", "Exercise_Start_Date", "Exercise_End_Date",
                "Old_Token", "Asset_Instrument", "Asset_Name", "Asset_Token", "Intrinsic_Value",
                "Extrinsic_Value", "Exercise_Style", "Egm", "Agm", "Interest", "Bonus", "Rights",
                "Dividends", "Ex_Allowed", "Ex_Rejection_Allowed", "Pl_Allowed", "Is_This_Asset","Board_Lot_Qty", 
                "Date_Of_Delisting","Date_Of_Listing","Face_Value","Freeze_Percent","High_Date","ISIN_Code",
                "Instrument_Type","Issue_Price","Life_Time_High","Life_Time_Low","Low_Date", 	"AVM_Buy_Margin",
	            "AVM_Sell_Margin","BCast_Flag","Group_Name","Market_Lot","NDE_Date","NDS_Date","Nd_Flag","Scrip_Code",
	            "Scrip_Name","Susp_Status","Suspension_Reason","Suspension_Date",
                "Is_Corp_Adjusted", "Local_Update_Datetime", "Delete_Flag", "Remarks", "Base_Price",
                "Exchange_Code", "Refresh_Flag", "Breeze_Token", "Kite_Token"
            ]

            if "FONSEScripMaster.txt" in filepath:
                symbols = pd.read_csv(filepath, header=0)
                # Clean up column names (MOST IMPORTANT)
                symbols.columns = symbols.columns.str.strip().str.replace('"', '', regex=False)
                rename_dict=self.rename_columns_from_list(symbols.columns)
                
                #rename_dict = {col: self.to_snake_case(col).replace(" ","_") for col in symbols.columns}
                symbols.rename(columns=rename_dict, inplace=True)

                Exchange = 'NFO'
                symbols['Exchange'] = Exchange
                symbols['Product_Type'] = np.where(symbols['Series'] == 'FUTURE', 'futures',
                                                np.where(symbols['Series'] == 'OPTION', 'options', symbols['Series']))
                symbols['Option_Type'] = np.where(symbols['Option_Type'] == 'CE', 'call',
                                                np.where(symbols['Option_Type'] == 'PE', 'put', symbols['Option_Type']))
                symbols = symbols.rename(columns={'ShortName': 'Stock_Code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                required_columns = ["Exchange_Code", "Product_Type", "Stock_Code", "Expiry_Date", "Option_Type", "Strike_Price"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")

                if 'Expiry_Date' in symbols:
                    symbols['Expiry_Date'] = symbols['Expiry_Date'].fillna('')

                if 'Option_Type' in symbols:
                    symbols['Option_Type'] = symbols['Option_Type'].fillna('')

                if 'Strike_Price' in symbols:
                    symbols['Strike_Price'] = symbols['Strike_Price'].fillna('')
                symbols['Strike_Price'] = symbols['Strike_Price'].astype(str).str.replace(r'\.0', '', regex=True)
                symbols['Strike_Price'] = pd.to_numeric(symbols['Strike_Price'], errors='coerce')
                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)


                symbols = symbols.sort_values(by=['Stock_Code', 'Product_Type', 'Expiry_Date', 'Option_Type', 'Strike_Price'])

            elif "NSEScripMaster.txt" in filepath:
                symbols = pd.read_csv(filepath, header=0)
                # Clean up column names (MOST IMPORTANT)
                symbols.columns = symbols.columns.str.strip().str.replace('"', '', regex=False)
                rename_dict=self.rename_columns_from_list(symbols.columns)
                #rename_dict = {col: self.to_snake_case(col).replace(" ","_") for col in symbols.columns}
                symbols.rename(columns=rename_dict, inplace=True)
                Exchange = 'NSE'
                symbols['Exchange'] = Exchange
                symbols['Product_Type'] = 'equities'
                symbols = symbols.rename(columns={'ShortName': 'Stock_Code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                required_columns = ["Exchange_Code", "Product_Type", "Stock_Code"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")

                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)

            elif "BSEScripMaster.txt" in filepath and "FOBSEScripMaster.txt" not in filepath:
                symbols = pd.read_csv(filepath, header=0)
                # Clean up column names (MOST IMPORTANT)
                symbols.columns = symbols.columns.str.strip().str.replace('"', '', regex=False)
                rename_dict=self.rename_columns_from_list(symbols.columns)
                #rename_dict = {col: self.to_snake_case(col).replace(" ","_") for col in symbols.columns}
                symbols.rename(columns=rename_dict, inplace=True)
                
                Exchange = 'BSE'
                symbols['Exchange'] = Exchange
                symbols = symbols.rename(columns={'ShortName': 'Stock_Code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                symbols['Product_Type'] = 'equities'
                required_columns = ["Exchange", "Product_Type", "Stock_Code"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")

                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)

            elif "FOBSEScripMaster.txt" in filepath:
                symbols = pd.read_csv(filepath, header=0)
                # Clean up column names (MOST IMPORTANT)
                symbols.columns = symbols.columns.str.strip().str.replace('"', '', regex=False)
                rename_dict=self.rename_columns_from_list(symbols.columns)
                #rename_dict = {col: self.to_snake_case(col).replace(" ","_") for col in symbols.columns}
                symbols.rename(columns=rename_dict, inplace=True)
                Exchange = 'BSO'

                symbols['Exchange'] = Exchange
                symbols['Product_Type'] = np.where(symbols['Series'] == 'FUTURE', 'futures',
                                                np.where(symbols['Series'] == 'OPTION', 'options', symbols['Series']))
                symbols['Option_Type'] = np.where(symbols['Option_Type'] == 'CE', 'call',
                                                np.where(symbols['Option_Type'] == 'PE', 'put', symbols['Option_Type']))
                symbols = symbols.rename(columns={'ShortName': 'Stock_Code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                required_columns = ["Exchange_Code", "Product_Type", "Stock_Code", "Expiry_Date", "Option_Type", "Strike_Price"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")
                if 'Expiry_Date' in symbols:
                    symbols['Expiry_Date'] = symbols['Expiry_Date'].fillna('')

                if 'Option_Type' in symbols:
                    symbols['Option_Type'] = symbols['Option_Type'].fillna('')

                if 'Strike_Price' in symbols:
                    symbols['Strike_Price'] = symbols['Strike_Price'].fillna('')
                symbols['Strike_Price'] = symbols['Strike_Price'].astype(str).str.replace(r'\.0', '', regex=True)
                symbols['Strike_Price'] = pd.to_numeric(symbols['Strike_Price'], errors='coerce')
                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)

                
                symbols = symbols.sort_values(by=['Stock_Code', 'Product_Type', 'Expiry_Date', 'Option_Type', 'Strike_Price'])

            else:
                logging.warning(f"Breeze: Unsupported file: {filepath}")
                return pd.DataFrame()

            symbols = symbols.replace(np.nan, None)
            # Check for empty instrument_key (Added code)
            empty_key_rows = symbols[symbols['instrument_key'] == '']
            if not empty_key_rows.empty:
                logging.warning(f"Breeze: Found {len(empty_key_rows)} rows with empty instrument_key in {filepath}")
                print(f"Rows with empty instrument_key in {filepath}:\n{empty_key_rows}")

            # inspector = inspect(self.db.bind) #This is not available here.
            processed_symbols = pd.DataFrame()
            new_series = []
            for col in db_columns:
                if col in symbols.columns:
                    new_series.append(pd.Series([None] * len(symbols), index=symbols.index, name=col))
                    #processed_symbols[col] = symbols[col]  # Copy the existing column
                else:
                    # Concatenate the new Series with the original DataFrame
                    if new_series:
                        symbols = pd.concat([symbols] + new_series, axis=1)
                    #processed_symbols[col] = pd.Series([None] * len(symbols), index=symbols.index) # Create a new Series
                # Reindex for consistency (optional, but often recommended)
                symbols = symbols.reindex(columns=db_columns, fill_value=None)
            processed_symbols=symbols
            for col in processed_symbols.select_dtypes(include=['object']).columns:
                if processed_symbols[col].dtype == 'object':
                    processed_symbols[col] = processed_symbols[col].str.strip("'")



            # Date Conversions
            if 'Expiry_Date' in symbols.columns:
                symbols['Expiry_Date'] = pd.to_datetime(symbols['Expiry_Date'], errors='coerce')
                symbols['Expiry_Date'] = symbols['Expiry_Date'].dt.tz_localize('Asia/Kolkata').dt.date
            symbols['Local_Update_Datetime']= datetime.now()

            # Boolean Conversions
            boolean_columns = [
                "Permitted_To_Trade", "Ex_Rejection_Allowed", "Pl_Allowed", "Is_This_Asset",
                "Is_Corp_Adjusted", "Delete_Flag", "Refresh_Flag"
            ]
            for col in boolean_columns:
                if col in symbols.columns:
                    symbols[col] = symbols[col].apply(lambda x: True if x == 1 or str(x).lower() == 'true' else False if x == 0 or str(x).lower() == 'false' else None)
            symbols_list = []

            for index, row in symbols.iterrows():
                symbol_dict = row.to_dict()  # Convert each row to a dictionary
                processed_row = self.process_data_row(symbol_dict) #Process the row
                symbols_list.append(processed_row)

            return symbols_list
        except Exception as e:
            logging.error(f"Breeze: Error processing data file {filepath}: {e}")

            return pd.DataFrame()

    def rename_columns_from_list(self, target_columns: List[str]) -> Dict[str, str]:
        """
        Creates a mapping dictionary to rename DataFrame columns based on a target list,
        ignoring case and underscores.

        Args:
            symbols: The input Pandas DataFrame.
            target_columns: A list of target column names (e.g., your schema fields).

        Returns:
            A dictionary where keys are original column names and values are new column names.
        """
        column_list = self.get_model_column_names(symbol_model.Symbol)
        rename_dict: Dict[str, str] = {}  # Initialize an empty dictionary

        for original_col in column_list:
            normalized_original = original_col.strip("_ ").lower().replace("_", "")
            for target_col in target_columns:
                normalized_target = target_col.strip("_ ").lower().replace("_", "")
                if normalized_original == normalized_target:
                    rename_dict[target_col] = original_col
                    break  # Stop searching once a match is found
        if "AuctionlMarketEligibility" in target_columns:
            rename_dict["AuctionlMarketEligibility"] = "Auction_Market_Eligibility"
        if "OddLotlMarketEligibility" in target_columns:
            rename_dict["OddLotlMarketEligibility"] = "Odd_Lot_Market_Eligibility"
        return rename_dict


    def process_data_row(self,row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a single data row to match the Symbol schema.
        """
        processed_row: Dict[str, Any] = {}

        # def safe_convert(value: Any, target_type: Type, default: Optional[Any] = None):
        #     """
        #     Safely converts a value to the target type, handling None and potential conversion errors.
        #     """
        #     if value is None:
        #         return default
        #     try:
        #         return target_type(value)
        #     except (ValueError, TypeError):
        #         return default
        #     except Exception as e:
        #         print(f"Unexpected error during conversion: {e}")
        #         return default
            
        # def safe_convert_int(value: Any, default: Optional[int] = None) -> Optional[int]:
        #     """
        #     Safely converts a value to an integer, handling None and potential errors.

        #     Args:
        #         value: The value to convert.
        #         default: The value to return if conversion fails or if value is None.

        #     Returns:
        #         The converted integer, or the default if conversion fails or value is None.
        #     """
        #     if value is None:
        #         return default
        #     try:
        #         return int(value)
        #     except (ValueError, TypeError):
        #         return default
        #     except Exception as e:
        #         print(f"Unexpected error converting to int: {e}")  # Log unexpected errors
        #         return default
        # def safe_convert_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        #     """
        #     Safely converts a value to a float, handling None and potential errors.

        #     Args:
        #         value: The value to convert.
        #         default: The value to return if conversion fails or if value is None.

        #     Returns:
        #         The converted float, or the default if conversion fails or value is None.
        #     """
        #     if value is None:
        #         return default
        #     try:
        #         return float(value)
        #     except (ValueError, TypeError):
        #         return default
        #     except Exception as e:
        #         print(f"Unexpected error converting to float: {e}")  # Log unexpected errors
        #         return default
        # def safe_convert_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
        #     """
        #     Safely converts a value to a boolean, handling None and various representations.

        #     Args:
        #         value: The value to convert.
        #         default: The value to return if conversion fails or if value is None.

        #     Returns:
        #         The converted boolean, or the default if conversion fails or value is None.
        #     """
        #     if value is None:
        #         return default
        #     if isinstance(value, (int, float)):
        #         return bool(value)  # 0 -> False, non-zero -> True
        #     elif isinstance(value, str):
        #         if value.lower() in ("true", "1", "yes"):
        #             return True
        #         elif value.lower() in ("false", "0", "no"):
        #             return False
        #         else:
        #             return default  # Return default for invalid strings
        #     else:
        #         try:
        #             return bool(value)  # General boolean conversion
        #         except (ValueError, TypeError):
        #             return default
        #         except Exception as e:
        #             print(f"Unexpected error converting to bool: {e}")
        #             return default
        # def safe_parse_datetime(date_input: Union[str, datetime, date, pd.Timestamp]) -> Optional[datetime]:
        #     """
        #     Safely parses a string or datetime-like object into a datetime object.
        #     Handles pd.NaT.
        #     """
        #     if date_input is None or pd.isna(date_input):
        #         return None
        #     if isinstance(date_input, datetime):
        #         return date_input
        #     if isinstance(date_input, date):
        #         return datetime(date_input.year, date_input.month, date_input.day)
        #     if isinstance(date_input, str):
        #         formats = [
        #             '%Y-%m-%d %H:%M:%S.%f',
        #             '%Y-%m-%d %H:%M:%S',
        #             '%Y-%m-%d',
        #             '%d-%b-%Y',
        #             '%d-%m-%Y %H:%M:%S',
        #             '%d/%m/%Y',
        #             '%m/%d/%Y',
        #             '%Y/%m/%d',
        #             '%Y%m%d',
        #             '%d%m%Y'
        #         ]
        #         for fmt in formats:
        #             try:
        #                 return datetime.strptime(date_input, fmt)
        #             except ValueError:
        #                 pass  # Try the next format
        #     return None

        schema_fields: Dict[str, Type] = {
            "instrument_key": str,
            "Stock_Token": str,
            "Instrument_Name": str,
            "Exchange":str,
            "Stock_Code": str,
            "Product_Type": str,
            "Expiry_Date": safe_parse_datetime,
            "Option_Type": str,
            "Strike_Price": safe_convert_float,
            "Series": str,
            "Ca_Level": str,
            "Permitted_To_Trade": safe_convert_bool,  # Use safe_convert_bool
            "Issue_Capital": safe_convert_float,    # Use safe_convert_float
            "Warning_Qty": safe_convert_int,      # Use safe_convert_int
            "Freeze_Qty": safe_convert_int,        # Use safe_convert_int
            "Credit_Rating": str,
            "Normal_Market_Status": str,
            "Odd_Lot_Market_Status": str,
            "Spot_Market_Status": str,
            "Auction_Market_Status": str,
            "Normal_Market_Eligibility": str,
            "Odd_Lot_Market_Eligibility": str,
            "Spot_Market_Eligibility": str,
            "Auction_Market_Eligibility": str,
            "Scrip_Id": str,
            "Issue_Rate": safe_convert_float,    # Use safe_convert_float
            "Issue_Start_Date": safe_parse_datetime,
            "Interest_Payment_Date": safe_parse_datetime,
            "Issue_Maturity_Date": safe_parse_datetime,
            "Margin_Percentage": safe_convert_float,  # Use safe_convert_float
            "Minimum_Lot_Qty": safe_convert_int,    # Use safe_convert_int
            "Lot_Size": safe_convert_int,          # Use safe_convert_int
            "Tick_Size": safe_convert_float,      # Use safe_convert_float
            "Company_Name": str,
            "Listing_Date": safe_parse_datetime,
            "Expulsion_Date": safe_parse_datetime,
            "Readmission_Date": safe_parse_datetime,
            "Record_Date": safe_parse_datetime,
            "Low_Price_Range": safe_convert_float,  # Use safe_convert_float
            "High_Price_Range": safe_convert_float, # Use safe_convert_float
            "Security_Expiry_Date": safe_parse_datetime,
            "No_Delivery_Start_Date": safe_parse_datetime,
            "No_Delivery_End_Date": safe_parse_datetime,
            "Mf": str,
            "Aon": str,
            "Participant_In_Market_Index": str,
            "Book_Cls_Start_Date": safe_parse_datetime,
            "Book_Cls_End_Date": safe_parse_datetime,
            "Excercise_Start_Date": safe_parse_datetime,
            "Excercise_End_Date": safe_parse_datetime,
            "Old_Token": str,
            "Asset_Instrument": str,
            "Asset_Name": str,
            "Asset_Token": safe_convert_int,      # Use safe_convert_int
            "Intrinsic_Value": safe_convert_float,  # Use safe_convert_float
            "Extrinsic_Value": safe_convert_float,  # Use safe_convert_float
            "Excercise_Style": str,
            "Egm": str,
            "Agm": str,
            "Interest": str,
            "Bonus": str,
            "Rights": str,
            "Dividends": str,
            "Ex_Allowed": str,
            "Ex_Rejection_Allowed": safe_convert_bool, # Use safe_convert_bool
            "Pl_Allowed": safe_convert_bool,         # Use safe_convert_bool
            "Is_This_Asset": safe_convert_bool,     # Use safe_convert_bool
            "Is_Corp_Adjusted": safe_convert_bool,  # Use safe_convert_bool
            "Local_Update_Datetime": safe_parse_datetime,
            "Delete_Flag": safe_convert_bool,       # Use safe_convert_bool
            "Remarks": str,
            "Base_Price": safe_convert_float,      # Use safe_convert_float
            "Exchange_Code": str,
            "Refresh_Flag": safe_convert_bool,
            "Breeze_Token": str,
            "Kite_Token": str,
        }

        for schema_field, schema_type in schema_fields.items():
            if schema_field in row:
                if schema_type is str:
                    processed_row[schema_field] = str(row[schema_field])
                elif schema_type is int:
                    processed_row[schema_field] = int(row[schema_field])
                elif schema_type is float:
                    processed_row[schema_field] = float(row[schema_field])
                elif schema_type is bool:
                    if isinstance(row[schema_field], (int, float)):
                        processed_row[schema_field] = bool(row[schema_field])
                    elif isinstance(row[schema_field], str):
                        processed_row[schema_field] = row[schema_field].lower() in ("true", "1", "yes")
                    else:
                        processed_row[schema_field] = bool(row[schema_field])
                elif schema_type is safe_parse_datetime:
                    processed_row[schema_field] = safe_parse_datetime(row[schema_field])
                else:
                    processed_row[schema_field] = safe_convert(row[schema_field], schema_type)
            else:
                processed_row[schema_field] = None

        return processed_row

    async def get_symbol_token(self, instrument_key: str) -> str:
        """
        Retrieves the Breeze-specific token for a given instrument key.
        """
        try:
            # Implement Breeze API call to get token
            # Replace with your actual Breeze API call
            token = self.broker.get_security_master(Stock_Code=instrument_key, Exchange_Code="NSE")  # or BSE.
            token_value = token['Success'][0]['Token']
            logging.info(f"Breeze: Getting token for {instrument_key}")
            # await asyncio.sleep(0.1) #dummy
            # token = f"BREEZE_TOKEN_{instrument_key}"  # Dummy token
            return token_value
        except Exception as e:
            logging.error(f"Breeze: Error getting symbol token for {instrument_key}: {e}")
            raise

    def get_iso_time(self,input_datetime: datetime) -> str:
        """
        Converts a string to an ISO 8601 formatted datetime string.
        """
        try:
            # Parse the input datetime string to a datetime object
            #dt_with_tz = datetime.fromisoformat(input_datetime)

            # Convert to UTC
            #dt_in_utc = dt_with_tz.astimezone(pytz.utc)

            # Format the datetime object to ISO 8601 UTC format
            output_datetime = input_datetime.strftime('%Y-%m-%dT%H:%M:%S.000Z')

            return output_datetime

        except ValueError:
            return None


    def get_historical_data(self, data_request: historical_data_schema.HistoricalDataRequest) -> pd.DataFrame:
        """
        Fetches historical data from the Breeze API and returns a pandas DataFrame.
        Handles partial datasets, removes duplicates, ensures proper datetime parsing,
        and validates the index.
        """
        try:
            # Parse and validate the date range
            from_date = parser.isoparse(data_request.from_date)
            to_date = parser.isoparse(data_request.to_date)
            current_datetime = datetime.now().astimezone(ZoneInfo("Asia/Kolkata"))

            if to_date > current_datetime:
                logging.warning(f"to_date {to_date} exceeds current datetime; restricting to {current_datetime}.")
                to_date = current_datetime

            if from_date >= to_date:
                raise ValueError(f"from_date {from_date} is greater than to_date; Invalid Request.")

            all_data = pd.DataFrame()
            interval_offsets = {
                "1second": pd.DateOffset(seconds=1),
                "1minute": pd.DateOffset(minutes=1),
                "5minute": pd.DateOffset(minutes=5),
                "30minute": pd.DateOffset(minutes=30),
                "1day": pd.DateOffset(days=1),
            }

            # Validate the interval
            interval = data_request.interval or "1minute"
            if interval not in interval_offsets:
                raise ValueError(f"Invalid interval: {interval}. Allowable intervals: {list(interval_offsets.keys())}")

            # Extract parameters from instrument_key
            parts = data_request.instrument_key.split("@")
            if len(parts) < 3:
                raise ValueError("instrument_key is missing required parts.")

            exchange = parts[0]
            stock_code = parts[1]
            product_type = parts[2]
            expiry_date = parts[3] if len(parts) > 3 else None
            right = parts[4] if len(parts) > 4 else None
            strike_price = parts[5] if len(parts) > 5 else None

            # Apply conditional logic based on product_type
            if product_type == "equities":
                expiry_date, right, strike_price = None, None, None
            elif product_type == "futures":
                right, strike_price = None, None
            elif product_type == "options" and (not expiry_date or not right or not strike_price):
                raise ValueError("Missing required parameters for options product type.")

            current_date = from_date
            while current_date < to_date:
                to_date_chunk = min(to_date, current_date + interval_offsets[interval])
                to_date_str = self.get_iso_time(to_date_chunk)

                # Call the Breeze API
                ret = self.broker.get_historical_data_v2(
                    interval=interval,
                    from_date=self.get_iso_time(current_date),
                    to_date=to_date_str,
                    stock_code=stock_code,
                    exchange_code=exchange,
                    product_type=product_type,
                    expiry_date=expiry_date,
                    right=right,
                    strike_price=strike_price,
                )

                data = pd.DataFrame(ret.get("Success", []))
                data["instrument_key"] = data_request.instrument_key
                data["interval"] = data_request.interval
                # Validate datetime column and parse it
                if data.empty:
                    # Log and terminate the loop when no more data is returned
                    logging.info(f"No data returned for the chunk from {current_date} to {to_date_chunk}. Terminating fetch.")
                    break  # Exit the loop as there is no more data to fetch

                data["datetime"] = pd.to_datetime(data["datetime"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")

                # Drop rows with invalid datetime or duplicate records
                data = data.dropna(subset=["datetime"])
                data = data.drop_duplicates(subset=["datetime", "instrument_key", "interval"])

                # Concatenate new data into all_data
                all_data = pd.concat([all_data, data]).drop_duplicates(subset=["datetime", "instrument_key", "interval"])

                # Update current_date for the next chunk
                max_datetime = data["datetime"].max()
                current_date = max_datetime + interval_offsets[interval]

            # Post-process all_data
            if not all_data.empty:
                all_data.set_index("datetime", inplace=True)
                all_data.index = pd.to_datetime(all_data.index, errors="coerce")
                #all_data.index = all_data.index.tz_localize("Asia/Kolkata")

                # Add additional columns for identification


                # Convert DataFrame to a list of dictionaries for output
                historical_data_list = all_data.reset_index().to_dict(orient="records")
                return historical_data_list

            return []  # Return an empty list if no data was fetched

        except Exception as e:
            logging.error(f"Error fetching historical data: {e}")
            raise

    def subscribe(self,instrument_key: str = None,broker_token: str = None,interval: str = '1second',get_market_depth: bool = False,get_exchange_quotes: bool = True):
        """
        Subscribes to real-time feeds for a given symbol.
        Now takes instrument_key, broker_token and interval as input.
        """
        try:
            # Check that at least one parameter is provided
            if instrument_key is None and broker_token is None:
                raise ValueError("Either 'instrument_key' or 'broker_token' must have a value. Both cannot be None.")
            subscribe_dict={}
            subscribe_dict['interval'] = interval
            subscribe_dict['get_market_depth'] = get_market_depth
            subscribe_dict['get_exchange_quotes'] =get_exchange_quotes

            if instrument_key is not None:
                parts = instrument_key.split("@")
                if len(parts) < 3:
                    raise ValueError("instrument_key is missing required parts.")
                subscribe_dict['exchange_code'] = parts[0]
                subscribe_dict['stock_code'] = parts[1]
                subscribe_dict['product_type'] = parts[2]

                if parts[2] == "futures":
                    subscribe_dict['expiry_date'] = parts[3]

                if parts[2] == "options" and (not parts[3] or not parts[4] or not parts[5]):
                    subscribe_dict['expiry_date'] = parts[3]
                    subscribe_dict['right'] = parts[4]
                    subscribe_dict['strike_price'] = parts[5]
                    raise ValueError("Missing required parameters for options product type.")
                # Implement Breeze API subscription logic
                self.broker.subscribe_feeds(**subscribe_dict)
                logging.info(f"Breeze: Subscribing to {instrument_key} with interval {interval}")
            elif broker_token is not None:
                subscribe_dict['stock_token'] = broker_token
                self.broker.subscribe_feeds(**subscribe_dict)
                logging.info(f"Breeze: Subscribing to  token {broker_token} with interval {interval}")                
        except Exception as e:
            logging.error(f"Breeze: Error subscribing to {instrument_key}: {e}")
            raise

    def unsubscribe(self,instrument_key: str = None,broker_token: str = None):
        """
        unSubscribes to real-time feeds for a given symbol.
        Now takes instrument_key, broker_token as input.
        """
        try:
            # Check that at least one parameter is provided
            if instrument_key is None and broker_token is None:
                raise ValueError("Either 'instrument_key' or 'broker_token' must have a value. Both cannot be None.")
            unsubscribe_dict={}
            if instrument_key is not None:
                parts = instrument_key.split("@")
                if len(parts) < 3:
                    raise ValueError("instrument_key is missing required parts.")
                unsubscribe_dict['exchange_code'] = parts[0]
                unsubscribe_dict['stock_code'] = parts[1]
                unsubscribe_dict['product_type'] = parts[2]

                if parts[2] == "futures":
                    unsubscribe_dict['expiry_date'] = parts[3]

                if parts[2] == "options" and (not parts[3] or not parts[4] or not parts[5]):
                    unsubscribe_dict['expiry_date'] = parts[3]
                    unsubscribe_dict['right'] = parts[4]
                    unsubscribe_dict['strike_price'] = parts[5]
                    raise ValueError("Missing required parameters for options product type.")
                # Implement Breeze API subscription logic
                self.broker.unsubscribe_feeds(**unsubscribe_dict)
                logging.info(f"Breeze: unsubscribing to {instrument_key}")
            elif broker_token is not None:
                unsubscribe_dict['stock_token'] = broker_token
                self.broker.unsubscribe_feeds(**unsubscribe_dict)
                logging.info(f"Breeze: unsubscribing to  token {broker_token}")                
        except Exception as e:
            logging.error(f"Breeze: Error unsubscribing to {instrument_key}: {e}")
            raise


    async def process_feed(self, feed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes real-time feed data.
        Returns a dictionary, all datetimes are IST.
        """
        try:
            exchange = str(feed_data.get("exchange", "UNKNOWN"))
            stock_name = str(feed_data.get("stock_name", "UNKNOWN"))
            product_type = str(feed_data.get("product_type", "UNKNOWN"))
            expiry_date = str(feed_data.get("expiry_date", ""))
            right = str(feed_data.get("right", ""))
            strike_price = str(feed_data.get("strike_price", ""))

            # Construct instrument key
            instrument_key = f"{exchange}@{stock_name}@{product_type}"
            if product_type.lower() == "futures" and expiry_date:
                instrument_key += f"@{expiry_date}"
            elif product_type.lower() == "options" and expiry_date and right and strike_price:
                instrument_key += f"@{expiry_date}@{right}@{strike_price}"
            # Standardized tick data, replacing None with appropriate defaults
            processed_feed = {
                "instrument_key": instrument_key,
                "symbol": str(feed_data.get("symbol", "UNKNOWN")),
                "open": float(feed_data.get("open", 0.0)),
                "last": float(feed_data.get("last", 0.0)),
                "high": float(feed_data.get("high", 0.0)),
                "low": float(feed_data.get("low", 0.0)),
                "change": float(feed_data.get("change", 0.0)),
                "bPrice": float(feed_data.get("bPrice", 0.0)),
                "bQty": int(feed_data.get("bQty", 0)),
                "sPrice": float(feed_data.get("sPrice", 0.0)),
                "sQty": int(feed_data.get("sQty", 0)),
                "ltq": int(feed_data.get("ltq", 0)),
                "avgPrice": float(feed_data.get("avgPrice", 0.0)),
                "quotes": str(feed_data.get("quotes", "")),
                "OI": int(feed_data.get("OI", 0)),
                "CHNGOI": int(feed_data.get("CHNGOI", 0)),
                "ttq": int(feed_data.get("ttq", 0)),
                "totalBuyQt": int(feed_data.get("totalBuyQt", 0)),
                "totalSellQ": int(feed_data.get("totalSellQ", 0)),
                "ttv": str(feed_data.get("ttv", "")),
                "trend": str(feed_data.get("trend", "")),
                "lowerCktLm": float(feed_data.get("lowerCktLm", 0.0)),
                "upperCktLm": float(feed_data.get("upperCktLm", 0.0)),
                "ltt": str(feed_data.get("ltt", "1970-01-01T00:00:00+00:00")),  # Default time
                "close": float(feed_data.get("close", 0.0)),
                "exchange": exchange,
                "stock_code": stock_name,
                "product_type": product_type,
                "expiry_date": expiry_date,
                "strike_price": strike_price,
                "right": right,
                "state": "U",  # Unprocessed state
                }
            logging.info(f"Breeze: Processed feed for {processed_feed['instrument_key']}")
            return processed_feed
        except Exception as e:
            logging.error(f"Breeze: Error processing feed data: {e}")
            raise

    def _handle_tick_data_sync(self, tick_data: Dict[str, Any]):
        """
        Synchronous wrapper to call the async _handle_tick_data in a FastAPI-compatible way.
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(self._handle_tick_data(tick_data), loop)
            else:
                logging.error("FastAPI event loop is not running. Tick processing may fail.")
        except Exception as e:
            logging.error(f"Breeze: Error in _handle_tick_data_sync: {e}")

    async def _handle_tick_data(self, tick_data: Dict[str, Any]):
        """
        Asynchronous callback function to handle incoming tick data from the broker.
        This should be as lightweight as possible.
        """
        try:
            # 1. Standardize the tick data format
            processed_feed = await self.process_feed(tick_data)

            # 2. Add the processed feed to RedisService for batching
            redis = self.app.state.connections.get("redis")  # Get the Redis connection from app state
            if not hasattr(self, "redis_service"):
                logging.error("RedisService is not initialized.")
                return

            await self.app.state.redis_service.add_tick(processed_feed)
            logging.info(f"Breeze: Tick data added to Redis batch - {processed_feed}")

        except Exception as e:
            logging.error(f"Breeze: Error handling tick data: {e}")

    def _is_downloaded_today(self, filepath):
        """
        Checks if the file was downloaded today.
        """
        if not os.path.exists(filepath):
            return False
        return datetime.fromtimestamp(os.path.getmtime(filepath)).date() == datetime.now().date()

    async def _download_file(self,url: str, target_dir: str) -> Optional[str]:
        """
        Downloads a file asynchronously.

        Args:
            url: The URL of the file to download.
            target_dir: The directory to save the downloaded file to.

        Returns:
            The path to the downloaded file on success, None on failure.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        filename = os.path.join(target_dir, os.path.basename(url))
                        with open(filename, 'wb') as f:
                            while True:
                                chunk = await response.content.readany()
                                if not chunk:
                                    break
                                f.write(chunk)
                        logging.info(f"Downloaded {url} to {filename}")
                        return filename
                    else:
                        logging.error(f"Failed to download {url}: {response.status}")
                        return None  # Indicate failure
        except aiohttp.ClientError as e:
            logging.error(f"aiohttp client error downloading {url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error downloading {url}: {e}")
            return None

    async def _extract_zip(self, zip_file_path, target_dir):
        """
        Extracts all files from a zip archive asynchronously.
        """
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self.executor, self._extract_zip_sync, zip_file_path, target_dir)
        except Exception as e:
            logging.error(f"Breeze: Error extracting zip file: {e}")
            raise

    def _extract_zip_sync(self, zip_file_path, target_dir):
        """
        Synchronously extracts all files from a zip archive.
        """
        with zipfile.ZipFile(zip_file_path, 'r') as zip_obj:
            zip_obj.extractall(target_dir)

    def _custom_date(self, date_input):
        '''
        custom date function
        '''
        if date_input is None:
            return self._custom_date(datetime.now())
        elif isinstance(date_input, datetime):
            datetime_Z = date_input.replace(tzinfo=ZoneInfo('Asia/Kolkata')).astimezone(ZoneInfo('UTC'))
            datetime_K = date_input.replace(tzinfo=ZoneInfo('Asia/Kolkata'))
            datetime_N = date_input
        elif isinstance(date_input, str):
            try:
                datetime_K = datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
                datetime_Z = datetime_K.replace(tzinfo=ZoneInfo('Asia/Kolkata')).astimezone(ZoneInfo('UTC'))
                datetime_N = datetime_K
            except ValueError:
                try:
                    datetime_K = datetime.strptime(date_input, '%Y-%m-%d')
                    datetime_Z = datetime_K.replace(tzinfo=ZoneInfo('Asia/Kolkata')).astimezone(ZoneInfo('UTC'))
                    datetime_N = datetime_K
                except ValueError:
                    datetime_Z = None
                    datetime_K = None
                    datetime_N = None
        else:
            datetime_Z = None
            datetime_K = None
            datetime_N = None

        return type('obj', (object,), {'datetime_Z': datetime_Z, 'datetime_K': datetime_K, 'datetime': datetime_N})()
    

    async def simulate_ticks(self, interval: float = 1.0):
        """
        Simulates sending tick data at the specified interval.

        Args:
            interval: Time in seconds between each tick (default: 1 second).
        """
        try:
            await asyncio.sleep(10) 
            while True:
                for instrument_key in self.instrument_keys:
                    tick_data = self._generate_mock_tick(instrument_key)
                    await self._handle_tick_data(tick_data)  # Call the callback
                    logging.info(f"Mock: Sent tick for {instrument_key}")
                    self.tick_count += 1
                    t.sleep(interval)  # Wait for the interval
                if self.tick_count > 100:
                    break
        except Exception as e:
            logging.error(f"MockBroker error: {e}")

    def _generate_mock_tick(self, instrument_key: str) -> Dict[str, Any]:
        """
        Generates a mock tick data dictionary.
        """
        # Example data - customize this to your needs
        exchange, stock_name, product_type, *rest = instrument_key.split("@")
        expiry_date = rest[0] if len(rest) > 0 else None
        option_type = rest[1] if len(rest) > 1 else None
        strike_price = rest[2] if len(rest) > 2 else None

        return {
            "instrument_key": instrument_key,
            "symbol": f"{stock_name}_{self.tick_count}",
            "open": 100.0 + (self.tick_count % 10),
            "high": 105.0 + (self.tick_count % 10),
            "low": 95.0 + (self.tick_count % 10),
            "close": 102.0 + (self.tick_count % 10),
            "volume": 1000 + self.tick_count,
            "ltt": "2025-02-12T12:12:55+05:30",  # Example IST
            "exchange": exchange,
            "stock_name": stock_name,
            "product_type": product_type,
            "expiry_date": expiry_date,
            "strike_price": strike_price,
            "right": option_type,
            "oi": 100,
            "change": 1.0,
            "bPrice": 100.0,
            "bQty": 100,
            "sPrice": 101.0,
            "sQty": 100,
            "ttq": 100,
            "avgPrice": 100.0,
            "quotes": "mocked",
            "CHNGOI": 100,
            "totalBuyQt": 100,
            "totalSellQ": 100,
            "ttv": 100,
            "trend": "up",
            "lowerCktLm": 100.0,
            "upperCktLm": 100.0
        }