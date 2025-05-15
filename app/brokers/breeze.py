import os
import zipfile
import pandas as pd
import numpy as np
import logging
import threading
from datetime import datetime, timedelta,date,timezone
from typing import List,Union, Dict, Any,Optional,Type,Callable
from app.schemas import symbol as symbol_schema
from shared_architecture.db.models.symbol import Symbol as symbol_model
from shared_architecture.utils.other_helpers import safe_parse_datetime,safe_int_conversion,safe_convert,safe_parse_date,safe_convert_bool,safe_convert_int,safe_convert_float  # Import the library
from app.schemas import historical_data as historical_data_schema
from app.services import rabbitmq_service
from shared_architecture.utils.datetime_utils import utc_now
from app.services.market_data_manager import MarketDataManager 
from shared_architecture.utils.logging_utils import log_info, log_warning, log_exception
from app.utils.instrument_key_helper import get_instrument_key
from shared_architecture.db.models import tick_data as tick_data_model
from sqlalchemy.orm import Session
from shared_architecture.connections import get_timescaledb_session
import aiohttp
import pytz
from zoneinfo import ZoneInfo
from app.core.config import Settings
from sqlalchemy import inspect
import asyncio
from concurrent.futures import ThreadPoolExecutor
from breeze_connect import BreezeConnect  # Import the library
from dateutil import parser
import json
import time as t
from sqlalchemy import func, select, update, insert
from app.core.dependencies import get_app
from shared_architecture.db.timescaledb_bulk_utils import bulk_upsert_async
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
                log_warning("Simulation is already running.")
                return {"message": "Simulation is already running."}

            async def simulate_ticks():
                while True:
                    for instrument_key in self.instrument_keys:
                        tick_data = self._generate_mock_tick(instrument_key)
                        await self._handle_tick_data(tick_data)  # Handle the tick data
                        log_info(f"Mock: Sent tick for {instrument_key}")
                        await asyncio.sleep(interval)  # Wait for the interval

            log_info("Starting tick simulation...")
            self.simulation_task = asyncio.create_task(simulate_ticks())
            return {"message": "Tick simulation started."}

        except Exception as e:
            log_exception(f"Error in start_simulation: {e}")
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
                    log_info("Simulation task canceled successfully.")
                self.simulation_task = None  # Reset the task
                return {"message": "Tick simulation stopped."}
            else:
                log_warning("No active simulation to stop.")
                return {"message": "No active simulation to stop."}

        except Exception as e:
            log_exception(f"Error in stop_simulation: {e}")
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
            self.market_data_manager = MarketDataManager(self.redis_service)
            log_info("Breeze: Connection initialized successfully.")
            self.broker.ws_connect()
            self.broker.on_ticks = self._handle_tick_data_sync
            log_info(f"Breeze: Ready to receive real time feeds")
        except Exception as e:
            log_exception(f"Breeze: Failed to initialize connection: {e}")
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
            log_info("Breeze: Connection closed.")
        except Exception as e:
            log_exception(f"Breeze: Failed to disconnect: {e}")

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
                    log_info("Breeze: Downloaded and extracted symbol files.")

            # Process files in parallel
            files_to_process = ["FONSEScripMaster.txt", "FOBSEScripMaster.txt", "NSEScripMaster.txt", "BSEScripMaster.txt"]
            file_paths = [os.path.join(symbol_target_dir, file) for file in files_to_process]
            tasks = [self._process_data_file(file_path) for file_path in file_paths]
            results = await asyncio.gather(*tasks)

            # symbols = [item for result in results for item in result]  # Flatten list



            # # loop = asyncio.get_event_loop()
            # # tasks = [loop.run_in_executor(self.executor, self._process_data_file, file_path) for file_path in file_paths]
            # # results = await asyncio.gather(*tasks)
            # symbols = []
            # for result in results:
            #     symbols.extend(result)
            # log_info("Prepared Symbol data. Next loading...")
            
            # symbols_df = pd.DataFrame(symbols)
            # # Ensure integer fields are properly converted
            # integer_columns = ["Warning_Qty", "Freeze_Qty", "Minimum_Lot_Qty", "Asset_Token"]

            # # Convert these columns to integer, replacing non-convertible values with default (e.g., 0)
            # for col in integer_columns:
            #     symbols_df[col] = pd.to_numeric(symbols_df[col], errors="coerce").fillna(0).astype(int)
            #         # Prepare data for upsert
            # now = utc_now()
            # symbols_df["localupdatedatetime"] = now
            # symbols_df["first_added_datetime"] = now
            # # Now, convert DataFrame to list of dictionaries safely
            # symbols = [symbol_schema.SymbolCreate(**row) for row in symbols_df.to_dict(orient="records")]
            
            # return symbols

        except Exception as e:
            log_exception(f"Breeze: Error getting symbols: {e}")
            raise
    async def _process_data_file(self, filepath) -> pd.DataFrame:
        """
        Processes a single symbol data file.
        """
        try:
            def generate_instrument_key(row):
                """Generates instrument_key based on the Product_Type and other columns."""
                
                if row['product_type'] == 'futures':
                    instrument_key= f"{row['exchange']}@{row['stock_code']}@{row['product_type']}@{row['expiry_date']}"
                elif row['product_type'] == 'options':
                    instrument_key= f"{row['exchange']}@{row['stock_code']}@{row['product_type']}@{row['expiry_date']}@{row['option_type']}@{row['strike_price']}"
                elif row['product_type'] == 'equities':
                    instrument_key= f"{row['exchange']}@{row['stock_code']}@{row['product_type']}"
                return instrument_key
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
                "Book_Cls_Start_Date", "Book_Cls_End_Date", "Excercise_Start_Date", "Excercise_End_Date",
                "Old_Token", "Asset_Instrument", "Asset_Name", "Asset_Token", "Intrinsic_Value",
                "Extrinsic_Value", "Excercise_Style", "Egm", "Agm", "Interest", "Bonus", "Rights",
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
                symbols['exchange'] = Exchange
                symbols['product_type'] = np.where(symbols['series'] == 'FUTURE', 'futures',
                                                np.where(symbols['series'] == 'OPTION', 'options', symbols['series']))
                symbols['option_type'] = np.where(symbols['option_type'] == 'CE', 'call',
                                                np.where(symbols['option_type'] == 'PE', 'put', symbols['option_type']))
                symbols = symbols.rename(columns={'short_name': 'stock_code'})
                symbols = symbols.rename(columns={'Token': 'breeze_token'})
                symbols = symbols.rename(columns={'strikeprice': 'strike_price'})
                symbols = symbols.rename(columns={'expirydate': 'expiry_date'})
                required_columns = ["exchange", "product_type", "stock_code", "expiry_date", "option_type", "strike_price"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")

                if 'expiry_date' in symbols:
                    symbols['expiry_date'] = symbols['expiry_date'].fillna('')

                if 'option_type' in symbols:
                    symbols['option_type'] = symbols['option_type'].fillna('')

                if 'strike_price' in symbols:
                    symbols['strike_price'] = symbols['strike_price'].fillna('')
                symbols['strike_price'] = symbols['strike_price'].astype(str).str.replace(r'\.0', '', regex=True)
                symbols['strike_price'] = pd.to_numeric(symbols['strike_price'], errors='coerce')
                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)


                symbols = symbols.sort_values(by=['stock_code', 'product_type', 'expiry_date', 'option_type', 'strike_price'])

            elif "NSEScripMaster.txt" in filepath:
                symbols = pd.read_csv(filepath, header=0)
                # Clean up column names (MOST IMPORTANT)
                symbols.columns = symbols.columns.str.strip().str.replace('"', '', regex=False)
                rename_dict=self.rename_columns_from_list(symbols.columns)
                #rename_dict = {col: self.to_snake_case(col).replace(" ","_") for col in symbols.columns}
                symbols.rename(columns=rename_dict, inplace=True)
                Exchange = 'NSE'
                symbols['exchange'] = Exchange
                symbols['product_type'] = 'equities'
                symbols = symbols.rename(columns={'short_name': 'stock_code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                
                required_columns = ["exchange", "product_type", "stock_code"]
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
                symbols['exchange'] = Exchange
                symbols = symbols.rename(columns={'short_name': 'stock_code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                symbols['product_type'] = 'equities'
                required_columns = ["exchange", "product_type", "stock_code"]
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

                symbols['exchange'] = Exchange
                symbols['product_type'] = np.where(symbols['series'] == 'FUTURE', 'futures',
                                                np.where(symbols['series'] == 'OPTION', 'options', symbols['series']))
                symbols['option_type'] = np.where(symbols['option_type'] == 'CE', 'call',
                                                np.where(symbols['option_type'] == 'PE', 'put', symbols['option_type']))
                symbols = symbols.rename(columns={'short_name': 'stock_code'})
                symbols = symbols.rename(columns={'Token': 'Breeze_Token'})
                symbols = symbols.rename(columns={'expirydate': 'expiry_date'})
                #symbols = symbols.rename(columns={'rights': 'option_type'})
                symbols = symbols.rename(columns={'strikeprice': 'strike_price'})
                required_columns = ["exchange", "product_type", "stock_code", "expiry_date", "option_type", "strike_price"]
                if not set(required_columns).issubset(symbols.columns):
                    raise KeyError(f"Missing columns: {set(required_columns) - set(symbols.columns)}")
                if 'expiry_date' in symbols:
                    symbols['expiry_date'] = symbols['expiry_date'].fillna('')

                if 'option_type' in symbols:
                    symbols['option_type'] = symbols['option_type'].fillna('')

                if 'strike_price' in symbols:
                    symbols['strike_price'] = symbols['strike_price'].fillna('')
                symbols['strike_price'] = symbols['strike_price'].astype(str).str.replace(r'\.0', '', regex=True)
                symbols['strike_price'] = pd.to_numeric(symbols['strike_price'], errors='coerce')
                symbols['instrument_key'] = symbols.apply(generate_instrument_key, axis=1)

                
                symbols = symbols.sort_values(by=['stock_code', 'product_type', 'expiry_date', 'option_type', 'strike_price'])

            else:
                logging.warning(f"Breeze: Unsupported file: {filepath}")
                return pd.DataFrame()

            symbols = symbols.replace(np.nan, None)
            symbols = symbols.rename(columns={'52_weeks_high': 'weeks_52_high'})
            symbols = symbols.rename(columns={'52_weeks_low': 'weeks_52_low'})
            symbols['short_name']=symbols['stock_code']
            #ist = pytz.timezone("Asia/Kolkata")
            #symbols["local_update_datetime"] = utc_now()
            symbols['first_added_datetime']= datetime.now()



            # Check for empty instrument_key (Added code)
            empty_key_rows = symbols[symbols['instrument_key'] == '']
            if not empty_key_rows.empty:
                logging.warning(f"Breeze: Found {len(empty_key_rows)} rows with empty instrument_key in {filepath}")

            # inspector = inspect(self.db.bind) #This is not available here.
            # processed_symbols = pd.DataFrame()
            # new_series = []
            # for col in db_columns:
            #     if col in symbols.columns:
            #         new_series.append(pd.Series([None] * len(symbols), index=symbols.index, name=col))
            #         #processed_symbols[col] = symbols[col]  # Copy the existing column
            #     else:
            #         # Concatenate the new Series with the original DataFrame
            #         if new_series:
            #             symbols = pd.concat([symbols] + new_series, axis=1)
            #         #processed_symbols[col] = pd.Series([None] * len(symbols), index=symbols.index) # Create a new Series
            #     # Reindex for consistency (optional, but often recommended)
            #     symbols = symbols.reindex(columns=db_columns, fill_value=None)
            # processed_symbols=symbols
            # for col in processed_symbols.select_dtypes(include=['object']).columns:
            #     if processed_symbols[col].dtype == 'object':
            #         processed_symbols[col] = processed_symbols[col].str.strip("'")



            # Date Conversions
            if 'expiry_date' in symbols.columns:
                symbols['expiry_date'] = pd.to_datetime(symbols['expiry_date'], errors='coerce')
                symbols['expiry_date'] = symbols['expiry_date'].dt.tz_localize('Asia/Kolkata').dt.date
            symbols['local_update_datetime']= datetime.now()

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
            await self._load_to_timescaledb_async(symbols_list) 
            return None
        except Exception as e:
            log_exception(f"Breeze: Error processing data file {filepath}: {e}")

    async def _load_to_timescaledb_async(self, symbols_list):  
        # Mark old symbols as inactive
        stmt = update(symbol_model).values({
            symbol_model.refresh_flag: True,
            symbol_model.remarks: "Inactive"
        })
        SessionLocal = get_timescaledb_session()
        async with SessionLocal() as db1:
            await db1.execute(stmt)
            await db1.commit()
        for row in symbols_list:
            if str(row.get("stock_token", "")).strip().lower() in ("nan", "none", ""):
                row["stock_token"] = None
        # Perform bulk upsert using shared utility
        await bulk_upsert_async(
            model=symbol_model,
            data_list=symbols_list,
            key_fields=["instrument_key"],
            session_factory=get_timescaledb_session  # ✅ pass the function, not get_timescaledb_session()
            )

        logging.info("Symbols refreshed successfully.")

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
        column_list = self.get_model_column_names(symbol_model)
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

        schema_fields: Dict[str, Type] = {
            "instrument_key": str,
            "stock_token": str,
            "instrument_name": str,
            "stock_code": str,
            "series": str,
            "expiry_date": safe_parse_datetime,
            "strike_price": safe_convert_float,
            "option_type": str,
            "ca_level": str,
            "permitted_to_trade": safe_convert_bool,
            "issue_capital": safe_convert_float,
            "warning_qty": safe_convert_int,
            "freeze_qty": safe_convert_int,
            "credit_rating": str,
            "normal_market_status": str,
            "odd_lot_market_status": str,
            "spot_market_status": str,
            "auction_market_status": str,
            "normal_market_eligibility": str,
            "odd_lot_market_eligibility": str,
            "spot_market_eligibility": str,
            "auction_market_eligibility": str,
            "scrip_id": str,
            "issue_rate": safe_convert_float,
            "issue_start_date": safe_parse_datetime,
            "interest_payment_date": safe_parse_datetime,
            "issue_maturity_date": safe_parse_datetime,
            "margin_percentage": safe_convert_float,
            "minimum_lot_qty": safe_convert_int,
            "lot_size": safe_convert_int,
            "tick_size": safe_convert_float,
            "company_name": str,
            "listing_date": safe_parse_datetime,
            "expulsion_date": safe_parse_datetime,
            "readmission_date": safe_parse_datetime,
            "record_date": safe_parse_datetime,
            "low_price_range": safe_convert_float,
            "high_price_range": safe_convert_float,
            "security_expiry_date": safe_parse_datetime,
            "no_delivery_start_date": safe_parse_datetime,
            "no_delivery_end_date": safe_parse_datetime,
            "aon": str,
            "participant_in_market_index": str,
            "book_cls_start_date": safe_parse_datetime,
            "book_cls_end_date": safe_parse_datetime,
            "excercise_start_date": safe_parse_datetime,
            "excercise_end_date": safe_parse_datetime,
            "old_token": str,
            "asset_instrument": str,
            "asset_name": str,
            "asset_token": safe_convert_int,
            "intrinsic_value": safe_convert_float,
            "extrinsic_value": safe_convert_float,
            "excercise_style": str,
            "egm": str,
            "agm": str,
            "interest": str,
            "bonus": str,
            "rights": str,
            "dividends": str,
            "ex_allowed": str,
            "ex_rejection_allowed": safe_convert_bool,
            "pl_allowed": safe_convert_bool,
            "is_this_asset": safe_convert_bool,
            "is_corp_adjusted": safe_convert_bool,
            "local_update_datetime": safe_parse_datetime,
            "delete_flag": safe_convert_bool,
            "remarks": str,
            "base_price": safe_convert_float,
            "exchange_code": str,
            "product_type": str,
            "option_type_alt": str,
            "breeze_token": str,
            "kite_token": str,
            "board_lot_qty": safe_convert_int,
            "date_of_delisting": safe_parse_datetime,
            "date_of_listing": safe_parse_datetime,
            "face_value": safe_convert_float,
            "freeze_percent": safe_convert_float,
            "high_date": safe_parse_datetime,
            "isin_code": str,
            "instrument_type": str,
            "issue_price": safe_convert_float,
            "life_time_high": safe_convert_float,
            "life_time_low": safe_convert_float,
            "low_date": safe_parse_datetime,
            "avm_buy_margin": safe_convert_float,
            "avm_sell_margin": safe_convert_float,
            "bcast_flag": safe_convert_bool,
            "group_name": str,
            "market_lot": safe_convert_int,
            "nde_date": safe_parse_datetime,
            "nds_date": safe_parse_datetime,
            "nd_flag": safe_convert_bool,
            "scrip_code": str,
            "scrip_name": str,
            "susp_status": str,
            "suspension_reason": str,
            "suspension_date": safe_parse_datetime,
            "refresh_flag": safe_convert_bool,
            "first_added_datetime": safe_parse_date,
            "weeks_52_high": safe_convert_float,
            "weeks_52_low": safe_convert_float,
            "symbol": str,
            "short_name": str,
            "mfill": str,
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
            log_info(f"Breeze: Getting token for {instrument_key}")
            # await asyncio.sleep(0.1) #dummy
            # token = f"BREEZE_TOKEN_{instrument_key}"  # Dummy token
            return token_value
        except Exception as e:
            log_exception(f"Breeze: Error getting symbol token for {instrument_key}: {e}")
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
                log_warning(f"to_date {to_date} exceeds current datetime; restricting to {current_datetime}.")
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
                    log_info(f"No data returned for the chunk from {current_date} to {to_date_chunk}. Terminating fetch.")
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
            log_exception(f"Error fetching historical data: {e}")
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
                log_info(f"Breeze: Subscribing to {instrument_key} with interval {interval}")
            elif broker_token is not None:
                subscribe_dict['stock_token'] = broker_token
                self.broker.subscribe_feeds(**subscribe_dict)
                log_info(f"Breeze: Subscribing to  token {broker_token} with interval {interval}")                
        except Exception as e:
            log_exception(f"Breeze: Error subscribing to {instrument_key}: {e}")
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
                log_info(f"Breeze: unsubscribing to {instrument_key}")
            elif broker_token is not None:
                unsubscribe_dict['stock_token'] = broker_token
                self.broker.unsubscribe_feeds(**unsubscribe_dict)
                log_info(f"Breeze: unsubscribing to  token {broker_token}")                
        except Exception as e:
            log_exception(f"Breeze: Error unsubscribing to {instrument_key}: {e}")
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
            log_info(f"Breeze: Processed feed for {processed_feed['instrument_key']}")
            return processed_feed
        except Exception as e:
            log_exception(f"Breeze: Error processing feed data: {e}")
            raise

    def _handle_tick_data_sync(self, tick_data: Dict[str, Any]):
        """
        Synchronous wrapper to call the async _handle_tick_data in a FastAPI-compatible way.
        """
        asyncio.create_task(self._handle_tick_data(tick_data))
        # try:
        #     loop = asyncio.get_event_loop()
        #     if loop.is_running():
        #         asyncio.run_coroutine_threadsafe(self._handle_tick_data(tick_data), loop)
        #     else:
        #         log_exception("FastAPI event loop is not running. Tick processing may fail.")
        # except Exception as e:
        #     log_exception(f"Breeze: Error in _handle_tick_data_sync: {e}")

    async def _handle_tick_data(self, tick_data: Dict[str, Any]):
        """
        Asynchronous callback function to handle incoming tick data from the broker.
        This should be as lightweight as possible.
        """
        try:
            # 1. Standardize the tick data format
            processed_feed = await self.process_feed(tick_data)
            await self.market_data_manager.process_breeze_ticks(processed_feed)
            # # 2. Add the processed feed to RedisService for batching
            # redis = self.app.state.connections.get("redis")  # Get the Redis connection from app state
            # if not hasattr(self, "redis_service"):
            #     log_exception("RedisService is not initialized.")
            #     return

            # await self.app.state.redis_service.add_tick(processed_feed)
            log_info(f"Breeze: Tick data added to Redis batch - {processed_feed}")

        except Exception as e:
            log_exception(f"Breeze: Error handling tick data: {e}")

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
                        log_info(f"Downloaded {url} to {filename}")
                        return filename
                    else:
                        log_exception(f"Failed to download {url}: {response.status}")
                        return None  # Indicate failure
        except aiohttp.ClientError as e:
            log_exception(f"aiohttp client error downloading {url}: {e}")
            return None
        except Exception as e:
            log_exception(f"Unexpected error downloading {url}: {e}")
            return None

    async def _extract_zip(self, zip_file_path, target_dir):
        """
        Extracts all files from a zip archive asynchronously.
        """
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self.executor, self._extract_zip_sync, zip_file_path, target_dir)
        except Exception as e:
            log_exception(f"Breeze: Error extracting zip file: {e}")
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
                    log_info(f"Mock: Sent tick for {instrument_key}")
                    self.tick_count += 1
                    t.sleep(interval)  # Wait for the interval
                if self.tick_count > 100:
                    break
        except Exception as e:
            log_exception(f"MockBroker error: {e}")

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