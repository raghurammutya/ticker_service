from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime


class SymbolBase(BaseModel):
    instrument_key: str
    stock_token: Optional[str]
    instrument_name: Optional[str] = Field(None, alias="Instrument_Name")
    stock_code: Optional[str]
    series: Optional[str]
    expiry_date: Optional[datetime]
    strike_price: Optional[float]
    option_type: Optional[str]
    ca_level: Optional[str]
    permitted_to_trade: Optional[bool]
    issue_capital: Optional[float]
    warning_qty: Optional[int]
    freeze_qty: Optional[int]
    credit_rating: Optional[str]
    normal_market_status: Optional[str]
    odd_lot_market_status: Optional[str]
    spot_market_status: Optional[str]
    auction_market_status: Optional[str]
    normal_market_eligibility: Optional[str]
    odd_lot_market_eligibility: Optional[str]
    spot_market_eligibility: Optional[str]
    auction_market_eligibility: Optional[str]
    scrip_id: Optional[str]
    issue_rate: Optional[float]
    issue_start_date: Optional[datetime]
    interest_payment_date: Optional[datetime]
    issue_maturity_date: Optional[datetime]
    margin_percentage: Optional[float]
    minimum_lot_qty: Optional[int]
    lot_size: Optional[int]
    tick_size: Optional[float]
    company_name: Optional[str]
    listing_date: Optional[datetime]
    expulsion_date: Optional[datetime]
    readmission_date: Optional[datetime]
    record_date: Optional[datetime]
    low_price_range: Optional[float]
    high_price_range: Optional[float]
    security_expiry_date: Optional[datetime]
    no_delivery_start_date: Optional[datetime]
    no_delivery_end_date: Optional[datetime]
    aon: Optional[str]
    participant_in_market_index: Optional[str]
    book_cls_start_date: Optional[datetime]
    book_cls_end_date: Optional[datetime]
    excercise_start_date: Optional[datetime]
    excercise_end_date: Optional[datetime]
    old_token: Optional[str]
    asset_instrument: Optional[str]
    asset_name: Optional[str]
    asset_token: Optional[int]
    intrinsic_value: Optional[float]
    extrinsic_value: Optional[float]
    excercise_style: Optional[str]
    egm: Optional[str]
    agm: Optional[str]
    interest: Optional[str]
    bonus: Optional[str]
    rights: Optional[str]
    dividends: Optional[str]
    ex_allowed: Optional[str]
    ex_rejection_allowed: Optional[bool]
    pl_allowed: Optional[bool]
    is_this_asset: Optional[bool]
    is_corp_adjusted: Optional[bool]
    local_update_datetime: Optional[datetime]
    delete_flag: Optional[bool]
    remarks: Optional[str]
    base_price: Optional[float]
    exchange_code: Optional[str]
    product_type: Optional[str]
    breeze_token: Optional[str]
    kite_token: Optional[str]
    board_lot_qty: Optional[int]
    date_of_delisting: Optional[datetime]
    date_of_listing: Optional[datetime]
    face_value: Optional[float]
    freeze_percent: Optional[float]
    high_date: Optional[datetime]
    isin_code: Optional[str]
    instrument_type: Optional[str]
    issue_price: Optional[float]
    lifetime_high: Optional[float]
    lifetime_low: Optional[float]
    low_date: Optional[datetime]
    avm_buy_margin: Optional[float]
    avm_sell_margin: Optional[float]
    bcast_flag: Optional[bool]
    group_name: Optional[str]
    market_lot: Optional[int]
    nde_date: Optional[datetime]
    nds_date: Optional[datetime]
    nd_flag: Optional[bool]
    scrip_code: Optional[str]
    scrip_name: Optional[str]
    susp_status: Optional[str]
    suspension_reason: Optional[str]
    suspension_date: Optional[datetime]
    refresh_flag: Optional[bool]
    first_added_datetime: Optional[datetime]
    weeks_52_high: Optional[float]
    weeks_52_low: Optional[float]
    symbol: Optional[str]
    short_name: Optional[str]
    mfill: Optional[str]

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

    
class SymbolCreate(SymbolBase):
    pass

class SymbolUpdate(SymbolBase):
    pass

class Symbol(SymbolBase):
    id: int

    class Config:
        orm_mode = True