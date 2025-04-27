from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime

class SymbolBase(BaseModel):
    instrument_key: str = Field(max_length=255)
    Exchange: str = Field(max_length=50)
    Stock_Code: str
    Product_Type: Optional[str] = Field(None, max_length=50)
    Expiry_Date: Optional[date] = None
    Option_Type: Optional[str] = Field(None, max_length=50)
    Strike_Price: Optional[float] = None
    Stock_Token: str
    Instrument_Name: Optional[str] = Field(None, max_length=255)
    Series: Optional[str] = Field(None, max_length=50)
    Ca_Level: Optional[str] = Field(None, max_length=50)
    Permitted_To_Trade: Optional[bool] = None
    Issue_Capital: Optional[float] = None
    Warning_Qty: Optional[int] = None
    Freeze_Qty: Optional[int] = None
    Credit_Rating: Optional[str] = Field(None, max_length=50)
    Normal_Market_Status: Optional[str] = Field(None, max_length=50)
    Odd_Lot_Market_Status: Optional[str] = Field(None, max_length=50)
    Spot_Market_Status: Optional[str] = Field(None, max_length=50)
    Auction_Market_Status: Optional[str] = Field(None, max_length=50)
    Normal_Market_Eligibility: Optional[str] = Field(None, max_length=50)
    Odd_Lot_Market_Eligibility: Optional[str] = Field(None, max_length=50)
    Spot_Market_Eligibility: Optional[str] = Field(None, max_length=50)
    Auction_Market_Eligibility: Optional[str] = Field(None, max_length=50)
    Scrip_Id: Optional[str] = Field(None, max_length=50)
    Issue_Rate: Optional[float] = None
    Issue_Start_Date: Optional[date] = None
    Interest_Payment_Date: Optional[date] = None
    Issue_Maturity_Date: Optional[date] = None
    Margin_Percentage: Optional[float] = None
    Minimum_Lot_Qty: Optional[int] = None
    Lot_Size: Optional[int] = None
    Tick_Size: Optional[float] = None
    Company_Name: Optional[str] = Field(None, max_length=255)
    Board_Lot_Qty: Optional[int] = None
    Date_Of_Delisting: Optional[int] = None
    Date_Of_Listing: Optional[date] = None
    Face_Value: Optional[float] = None
    Freeze_Percent: Optional[float] = None  
    High_Date: Optional[date] = None
    ISIN_Code: Optional[str] = Field(None, max_length=50)
    Instrument_Type: Optional[str] = Field(None, max_length=50)
    Issue_Price: Optional[float] = None
    Life_Time_High: Optional[float] = None
    Life_Time_Low: Optional[float] = None
    Low_Date: Optional[date] = None
    AVM_Buy_Margin: Optional[float] = None
    AVM_Sell_Margin: Optional[float] = None
    BCast_Flag: Optional[bool] = None
    Group_Name: Optional[str] = Field(None, max_length=50)
    Market_Lot: Optional[int] = None
    NDE_Date: Optional[date] = None
    NDS_Date: Optional[date] = None
    Nd_Flag: Optional[bool] = None
    Scrip_Code: Optional[str] = Field(None, max_length=50)
    Scrip_Name: Optional[str] = Field(None, max_length=50)
    Susp_Status: Optional[str] = Field(None, max_length=50)
    Suspension_Reason: Optional[str] = Field(None, max_length=255)
    Suspension_Date: Optional[date] = None
    Listing_Date: Optional[date] = None
    Expulsion_Date: Optional[date] = None
    Readmission_Date: Optional[date] = None
    Record_Date: Optional[date] = None
    Low_Price_Range: Optional[float] = None
    High_Price_Range: Optional[float] = None
    Security_Expiry_Date: Optional[date] = None
    No_Delivery_Start_Date: Optional[date] = None
    No_Delivery_End_Date: Optional[date] = None
    Mf: Optional[str] = Field(None, max_length=50)
    Aon: Optional[str] = Field(None, max_length=50)
    Participant_In_Market_Index: Optional[str] = Field(None, max_length=50)
    Book_Cls_Start_Date: Optional[date] = None
    Book_Cls_End_Date: Optional[date] = None
    Excercise_Start_Date: Optional[date] = None
    Excercise_End_Date: Optional[date] = None
    Old_Token: Optional[str] = Field(None, max_length=255)
    Asset_Instrument: Optional[str] = Field(None, max_length=255)
    Asset_Name: Optional[str] = Field(None, max_length=255)
    Asset_Token: Optional[int] = None
    Intrinsic_Value: Optional[float] = None
    Extrinsic_Value: Optional[float] = None
    Excercise_Style: Optional[str] = Field(None, max_length=50)
    Egm: Optional[str] = Field(None, max_length=50)
    Agm: Optional[str] = Field(None, max_length=50)
    Interest: Optional[str] = Field(None, max_length=50)
    Bonus: Optional[str] = Field(None, max_length=50)
    Rights: Optional[str] = Field(None, max_length=50)
    Dividends: Optional[str] = Field(None, max_length=50)
    Ex_Allowed: Optional[str] = Field(None, max_length=50)
    Ex_Rejection_Allowed: Optional[bool] = None
    Pl_Allowed: Optional[bool] = None
    Is_This_Asset: Optional[bool] = None
    Is_Corp_Adjusted: Optional[bool] = None
    Local_Update_Datetime: Optional[datetime] = None
    Delete_Flag: Optional[bool] = None
    Remarks: Optional[str] = Field(None, max_length=255)
    Base_Price: Optional[float] = None
    Exchange_Code: Optional[str] = Field(None, max_length=50)
    Breeze_Token: Optional[str] = Field(None, max_length=255)
    Kite_Token: Optional[str] = Field(None, max_length=255)
    first_added_datetime: Optional[date] = None
    
class SymbolCreate(SymbolBase):
    pass

class SymbolUpdate(SymbolBase):
    pass

class Symbol(SymbolBase):
    id: int

    class Config:
        orm_mode = True