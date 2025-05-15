from sqlalchemy import Column, TIMESTAMP, Text, Double, BigInteger, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class HistoricalData(Base):
    __tablename__ = 'historical_data'
    __table_args__ = {'schema': 'tradingdb'}
    time = Column(TIMESTAMP(timezone=True), primary_key=True, nullable=False)  # Store with timezone
    instrument_key = Column(Text, primary_key=True, nullable=False)
    interval = Column(Text, primary_key=True, nullable=False)

    open = Column(Double)
    high = Column(Double)
    low = Column(Double)
    close = Column(Double)
    volume = Column(BigInteger)
    oi = Column(BigInteger)
    expirydate = Column(Date)
    option_type = Column(Text)
    strikeprice = Column(Double)

    # Greeks data columns
    greeks_open_iv = Column(Double)
    greeks_open_delta = Column(Double)
    greeks_open_gamma = Column(Double)
    greeks_open_theta = Column(Double)
    greeks_open_rho = Column(Double)
    greeks_open_vega = Column(Double)

    greeks_high_iv = Column(Double)
    greeks_high_delta = Column(Double)
    greeks_high_gamma = Column(Double)
    greeks_high_theta = Column(Double)
    greeks_high_rho = Column(Double)
    greeks_high_vega = Column(Double)

    greeks_low_iv = Column(Double)
    greeks_low_delta = Column(Double)
    greeks_low_gamma = Column(Double)
    greeks_low_theta = Column(Double)
    greeks_low_rho = Column(Double)
    greeks_low_vega = Column(Double)

    greeks_close_iv = Column(Double)
    greeks_close_delta = Column(Double)
    greeks_close_gamma = Column(Double)
    greeks_close_theta = Column(Double)
    greeks_close_rho = Column(Double)
    greeks_close_vega = Column(Double)