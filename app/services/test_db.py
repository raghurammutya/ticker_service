from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text

# Define your database connection
DATABASE_URL = "postgresql+psycopg2://tradmin:tradpass@localhost/tradingdb"
engine = create_engine(DATABASE_URL)

# Create a session factory
SessionLocal = sessionmaker(bind=engine)

# Open a session and test the query
with SessionLocal() as session:
    result = session.execute(text("SELECT * FROM tradingdb.brokers LIMIT 5"))
    print(result.fetchall())