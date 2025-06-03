from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Choose INTERNAL first (Render), fall back to EXTERNAL (local)
DB_URL = os.getenv("EXTERNAL_DATABASE_URL")

if not DB_URL:
    raise ValueError("No database URL found in environment variables.")

# SQL statements for your warehouse schema
create_table_sql = """
-- Drop tables if they already exist
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;

-- Customer dimension
CREATE TABLE dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_name TEXT,
    region TEXT
); --

-- Product dimension
CREATE TABLE dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC
);

-- Sales fact
CREATE TABLE fact_sales (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR REFERENCES dim_customer(customer_id),
    product_id VARCHAR REFERENCES dim_product(product_id),
    order_date DATE,
    quantity INTEGER,
    total_amount NUMERIC
);
"""

# Create engine and run SQL
engine = create_engine(DB_URL)

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    print("✅ Tables created successfully.")
