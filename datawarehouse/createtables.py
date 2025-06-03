import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def create_warehouse_tables():
    # Construct the path to the .env file, assuming it's one level up from createtable.py
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    DB_URL = os.getenv("EXTERNAL_DATABASE_URL")

    if not DB_URL:
        raise ValueError("Error: The 'EXTERNAL_DATABASE_URL' environment variable is not set. Please ensure it's set in your .env file or environment.")

    # SQL statements for your warehouse schema
    # Order of DROP TABLE matters due to foreign key constraints (drop fact first, then dimensions)
    create_table_sql = """
    -- Drop tables if they already exist, cascading to drop dependent objects
    DROP TABLE IF EXISTS fact_sales CASCADE;
    DROP TABLE IF EXISTS dim_product CASCADE;
    DROP TABLE IF EXISTS dim_customer CASCADE;

    -- Customer dimension table
    CREATE TABLE dim_customer (
        customer_pk SERIAL PRIMARY KEY, -- Using a surrogate key for the primary key
        purchase_address TEXT UNIQUE NOT NULL, -- The original unique address
        city TEXT,
        state TEXT,
        zip_code TEXT
        -- Add more customer attributes if available (e.g., customer_name if derivable)
    );

    -- Product dimension table
    CREATE TABLE dim_product (
        product_pk SERIAL PRIMARY KEY, -- Using a surrogate key for the primary key
        product_name TEXT UNIQUE NOT NULL, -- The original product name
        price_each NUMERIC NOT NULL, -- The price from the source
        category TEXT -- Category is not in source, will be NULL for now
    );

    -- Sales fact table
    CREATE TABLE fact_sales (
        order_id TEXT PRIMARY KEY,
        customer_pk INTEGER REFERENCES dim_customer(customer_pk), -- Foreign key to dim_customer
        product_pk INTEGER REFERENCES dim_product(product_pk),   -- Foreign key to dim_product
        order_date DATE NOT NULL,
        quantity INTEGER NOT NULL,
        total_amount NUMERIC NOT NULL
    );
    """

    engine = None # Initialize engine to None
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit() # Commit the changes to make them permanent
            print("✅ Data warehouse tables (dim_customer, dim_product, fact_sales) created successfully.")
    except Exception as e:
        print(f"Error creating data warehouse tables: {e}")
        if hasattr(e, 'orig') and e.orig is not None:
            print(f"Original DBAPI error: {e.orig}")
            if hasattr(e.orig, 'pgcode'): # Specific to psycopg2 for PostgreSQL error codes
                print(f"PostgreSQL Error Code: {e.orig.pgcode}")
                # You can look up pgcode here: https://www.postgresql.org/docs/current/errcodes-appendix.html
            if hasattr(e.orig, 'diag') and hasattr(e.orig.diag, 'message_detail'):
                 print(f"PostgreSQL Error Detail: {e.orig.diag.message_detail}")
        raise # Re-raise the exception to stop script if tables aren't created

if __name__ == "__main__":
    create_warehouse_tables()