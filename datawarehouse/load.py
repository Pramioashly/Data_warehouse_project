import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Date, Numeric # Import SQLAlchemy types
import psycopg2 # Keep for potential original error messages if needed, though not directly used for insertion
from dotenv import load_dotenv

def main():
    # --- Load environment variables from .env file ---
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    # --- Database Connection Configuration ---
    DATABASE_URL = os.getenv("EXTERNAL_DATABASE_URL")

    if not DATABASE_URL:
        print("Error: The 'EXTERNAL_DATABASE_URL' environment variable is not set.")
        print("Please ensure it's set in your .env file or environment.")
        print(f"Script looked for .env at: {dotenv_path}")
        return

    engine = None
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Successfully connected to the database.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        print("Please check your 'EXTERNAL_DATABASE_URL' and ensure the database server is accessible.")
        return

    if engine is None:
        print("Critical Error: Database engine could not be initialized, and script did not exit as expected.")
        return

    # --- CSV Files Location ---
    cleaned_folder_path = os.path.join(os.path.dirname(__file__), '..', 'cleaned')
    all_dfs = []

    print(f"--- Starting to process CSV files from '{cleaned_folder_path}' folder ---")
    if not os.path.exists(cleaned_folder_path):
        print(f"Error: The folder '{cleaned_folder_path}' does not exist.")
        print("Please ensure 'cleaned' folder is parallel to 'datawarehouse' folder.")
        return

    csv_files = [f for f in os.listdir(cleaned_folder_path) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in '{cleaned_folder_path}'. Exiting.")
        return

    for filename in csv_files:
        file_path = os.path.join(cleaned_folder_path, filename)
        print(f"  Processing file: {filename}")

        try:
            df = pd.read_csv(file_path, low_memory=False)

            # Check if the first row is a repeated header and skip it if it is
            header_lower = [col.lower() for col in df.columns]
            df = df[~df.apply(lambda row: all(str(item).strip().lower() == header_lower[i] for i, item in enumerate(row)), axis=1)]

            df.columns = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in df.columns]
            print(f"    Normalized columns: {df.columns.tolist()}")

            # Convert date columns to datetime objects - Consider specifying format if known
            for col in df.columns:
                if 'date' in col:
                    # Example: if format is "MM/DD/YYYY HH:MM" use format="%m/%d/%Y %H:%M"
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

            all_dfs.append(df)

        except pd.errors.EmptyDataError:
            print(f"  Warning: {filename} is empty. Skipping this file.")
        except Exception as e:
            print(f"  Error processing {filename}: {e}. Skipping this file.")
            continue

    if not all_dfs:
        print("No DataFrames were successfully processed from CSVs. Exiting.")
        return

    print("\n--- Merging all processed CSVs into one comprehensive DataFrame ---")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined DataFrame created with {len(combined_df)} rows and {len(combined_df.columns)} columns.")
    print(f"Combined DataFrame sample columns: {combined_df.columns.tolist()}")

    # --- Explicit Data Type Conversions for Database Compatibility (Critical for Insertion) ---
    print("\n--- Performing explicit data type conversions before insertion ---")
    combined_df['quantity_ordered'] = pd.to_numeric(combined_df['quantity_ordered'], errors='coerce').fillna(0).astype('Int64')
    print(f"  'quantity_ordered' converted to Pandas Int64.")

    combined_df['price_each'] = pd.to_numeric(combined_df['price_each'], errors='coerce').fillna(0.0)
    print(f"  'price_each' converted to numeric (float).")

    print("\n--- Verifying Combined DataFrame dtypes after initial conversions ---")
    combined_df.info(verbose=False, show_counts=True)
    print(combined_df.head())
    print("---------------------------------------------------\n")

    print("\n--- Beginning Dimensional Model Data Loading ---")

    # --- 1. Load dim_customer ---
    dim_customer_temp_df = combined_df[['purchase_address']].drop_duplicates().dropna(subset=['purchase_address'])

    if not dim_customer_temp_df.empty:
        try:
            print(f"  Loading {len(dim_customer_temp_df)} unique customers into a temporary table for dim_customer...")
            dim_customer_temp_df.to_sql(
                'temp_dim_customer',
                con=engine,
                if_exists='replace', # Replace if exists, ensures a clean slate
                index=False,
                dtype={'purchase_address': String()} # Corrected type
            )

            print("  Merging new customers from temp_dim_customer into dim_customer with ON CONFLICT...")
            with engine.begin() as connection:
                merge_sql = text("""
                    INSERT INTO dim_customer (purchase_address)
                    SELECT DISTINCT purchase_address FROM temp_dim_customer
                    ON CONFLICT (purchase_address) DO NOTHING;
                """)
                connection.execute(merge_sql)
            print(f"  Successfully loaded unique customers into dim_customer.")

        except Exception as e:
            print(f"  Error loading dim_customer table: {e}")
            if hasattr(e, 'orig') and e.orig is not None:
                print(f"  Original DBAPI error for dim_customer: {e.orig}")
            return
    else:
        print("  No unique customer addresses to load.")

    # --- 2. Load dim_product ---
    dim_product_temp_df = combined_df[['product', 'price_each']].drop_duplicates().dropna(subset=['product', 'price_each'])
    dim_product_temp_df = dim_product_temp_df.rename(columns={'product': 'product_name'})
    dim_product_temp_df['category'] = None # Assign None directly, which will be NULL in DB

    if not dim_product_temp_df.empty:
        try:
            print(f"  Loading {len(dim_product_temp_df)} unique products into a temporary table for dim_product...")
            dim_product_temp_df.to_sql(
                'temp_dim_product',
                con=engine,
                if_exists='replace', # Replace if exists, ensures a clean slate
                index=False,
                dtype={
                    'product_name': String(),
                    'price_each': Numeric(10, 2), # Corrected type
                    'category': String()
                }
            )

            print("  Merging new products from temp_dim_product into dim_product with ON CONFLICT...")
            with engine.begin() as connection:
                merge_sql = text("""
                    INSERT INTO dim_product (product_name, price_each, category)
                    SELECT DISTINCT product_name, price_each, category FROM temp_dim_product
                    ON CONFLICT (product_name) DO UPDATE SET price_each = EXCLUDED.price_each, category = EXCLUDED.category;
                """)
                connection.execute(merge_sql)
            print(f"  Successfully loaded unique products into dim_product.")

        except Exception as e:
            print(f"  Error loading dim_product table: {e}")
            if hasattr(e, 'orig') and e.orig is not None:
                print(f"  Original DBAPI error for dim_product: {e.orig}")
            return
    else:
        print("  No unique products to load.")


    # --- 3. Load fact_sales ---
    print("  Fetching customer and product primary keys for fact table mapping...")
    # Read the full tables to ensure all keys are available, even new ones from this run.
    db_customers = pd.read_sql_table('dim_customer', con=engine, columns=['customer_pk', 'purchase_address'])
    db_products = pd.read_sql_table('dim_product', con=engine, columns=['product_pk', 'product_name'])

    fact_sales_df = combined_df.merge(db_customers, on='purchase_address', how='left')
    fact_sales_df = fact_sales_df.merge(db_products, left_on='product', right_on='product_name', how='left')

    fact_sales_df['total_amount'] = fact_sales_df['quantity_ordered'] * fact_sales_df['price_each']

    fact_sales_final_df = fact_sales_df[[
        'order_id', 'customer_pk', 'product_pk', 'order_date', 'quantity_ordered', 'total_amount'
    ]]
    fact_sales_final_df = fact_sales_final_df.rename(columns={'quantity_ordered': 'quantity'})

    initial_fact_rows = len(fact_sales_final_df)
    # >>> START OF MODIFICATION FOR NULL VALUES <<<
    # Drop rows where essential keys or not-null columns are missing
    fact_sales_final_df.dropna(subset=['order_id', 'customer_pk', 'product_pk', 'order_date'], inplace=True)
    # >>> END OF MODIFICATION FOR NULL VALUES <<<
    if len(fact_sales_final_df) < initial_fact_rows:
        print(f"  Warning: Dropped {initial_fact_rows - len(fact_sales_final_df)} fact rows due to missing essential keys or dates.")

    # Convert primary keys to integer type after dropping NaNs
    # Use Int64 for nullable integer support in pandas
    fact_sales_final_df['customer_pk'] = fact_sales_final_df['customer_pk'].astype('Int64')
    fact_sales_final_df['product_pk'] = fact_sales_final_df['product_pk'].astype('Int64')

    print("\n--- Verifying Fact Sales DataFrame dtypes immediately before insertion ---")
    fact_sales_final_df.info(verbose=False, show_counts=True)
    print(fact_sales_final_df.head())
    print("---------------------------------------------------\n")

    if not fact_sales_final_df.empty:
        try:
            print(f"  Loading {len(fact_sales_final_df)} sales records into temp_fact_sales using to_sql...")
            fact_sales_final_df.to_sql(
                'temp_fact_sales',
                con=engine,
                if_exists='replace',
                index=False,
                dtype={
                    'order_id': String(), # Corrected type
                    'customer_pk': Integer(), # Corrected type
                    'product_pk': Integer(), # Corrected type
                    'order_date': Date(), # Corrected type
                    'quantity': Integer(), # Corrected type
                    'total_amount': Numeric(10, 2) # Corrected type
                }
            )
            print("  Merging new sales records from temp_fact_sales into fact_sales with ON CONFLICT...")
            with engine.begin() as connection:
                merge_sql = text("""
                    INSERT INTO fact_sales (order_id, customer_pk, product_pk, order_date, quantity, total_amount)
                    SELECT
                        order_id,
                        customer_pk,
                        product_pk,
                        order_date,
                        quantity,
                        total_amount
                    FROM temp_fact_sales
                    ON CONFLICT (order_id) DO NOTHING;
                """)
                connection.execute(merge_sql)
            print(f"  Successfully loaded sales records into fact_sales.")

        except Exception as e:
            print(f"  Error during bulk data insertion into fact_sales: {e}")
            print("\nFirst 5 rows of Fact DataFrame that caused insertion error:")
            print(fact_sales_final_df.head())
            print("\nFull error details from SQLAlchemy (and potentially driver):")
            if hasattr(e, 'orig') and e.orig is not None:
                print(f"Original DBAPI error: {e.orig}")
                if hasattr(e.orig, 'pgcode'):
                    print(f"PostgreSQL Error Code: {e.orig.pgcode}")
                if hasattr(e.orig.diag, 'message_detail'):
                    print(f"PostgreSQL Error Detail: {e.orig.diag.message_detail}")
            else:
                print(e)
            return
    else:
        print("  No fact sales records to load after linking to dimensions.")

    # --- 4. Verify Data (Optional) ---
    print(f"\n--- Verifying a sample of inserted data from 'fact_sales' ---")
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f'SELECT * FROM "fact_sales" LIMIT 5;'))
            rows = result.fetchall()
            if rows:
                print(f"Sample rows from 'fact_sales':")
                for row in rows:
                    print(row)
            else:
                print("No data found in the fact_sales table for preview, or table is empty.")

        print(f"\n--- Verifying a sample of inserted data from 'dim_customer' ---")
        with engine.connect() as connection:
            result = connection.execute(text(f'SELECT * FROM "dim_customer" LIMIT 5;'))
            rows = result.fetchall()
            if rows:
                print(f"Sample rows from 'dim_customer':")
                for row in rows:
                    print(row)
            else:
                print("No data found in the dim_customer table for preview, or table is empty.")

        print(f"\n--- Verifying a sample of inserted data from 'dim_product' ---")
        with engine.connect() as connection:
            result = connection.execute(text(f'SELECT * FROM "dim_product" LIMIT 5;'))
            rows = result.fetchall()
            if rows:
                print(f"Sample rows from 'dim_product':")
                for row in rows:
                    print(row)
            else:
                print("No data found in the dim_product table for preview, or table is empty.")

    except Exception as e:
        print(f"Error verifying data: {e}")

    print("\n--- Data load process completed ---")

if __name__ == "__main__":
    main()