import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from dotenv import load_dotenv

def main():
    # --- Hardcoded City Coordinates (for demonstration) ---
    # In a real application, you'd use a geocoding service or a more comprehensive dataset.
    city_coordinates = {
        'San Francisco': {'latitude': 37.7749, 'longitude': -122.4194},
        'Los Angeles': {'latitude': 34.0522, 'longitude': -118.2437},
        'New York City': {'latitude': 40.7128, 'longitude': -74.0060},
        'Boston': {'latitude': 42.3601, 'longitude': -71.0589},
        'Dallas': {'latitude': 32.7767, 'longitude': -96.7970},
        'Seattle': {'latitude': 47.6062, 'longitude': -122.3321},
        'Portland': {'latitude': 45.5152, 'longitude': -122.6784},
        'Austin': {'latitude': 30.2672, 'longitude': -97.7431},
        'Atlanta': {'latitude': 33.7490, 'longitude': -84.3880},
        'Chicago': {'latitude': 41.8781, 'longitude': -87.6298},
        'Denver': {'latitude': 39.7392, 'longitude': -104.9903},
        # Add more cities as needed
    }

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

            header_lower = [col.lower() for col in df.columns]
            df = df[~df.apply(lambda row: all(str(item).strip().lower() == header_lower[i] for i, item in enumerate(row)), axis=1)]

            df.columns = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in df.columns]
            print(f"    Normalized columns: {df.columns.tolist()}")

            for col in df.columns:
                if 'date' in col:
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

    # Debugging: Check dtypes immediately before splitting for dimensions/facts
    print("\n--- Verifying Combined DataFrame dtypes after initial conversions ---")
    combined_df.info(verbose=False, show_counts=True)
    print(combined_df.head())
    print("---------------------------------------------------\n")

    print("\n--- Beginning Dimensional Model Data Loading ---")

    # --- 1. Load dim_customer ---
    customer_data = combined_df['purchase_address'].str.extract(r'(.*?), (.*), (\w{2}) (\d{5})')
    customer_data.columns = ['street_address', 'city', 'state', 'zip_code']
    customer_data['purchase_address'] = combined_df['purchase_address'] # Keep original for uniqueness check

    dim_customer_df = customer_data[['purchase_address', 'city', 'state', 'zip_code']].drop_duplicates().dropna(subset=['purchase_address'])

    # Add latitude and longitude based on the hardcoded dictionary
    dim_customer_df['latitude'] = dim_customer_df['city'].map(lambda x: city_coordinates.get(x, {}).get('latitude'))
    dim_customer_df['longitude'] = dim_customer_df['city'].map(lambda x: city_coordinates.get(x, {}).get('longitude'))
    # Fill NaN lat/lon for cities not in our dictionary, or drop if strictly needed
    dim_customer_df['latitude'].fillna(0, inplace=True) # Fill with 0 or NaN, or drop rows
    dim_customer_df['longitude'].fillna(0, inplace=True)

    try:
        print("  Loading dim_customer...")
        with engine.begin() as connection:
            for index, row in dim_customer_df.iterrows():
                try:
                    insert_sql = text("""
                        INSERT INTO dim_customer (purchase_address, city, state, zip_code, latitude, longitude)
                        VALUES (:purchase_address, :city, :state, :zip_code, :latitude, :longitude)
                        ON CONFLICT (purchase_address) DO NOTHING;
                    """)
                    connection.execute(insert_sql, {
                        'purchase_address': row['purchase_address'],
                        'city': row['city'],
                        'state': row['state'],
                        'zip_code': row['zip_code'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude']
                    })
                except Exception as row_e:
                    print(f"    Error inserting customer {row['purchase_address']}: {row_e}. Skipping.")
        print(f"  Attempted to load {len(dim_customer_df)} unique customers into dim_customer.")
    except Exception as e:
        print(f"  Error loading dim_customer table: {e}")
        if hasattr(e, 'orig') and e.orig is not None:
            print(f"  Original DBAPI error for dim_customer: {e.orig}")
        return

    # --- 2. Load dim_product ---
    dim_product_df = combined_df[['product', 'price_each']].drop_duplicates().dropna(subset=['product', 'price_each'])
    dim_product_df = dim_product_df.rename(columns={'product': 'product_name'})
    dim_product_df['category'] = None

    try:
        print("  Loading dim_product...")
        with engine.begin() as connection:
            for index, row in dim_product_df.iterrows():
                try:
                    insert_sql = text("""
                        INSERT INTO dim_product (product_name, price_each, category)
                        VALUES (:product_name, :price_each, :category)
                        ON CONFLICT (product_name) DO UPDATE SET price_each = EXCLUDED.price_each, category = EXCLUDED.category;
                    """)
                    connection.execute(insert_sql, {
                        'product_name': row['product_name'],
                        'price_each': row['price_each'],
                        'category': row['category']
                    })
                except Exception as row_e:
                    print(f"    Error inserting product {row['product_name']}: {row_e}. Skipping.")
        print(f"  Attempted to load {len(dim_product_df)} unique products into dim_product.")
    except Exception as e:
        print(f"  Error loading dim_product table: {e}")
        if hasattr(e, 'orig') and e.orig is not None:
            print(f"  Original DBAPI error for dim_product: {e.orig}")
        return

    # --- 3. Load fact_sales ---
    print("  Fetching customer and product primary keys for fact table mapping...")
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
    fact_sales_final_df.dropna(subset=['customer_pk', 'product_pk'], inplace=True)
    if len(fact_sales_final_df) < initial_fact_rows:
        print(f"  Warning: Dropped {initial_fact_rows - len(fact_sales_final_df)} fact rows due to missing dim keys.")

    fact_sales_final_df['customer_pk'] = fact_sales_final_df['customer_pk'].astype(int)
    fact_sales_final_df['product_pk'] = fact_sales_final_df['product_pk'].astype(int)

    print("\n--- Verifying Fact Sales DataFrame dtypes immediately before insertion ---")
    fact_sales_final_df.info(verbose=False, show_counts=True)
    print(fact_sales_final_df.head())
    print("---------------------------------------------------\n")

    try:
        print("  Loading fact_sales...")
        with engine.begin() as connection:
            for index, row in fact_sales_final_df.iterrows():
                try:
                    insert_sql = text("""
                        INSERT INTO fact_sales (order_id, customer_pk, product_pk, order_date, quantity, total_amount)
                        VALUES (:order_id, :customer_pk, :product_pk, :order_date, :quantity, :total_amount)
                        ON CONFLICT (order_id) DO NOTHING;
                    """)
                    connection.execute(insert_sql, row.to_dict())
                except Exception as row_e:
                    print(f"    Error inserting fact for order_id {row['order_id']}: {row_e}. Skipping.")
        print(f"  Attempted to load {len(fact_sales_final_df)} sales records into fact_sales.")
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