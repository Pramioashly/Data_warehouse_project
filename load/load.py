import os
import pandas as pd
import psycopg2

def insert_data_from_df(cursor, table_name, df):
    cols = list(df.columns)
    col_names = ', '.join([f'"{col}"' for col in cols])
    placeholders = ', '.join(['%s'] * len(cols))
    insert_query = f'''
        INSERT INTO "{table_name}" ({col_names})
        VALUES ({placeholders})
    '''
    for idx, row in df.iterrows():
        values = tuple(row[col] for col in cols)
        
        # Debug: print rows that have 'Order Date' as value in any column
        if 'Order Date' in values:
            print(f"Skipping row {idx} with header string: {values}")
            continue
        
        cursor.execute(insert_query, values)

def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="dpg-d0k5tbruibrs73983cs0-a.singapore-postgres.render.com",
        dbname="airyll",
        user="airyll_user",
        password="iKiLhVkL0nHuRn2BFTsGWdmM4vEQI7Ls",
        port="5432"
    )
    cursor = conn.cursor()

    cleaned_folder = "cleaned"
    for filename in os.listdir(cleaned_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(cleaned_folder, filename)
            print(f"Processing file: {file_path}")

            df = pd.read_csv(file_path)

            # Remove any rows that match the header (duplicate headers inside data)
            df = df[~df.apply(lambda row: all(row.astype(str).str.lower() == df.columns), axis=1)]
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]


            # Remove duplicate header rows
            df = df[df[df.columns[0]] != df.columns[0]]

            # Normalize column names
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
            print(f"Detected columns: {df.columns.tolist()}")

            # Convert any 'date' columns to datetime.date
            for col in df.columns:
                if 'date' in col:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

            table_name = os.path.splitext(filename)[0].lower().replace(" ", "_")

            # Create table if it doesn't exist
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                order_id TEXT,
                product TEXT,
                quantity_ordered INTEGER,
                price_each NUMERIC,
                order_date DATE,
                purchase_address TEXT
            )
            """)

            insert_data_from_df(cursor, table_name, df)
            conn.commit()

            # Preview inserted data
            cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 5')
            rows = cursor.fetchall()
            print(f"\nSample rows inserted into {table_name}:")
            for row in rows:
                print(row)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()