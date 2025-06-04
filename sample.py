import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DATABASE_URL = os.getenv("EXTERNAL_DATABASE_URL")

@st.cache_data(ttl=3600)
def get_database_engine():
    """Establishes and returns a SQLAlchemy engine for database connection."""
    if not DATABASE_URL:
        st.error("DATABASE_URL is not set in environment variables.")
        return None
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        st.success("Successfully connected to the database!") # Add a success message
        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        st.info("Please check your DATABASE_URL and ensure the database server is accessible.")
        return None


@st.cache_data(ttl=3600)
def load_data():
    """Loads data from PostgreSQL tables into Pandas DataFrames."""
    engine = get_database_engine()
    if engine is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # Return empty DFs on error

    try:
        fact_sales_df = pd.read_sql_table('fact_sales', con=engine)
        dim_customer_df = pd.read_sql_table('dim_customer', con=engine)
        dim_product_df = pd.read_sql_table('dim_product', con=engine)
        return fact_sales_df, dim_customer_df, dim_product_df
    except Exception as e:
        st.error(f"Error loading data from database tables: {e}")
        st.info("Please ensure tables ('fact_sales', 'dim_customer', 'dim_product') exist and contain data.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # Return empty DFs on error

def main():
    st.set_page_config(layout="wide") # Use wide layout for better visualization space
    st.title("E-commerce Sales Dashboard")

    # --- Navigation Sidebar ---
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Sales Overview", "View Datasets"]) # Moved inside main()

    # Load data once at the start of main()
    fact_sales, dim_customer, dim_product = load_data()

    # Centralized check for empty dataframes
    if fact_sales.empty or dim_customer.empty or dim_product.empty:
        st.warning("No data available to display. Please check database connection and ensure data has been loaded using `load.py`.")
        # If no data, stop here for charts and dataset view will show empty.
        # No need for st.stop() or return pd.DataFrame() inside the if page == "Sales Overview" block
        # as the warning handles it.
        if page == "Sales Overview": # Only show this for the dashboard view
            return # Exit main if no data and on sales overview page

    if page == "Sales Overview":
        st.header("Sales Overview")

        # --- Merge DataFrames for analysis ---
        sales_data = pd.merge(fact_sales, dim_customer, on='customer_pk', how='left')
        sales_data = pd.merge(sales_data, dim_product, on='product_pk', how='left')

        # Drop rows where essential merge keys might be missing after left joins
        # (e.g., if a customer_pk or product_pk in fact_sales doesn't exist in dims)
        sales_data.dropna(subset=['product_name', 'purchase_address'], inplace=True) # Ensure product_name/address exist

        # --- Convert 'order_date' to datetime ---
        # Ensure 'order_date' is datetime after potential merge and NaT handling
        sales_data['order_date'] = pd.to_datetime(sales_data['order_date'], errors='coerce')
        sales_data.dropna(subset=['order_date'], inplace=True) # Drop rows if order_date became NaT


        # --- 1. Total Sales Over Time (Line Chart) ---
        st.subheader("Total Sales Over Time")
        daily_sales = sales_data.groupby(sales_data['order_date'].dt.date)['total_amount'].sum().reset_index()
        daily_sales.columns = ['order_date', 'total_sales']
        fig_sales_over_time = px.line(daily_sales, x='order_date', y='total_sales',
                                       title='Daily Total Sales',
                                       labels={'total_sales': 'Total Sales Amount', 'order_date': 'Order Date'})
        st.plotly_chart(fig_sales_over_time, use_container_width=True)

        # --- 2. Sales by Product (Bar Chart) ---
        st.subheader("Sales by Product")
        product_sales = sales_data.groupby('product_name')['total_amount'].sum().sort_values(ascending=False).head(10).reset_index()
        product_sales.columns = ['product_name', 'total_sales']
        fig_sales_by_product = px.bar(product_sales, x='total_sales', y='product_name', orientation='h', # horizontal bars
                                       title='Top 10 Products by Total Sales',
                                       labels={'total_sales': 'Total Sales Amount', 'product_name': 'Product Name'})
        fig_sales_by_product.update_layout(yaxis={'categoryorder':'total ascending'}) # Order bars from smallest to largest
        st.plotly_chart(fig_sales_by_product, use_container_width=True)

        # --- 3. Quantity of Items Sold by Product (Bar Chart) ---
        st.subheader("Quantity of Items Sold by Product")
        product_quantity = sales_data.groupby('product_name')['quantity'].sum().sort_values(ascending=False).head(10).reset_index()
        product_quantity.columns = ['product_name', 'quantity_sold']
        fig_quantity_by_product = px.bar(product_quantity, x='quantity_sold', y='product_name', orientation='h', # horizontal bars
                                           title='Top 10 Products by Quantity Sold',
                                           labels={'quantity_sold': 'Quantity Sold', 'product_name': 'Product Name'})
        fig_quantity_by_product.update_layout(yaxis={'categoryorder':'total ascending'}) # Order bars from smallest to largest
        st.plotly_chart(fig_quantity_by_product, use_container_width=True)

    elif page == "View Datasets":
        st.header("View Raw Datasets")

        dataset_choice = st.selectbox(
            "Select a dataset to view:",
            ("Fact Sales", "Dim Customer", "Dim Product")
        )

        if dataset_choice == "Fact Sales":
            st.subheader("Fact Sales Table")
            if not fact_sales.empty:
                st.write(f"Number of rows: {len(fact_sales)}")
                st.dataframe(fact_sales)
            else:
                st.info("Fact Sales data not loaded or is empty.")
        elif dataset_choice == "Dim Customer":
            st.subheader("Dim Customer Table")
            if not dim_customer.empty:
                st.write(f"Number of rows: {len(dim_customer)}")
                st.dataframe(dim_customer)
            else:
                st.info("Dim Customer data not loaded or is empty.")
        elif dataset_choice == "Dim Product":
            st.subheader("Dim Product Table")
            if not dim_product.empty:
                st.write(f"Number of rows: {len(dim_product)}")
                st.dataframe(dim_product)
            else:
                st.info("Dim Product data not loaded or is empty.")

if __name__ == "__main__":
    main()