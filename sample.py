import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import plotly.express as px # Import Plotly Express

st.set_page_config(layout="wide")
# --- Configuration ---
# Construct the path to the .env file, assuming it's in the project root
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

DATABASE_URL = os.getenv("EXTERNAL_DATABASE_URL")

# --- Database Connection ---
@st.cache_resource # Cache the database connection for efficiency
def get_db_connection():
    if not DATABASE_URL:
        st.error("Error: 'EXTERNAL_DATABASE_URL' environment variable is not set.")
        st.stop()
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop()
        return None

engine = get_db_connection()

# --- Data Loading Functions ---
@st.cache_data(ttl=3600) # Cache data for 1 hour to avoid constant re-querying
def load_data():
    if engine is None:
        return pd.DataFrame() # Return empty if no engine

    try:
        # Load Fact Table
        fact_sales_df = pd.read_sql_table('fact_sales', con=engine)

        # Load Dimension Tables
        dim_customer_df = pd.read_sql_table('dim_customer', con=engine)
        dim_product_df = pd.read_sql_table('dim_product', con=engine)

        # Merge for analysis
        df = fact_sales_df.merge(dim_customer_df, on='customer_pk', how='left')
        df = df.merge(dim_product_df, on='product_pk', how='left')

        # Ensure date column is datetime type for plotting
        df['order_date'] = pd.to_datetime(df['order_date'])

        st.success("Data loaded successfully!")
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        st.stop()
        return pd.DataFrame()

df = load_data()



st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard Overview", "Geographical Analysis"])

if df.empty:
    st.warning("No data available to display. Please ensure your `load.py` script ran successfully.")
    st.stop()

if page == "Dashboard Overview":
    st.title("Sales Performance Dashboard")

    # --- Filters (Sidebar - still on Overview page) ---
    st.sidebar.header("Overview Filters")

    min_date = df['order_date'].min()
    max_date = df['order_date'].max()
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        start_date = date_range[0]
        end_date = date_range[1]
        df_filtered = df[(df['order_date'] >= pd.to_datetime(start_date)) & (df['order_date'] <= pd.to_datetime(end_date))]
    else:
        df_filtered = df.copy()

    all_regions = ['All'] + sorted(df_filtered['city'].dropna().unique().tolist())
    selected_region = st.sidebar.selectbox("Select Region (City)", all_regions)

    if selected_region != 'All':
        df_filtered = df_filtered[df_filtered['city'] == selected_region]

    st.subheader("Data Overview")
    st.dataframe(df_filtered.head())
    st.write(f"Total records after filters: {len(df_filtered)}")

    if df_filtered.empty:
        st.warning("No data matches the selected filters for this view.")
    else:
        # --- Metrics ---
        total_sales = df_filtered['total_amount'].sum()
        total_quantity = df_filtered['quantity'].sum()
        unique_products = df_filtered['product_name'].nunique()
        unique_customers = df_filtered['purchase_address'].nunique()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sales", f"${total_sales:,.2f}")
        col2.metric("Total Quantity Sold", f"{total_quantity:,.0f}")
        col3.metric("Unique Products Sold", f"{unique_products}")
        col4.metric("Unique Customers", f"{unique_customers}")

        st.markdown("---")

        # --- Visualizations ---
        st.subheader("Sales Trends and Performance")

        # Sales Over Time
        sales_over_time = df_filtered.groupby('order_date')['total_amount'].sum().reset_index()
        st.line_chart(sales_over_time.set_index('order_date'))
        st.write("Daily Sales Over Time")

        st.markdown("---")

        # Sales by Product
        sales_by_product = df_filtered.groupby('product_name')['total_amount'].sum().nlargest(10).reset_index()
        st.bar_chart(sales_by_product.set_index('product_name'))
        st.write("Top 10 Products by Sales")

        st.markdown("---")

        # Sales by Region (City in this case)
        sales_by_region_chart_df = df_filtered.groupby('city')['total_amount'].sum().sort_values(ascending=False).reset_index()
        st.bar_chart(sales_by_region_chart_df.set_index('city'))
        st.write("Sales by Region (City)")


elif page == "Geographical Analysis":
    st.title("Sales Geographical Analysis")
    st.markdown("This map shows the total sales amount per city.")

    # Aggregate data for the map
    # Ensure latitude/longitude are numeric and not None/NaN for cities
    map_df = df.dropna(subset=['latitude', 'longitude']) # Drop rows without coordinates

    if map_df.empty:
        st.warning("No geographical data available for mapping after filtering.")
    else:
        # Calculate 'Population' (Number of Customers per City) for the map
        customers_per_city = map_df.groupby(['city', 'latitude', 'longitude']).agg(
            total_sales=('total_amount', 'sum'),
            num_customers=('customer_pk', 'nunique'),
            num_orders=('order_id', 'nunique')
        ).reset_index()

        st.subheader("Map of Sales/Customers per City")

        # Create the scatter mapbox
        fig = px.scatter_mapbox(
            customers_per_city,
            lat="latitude",
            lon="longitude",
            size="total_sales", # Size of circle based on total sales
            color="num_customers", # Color of circle based on number of customers
            hover_name="city",
            hover_data={
                "total_sales": ":,.2f", # Format sales
                "num_customers": True,
                "num_orders": True,
                "latitude": False, # Hide lat/lon from hover
                "longitude": False
            },
            zoom=3, # Adjust zoom level as needed
            height=600,
            mapbox_style="carto-positron", # A clean map style
            title="Sales and Customer Density by City"
        )
        # fig.update_layout(mapbox_bounds={"west": -130, "east": -60, "south": 20, "north": 50}) # Optional: set boundaries for USA

        st.plotly_chart(fig, use_container_width=True)
        st.write("Circles are sized by Total Sales and colored by the Number of Unique Customers in each city.")