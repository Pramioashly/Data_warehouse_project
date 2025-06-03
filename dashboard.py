import streamlit
import pandas as pd

# Load the data
df = pd.read_csv('Sales_July_2019.csv')

# Create a Streamlit dashboard
streamlit.title('Sales Dashboard')

# Create a table
streamlit.write(df)

# Create a chart
streamlit.line_chart(df['order_date'], df['quantity_ordered'])

# Create a bar chart
streamlit.bar_chart(df['product'], df['quantity_ordered'])

# Create a pie chart
streamlit.pie_chart(df['product'], df['quantity_ordered'])

# Create a histogram
streamlit.histogram(df['quantity_ordered'])

# Create a scatter plot
streamlit.scatter(df['order_date'], df['quantity_ordered'])
