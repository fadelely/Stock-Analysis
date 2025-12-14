import os
import sys
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_utils import load_from_db

# Set page configuration
st.set_page_config(page_title="Trading Volume Dashboard", layout="wide")

# Connect to PostgreSQL
engine = create_engine("postgresql://root:root@pgdatabase:5432/stockanalysis")


st.title("Real-Time Stock Portfolio Analytics")
st.sidebar.header("Filters")
data = load_from_db("visualization") 
stocks_df = pd.DataFrame(data)
stocks_df['timestamp'] = pd.to_datetime(stocks_df['timestamp'])

st.sidebar.markdown("---")
st.sidebar.subheader("Date Filter")

min_date = stocks_df['timestamp'].min().date()
max_date = stocks_df['timestamp'].max().date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

### QUERY 1 ####
# Data
query_1_data = load_from_db("spark_analytics_1") 
total_volume_df = pd.DataFrame(query_1_data)
total_volume_df["stock_ticker"] = total_volume_df["stock_ticker"].str.upper()
total_volume_df = total_volume_df.sort_values("total_volume", ascending=False)
print(total_volume_df)

all_tickers = sorted(total_volume_df['stock_ticker'].unique().tolist())
# Initialize session state
if 'selected_tickers' not in st.session_state:
    st.session_state.selected_tickers = all_tickers

# Select All button
if st.sidebar.button("Select All"):
    st.session_state.selected_tickers = all_tickers

selected_tickers = st.sidebar.multiselect(
    "Filter by Stock Tickers",
    options=all_tickers,
    default=st.session_state.selected_tickers,
    help="Search and select stock tickers to display"
)

st.session_state.selected_tickers = selected_tickers

if selected_tickers:
    filtered_df = total_volume_df[total_volume_df['stock_ticker'].isin(selected_tickers)]
else:
    filtered_df = total_volume_df
    st.sidebar.info("No tickers selected - showing all stocks")

min_vol = st.sidebar.slider("Minimum Volume", 0, int(total_volume_df['total_volume'].max()), 0)
filtered_df = filtered_df[filtered_df['total_volume'] >= min_vol]

sort_order = st.sidebar.radio("Sort Order", ["Descending", "Ascending"])
if sort_order == "Ascending":
    filtered_df = filtered_df.sort_values("total_volume", ascending=True)

fig_bar = px.bar(filtered_df, 
    x="stock_ticker", 
    y="total_volume",
    color="total_volume",
    color_continuous_scale="Blues",
    title="Trading Volume Distribution",
    labels={"total_volume": "Trading Volume", "stock_ticker": "Stock Ticker"})

fig_bar.update_layout(height=500, showlegend=False)
fig_bar.update_traces(texttemplate='%{y:,}', textposition='outside')
st.plotly_chart(fig_bar, use_container_width=True)


### QUERY 2 ###
# Query for stock price trends by sector
price_by_sector = stocks_df.groupby(['timestamp', 'stock_sector'])['stock_price'].mean().reset_index()

# Convert timestamp to datetime if not already
price_by_sector['timestamp'] = pd.to_datetime(price_by_sector['timestamp'])

# Streamlit visualization
st.markdown("---")
st.subheader("📊 Stock Price Trends by Sector")

# Create line chart
fig_sector_trends = px.line(
    price_by_sector,
    x='timestamp',
    y='stock_price',
    color='stock_sector',
    title='Average Stock Price Trends by Sector',
    labels={'stock_price': 'Average Stock Price ($)', 'timestamp': 'Date', 'stock_sector': 'Sector'}
)

fig_sector_trends.update_layout(height=500)
st.plotly_chart(fig_sector_trends, use_container_width=True)


### QUERY 3 ###
buys_sells_by_date = stocks_df.groupby(['timestamp', 'transaction_type']).size().reset_index(name='count')

if len(date_range) == 2:
    start_date, end_date = date_range
    buys_sells_filtered = buys_sells_by_date[
        (buys_sells_by_date['timestamp'].dt.date >= start_date) & 
        (buys_sells_by_date['timestamp'].dt.date <= end_date)
    ]
else:
    buys_sells_filtered = buys_sells_by_date

st.markdown("---")
st.subheader("Buy vs Sell Transactions Over Time")

fig_transactions = px.line(
    buys_sells_filtered,
    x='timestamp',
    y='count',
    color='transaction_type',
    title='Buy and Sell Transactions by Date',
    labels={'count': 'Number of Transactions', 'timestamp': 'Date', 'transaction_type': 'Transaction Type'},
    color_discrete_map={'buy': '#2ecc71', 'sell': '#e74c3c'}  # Green for buy, red for sell
)

fig_transactions.update_layout(height=500)
st.plotly_chart(fig_transactions, use_container_width=True)

### QUERY 4 ###

activity_by_day = stocks_df.groupby('day_name').size().reset_index(name='transaction_count')

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
activity_by_day['day_name'] = pd.Categorical(activity_by_day['day_name'], categories=day_order, ordered=True)
activity_by_day = activity_by_day.sort_values('day_name')

st.markdown("---")
st.subheader("📅 Trading Activity by Day of Week")

# Create bar chart
fig_day_activity = px.bar(
    activity_by_day,
    x='day_name',
    y='transaction_count',
    title='Trading Activity by Day of Week',
    labels={'transaction_count': 'Number of Transactions', 'day_name': 'Day of Week'},
    color='transaction_count',
    color_continuous_scale='Viridis'
)

fig_day_activity.update_layout(height=500, showlegend=False)
fig_day_activity.update_traces(texttemplate='%{y:,}', textposition='outside')
st.plotly_chart(fig_day_activity, use_container_width=True)


### QUERY 5 ###
# Query for customer transaction distribution
customer_transactions = stocks_df.groupby('customer_id').size().reset_index(name='transaction_count')

# Streamlit visualization
st.markdown("---")
st.subheader("👤 Customer Transaction Distribution")

# Create histogram
fig_customer_dist = px.histogram(
    customer_transactions,
    x='transaction_count',
    title='Distribution of Transactions per Customer',
    labels={'transaction_count': 'Number of Transactions', 'count': 'Number of Customers'},
    nbins=50,
    color_discrete_sequence=['#3498db']
)

fig_customer_dist.update_layout(
    height=500,
    showlegend=False,
    bargap=0.1
)
st.plotly_chart(fig_customer_dist, use_container_width=True)

# Show summary statistics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Customers", f"{len(customer_transactions):,}")
with col2:
    st.metric("Avg Transactions", f"{customer_transactions['transaction_count'].mean():.1f}")
with col3:
    st.metric("Max Transactions", f"{customer_transactions['transaction_count'].max():,}")
with col4:
    st.metric("Min Transactions", f"{customer_transactions['transaction_count'].min():,}")

### QUERY 6 ###
# Query for top 10 customers by trade amount
top_customers = stocks_df.groupby('customer_id')['total_trade_amount'].sum().reset_index()
top_customers = top_customers.sort_values('total_trade_amount', ascending=False).head(10)
top_customers = top_customers.reset_index(drop=True)
top_customers['rank'] = range(1, len(top_customers) + 1)

# Streamlit visualization
st.markdown("---")
st.subheader("👥 Top 10 Customers by Trade Amount")

# Create horizontal bar chart
fig_top_customers = px.bar(
    top_customers,
    y='rank',
    x='total_trade_amount',
    orientation='h',
    title='Top 10 Customers by Total Trade Amount',
    labels={'total_trade_amount': 'Total Trade Amount ($)', 'rank': 'Rank'},
    color='total_trade_amount',
    color_continuous_scale='Blues',
    hover_data={'customer_id': True, 'total_trade_amount': ':$,.0f', 'rank': False}
)

fig_top_customers.update_layout(
    height=500, 
    showlegend=False,
    yaxis={'autorange': 'reversed', 'tickmode': 'linear', 'tick0': 1, 'dtick': 1}
)
fig_top_customers.update_traces(texttemplate='$%{x:,.0f}', textposition='outside')
st.plotly_chart(fig_top_customers, use_container_width=True)