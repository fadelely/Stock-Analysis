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

# Load data
data = load_from_db("visualization") 
stocks_df = pd.DataFrame(data)
stocks_df['timestamp'] = pd.to_datetime(stocks_df['timestamp'])

### FILTERS ###
st.sidebar.header("Filters")

st.sidebar.subheader("Date Filter")
min_date = stocks_df['timestamp'].min().date()
max_date = stocks_df['timestamp'].max().date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

st.sidebar.markdown("---")
st.sidebar.subheader("Stock Ticker Filter")
all_tickers = sorted(stocks_df['stock_ticker'].unique().tolist())

selected_tickers = st.sidebar.multiselect(
    "Filter by Stock Tickers",
    options=all_tickers,
    default=all_tickers,
    help="Search and select stock tickers to display"
)

st.sidebar.markdown("---")
st.sidebar.subheader("Sector Filter")
all_sectors = sorted(stocks_df['stock_sector'].unique().tolist())

selected_sectors = st.sidebar.multiselect(
    "Filter by Sectors",
    options=all_sectors,
    default=all_sectors,
    help="Search and select sectors to display"
)

st.sidebar.markdown("---")
st.sidebar.subheader("Customer Type Filter")
all_account_types = sorted(stocks_df['customer_account_type'].unique().tolist())

selected_account_types = st.sidebar.multiselect(
    "Filter by Account Types",
    options=all_account_types,
    default=all_account_types,
    help="Search and select account types to display"
)

### FILTERING DATAFRAME ###
filtered_stocks_df = stocks_df.copy()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_stocks_df = filtered_stocks_df[
        (filtered_stocks_df['timestamp'].dt.date >= start_date) & 
        (filtered_stocks_df['timestamp'].dt.date <= end_date)
    ]

if selected_tickers:
    filtered_stocks_df = filtered_stocks_df[filtered_stocks_df['stock_ticker'].isin(selected_tickers)]

if selected_sectors:
    filtered_stocks_df = filtered_stocks_df[filtered_stocks_df['stock_sector'].isin(selected_sectors)]

if selected_account_types:
    filtered_stocks_df = filtered_stocks_df[filtered_stocks_df['customer_account_type'].isin(selected_account_types)]

st.markdown("---")
st.subheader("Portfolio Performance Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

total_volume = filtered_stocks_df['quantity'].sum()
total_trade_amount = filtered_stocks_df['total_trade_amount'].sum()
total_transactions = len(filtered_stocks_df)
avg_trade_size = filtered_stocks_df['total_trade_amount'].mean()
unique_customers = filtered_stocks_df['customer_id'].nunique()

col1.metric("Total Volume", f"{total_volume:,}")
col2.metric("Total Trade Amount", f"${total_trade_amount:,.0f}")
col3.metric("Total Transactions", f"{total_transactions:,}")
col4.metric("Avg Trade Size", f"${avg_trade_size:,.2f}")
col5.metric("Active Customers", f"{unique_customers:,}")

# QUERY 1: Trading Volume by Stock Ticker ###
total_volume_df = filtered_stocks_df.groupby('stock_ticker')['quantity'].sum().reset_index()
total_volume_df.columns = ['stock_ticker', 'total_volume']
total_volume_df = total_volume_df.sort_values('total_volume', ascending=False)

st.markdown("---")
st.subheader("Trading Volume by Stock Ticker")
fig_bar = px.bar(
    total_volume_df, 
    x="stock_ticker", 
    y="total_volume",
    color="total_volume",
    color_continuous_scale="Blues",
    title="Trading Volume Distribution",
    labels={"total_volume": "Trading Volume", "stock_ticker": "Stock Ticker"}
)

fig_bar.update_layout(height=500, showlegend=False)
fig_bar.update_traces(texttemplate='%{y:,}', textposition='outside')
st.plotly_chart(fig_bar, use_container_width=True)

### QUERY 2: Stock Price Trends by Sector  ###
price_by_sector = filtered_stocks_df.groupby(['timestamp', 'stock_sector'])['stock_price'].mean().reset_index()

st.markdown("---")
st.subheader("Stock Price Trends by Sector")

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

### QUERY 3: Buy vs Sell Transactions ###
buys_sells_by_date = filtered_stocks_df.groupby(['timestamp', 'transaction_type']).size().reset_index(name='count')

st.markdown("---")
st.subheader("Buy vs Sell Transactions Over Time")

fig_transactions = px.line(
    buys_sells_by_date,
    x='timestamp',
    y='count',
    color='transaction_type',
    title='Buy and Sell Transactions by Date',
    labels={'count': 'Number of Transactions', 'timestamp': 'Date', 'transaction_type': 'Transaction Type'},
    color_discrete_map={'Buy': '#2ecc71', 'Sell': '#e74c3c'}
)

fig_transactions.update_layout(height=500)
st.plotly_chart(fig_transactions, use_container_width=True)

### QUERY 4: Trading Activity by Day of Week ###
activity_by_day = filtered_stocks_df.groupby('day_name').size().reset_index(name='transaction_count')

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
activity_by_day['day_name'] = pd.Categorical(activity_by_day['day_name'], categories=day_order, ordered=True)
activity_by_day = activity_by_day.sort_values('day_name')

st.markdown("---")
st.subheader("Trading Activity by Day of Week")

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

### QUERY 5: Customer Transaction Distribution ###
customer_transactions = filtered_stocks_df.groupby(['customer_id', 'customer_account_type']).size().reset_index(name='transaction_count')

st.markdown("---")
st.subheader("Customer Transaction Distribution")

fig_customer_dist = px.histogram(
    customer_transactions,
    x='transaction_count',
    color='customer_account_type',
    title='Distribution of Transactions per Customer by Account Type',
    labels={'transaction_count': 'Number of Transactions', 'count': 'Number of Customers'},
    nbins=150,
    barmode='overlay'
)

fig_customer_dist.update_layout(height=500, bargap=0.1)
fig_customer_dist.update_traces(opacity=0.7)
st.plotly_chart(fig_customer_dist, use_container_width=True)

st.subheader("Statistics by Account Type")
account_stats = customer_transactions.groupby('customer_account_type')['transaction_count'].agg(['count', 'mean', 'max']).reset_index()
account_stats.columns = ['Account Type', 'Total Customers', 'Avg Transactions', 'Total Transactions']
st.dataframe(account_stats, use_container_width=True)

### QUERY 6: Top 10 Customers by Trade Amount ###
top_customers = filtered_stocks_df.groupby('customer_id')['total_trade_amount'].sum().reset_index()
top_customers = top_customers.sort_values('total_trade_amount', ascending=False).head(10)
top_customers = top_customers.reset_index(drop=True)
top_customers['rank'] = range(1, len(top_customers) + 1)

st.markdown("---")
st.subheader("Top 10 Customers by Trade Amount")

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

### ADVANCED VISUALIZATION QUERY: Holiday vs Non-Holiday Trading Analysis ###
st.markdown("---")
st.header("Advanced Visualizations")
st.markdown("---")
st.subheader("Holiday vs Non-Holiday Trading Analysis")

# Total transactions comparison
holiday_comparison = filtered_stocks_df.groupby('is_holiday').size().reset_index(name='transaction_count')
holiday_comparison['holiday_label'] = holiday_comparison['is_holiday'].map({'True': 'Holiday', 'False': 'Non-Holiday'})

col1, col2 = st.columns(2)

with col1:
    fig_holiday_pie = px.pie(
        holiday_comparison,
        values='transaction_count',
        names='holiday_label',
        title='Total Transactions: Holiday vs Non-Holiday',
        color='holiday_label',
        color_discrete_map={'Holiday': '#e74c3c', 'Non-Holiday': '#3498db'}
    )
    fig_holiday_pie.update_traces(textposition='inside', textinfo='percent+label+value')
    st.plotly_chart(fig_holiday_pie, use_container_width=True)


# Sector distribution for Holiday transactions
st.subheader("Sector Distribution")

col3, col4 = st.columns(2)

with col3:
    holiday_sectors = filtered_stocks_df[filtered_stocks_df['is_holiday'] == 'True'].groupby('stock_sector').size().reset_index(name='count')
    
    fig_holiday_sectors = px.pie(
        holiday_sectors,
        values='count',
        names='stock_sector',
        title='Sectors Traded on Holidays',
        color_discrete_sequence=px.colors.sequential.Reds_r
    )
    fig_holiday_sectors.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_holiday_sectors, use_container_width=True)

with col4:
    non_holiday_sectors = filtered_stocks_df[filtered_stocks_df['is_holiday'] == 'False'].groupby('stock_sector').size().reset_index(name='count')
    
    fig_non_holiday_sectors = px.pie(
        non_holiday_sectors,
        values='count',
        names='stock_sector',
        title='Sectors Traded on Non-Holidays',
        color_discrete_sequence=px.colors.sequential.Blues_r
    )
    fig_non_holiday_sectors.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_non_holiday_sectors, use_container_width=True)

# Stock ticker distribution
st.subheader("Top Stock Tickers Distribution")

col5, col6 = st.columns(2)

with col5:
    holiday_tickers = filtered_stocks_df[filtered_stocks_df['is_holiday'] == 'True'].groupby('stock_ticker').size().reset_index(name='count')
    holiday_tickers = holiday_tickers.sort_values('count', ascending=False).head(10)
    
    fig_holiday_tickers = px.pie(
        holiday_tickers,
        values='count',
        names='stock_ticker',
        title='Top 10 Stock Tickers Traded on Holidays',
        color_discrete_sequence=px.colors.sequential.Reds_r
    )
    fig_holiday_tickers.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_holiday_tickers, use_container_width=True)

with col6:
    non_holiday_tickers = filtered_stocks_df[filtered_stocks_df['is_holiday'] == 'False'].groupby('stock_ticker').size().reset_index(name='count')
    non_holiday_tickers = non_holiday_tickers.sort_values('count', ascending=False).head(10)
    
    fig_non_holiday_tickers = px.pie(
        non_holiday_tickers,
        values='count',
        names='stock_ticker',
        title='Top 10 Stock Tickers Traded on Non-Holidays',
        color_discrete_sequence=px.colors.sequential.Blues_r
    )
    fig_non_holiday_tickers.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_non_holiday_tickers, use_container_width=True)
