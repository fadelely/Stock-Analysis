# import streamlit as st
# import pandas as pd
# import plotly.express as px
# from sqlalchemy import create_engine
# # Connect to PostgreSQL
# engine = create_engine('postgresql://user:pass@postgres:5432/stocks_db')
# # Dashboard layout
# st.title("Real-Time Stock Portfolio Analytics")
# st.sidebar.header("Filters")

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# Sample Data
data = {'Stage': ['Extraction', 'Transformation', 'Loading'], 'Time (s)': [10, 20, 5]}
df = pd.DataFrame(data)

# Dashboard
st.title("Data Pipeline Dashboard")
st.bar_chart(df.set_index('Stage'))

# Adding a Line Chart
st.line_chart([10, 20, 15, 25])


# ---
# Simulated Data
log_data = {
    'Stage': ['Extraction', 'Transformation', 'Loading', 'Extraction', 'Transformation', 'Loading'],
    'Time (s)': [10, 20, 5, 12, 18, 7],
    'Errors': [0, 1, 0, 0, 2, 0]
}
df = pd.DataFrame(log_data)

fig, ax = plt.subplots(1, 2, figsize=(12, 6))
# Seaborn Visualizations
st.title("ETL Pipeline Analysis")
sns.barplot(x='Stage', y='Time (s)', data=df, ax=ax[0])

# Error Analysis
sns.boxplot(x='Stage', y='Errors', data=df, ax=ax[1])
st.pyplot(fig)