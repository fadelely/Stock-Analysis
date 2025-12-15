# Trading and Portfolio Dataset Description

This dataset contains **10,000 rows and 16 features** representing trading and portfolio transactions from **early 2023 to late 2024**.  
The data was compiled by merging several single-source datasets of different types — including trading records, customer profiles, and market data — into one unified dataset.  
Each record corresponds to a single transaction made by a customer in a specific stock on a given date, capturing details about trading behavior, market conditions, and stock characteristics.

This project delivers an **end-to-end, containerized data pipeline** integrating data ingestion, processing, analytics, visualization, and an AI-powered natural language query agent.

## Technologies Used
- Apache Airflow
- PostgreSQL
- Kafka
- Apache Spark
- Streamlit
- Hugging Face LLM (AI Agent)
- Docker & Docker Compose

---
## Project Architecture
The system consists of the following components:

- **Airflow**: Orchestrates ETL and analytics workflows
- **PostgreSQL**: Stores cleaned and analytical datasets
- **Kafka**: Handles streaming data (producer/consumer)
- **Spark**: Performs batch analytics and transformations
- **Streamlit**: Displays analytics dashboards
- **AI Agent**: Converts natural language queries into SQL

All components communicate over a shared Docker network.

## Feature Description

| Feature Name             | Description                                                                 | Data Type  | Example Values                |
|---------------------------|------------------------------------------------------------------------------|-------------|--------------------------------|
| **transaction_id**        | Unique identifier for each trading transaction.                             | Integer     | `2451`                         |
| **timestamp**             | Date when the transaction occurred.                                         | Date        | `2024-05-18`                   |
| **customer_id**           | Unique identifier assigned to each customer participating in trades.        | Integer     | `1327`                         |
| **stock_ticker**          | The symbol representing the traded stock on the market.                     | String      | `STK001`, `STK010`, `STK020`   |
| **transaction_type**      | Type of transaction executed by the customer.                               | String      | `Buy`, `Sell`                  |
| **quantity**              | Number of shares traded in the transaction.                                 | Integer     | —                              |
| **average_trade_size**    | Average number of shares traded by the same customer across transactions.   | Float       | —                              |
| **stock_price**           | Price per share of the stock at the time of the transaction.                | Float       | —                              |
| **total_trade_amount**    | Total value of the trade calculated as quantity × stock_price.              | Float       | —                              |
| **customer_account_type** | Classification of the customer’s trading account.                           | String      | `Retail`, `Institutional`      |
| **day_name**              | Day of the week when the transaction took place.                            | String      | `Monday`, `Tuesday`, `Friday`  |
| **is_weekend**            | Indicates whether the transaction occurred during the weekend.              | Boolean     | `True`, `False`                |
| **is_holiday**            | Indicates if the trading day coincided with a public or market holiday.     | Boolean     | `True`, `False`                |
| **stock_liquidity_tier**  | Categorization of the stock based on trading volume and liquidity.           | String      | `High`, `Medium`, `Low`        |
| **stock_sector**          | The economic sector the traded stock belongs to.                            | String      | `Technology`, `Healthcare`, `Finance` |
| **stock_industry**        | The specific industry classification of the traded stock.                   | String      | `Software`, `Pharmaceuticals`, `Banking` |

Each of the categorical features were encoded, with their lookup table being seen below.

| Column Name            | Original Value                                        | Encoded Value |
|------------------------|------------------------------------------------------|---------------|
| transaction_type       | BUY                                                  | 1             |
| transaction_type       | SELL                                                 | 0             |
| customer_account_type  | Retail                                               | 1             |
| customer_account_type  | Institutional                                        | 0             |
| stock_liquidity_tier   | Low                                                  | 0             |
| stock_liquidity_tier   | Mid                                                  | 1             |
| stock_liquidity_tier   | High                                                 | 2             |
| average_trade_size     | Small                                                | 0             |
| average_trade_size     | Medium                                               | 1             |
| average_trade_size     | Large                                                | 2             |
| average_trade_size     | Very Large                                           | 3             |
| average_trade_size     | Extreme                                              | 4             |
| is_weekend             | True                                                 | 1             |
| is_weekend             | False                                                | 0             |
| is_holiday             | True                                                 | 1             |
| is_holiday             | False                                                | 0             |

Below are the features that are one hot encoded, with all of the possible values for that feature.
| Column Name     | Original Values (Categories)                                 | Encoding Type |
|-----------------|-------------------------------------------------------------|---------------|
| stock_ticker    | STK001 – STK020                                             | One-Hot       |
| day_name        | Monday, Tuesday, Wednesday, Thursday, Friday               | One-Hot       |
| stock_sector    | Consumer, Energy, Finance, Healthcare, Technology          | One-Hot       |
| stock_industry  | Asset Management, Hardware, Oil & Gas, Pharmaceuticals,    | One-Hot       |
|                 | Renewables, Retail, Software, Utilities                     | One-Hot       |


# Running with Docker Compose
### Build and Start All Services
`docker compose up --build`

### To run after building
` docker compose up -d`

### To watch logs 
`docker logs stock-analysis --follow`

### Stop All Services
`docker compose down`


### Accessing services
Airflow UI: http://localhost:8080

Use the Airflow UI to:
- Monitor DAG execution
- View task logs
- Debug failures

Acess Streamlit: http://localhost:8501


## agent
The AI Agent allows users to ask questions in natural language and receive
automatically generated SQL queries.

### Create a .env file in the project root if using the AI Agent:
`HF_TOKEN= ...`
### command to run the agent seperatly 
python3 agent/ai_agent.py



