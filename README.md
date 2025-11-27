# Trading and Portfolio Dataset Description

This dataset contains **10,000 rows and 16 features** representing trading and portfolio transactions from **early 2023 to late 2024**.  
The data was compiled by merging several single-source datasets of different types — including trading records, customer profiles, and market data — into one unified dataset.  
Each record corresponds to a single transaction made by a customer in a specific stock on a given date, capturing details about trading behavior, market conditions, and stock characteristics.

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


# Running with Docker Compose
### Build and Start All Services
`docker compose up --build`

  This command builds the image (if not already built) and starts:
* **PostgreSQL database** (`pgdatabase`)
* **pgAdmin web interface** (`pg_admin`)
* **Main analysis app** (`stock-analysis`)

### To run after building
` docker compose up -d`

### Stop All Services
`docker compose down`
