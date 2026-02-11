Tags:[[Projects]] , [[Python]]

Version: V1.0

This is mainly documentation for my ideas
# Projects Goals
Develop a server in Python that gives financial calculations based on the stock market

## Specific Goals
* Build a robust data pipeline fetching weekly market data from Yahoo Finance (`yfinance package`)
* Persist raw financial data in PostgreSQL
* Compute technical indicators (Simple Moving Average, Daily Returns, Volatility)
* Expose processed data via a RESTful backend
* Containerize the Application using Docker

# Learning Goals
Learn how to develop a simple backend in Python

## Specific Goals
* Learn Python good practices
* Learn how to use packages like panda, numpy, fastapi
* Learn how to model and present data correctly
* Learn about Docker correctly
* Get better at data manipulation
* Get better at making APIs
* Get better at making database queries

# Requirements

| Req ID | Req Title             | Description                                                                                                  |
| ------ | --------------------- | ------------------------------------------------------------------------------------------------------------ |
| FR-01  | Data Ingestion        | Fetch historical daily stock prices for selected tickers like Google, Apple, Microsoft, etc                  |
| FR-02  | Indicator Calculation | Compute historic SMA, Daily Returns (%) and Volatility on stored data                                        |
| FR-03  | API Endpoints         | Serve JSON responses containing relevant data like raw data and calculated indicators based on user queries. |
| NFR-01 | Persistence           | Store all fetched data in a PostreSQL Database                                                               |
| NFR-02 | Containerization      | The API and Database must run together using Docker containers                                               |

# Tools
*  Python 3.14.3
* yfinance 1.1.0
* SQLAlchemy 2.0.46
* numpy 2.4.2
* pandas 3.0.0
* Docker 29.2.1
* PostgreSQL
* Git and Github
# Restrictions
* It will be limited to the stock information of specific companies (Google, Apple, NVIDIA, AMD, Microsoft, Samsung,  Tesla, Cloudfare, Intel)
* The initial calculations will be limited to SMA, Daily Returns and Volatility only.
* The fetched information will be limited to OHLCV (Open, High, Low, Close, Volume).
# For the Future
* C++ Montecarlo Simulations
* C++ Order book to simulate stock exchange