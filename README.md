# PFin Financial Analyst

`pfin-financial-analyst` is a data engineering project focused on building an ETL pipeline for financial market data and serving that data through an API.

The current repository is centered on the data pipeline and persistence layer. The presentation layer with `FastAPI` is the next step and is not implemented yet.

## Current Status

- Extract company and market data from `yfinance`
- Transform raw responses into normalized records
- Load company and stock history data into PostgreSQL
- Define the relational schema in SQL
- Cover the ingestion layer with unit tests
- FastAPI API layer is planned next

## Project Scope

The pipeline currently supports:

1. Reading ticker symbols from a CSV seed file
2. Fetching company metadata from `yfinance`
3. Cleaning and reshaping company data
4. Fetching historical stock prices
5. Normalizing stock history into row-based records
6. Loading companies, sectors, industries, locations, and stock history into PostgreSQL

## Tech Stack

- Python
- Pandas
- yFinance
- PostgreSQL
- SQLAlchemy
- Docker Compose
- Pytest
- FastAPI (dependency added, API not built yet)

## Repository Structure

```text
.
├── data_pipeline/
│   ├── companies.csv
│   ├── database_utils.py
│   └── ingestion.py
├── database-scripts/
│   └── fin_tables.sql
├── tests/
│   └── test_ingestion.py
├── airflow/
│   └── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Data Model

The database schema is defined in `database-scripts/fin_tables.sql`.

Main tables:

- `LOCATIONS`
- `INDUSTRIES`
- `SECTORS`
- `COMPANIES`
- `STOCK_HISTORY`

This schema separates company reference data from time-series stock history and keeps sector, industry, and location data normalized.

## ETL Flow

The ingestion logic lives in `data_pipeline/ingestion.py`.

High-level flow:

1. Read symbols from `data_pipeline/companies.csv`
2. Fetch company information with `yfinance`
3. Keep only the fields needed by the project
4. Filter out incomplete company records before loading
5. Build unique sector, industry, and location sets
6. Insert reference data into PostgreSQL
7. Upsert companies into the `COMPANIES` table
8. Read stored company symbols back from the database
9. Fetch historical stock prices using `FETCHING_PERIOD` with a default of `1mo`
10. Normalize historical data and map symbols to company IDs
11. Insert stock history into `STOCK_HISTORY`

## Local Setup

### 1. Install dependencies

This project includes a `pyproject.toml` and `uv.lock`.

Using `uv`:

```bash
uv sync
```

Or with a virtual environment and `pip`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Start PostgreSQL locally

The repository includes a `docker-compose.yml` with:

- PostgreSQL on `localhost:5432`
- pgAdmin on `localhost:8085`

Start services with:

```bash
docker compose up -d
```

### 3. Configure environment variables

The database connection is loaded from environment variables in `data_pipeline/database_utils.py`.

Required variables:

```bash
DB_NAME=
DB_USERNAME=
DB_PASSWORD=
DB_HOST=localhost
DB_PORT=5432
```

### 4. Create the schema

Run the SQL in `database-scripts/fin_tables.sql` against your PostgreSQL database before executing the load steps.

## Running Tests

The ingestion layer has unit tests in `tests/test_ingestion.py`.

Run them with:

```bash
pytest
```

If `pytest` is only available in the virtual environment, use:

```bash
.venv/bin/pytest
```

## Notes

- The current codebase is focused on the ETL pipeline, not the API layer yet.
- The project depends on external market data from `yfinance`.
- The ETL entrypoint lives in `data_pipeline/ingestion.py` and runs the full company and stock-history load flow.
- During ingestion, company records missing required fields such as symbol, display name, city, or country are skipped before loading.
- An `airflow/` directory exists, but orchestration is not yet integrated into the pipeline workflow.

## Running The ETL

After the database schema is created and the environment variables are configured, run:

```bash
python -m data_pipeline.ingestion
```

The pipeline will:

1. Read symbols from `data_pipeline/companies.csv`
2. Load valid company records into PostgreSQL
3. Fetch stock history for the companies stored in the database
4. Load normalized stock history into `STOCK_HISTORY`

### History Period

The stock-history fetch period defaults to `1mo`.

You can override it with:

```bash
FETCHING_PERIOD=3mo python -m data_pipeline.ingestion
```

## Next Step

The next milestone is the presentation layer:

- Build a `FastAPI` application on top of the loaded PostgreSQL data
- Expose financial and company data through API endpoints
- Add a clear service boundary between ETL and API concerns

## Roadmap

- Finish the FastAPI presentation layer
- Add API routes for companies and stock history
- Add integration tests for the database layer
- Add orchestration for scheduled ETL runs
- Improve end-to-end pipeline execution
