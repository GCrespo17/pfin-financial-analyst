import pandas as pd
import yfinance as yf

import data_pipeline.ingestion as ingestion
from data_pipeline.database_utils import Location
from data_pipeline.ingestion import (
    clean_companies_data,
    clean_stock_history_data,
    fetch_companies_data,
    fetch_stock_history,
    get_companies_from_csv,
    get_industries_set,
    get_locations_set,
    get_sectors_set,
    get_stock_history_by_id,
    get_symbols_from_db,
    main,
    load_companies_data,
    load_history_data,
)


def test_get_companies_from_csv(tmp_path):
    file_name = tmp_path / "companies.csv"
    file_name.write_text("Symbols\nMSFT\nAAPL\n", encoding="utf-8")

    companies_list = get_companies_from_csv(file_name)

    assert companies_list == ["MSFT", "AAPL"]


def test_fetch_companies_data(monkeypatch):
    symbol_list = ["MSFT", "AAPL"]

    class MockTicker:
        def __init__(self, symbol):
            self.info = {
                "symbol": symbol,
                "displayName": f"{symbol} INC",
                "sector": "Technology",
            }

    monkeypatch.setattr(yf, "Ticker", MockTicker)

    companies_dict_list = fetch_companies_data(symbol_list)

    assert companies_dict_list == [
        {"symbol": "MSFT", "displayName": "MSFT INC", "sector": "Technology"},
        {"symbol": "AAPL", "displayName": "AAPL INC", "sector": "Technology"},
    ]


def test_clean_companies_data():
    dirty_data = [
        {
            "symbol": "MSFT",
            "displayName": "Microsoft",
            "sector": "Technology",
            "industry": "Software",
            "city": "Redmond",
            "country": "United States",
            "toDelete": "delete",
        }
    ]

    result = clean_companies_data(dirty_data)

    assert result == [
        {
            "symbol": "MSFT",
            "displayName": "Microsoft",
            "sector": "Technology",
            "industry": "Software",
            "city": "Redmond",
            "country": "United States",
        }
    ]


def test_get_industries_set_returns_unique_values():
    companies = [
        {"industry": "Software"},
        {"industry": "Semiconductors"},
        {"industry": "Software"},
        {},
        {"industry": None},
    ]

    assert get_industries_set(companies) == {"Software", "Semiconductors"}


def test_get_sectors_set_returns_unique_values():
    companies = [
        {"sector": "Technology"},
        {"sector": "Consumer Discretionary"},
        {"sector": "Technology"},
        {},
        {"sector": None},
    ]

    assert get_sectors_set(companies) == {"Technology", "Consumer Discretionary"}


def test_get_locations_set_skips_incomplete_locations():
    companies = [
        {"country": "United States", "city": "Redmond"},
        {"country": "United States", "city": "Cupertino"},
        {"country": "United States", "city": "Redmond"},
        {"country": "United States", "city": None},
        {"country": None, "city": "Austin"},
    ]

    assert get_locations_set(companies) == {
        Location(country="United States", city="Redmond"),
        Location(country="United States", city="Cupertino"),
    }


def test_load_companies_data_calls_bulk_insert_companies():
    class MockDatabaseWriter:
        def __init__(self):
            self.called_with = None

        def bulk_insert_companies(self, companies, industries, sectors, locations):
            self.called_with = (companies, industries, sectors, locations)

    companies = [{"symbol": "MSFT", "displayName": "Microsoft", "sector": "Technology"}]
    industries = {"Software"}
    sectors = {"Technology"}
    locations = {Location(country="United States", city="Redmond")}
    mock_writer = MockDatabaseWriter()

    load_companies_data(companies, industries, sectors, locations, mock_writer)

    assert mock_writer.called_with == (companies, industries, sectors, locations)


def test_get_symbols_from_db_delegates_to_reader():
    class MockDatabaseReader:
        def get_symbols(self):
            return ["MSFT", "AAPL"]

    assert get_symbols_from_db(MockDatabaseReader()) == ["MSFT", "AAPL"]


def test_fetch_stock_history_calls_yfinance_with_joined_symbols(monkeypatch):
    history = pd.DataFrame({"Close": [1.0]})

    class MockTickers:
        def __init__(self, symbols):
            self.symbols = symbols

        def history(self, period):
            assert self.symbols == "MSFT AAPL"
            assert period == "1mo"
            return history

    monkeypatch.setattr(yf, "Tickers", MockTickers)

    result = fetch_stock_history(["MSFT", "AAPL"], "1mo")

    assert result is history


def test_clean_stock_history_data_normalizes_dataframe_records():
    index = pd.to_datetime(["2024-01-02", "2024-01-03"])
    columns = pd.MultiIndex.from_tuples(
        [
            ("Open", "MSFT"),
            ("Open", "AAPL"),
            ("High", "MSFT"),
            ("High", "AAPL"),
            ("Low", "MSFT"),
            ("Low", "AAPL"),
            ("Close", "MSFT"),
            ("Close", "AAPL"),
            ("Volume", "MSFT"),
            ("Volume", "AAPL"),
            ("Dividends", "MSFT"),
            ("Dividends", "AAPL"),
            ("Stock Splits", "MSFT"),
            ("Stock Splits", "AAPL"),
        ],
        names=[None, "Ticker"],
    )
    stock_history = pd.DataFrame(
        [
            [
                100.0,
                200.0,
                110.0,
                210.0,
                90.0,
                190.0,
                105.0,
                205.0,
                1000,
                2000,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
            [
                101.0,
                201.0,
                111.0,
                211.0,
                91.0,
                191.0,
                106.0,
                206.0,
                1100,
                2100,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
        ],
        index=index,
        columns=columns,
    )
    stock_history.index.name = "Date"

    result = clean_stock_history_data(stock_history)

    assert result == [
        {
            "Date": "2024-01-02",
            "Symbol": "MSFT",
            "Open": 100.0,
            "High": 110.0,
            "Low": 90.0,
            "Close": 105.0,
            "Volume": 1000,
        },
        {
            "Date": "2024-01-02",
            "Symbol": "AAPL",
            "Open": 200.0,
            "High": 210.0,
            "Low": 190.0,
            "Close": 205.0,
            "Volume": 2000,
        },
        {
            "Date": "2024-01-03",
            "Symbol": "MSFT",
            "Open": 101.0,
            "High": 111.0,
            "Low": 91.0,
            "Close": 106.0,
            "Volume": 1100,
        },
        {
            "Date": "2024-01-03",
            "Symbol": "AAPL",
            "Open": 201.0,
            "High": 211.0,
            "Low": 191.0,
            "Close": 206.0,
            "Volume": 2100,
        },
    ]


def test_get_stock_history_by_id_replaces_symbol_with_company_id():
    stock_history = [
        {"Date": "2024-01-02", "Symbol": "MSFT", "Close": 105.0},
        {"Date": "2024-01-02", "Symbol": "AAPL", "Close": 205.0},
    ]

    class MockDatabaseReader:
        def get_company_symbol_id(self):
            return {"MSFT": 1, "AAPL": 2}

    result = get_stock_history_by_id(stock_history, MockDatabaseReader())

    assert result == [
        {"Date": "2024-01-02", "Close": 105.0, "id_company": 1},
        {"Date": "2024-01-02", "Close": 205.0, "id_company": 2},
    ]


def test_load_history_data_calls_bulk_insert_history():
    class MockDatabaseWriter:
        def __init__(self):
            self.called_with = None

        def bulk_insert_history(self, stock_history):
            self.called_with = stock_history

    stock_history = [{"Date": "2024-01-02", "id_company": 1, "Close": 105.0}]
    mock_writer = MockDatabaseWriter()

    load_history_data(stock_history, mock_writer)

    assert mock_writer.called_with == stock_history


def test_main_runs_full_etl_flow_with_default_period(monkeypatch):
    valid_company = {
        "symbol": "MSFT",
        "displayName": "Microsoft",
        "sector": "Technology",
        "industry": "Software",
        "city": "Redmond",
        "country": "United States",
    }
    invalid_company = {
        "symbol": "BAD",
        "displayName": "Broken Co",
        "sector": "Technology",
        "industry": "Software",
        "city": None,
        "country": "United States",
    }
    history_frame = pd.DataFrame({"Close": [1.0]})
    cleaned_history = [{"Date": "2024-01-02", "Symbol": "MSFT", "Close": 105.0}]
    history_with_ids = [{"Date": "2024-01-02", "id_company": 1, "Close": 105.0}]
    expected_locations = {Location(country="United States", city="Redmond")}

    monkeypatch.delenv("FETCHING_PERIOD", raising=False)

    class MockDatabaseConfig:
        pass

    class MockDatabaseWrite:
        def __init__(self, config):
            self.config = config

    class MockDatabaseRead:
        def __init__(self, config):
            self.config = config

    monkeypatch.setattr(ingestion, "DatabaseConfig", MockDatabaseConfig)
    monkeypatch.setattr(ingestion, "DatabaseWrite", MockDatabaseWrite)
    monkeypatch.setattr(ingestion, "DatabaseRead", MockDatabaseRead)

    def mock_get_companies_from_csv(file_name):
        assert file_name.name == "companies.csv"
        return ["MSFT", "BAD"]

    def mock_fetch_companies_data(company_symbol_list):
        assert company_symbol_list == ["MSFT", "BAD"]
        return [{"symbol": "raw"}]

    def mock_clean_companies_data(companies_list):
        assert companies_list == [{"symbol": "raw"}]
        return [valid_company, invalid_company]

    def mock_get_industries_set(companies_list):
        assert companies_list == [valid_company]
        return {"Software"}

    def mock_get_sectors_set(companies_list):
        assert companies_list == [valid_company]
        return {"Technology"}

    def mock_get_locations_set(companies_list):
        assert companies_list == [valid_company]
        return expected_locations

    def mock_load_companies_data(
        companies_list, industries, sectors, locations, database_writer
    ):
        assert companies_list == [valid_company]
        assert industries == {"Software"}
        assert sectors == {"Technology"}
        assert locations == expected_locations
        assert isinstance(database_writer, MockDatabaseWrite)

    def mock_get_symbols_from_db(database_reader):
        assert isinstance(database_reader, MockDatabaseRead)
        return ["MSFT"]

    def mock_fetch_stock_history(companies_symbol, fetching_period):
        assert companies_symbol == ["MSFT"]
        assert fetching_period == "1mo"
        return history_frame

    def mock_clean_stock_history_data(stock_history):
        assert stock_history is history_frame
        return cleaned_history

    def mock_get_stock_history_by_id(stock_history, database_reader):
        assert stock_history == cleaned_history
        assert isinstance(database_reader, MockDatabaseRead)
        return history_with_ids

    def mock_load_history_data(stock_history, database_writer):
        assert stock_history == history_with_ids
        assert isinstance(database_writer, MockDatabaseWrite)

    monkeypatch.setattr(
        ingestion, "get_companies_from_csv", mock_get_companies_from_csv
    )
    monkeypatch.setattr(ingestion, "fetch_companies_data", mock_fetch_companies_data)
    monkeypatch.setattr(ingestion, "clean_companies_data", mock_clean_companies_data)
    monkeypatch.setattr(ingestion, "get_industries_set", mock_get_industries_set)
    monkeypatch.setattr(ingestion, "get_sectors_set", mock_get_sectors_set)
    monkeypatch.setattr(ingestion, "get_locations_set", mock_get_locations_set)
    monkeypatch.setattr(ingestion, "load_companies_data", mock_load_companies_data)
    monkeypatch.setattr(ingestion, "get_symbols_from_db", mock_get_symbols_from_db)
    monkeypatch.setattr(ingestion, "fetch_stock_history", mock_fetch_stock_history)
    monkeypatch.setattr(
        ingestion, "clean_stock_history_data", mock_clean_stock_history_data
    )
    monkeypatch.setattr(
        ingestion, "get_stock_history_by_id", mock_get_stock_history_by_id
    )
    monkeypatch.setattr(ingestion, "load_history_data", mock_load_history_data)

    main()
