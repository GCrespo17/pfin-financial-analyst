from data_pipeline.ingestion import get_companies_from_csv, fetch_companies_data, clean_companies_data, load_companies_data
import data_pipeline.database_utils as database_utils
import pytest
import yfinance as yf

def test_get_companies_from_csv():
    file_name = 'data_pipeline/companies.csv'
    companies_list = get_companies_from_csv(file_name)
    companies_size = len(companies_list)
    assert isinstance(companies_list, list) , "should be a list"
    assert companies_size > 0 ,"should not be empty"

def test_fetch_companies_data(monkeypatch):
    symbol_list = ['MSFT', 'AAPL']
    class MockTicker:
        def __init__(self, symbol):
            self.info = {'symbol':symbol, 'displayName':'MOCK_MSFT', 'sector':'Technology'}

    monkeypatch.setattr(yf, 'Ticker', MockTicker)
    companies_dict_list = fetch_companies_data(symbol_list)
    first_element = companies_dict_list[0]

    assert isinstance(companies_dict_list, list), "should be a list"
    assert len(companies_dict_list) == 2, "should be a list of 2 elements"
    assert first_element['symbol'] == 'MSFT', "should be MSFT"

def test_clean_companies_data():
    dirty_data = [{'symbol':'MSFT', 'displayName':'Microsoft', 'sector':'Technology', 'toDelete':'delete'}]

    result = clean_companies_data(dirty_data)
    element = result[0]

    assert 'toDelete' not in element, "should not have this key"
    assert element['symbol'] == 'MSFT', "should have symbol:MSFT"
    

def test_load_companies_data(monkeypatch):
    class MockDatabaseUtils:
        def __init__(self):
            self.called_with = None

        def bulk_insert_companies(self, companies):
            self.called_with = companies

    companies = [{'symbol':'MSFT', 'displayName':'Microsoft', 'sector':'Technology'}]
    mock_instance = MockDatabaseUtils()

    monkeypatch.setattr(database_utils, "DatabaseUtilites", lambda: mock_instance)

    load_companies_data(companies)

    assert mock_instance.called_with == companies

    

