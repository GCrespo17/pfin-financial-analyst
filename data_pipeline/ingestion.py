import yfinance as yf
import csv
import pandas as pd
from pathlib import Path
from data_pipeline.database_utils import DatabaseRead, DatabaseWrite

def get_companies_from_csv(file_name: Path) -> list[str]:
    with open(file_name, newline='') as csvfile:
        csv_companies = csv.reader(csvfile, delimiter=' ')
        companies = list(csv_companies)
        companies = companies[1:]
    return [company for company_list in companies for company in company_list]

def fetch_companies_data(company_symbol_list:list[str])->list[dict]:
    companies_list = [yf.Ticker(company).info for company in company_symbol_list]
    if companies_list is None:
        raise ValueError("No companies information returned from yfinance...")
    return companies_list

def clean_companies_data(companies_list:list[dict])->list[dict]:
    keys_to_keep = ['symbol', 'sector', 'industry', 'displayName', 'city', 'country']
    cleaned_companies:list[dict] = []
    for company in companies_list:
        cleaned_company = {key:value for key, value in company.items() if key in keys_to_keep}
        cleaned_companies.append(cleaned_company)
    return cleaned_companies

def load_companies_data(companies_list: list[dict], database_writer:DatabaseWrite)->None:
    database_writer.bulk_insert_companies(companies_list)

def get_symbols_from_db(database_reader:DatabaseRead)->list[str]:
    return database_reader.get_symbols()

def fetch_stock_history(companies_symbol:list[str], fetching_period:str)->pd.DataFrame:
    companies = ' '.join(companies_symbol)
    history = yf.Tickers(companies).history(period=fetching_period)
    if history is None:
        raise ValueError("No stock history returned from yfinance...")
    return history

def clean_stock_history_data(stock_history: pd.DataFrame)->list[dict]:
    stock_history = stock_history.drop(columns = ['Stock Splits', 'Dividends'])
    stacked_history = stock_history.stack(level = 1)
    normalized = stacked_history.reset_index()
    normalized['Date'] = normalized['Date'].dt.strftime('%Y-%m-%d')
    normalized = normalized.rename(columns = {"Ticker":"Symbol"})
    return normalized.to_dict(orient="records")
    
def load_history_data(stock_history: list[dict], database_writer:DatabaseWrite, companies_map: dict)->None:
    database_writer.bulk_insert_history(stock_history, companies_map)

def main()->None:
    print("Hello")


if __name__ == "__main__":
    file_name = Path(__file__).with_name('companies.csv')
