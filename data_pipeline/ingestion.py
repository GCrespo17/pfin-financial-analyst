import yfinance as yf
import csv
import os
import pandas as pd
from pathlib import Path
from data_pipeline.database_utils import DatabaseRead, DatabaseWrite, DatabaseConfig, Location

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

def get_industries_set(companies_list:list[dict])->set[str]:
    return {name for name in {company["industry"] for company in companies_list if company.get("industry")}}

def get_sectors_set(companies_list:list[dict])->set[str]:
    return {sector for sector in {company["sector"] for company in companies_list if company.get("sector")}}

def get_locations_set(companies_list:list[dict])->set[Location]:
    return {Location(country=company["country"], city=company["city"])
            for company in companies_list
            if company.get("country") and company.get("city")}

def load_companies_data(companies_list: list[dict], industries:set[str], sectors:set[str], locations:set[Location], database_writer:DatabaseWrite)->None:
    database_writer.bulk_insert_companies(companies_list, industries, sectors, locations)
    print("Initial Database Population completed!")

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

def get_stock_history_by_id(stock_history:list[dict], database_reader:DatabaseRead)->list[dict]:
    companies_map = database_reader.get_company_symbol_id()
    for item in stock_history:
        company = item.pop("Symbol")
        item["id_company"] = companies_map.get(company)
    return stock_history

def load_history_data(stock_history: list[dict], database_writer:DatabaseWrite)->None:
    database_writer.bulk_insert_history(stock_history)

def main()->None:
    file_name = Path(__file__).with_name("companies.csv")
    fetching_period = os.getenv("FETCHING_PERIOD", "1mo")
    database_config = DatabaseConfig()
    database_writer = DatabaseWrite(database_config)
    database_reader = DatabaseRead(database_config)
    company_symbols = get_companies_from_csv(file_name)
    companies = clean_companies_data(fetch_companies_data(company_symbols))
    companies = [
        company
        for company in companies
        if company.get("symbol")
        and company.get("displayName")
        and company.get("city")
        and company.get("country")
    ]
    if not companies:
        raise ValueError("No valid companies data available to load.")
    industries = get_industries_set(companies)
    sectors = get_sectors_set(companies)
    locations = get_locations_set(companies)
    load_companies_data(companies, industries, sectors, locations, database_writer)
    stored_symbols = get_symbols_from_db(database_reader)
    if not stored_symbols:
        raise ValueError("No company symbols found in the database after loading companies.")
    stock_history = fetch_stock_history(stored_symbols, fetching_period)
    stock_history = clean_stock_history_data(stock_history)
    stock_history = get_stock_history_by_id(stock_history, database_reader)
    load_history_data(stock_history, database_writer)
    print(
        f"Stock history load completed for {len(stored_symbols)} companies using period '{fetching_period}'."
    )
   


if __name__ == "__main__":
    main()
