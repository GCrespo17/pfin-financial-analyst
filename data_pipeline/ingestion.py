import yfinance as yf
import csv
from data_pipeline import database_utils as db
import json

def get_companies_from_csv(file_name):
    companies = []
    with open(file_name, newline='') as csvfile:
        csv_companies = csv.reader(csvfile, delimiter=' ')
        companies = list(csv_companies)
        companies = companies[1:]
    return [company for company_list in companies for company in company_list]

def fetch_companies_data(company_symbol_list):
    companies_list = [yf.Ticker(company).info for company in company_symbol_list]
    return companies_list

def clean_companies_data(companies_list):
    keys_to_keep = ['symbol', 'sector', 'industry', 'displayName', 'city', 'country']
    cleaned_companies = []
    for company in companies_list:
        cleaned_company = {key:value for key, value in company.items() if key in keys_to_keep}
        cleaned_companies.append(cleaned_company)
    return cleaned_companies

def load_companies_data(companies_list):
    database_utils = db.DatabaseUtilites()
    database_utils.bulk_insert_companies(companies_list)

def get_symbols_from_db():
    database_utils = db.DatabaseUtilites()
    return database_utils.get_symbols()

def fetch_stock_history(companies_symbol):
    fetching_period = "6mo"
    companies = ' '.join(companies_symbol)
    history = yf.Tickers(companies).history(period=fetching_period)
    return history

def clean_stock_history_data(stock_history):
    stock_history = stock_history.drop(columns = ['Stock Splits', 'Dividends'])
    stock_history = stock_history.stack(level=1)
    stock_history = stock_history.reset_index()
    stock_history['Date'] = stock_history['Date'].dt.strftime('%Y-%m-%d')
    stock_history = stock_history.rename(columns = {"Ticker":"Symbol"})
    history_dict = json.loads(stock_history.to_json(orient = 'records'))
    return history_dict
    

        

if __name__ == "__main__":
    # file_name = './companies.csv'
    # required_companies = get_companies_from_csv(file_name)
    # companies_data = fetch_companies_data(required_companies)
    # load_companies_data(clean_companies_data(companies_data))
    print(clean_stock_history_data(fetch_stock_history(get_symbols_from_db())))
