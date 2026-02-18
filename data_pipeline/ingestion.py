import yfinance as yf
import pandas as pd
import psycopg

def get_companies_from_csv(file_name):
    companies = list(pd.read_csv(file_name))
    return companies

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

def load_companies_data():
    return 0
