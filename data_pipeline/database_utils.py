from sqlalchemy import MetaData, Table, create_engine, Connection, text, inspect
from sqlalchemy.engine import Engine
import os
from dotenv import load_dotenv
from dataclasses import dataclass, field

load_dotenv()
# Dataclass to store my DB Config
@dataclass
class DatabaseConfig:
    db_name:str = field(default_factory = lambda:os.getenv("DB_NAME", ""))
    db_username:str = field(default_factory = lambda:os.getenv("DB_USERNAME", ""))
    db_password:str = field(default_factory = lambda:os.getenv("DB_PASSWORD", ""))
    db_host:str = field(default_factory = lambda:os.getenv("DB_HOST", "localhost"))
    db_port:str = field(default_factory = lambda:os.getenv("DB_PORT", "5432"))
    engine:Engine = field(init = False, repr = False)

    def __post_init__(self)->None:
        if not self.db_name or not self.db_username or not self.db_password:
            raise ValueError("Environment variables missing (DB_NAME, DB_USERNAME, DB_PASSWORD)")
        self.engine = create_engine(f'postgresql+psycopg://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')

@dataclass(frozen=True)
class Location:
    country:str
    city:str

# Functions to read in the database
class DatabaseRead:
    def __init__(self, config:DatabaseConfig):
        self.engine = config.engine

    def is_table_empty(self, table_name:str)->bool:
        if not self.table_exists(table_name.lower()):
            raise ValueError(f"Table {table_name} does not exist...")
        else:
            query = text(f"SELECT NOT EXISTS (SELECT 1 FROM {table_name.lower()} LIMIT 1)")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar_one()
            

    def table_exists(self, table_name: str, schema:str = "public")->bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name, schema)

    def get_company_symbol_id(self)->dict:
        query = text("SELECT symbol, id_company FROM COMPANIES")
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return {name:id_company for name, id_company in result.fetchall()}

    def get_symbols(self)->list[str]:
        with self.engine.connect() as conn:
            query = text("SELECT symbol FROM COMPANIES")

            result = conn.execute(query)

            return [company[0] for company in result.fetchall()]

class DatabaseWrite:
    def __init__(self, config:DatabaseConfig):
        self.engine = config.engine

    def _insert_sectors(self, sectors: set[str], conn: Connection)->dict:
        params = [{'sector_name':name} for name in sectors]

        insert_query = text('INSERT INTO SECTORS (name) VALUES (:sector_name) ON CONFLICT (name) DO NOTHING')
        select_query = text('SELECT id_sector, name FROM SECTORS')

        conn.execute(insert_query, params)
        conn.commit()
        result = conn.execute(select_query)

        return {name:id_sector for id_sector, name in result.fetchall()}

    def _insert_industries(self, industries:set[str], conn: Connection)->dict:
        params = [{'industry_name':name} for name in industries]

        insert_query = text('INSERT INTO INDUSTRIES (name) VALUES (:industry_name) ON CONFLICT (name) DO NOTHING')
        select_query = text('SELECT id_industry, name FROM INDUSTRIES')

        conn.execute(insert_query, params)
        conn.commit()
        result = conn.execute(select_query)

        return {name:id_industry for id_industry, name in result.fetchall()}

    def _insert_locations(self, locations:set[Location], conn:Connection)->dict:
        countries = {location.country for location in locations}
        country_params = [{"country_name":country} for country in countries]

        insert_country_query = text("INSERT INTO LOCATIONS (name, type) VALUES(:country_name, 'COUNTRY') ON CONFLICT (name) DO NOTHING")
        insert_city_query = text("INSERT INTO LOCATIONS (name, type, id_parent_location) VALUES(:city_name, 'CITY', :id_parent) ON CONFLICT (name) DO NOTHING")
        select_city_query = text("SELECT name, id_location FROM LOCATIONS WHERE type = 'CITY'")
        select_country = text("SELECT name, id_location FROM LOCATIONS WHERE type = 'COUNTRY'")

        conn.execute(insert_country_query, country_params)
        conn.commit()

        countries_id = {name:id_location for name, id_location in conn.execute(select_country).fetchall()}
        cities = [{'city_name':location.city, 'id_parent':countries_id.get(location.country)} for location in locations]

        conn.execute(insert_city_query, cities)

        result = conn.execute(select_city_query)
        return {name:id_location for name, id_location in result.fetchall()}

    def _upsert_companies(self, companies: list[dict], sectors_map:dict, industries_map:dict, locations_map:dict, conn:Connection)->None:
        companies_data = [{'symbol':company.get('symbol'), 
                          'name':company.get('displayName'), 
                          'id_location':locations_map.get(company.get('city')),
                          'id_industry':industries_map.get(company.get('industry')),
                          'id_sector':sectors_map.get(company.get('sector'))}
                          for company in companies]

        query = text("""INSERT INTO COMPANIES (symbol, name, id_location, id_industry, id_sector) VALUES(:symbol, :name, :id_location, :id_industry, :id_sector)
                        ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        id_location = EXCLUDED.id_location,
                        id_industry = EXCLUDED.id_industry,
                        id_sector = EXCLUDED.id_sector
                        """)
        conn.execute(query, companies_data)
        conn.commit()

    def bulk_insert_companies(self, companies:list[dict], industries:set, sectors:set, locations:set[Location]):
        with self.engine.connect() as conn:
            sectors_map = self._insert_sectors(sectors, conn)
            industries_map = self._insert_industries(industries, conn)
            locations_map = self._insert_locations(locations, conn)
            self._upsert_companies(companies, sectors_map, industries_map, locations_map, conn)

    def bulk_insert_history(self, stock_history:list[dict])->None:
        with self.engine.connect() as conn:
            query = text("INSERT INTO STOCK_HISTORY(id_company, date, open, high, low, close, volume) VALUES(:id_company, :Date, :Open, :High, :Low, :Close, :Volume)")
            conn.execute(query, stock_history)
            conn.commit()

