from sqlalchemy import create_engine, engine, text, MetaData, Table, insert
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseUtilites:
    def __init__(self):
        self.db_name = os.getenv('DB_NAME')
        self.db_username = os.getenv('DB_USERNAME')
        self.db_password = os.getenv('DB_PASSWORD')
        self.engine = create_engine('postgresql+psycopg://db_username:db_password@localhost/db_name')
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.sector_table = self.metadata.tables["SECTORS"]
        self.sector_table = self.metadata.tables["INDUSTRIES"]
        self.sector_table = self.metadata.tables["LOCATIONS"]
        self.sector_table = self.metadata.tables["COMPANIES"]
        self.sector_table = self.metadata.tables["STOCK_HISTORY"]
    

    def get_location_id(self, location):
        query = text('SELECT id_location FROM LOCATIONS WHERE name = :location_name')
        with self.engine.connect() as conn:
            result = conn.execute(query, {'location_name':location.upper()})
            row = result.fetchone()
            if row:
                return row[0]
            else:
                return None

    def get_sector_id(self, sector):
        query = text("SELECT s.id_sector FROM SECTORS s WHERE s.name = :sector_name")
        with self.engine.connect() as conn:
            result = conn.execute(query, {'sector_name':sector.upper()})
            row = result.fetchone()
            if row:
                return row[0]
            else:
                return None

    def get_industry_id(self, industry):
        query = text("SELECT id_industry FROM INDUSTRIES WHERE name = :industry_name")
        with self.engine.connect() as conn:
            result = conn.execute(query, {'industry_name':industry.upper()})
            row = result.fetchone()
            if row:
                return row[0]
            else:
                return None
    
    def insert_sector(self, sector):
        query = text('INSERT INTO SECTORS (name) VALUES (:sector_name) ON CONFLICT (name) DO NOTHING')
        with self.engine.connect() as conn:
            conn.execute(query, {'sector_name':sector.upper()})
            conn.commit()

    def insert_industry(self, industry):
        query = text('INSERT INTO INDUSTRIES (name) VALUES (:industry_name) ON CONFLICT (name) DO NOTHING')
        with self.engine.connect() as conn:
            conn.execute(query, {'industry_name':industry.upper()})
            conn.commit()

    def _upsert_sectors(self, companies, conn):
        sector_map = [{'sector_name':name} for name in {company['sector'] for company in companies}]

        insert_query = text('INSERT INTO SECTORS (name) VALUES (:sector_name) ON CONFLICT (name) DO NOTHING')
        select_query = text('SELECT id_sector, name FROM SECTORS')

        conn.execute(insert_query, sector_map)
        result = conn.execute(select_query)

        return {name:id_sector for id_sector, name in result.fetchall()}

    def _upsert_industry(self, companies, conn):
        industry_map = [{'industry_name':name} for name in {company['industry'] for company in companies}]

        insert_query = text('INSERT INTO INDUSTRIES (name) VALUES (:industry_name) ON CONFLICT (name) DO NOTHING')
        select_query = text('SELECT id_industry, name FROM INDUSTRIES')

        conn.execute(insert_query, industry_map)
        result = conn.execute(select_query)

        return {name:id_industry for id_industry, name in result.fetchall()}

    def _upsert_locations(self, companies, conn):
        countries_map = [{'country_name':name} for name in {company['country'] for company in companies}]
        locations = [{'country_name':country, 'city_name':city} for country, city in {(company['country'], company['city']) for company in companies}]

        insert_country_query = text('INSERT INTO LOCATIONS (name, type) VALUES(:country_name, COUNTRY) ON CONFLICT (name) DO NOTHING')
        insert_city_query = text('INSERT INTO LOCATIONS (name, type, id_parent_location) VALUES(:name, CITY, :id_parent) ON CONFLICT (name) DO NOTHING')
        select_city_query = text("SELECT id_location, id_parent_location FROM LOCATIONS WHERE type = 'CITY'")
        select_country = text("SELECT name, id_location FROM LOCATIONS WHERE type = 'COUNTRY'")

        conn.execute(insert_country_query, countries_map)

        countries_id = {name:id_location for name, id_location in conn.execute(select_country).fetchall()}
        cities = [{'city_name':location['city_name'], 'id_parent':countries_id.get(location['country_name'])} for location in locations]
        # cities = []
        # for location in locations:
        #     parent = countries_id.get(location.get('country'))
        #     cities.append({'name':location.get('city_name'), 'id_parent':parent})

        conn.execute(insert_city_query, cities)

        result = conn.execute(select_city_query)
        return {name:id_location for name, id_location in result.fetchall()}
        


        



        

    def bulk_insert_companies(self, companies):
         
