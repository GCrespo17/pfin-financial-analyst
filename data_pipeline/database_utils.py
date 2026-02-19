from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseUtilites:
    def __init__(self):
        self.db_name = os.getenv('DB_NAME')
        self.db_username = os.getenv('DB_USERNAME')
        self.db_password = os.getenv('DB_PASSWORD')
        self.engine = create_engine('postgresql+psycopg://db_username:db_password@localhost/db_name')
    

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
    
    def insert_location(self, name, type):
        query = text("INSERT INTO LOCATIONS (name, type) VALUES (:location_name, :location_type)")
        with self.engine.connect() as conn:
            conn.execute(query, {'location_name':name, 'locaiton_type':type})
            conn.commit()


    def insert_sector(self, sector):
        query = text('INSERT INTO SECTORS (name) VALUES (:sector_name)')
        with self.engine.connect() as conn:
            conn.execute(query, {'sector_name':sector.upper()})
            conn.commit()

    def insert_industry(self, industry):
        query = text('INSERT INTO INDUSTRIES (name) VALUES (:industry_name)')
        with self.engine.connect() as conn:
            conn.execute(query, {'industry_name':industry.upper()})
            conn.commit()

    # I need to add the locations to this
    def insert_company(self, company):
        sector_name = company.get('sector')
        industry_name = company.get('industry')
        sector_id = self.get_sector_id(sector_name)
        if not sector_id:
            self.insert_sector(sector_name)
            sector_id = self.get_sector_id(sector_name)
        
        industry_id = self.get_industry_id(industry_name)
        if not industry_id:
            self.insert_industry(industry_name)
            industry_id = self.get_industry_id(industry_name)

        query = text('INSERT INTO COMPANIES (name, symbol, industry, sector) VALUES (:name, :symbol, :industry_id, :sector_id)')
        values = {'name':company.get('name'), 'symbol':company.get('symbol'), 'industry_id':industry_id, 'sector_id':sector_id}
        with self.engine.connect() as conn:
            conn.execute(query, values)
            conn.commit()




