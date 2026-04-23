CREATE TABLE LOCATIONS(
  id_location SERIAL PRIMARY KEY,
  name VARCHAR(150) UNIQUE NOT NULL,
  type VARCHAR(15) NOT NULL,
  id_parent_location INTEGER,
  CONSTRAINT fk_id_parent_location FOREIGN KEY (id_parent_location) REFERENCES LOCATIONS(id_location),
  CONSTRAINT parent_check CHECK((type = 'COUNTRY' AND id_parent_location IS NULL)
                                OR (type = 'CITY' AND id_parent_location IS NOT NULL)),
  CONSTRAINT type_values_check CHECK(type = 'COUNTRY' OR type = 'CITY')
);

CREATE TABLE INDUSTRIES(
  id_industry SERIAL PRIMARY KEY,
  name VARCHAR(40) UNIQUE
);

CREATE TABLE SECTORS(
  id_sector SERIAL PRIMARY KEY,
  name VARCHAR(70) UNIQUE
);

CREATE TABLE COMPANIES(
  id_company SERIAL PRIMARY KEY,
  symbol VARCHAR(10) UNIQUE NOT NULL,
  name VARCHAR(100) UNIQUE NOT NULL,
  id_location INTEGER NOT NULL,
  id_industry INTEGER,
  id_sector INTEGER,
  CONSTRAINT fk_id_industry FOREIGN KEY(id_industry) REFERENCES INDUSTRIES(id_industry),
  CONSTRAINT fk_id_sector FOREIGN KEY(id_sector) REFERENCES SECTORS(id_sector),
  CONSTRAINT fk_id_location FOREIGN KEY(id_location) REFERENCES LOCATIONS(id_location)
);

CREATE TABLE STOCK_HISTORY(
  id_company INTEGER NOT NULL,
  date DATE NOT NULL,
  open NUMERIC(10, 4) NOT NULL,
  high NUMERIC(10, 4) NOT NULL,
  low NUMERIC(10, 4) NOT NULL,
  close NUMERIC(10, 4) NOT NULL,
  volume BIGINT NOT NULL,
  CONSTRAINT fk_id_company FOREIGN KEY(id_company) REFERENCES COMPANIES(id_company),
  CONSTRAINT pk_stock_company PRIMARY KEY(id_company, date)
);

