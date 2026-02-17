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

CREATE TABLE COMPANIES(
  id_company SERIAL PRIMARY KEY,
  symbol VARCHAR(4) UNIQUE NOT NULL,
  name VARCHAR(100) UNIQUE NOT NULL,
  id_location INTEGER NOT NULL,
  industry VARCHAR(150),
  secotr VARCHAR(150),
  CONSTRAINT fk_id_location FOREIGN KEY(id_location) REFERENCES LOCATIONS(id_location)
);

CREATE TABLE STOCK_HISTORY(
  id_stock SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  open NUMERIC(20, 8) NOT NULL,
  high NUMERIC(20, 8) NOT NULL,
  low NUMERIC(20, 8) NOT NULL,
  close NUMERIC(20, 8) NOT NULL,
  volume NUMERIC(20, 8) NOT NULL,
  id_company INTEGER NOT NULL,
  CONSTRAINT fk_id_company FOREIGN KEY(id_company) REFERENCES COMPANIES(id_company)
);
