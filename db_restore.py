import json
import psycopg2

# Read the config file
with open('config.json', 'r', encoding='utf-8') as config_file:
    config = json.load(config_file)

    # Establish database connection
    db_connection = psycopg2.connect(
        host=config["db_host"],
        database=config["db_database"],
        user=config["db_user"],
        password=config["db_password"],
        port=config["db_port"]
        )

    # Open a cursor to perform database operations
    cursor = db_connection.cursor()

create_database = """
CREATE DATABASE asdek
    WITH 
    OWNER = %s
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
"""

create_table_uzytkownicy = """
CREATE TABLE uzytkownicy(
 uzytkownik_id SMALLSERIAL PRIMARY KEY,
 imie VARCHAR(50) NOT NULL,
 nazwisko VARCHAR(50) NOT NULL,
 login VARCHAR(30) NOT NULL,
 haslo CHAR(96) NOT NULL)
"""

create_table_przewoznicy = """
CREATE TABLE przewoznicy(
 przewoznik_id SMALLSERIAL PRIMARY KEY,
 przewoznik VARCHAR(128) NOT NULL,
 przewoz_osob_nr_licencji VARCHAR(30),
 przewoz_rzeczy_nr_licencji VARCHAR(30),
 swiadczenie_uslug_trakcyjnych_nr_licencji VARCHAR(30))
"""

create_table_pomiary = """
CREATE TABLE pomiary(
pomiar_id SERIAL PRIMARY KEY,
data DATE NOT NULL,
godzina TIME NOT NULL,
stacja VARCHAR(128) NOT NULL,
predkosc DECIMAL(6, 2), 
liczba_osi SMALLINT NOT NULL,
dlugosc DECIMAL(6, 2),
GH CHAR(4),
GM CHAR(4),
OK CHAR(4),
PM CHAR(4))
"""

create_table_ersat = """
CREATE TABLE ersat(
ersat_id SMALLSERIAL PRIMARY KEY,
data DATE NOT NULL,
godzina TIME NOT NULL,
stacja VARCHAR(50) NOT NULL,
kierunek CHAR(1) NOT NULL,
predkosc DECIMAL(6, 2), 
liczba_osi SMALLINT NOT NULL,
dlugosc DECIMAL(6, 2),
nr_pociagu SMALLINT NOT NULL,
imie_nazwisko_potwierdzajacego VARCHAR(100) NOT NULL,
funkcja_potwierdzajacego VARCHAR(20) NOT NULL,
nr_kolejny_pojazdu SMALLINT NOT NULL,
nr_pojazdu_kolejowego SMALLINT NOT NULL,
uwagi TEXT)
"""

add_fk_to_ersat = """
ALTER TABLE ersat
ADD COLUMN fk_przewoznik SMALLINT,
ADD FOREIGN KEY (fk_przewoznik) REFERENCES przewoznicy(przewoznik_id)
"""

create_table_awarie = """
CREATE TABLE awarie(
awaria_id SMALLSERIAL PRIMARY KEY,
os_od_poczatku SMALLINT NOT NULL,
os_od_konca SMALLINT NOT NULL,
GH CHAR(4),
GM CHAR(4),
OK CHAR(4),
PM CHAR(4),
wart_przekroczenia DECIMAL(7, 3),
kontynuacja_jazdy BOOLEAN,
potwierdzona BOOLEAN
)
"""

add_fk_to_awarie = """
ALTER TABLE awarie
ADD COLUMN fk_ersat SMALLINT,
ADD FOREIGN KEY (fk_ersat) REFERENCES ersat(ersat_id)
"""

commands = [create_table_uzytkownicy, create_table_przewoznicy, create_table_pomiary,
            create_table_ersat, add_fk_to_ersat, create_table_awarie, add_fk_to_awarie]


for command in commands:
    cursor.execute(command)
# db_connection.commit()


