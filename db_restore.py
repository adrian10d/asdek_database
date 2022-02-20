import csv
from testing import tests
import _config

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
 haslo CHAR(96) NOT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMP NOT NULL DEFAULT NOW())
"""

create_table_przewoznicy = """
CREATE TABLE przewoznicy(
 przewoznik_id SMALLSERIAL PRIMARY KEY,
 przewoznik VARCHAR(128) NOT NULL,
 przewoz_osob_nr_licencji VARCHAR(30),
 przewoz_rzeczy_nr_licencji VARCHAR(30),
 swiadczenie_uslug_trakcyjnych_nr_licencji VARCHAR(30),
 created_at TIMESTAMP NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMP NOT NULL DEFAULT NOW())
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
PM CHAR(4),
created_at TIMESTAMP NOT NULL DEFAULT NOW(),
updated_at TIMESTAMP NOT NULL DEFAULT NOW())
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
nr_pociagu VARCHAR(36) NOT NULL,
imie_nazwisko_potwierdzajacego VARCHAR(100) NOT NULL,
funkcja_potwierdzajacego VARCHAR(20) NOT NULL,
uwagi TEXT,
created_at TIMESTAMP NOT NULL DEFAULT NOW(),
updated_at TIMESTAMP NOT NULL DEFAULT NOW())
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
wart_przekroczenia CHAR(4),
kontynuacja_jazdy BOOLEAN,
potwierdzona BOOLEAN,
nr_kolejny_pojazdu VARCHAR(36) NOT NULL,
nr_pojazdu_kolejowego VARCHAR(36) NOT NULL,
created_at TIMESTAMP NOT NULL DEFAULT NOW(),
updated_at TIMESTAMP NOT NULL DEFAULT NOW())
"""

add_fk_ersat_to_awarie = """
ALTER TABLE awarie
ADD COLUMN fk_ersat SMALLINT,
ADD FOREIGN KEY (fk_ersat) REFERENCES ersat(ersat_id)
"""

add_fk_przewoznik_to_awarie = """
ALTER TABLE awarie
ADD COLUMN fk_przewoznik SMALLINT,
ADD FOREIGN KEY (fk_przewoznik) REFERENCES przewoznicy(przewoznik_id)
"""

transaction_timestamp_triggers = """
CREATE OR REPLACE FUNCTION updated_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW; 
END;
$$ language 'plpgsql';


CREATE TRIGGER uzytkownicy_timestamp BEFORE UPDATE ON uzytkownicy FOR EACH ROW EXECUTE PROCEDURE  updated_timestamp();
CREATE TRIGGER przewoznicy_timestamp BEFORE UPDATE ON przewoznicy FOR EACH ROW EXECUTE PROCEDURE  updated_timestamp();
CREATE TRIGGER pomiary_timestamp BEFORE UPDATE ON pomiary FOR EACH ROW EXECUTE PROCEDURE  updated_timestamp();
CREATE TRIGGER ersat_timestamp BEFORE UPDATE ON ersat FOR EACH ROW EXECUTE PROCEDURE  updated_timestamp();
CREATE TRIGGER awarie_timestamp BEFORE UPDATE ON awarie FOR EACH ROW EXECUTE PROCEDURE  updated_timestamp();
"""


def table_exists():
    _config.cursor.execute("SELECT EXISTS(SELECT * from information_schema.tables WHERE table_name=%s)", ('uzytkownicy',))
    return _config.cursor.fetchone()[0]


def insert_przewoznicy_from_csv():
    with open('_files\\przewoznicy.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        przewoznik, przewoz_osob_nr_licencji, przewoz_rzeczy_nr_licencji, swiadczenie_uslug_trakcyjnych_nr_licencji =\
            [], [], [], []
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
                continue
            przewoznik.append(row[1])
            przewoz_osob_nr_licencji.append(row[2])
            przewoz_rzeczy_nr_licencji.append(row[4])
            swiadczenie_uslug_trakcyjnych_nr_licencji.append(row[6])
            line_count += 1
        print(swiadczenie_uslug_trakcyjnych_nr_licencji)
        print(f'Processed {line_count} lines.')
        command = "INSERT INTO przewoznicy (przewoznik, przewoz_osob_nr_licencji, przewoz_rzeczy_nr_licencji," \
                  "swiadczenie_uslug_trakcyjnych_nr_licencji) VALUES(%s, %s, %s, %s);"

        for i in range(len(przewoznik)):
            print(i)
            _config.cursor.execute(command, (przewoznik[i], przewoz_osob_nr_licencji[i], przewoz_rzeczy_nr_licencji[i],
                                     swiadczenie_uslug_trakcyjnych_nr_licencji[i]))


commands = [create_table_uzytkownicy, create_table_przewoznicy, create_table_pomiary,
            create_table_ersat, create_table_awarie, add_fk_ersat_to_awarie, add_fk_przewoznik_to_awarie,
            transaction_timestamp_triggers]


def restore():
    if table_exists():
        return
    else:
        print('Restoring database...')
        for command in commands:
            _config.cursor.execute(command)
            _config.db_connection.commit()
        insert_przewoznicy_from_csv()
        _config.db_connection.commit()

        print("Database restored.")
        choice = input("Insert test values? [Y/N] ")
        if choice == 'Y' or choice == 'y':
            tests.run()
        else:
            pass










