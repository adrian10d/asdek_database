import random
import csv
import json
import psycopg2
from datetime import timedelta
from datetime import datetime
import server_functions as sf

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


def random_line(filename):
    """ Returns random line from file. """

    with open(filename, encoding="utf8") as f:
        lines = f.read().splitlines()
    return random.choice(lines)


def generate_login(name, surname):
    return name[:3] + '_' + surname[:3].upper() + str(random.randint(100, 999))


def generate_password(name, surname):
    return name + surname[:1] + str(123)


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


def insert_random_uzytkownik():
    imie, nazwisko = random_line('_files\\imiona.txt'), random_line('_files\\nazwiska.txt')
    login = generate_login(imie, nazwisko)
    haslo = sf.hash_function(generate_password(imie, nazwisko))

    command = "INSERT INTO uzytkownicy (imie, nazwisko, login, haslo) VALUES(%s, %s, %s, %s)"\

    cursor.execute(command, (imie, nazwisko, login, haslo))


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
            cursor.execute(command, (przewoznik[i], przewoz_osob_nr_licencji[i], przewoz_rzeczy_nr_licencji[i],
                                     swiadczenie_uslug_trakcyjnych_nr_licencji[i]))


def insert_random_pomiary(n):
    data, godzina, stacja, predkosc, liczba_osi, dlugosc, masa, GH, GM, OK, PM, PD =\
    [], [], [], [], [], [], [], [], [], [], [], []

    d1 = datetime.strptime('7/7/1998 3:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/9/2022 10:50 PM', '%m/%d/%Y %I:%M %p')
    stacje = ["Września", "Podstolice", "Nekla", "Gułtowy", "Poznań Wschód", "Poznań Garbary", "Poznań Główny", "Sopot"]
    stany_awaryjne = ["OSTR", "GRAN", "OJOJ"]

    command = """
    INSERT INTO pomiary (data, godzina, stacja, predkosc, liczba_osi, dlugosc, GH, GM, OK, PM) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for i in range(0, n):
        random_datetime = random_date(d1, d2)
        data.append(random_datetime.date())
        godzina.append(random_datetime.time())
        stacja.append(random.choice(stacje))
        predkosc.append(round(random.uniform(33.33, 215.66), 2))
        liczba_osi.append(random.randint(6, 150))
        dlugosc.append(round(random.uniform(33.33, 215.66), 2))

        if random.randint(0, 100) >= 80:
            GH.append(random.choice(stany_awaryjne))
        else:
            GH.append(None)

        if random.randint(0, 100) >= 80:
            GM.append(random.choice(stany_awaryjne))
        else:
            GM.append(None)

        if random.randint(0, 100) >= 80:
            OK.append(random.choice(stany_awaryjne))
        else:
            OK.append(None)

        if random.randint(0, 100) >= 80:
            PM.append(random.choice(stany_awaryjne))
        else:
            PM.append(None)

    for i in range(0, n):
        cursor.execute(command, [data[i], godzina[i], stacja[i], predkosc[i], liczba_osi[i], dlugosc[i], GH[i],
                                       GM[i], OK[i], PM[i]])


print('1. insert_random_uzytkownik()')
print('2. insert_przewoznicy_from_csv()')
print('3. insert_random_pomiary(n)')
print('0. Wyjście')

while True:
    choice = int(input('Wybór: '))
    if choice == 1:
        n = int(input('Liczba użytkowników: '))
        for i in range(n):
            insert_random_uzytkownik()
    elif choice == 2:
        insert_przewoznicy_from_csv()
    elif choice == 3:
        n = int(input('Liczba pomiarów: '))
        insert_random_pomiary(n)
    elif choice == 0:
        break

# db_connection.commit()

