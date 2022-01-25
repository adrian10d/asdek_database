import json
import hashlib

"""
______________________________________________
    Functions used by ASDEKServer class
    to communicate with the 'asdek' database
______________________________________________
"""


def hash_function(plaintext):
    salt = b'\x89\x9b\xeb\x99\xf6\xb7\xfb\x93\xcdY\x97\x86\xcfOp?wBw\x80\x95\xb0\xb1\x0c\xf4\xbb\xd5\xab\xb1\x1a;\x1e'
    plaintext = plaintext.encode()
    digest = hashlib.pbkdf2_hmac('sha384', plaintext, salt, 111198)
    hex_hash = digest.hex()
    return hex_hash


def user_auth(cursor, username, password):
    """ Checking if given credentials in database and returning True if so, otherwise False """

    command = """
    SELECT EXISTS (
        SELECT * FROM uzytkownicy WHERE login = %s AND haslo = %s
    )
    """

    # Execute a query
    cursor.execute(command, [username, hash_function(password)])

    # Retrieve query results
    result = cursor.fetchone()
    return result[0]


def insert_ersat(db_connection, cursor, record):
    """ Inserting given record to 'ersat' table, return True if successful """

    record = json.loads(record)
    command = """
    INSERT INTO ersat (data, godzina, stacja, kierunek, predkosc, liczba_osi, dlugosc,
    nr_pociagu, imie_nazwisko_potwierdzajacego, funkcja_potwierdzajacego, nr_kolejny_pojazdu, nr_pojazdu_kolejowego,
    fk_przewoznik, uwagi) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """

    cursor.execute('SELECT przewoznik_id FROM przewoznicy WHERE przewoznik = %s', [record['przewoznik']])
    fk_przewoznik = cursor.fetchone()
    print(fk_przewoznik)
    cursor.execute(command, [record["data"], record["godzina"], record["stacja"],
                             record["kierunek"], record["predkosc"], record["liczba_osi"], record["dlugosc"],
                             record["nr_pociagu"], record["imie_nazwisko_potwierdzajacego"],
                             record["funkcja_potwierdzajacego"], record["nr_kolejny_pojazdu"],
                             record["nr_pojazdu_kolejowego"], fk_przewoznik, record["uwagi"]])

    db_connection.commit()
    return True


def get_reports(cursor, records_qty):
    """ Fetching given number of recods from 'pomiary' table and returning as a list of JSON objects """

    command = """
    SELECT * FROM pomiary
    ORDER BY data DESC, godzina DESC
    """
    cursor.execute(command)
    fetched_rows = cursor.fetchmany(records_qty)
    records_json = []
    for row in fetched_rows:
        print(row)
        record_dict = {
            "data": row[1].strftime("%d.%m.%Y"),
            "godzina": row[2].strftime("%H:%M:%S"),
            "stacja": row[3],
            "predkosc": float(row[4]),
            "liczba_osi": int(row[5]),
            "dlugosc": float(row[6]),
            "GH": row[7],
            "GM": row[8],
            "OK": row[9],
            "PM": row[10]
        }
        records_json.append(json.dumps(record_dict, ensure_ascii=False))
    return records_json


def get_ersat(cursor, connection, stacja_diagnost):
    """ Fetching all records from 'ersat' table, joining 'awarie' info and returning as a list of JSON objects """

    command = """
        SELECT ersat.nr_pojazdu_kolejowego, ersat.data, ersat.godzina,
        ersat.stacja, awarie.gh, awarie.gm, awarie.ok, awarie.pm
        FROM ersat
        LEFT JOIN awarie
        ON ersat.ersat_id = awarie.fk_ersat
        WHERE ersat.stacja = %s
        """
    cursor.execute(command, [stacja_diagnost])
    fetched_rows = cursor.fetchall()
    for row in fetched_rows:
        if row[4] is not None:
            stan_awaryjny = 'GH'
            wartosc_stanu_awaryjnego = row[4]
        elif row[5] is not None:
            stan_awaryjny = 'GM'
            wartosc_stanu_awaryjnego = row[5]
        elif row[6] is not None:
            stan_awaryjny = 'OK'
            wartosc_stanu_awaryjnego = row[6]
        elif row[7] is not None:
            stan_awaryjny = 'PM'
            wartosc_stanu_awaryjnego = row[7]
        else:
            continue

        record_dict = {
            "nr_pojazdu_kolejowego": row[0],
            "stan_awaryjny": stan_awaryjny,
            "data": row[1].strftime("%d.%m.%Y"),
            "godzina": row[2].strftime("%H:%M:%S"),
            "stacja": row[3],
            "wartosc_stanu_awaryjnego": wartosc_stanu_awaryjnego
        }
        record_json = json.dumps(record_dict, ensure_ascii=False)
        connection.send(bytes(record_json, encoding='utf-8'))


def get_przewoznicy(cursor):
    """ Fetching all records from 'przewoznicy' table and returning as a list of JSON objects """

    command = """
        SELECT przewoznik FROM przewoznicy
    """
    cursor.execute(command)
    fetched_rows = cursor.fetchall()
    records_json = []
    for row in fetched_rows:
        record_dict = {
            "nazwa_przewoznika": row[0]
        }
        records_json.append(json.dumps(record_dict, ensure_ascii=False))
    return records_json


def get_raportg_data(cursor, stacja_diagn):
    """ Getting number of records and not null values in columns: 'gh', 'gm', 'ok', 'pm'
    for specific station in table 'pomiary', returning as JSON """

    command = """
        SELECT
        COUNT(*) AS total,
        COUNT (CASE WHEN gh IS NOT NULL THEN 1 END) AS gh,
        COUNT (CASE WHEN gm IS NOT NULL THEN 1 END) AS gm,
        COUNT (CASE WHEN ok IS NOT NULL THEN 1 END) AS ok,
        COUNT (CASE WHEN pm IS NOT NULL THEN 1 END) AS pm
        FROM pomiary
        WHERE pomiary.stacja = %s
    """
    cursor.execute(command, [stacja_diagn])
    fetched_row = cursor.fetchone()
    record_dict = {
        "total": fetched_row[0],
        "gh": fetched_row[1],
        "gm": fetched_row[2],
        "ok": fetched_row[3],
        "pm": fetched_row[4],
    }
    return json.dumps(record_dict, ensure_ascii=False)


def insert_pomiar(db_connection, cursor, pomiar_json):
    """ Inserting values from given JSON object into 'pomiary' table """

    command = """
        INSERT INTO pomiary (data, godzina, stacja, predkosc, liczba_osi, dlugosc, gh, gm, ok, pm)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    pomiar_dict = json.loads(pomiar_json)

    # Execute query
    cursor.execute(command, (pomiar_dict["data"], pomiar_dict["godzina"], pomiar_dict["stacja"],
                             pomiar_dict["predkosc"], pomiar_dict["liczba_osi"], pomiar_dict["dlugosc"],
                             pomiar_dict["gh"], pomiar_dict["gm"], pomiar_dict["ok"], pomiar_dict["pm"],))

    # Commit changes
    db_connection.commit()
    return True
