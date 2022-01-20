import json


def auth_successful(cursor, username, password):
    command = """
    SELECT EXISTS (
        SELECT * FROM uzytkownicy WHERE login = %s AND haslo = %s
    )
    """

    # Execute a query
    cursor.execute(command, (username, password))

    # Retrieve query results
    result = cursor.fetchone()
    return result[0]


def insert_into_ersat(db_connection, cursor, record):

    record = json.loads(record)
    command = """
    INSERT INTO ersat (data, godzina, nazwa_stacji_diagnostycznej, kierunek, predkosc, liczba_osi, dlugosc,
    nr_pociagu, imie_nazwisko_potwierdzajacego, funkcja_potwierdzajacego, nr_kolejny_pojazdu, nr_pojazdu_kolejowego,
    fk_przewoznik, uwagi) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """

    # Execute a query
    cursor.execute(command, (record["data"], record["godzina"], record["nazwa_stacji_diagnostycznej"],
                             record["kierunek"], record["predkosc"], record["liczba_osi"], record["dlugosc"],
                             record["nr_pociagu"], record["imie_nazwisko_potwierdzajacego"],
                             record["funkcja_potwierdzajacego"], record["nr_kolejny_pojazdu"],
                             record["nr_pojazdu_kolejowego"], record["fk_przewoznik"], record["uwagi"], ))

    # Commit changes
    db_connection.commit()
    return True


def fetch_reports(cursor, records_qty, connection):
    command = """
    SELECT * FROM pomiary
    ORDER BY data DESC, godzina DESC
    """
    cursor.execute(command, records_qty)
    fetched_rows = cursor.fetchmany(records_qty)
    for row in fetched_rows:
        record_dict = {
            "data": row[1].strftime("%d.%m.%Y"),
            "godzina": row[2].strftime("%H:%M:%S"),
            "stacja": row[3],
            "predkosc": float(row[4]),
            "liczba_osi": int(row[5]),
            "dlugosc": float(row[6]),
            "GH": row[8],
            "GM": row[9],
            "OK": row[10],
            "PM": row[11]
        }
        record_json = json.dumps(record_dict, ensure_ascii=False)
        connection.send(bytes(record_json, encoding='utf-8'))


def fetchall_ersat(cursor, connection):
    command = """
        SELECT ersat.nr_pojazdu_kolejowego, ersat.data, ersat.godzina, ersat.nazwa_stacji_diagnostycznej, awarie.gh,
        awarie.gm, awarie.ok, awarie.pm, awarie.pd
        FROM ersat
        LEFT JOIN awarie
        ON ersat.ersat_id = awarie.fk_ersat
        """
    cursor.execute(command)
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
        elif row[8] is not None:
            stan_awaryjny = 'PD'
            wartosc_stanu_awaryjnego = row[8]
        else:
            stan_awaryjny = 'Brak'
            wartosc_stanu_awaryjnego = 'Brak'

        record_dict = {
            "nr_pojazdu_kolejowego": row[0],
            "stan_awaryjny": stan_awaryjny,
            "data": row[1].strftime("%d.%m.%Y"),
            "godzina": row[2].strftime("%H:%M:%S"),
            "nazwa_stacji_diagnostycznej": row[3],
            "wartosc_stanu_awaryjnego": wartosc_stanu_awaryjnego
        }
        record_json = json.dumps(record_dict, ensure_ascii=False)
        connection.send(bytes(record_json, encoding='utf-8'))

