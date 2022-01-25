import socket
import json


def choose_request():
    response = client.recv(2048)
    print(response.decode())
    request = input('request: ')
    client.send(str.encode(request))
    print(client.recv(2048).decode())
    return request


def request_log_in():
    username = input('username: ')
    client.send(str.encode(username))

    password = input('password: ')
    client.send(str.encode(password))

    response = client.recv(2048)
    print(response.decode())


def request_esrat():
    ersat_record = {
        "data": "18.01.2022",
        "godzina": "13:46",
        "stacja": "Września",
        "kierunek": "1",
        "predkosc": "62.4",
        "liczba_osi": "12",
        "dlugosc": "110",
        "nr_pociagu": "12345",
        "imie_nazwisko_potwierdzajacego": "Jan Kowalski",
        "funkcja_potwierdzajacego": "Konduktor",
        "nr_kolejny_pojazdu": "98",
        "nr_pojazdu_kolejowego": "97",
        "przewoznik": "Alusta S.A.",
        "uwagi": "Tory byly zle i podwozie tez bylo zle"
    }
    ersat_json = json.dumps(ersat_record, ensure_ascii=False)
    print(ersat_json)
    client.sendall(bytes(ersat_json, encoding='utf-8'))

    response = client.recv(2048)
    print(response.decode('utf-8'))


def request_raport():

    while True:
        response = client.recv(2048).decode()
        if response == '0':
            break
        print(response)


def request_archiwum():
    nazwa_stacji_diagnostycznej = input("Nazwa stacji diagnostycznej: ")
    client.send(str.encode(nazwa_stacji_diagnostycznej))
    while True:
        response = client.recv(2048).decode()
        if response == '0':
            break
        print(response)


def request_przewoznicy():

    while True:
        response = client.recv(2048).decode()
        if response == '0':
            break
        print(response)


def request_raportg():
    nazwa_stacji_diagnostycznej = input("Nazwa stacji diagnostycznej: ")
    client.send(str.encode(nazwa_stacji_diagnostycznej))
    response = client.recv(2048).decode()
    print(response)


def request_pomiar():

    pomiar_dict = {
        "data": "13.02.2020",
        "godzina": "8:31",
        "stacja": "Sopot 1",
        "predkosc": 10,
        "liczba_osi": 12,
        "dlugosc": 99,
        "gh": "null",
        "gm": "null",
        "ok": "OSTR",
        "pm": "OJOJ"
    }
    pomiar_json = json.dumps(pomiar_dict, ensure_ascii=False)
    client.send(str.encode(pomiar_json))
    response = client.recv(2048).decode()
    print(response)


client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('127.0.0.1', 1233))
request_choice = choose_request()
if request_choice == 'login':
    request_log_in()
elif request_choice == 'ersat':
    request_esrat()
elif request_choice == 'raport1' or request_choice == 'raport100':
    request_raport()
elif request_choice == 'archiwum':
    request_archiwum()
elif request_choice == 'przewoznicy':
    request_przewoznicy()
elif request_choice == 'raportg':
    request_raportg()
elif request_choice == 'pomiar':
    request_pomiar()

client.close()
