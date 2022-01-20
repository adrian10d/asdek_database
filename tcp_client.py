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
        "nazwa_stacji_diagnostycznej": "Września",
        "kierunek": "1",
        "predkosc": "62.4",
        "liczba_osi": "12",
        "dlugosc": "110",
        "nr_pociagu": "12345",
        "imie_nazwisko_potwierdzajacego": "Jan Kowalski",
        "funkcja_potwierdzajacego": "Konduktor",
        "nr_kolejny_pojazdu": "98",
        "nr_pojazdu_kolejowego": "97",
        "fk_przewoznik": "5",
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

    while True:
        response = client.recv(2048).decode()
        if response == '0':
            break
        print(response)


client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('127.0.0.1', 1233))
request_choice = choose_request()
if request_choice == 'login':
    request_log_in()
elif request_choice == 'ersat':
    request_esrat()
elif request_choice == 'raport1' or 'raport100':
    request_raport()
elif request_choice == 'archiwum':
    request_archiwum()

client.close()
