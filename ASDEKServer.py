import socket
import threading
import psycopg2
import json
import server_functions as sf


class ASDEKServer:
    """ Class for handling communication between users and the database """

    def __init__(self):
        """ Initialing the TCP server and database connection """

        with open('config.json', 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
            self.HOST = config["server_host"]
            self.PORT = config["server_port"]

            # Establishing database connection
            self.db_connection = psycopg2.connect(
                host=config["db_host"],
                database=config["db_database"],
                user=config["db_user"],
                password=config["db_password"],
                port=config["db_port"]
            )

        # Opening a cursor to perform database operations
        self.cursor = self.db_connection.cursor()

        # Current connected clients count
        self.connections = 0

    def __del__(self):
        """ Closing database connection after terminating the server application """

        self.cursor.close()
        self.db_connection.close()

    def run(self):
        """ Running the server application, waiting for clients """

        # Creating TCP socket
        with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
            try:
                s.bind((self.HOST, self.PORT))
            except socket.error as e:
                print(str(e))

            print('Listening...')
            s.listen(5)

            while True:
                client, address = s.accept()

                # Handle the client in a new thread
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client,)
                )
                client_handler.start()
                self.connections += 1
                print('Connection Request: ' + str(self.connections))

    def handle_client(self, connection):
        """ Getting request from the client """

        connection.send(str.encode('connected'))

        # Get the request
        request = connection.recv(2048).decode()

        # Send confirmation of the request
        connection.send(str.encode('request=' + request))

        if request == 'login':
            self.request_log_in(connection)

        elif request == 'ersat':
            self.request_ersat(connection)

        elif request == 'raport1':
            self.request_raport(connection, 1)

        elif request == 'raport100':
            self.request_raport(connection, 100)

        elif request == 'archiwum':
            self.request_archiwum(connection)

        else:
            connection.send(str.encode('wrong request'))

        self.connections -= 1

    def request_log_in(self, connection):
        """ Handling logging-in request """

        # Receive username
        username = connection.recv(2048)
        # Receive password
        password = connection.recv(2048)
        username = username.decode()
        password = password.decode()
        if sf.auth_successful(self.cursor, username, password):
            connection.send(str.encode('1'))
        else:
            connection.send(str.encode('0'))

    def request_ersat(self, connection):
        """ Handling inserting into 'ersat' table request """

        # Receive record from the client in JSON format
        record = connection.recv(2048)
        record = record.decode('utf-8')
        if sf.insert_into_ersat(self.db_connection, self.cursor, record):

            # If Success, send '1'
            connection.send(str.encode('1'))

    def request_raport(self, connection, records_qty):
        """ Handling query for most recent 1 or 100 record/s from 'pomiary' table and sending as JSON """

        sf.fetch_reports(self.cursor, records_qty, connection)
        connection.send(str.encode('0'))

    def request_archiwum(self, connection):
        """ Handling query for all records from 'ersat' table and sending as JSON """

        sf.fetchall_ersat(self.cursor, connection)
        connection.send(str.encode('0'))


server = ASDEKServer()
server.run()
