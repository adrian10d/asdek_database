import socket
import threading
import psycopg2
import json
import queries as query
import db_restore
import testing


class ASDEKServer:
    """ Class for handling communication between users and the database """

    def __init__(self):
        """ Initialing the TCP server and database connection """

        # Read the config file
        with open('config.json', 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
            self.HOST = config["server_host"]
            self.PORT = config["server_port"]

            # Establish database connection
            self.db_connection = psycopg2.connect(
                host=config["db_host"],
                database=config["db_database"],
                user=config["db_user"],
                password=config["db_password"],
                port=config["db_port"]
            )

        # Open a cursor to perform database operations
        self.cursor = self.db_connection.cursor()

    def __del__(self):
        """ Closing database connection after terminating the server application """

        try:
            self.cursor.close()
            self.db_connection.close()
        except AttributeError:
            return

    def run(self):
        """ Opening the server application and waiting for clients """

        # Create TCP socket
        with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
            try:
                s.bind((self.HOST, self.PORT))
            except socket.error as e:
                print(str(e))

            print('Listening...')
            s.listen(5)

            # Wait for clients
            while True:
                client, address = s.accept()

                # Handle the client in a new thread
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client,)
                )
                client_handler.start()

                print('New connection request')

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

        elif request == 'przewoznicy':
            self.request_przewoznicy(connection)

        elif request == 'raportg':
            self.request_raportg(connection)

        elif request == 'pomiar':
            self.request_pomiar(connection)

        elif request == 'add_user':
            self.request_add_user(connection)

        elif request == 'insert_test':
            testing.tests.run()

        else:
            connection.send(str.encode('wrong request'))

    def request_log_in(self, connection):
        """ Handling logging-in request """

        # Receive username
        username = connection.recv(2048).decode('utf-8')
        # Receive password
        password = connection.recv(2048).decode('utf-8')

        if query.user_auth(self.cursor, username, password):
            connection.send(str.encode('1'))
        else:
            connection.send(str.encode('0'))

    def request_ersat(self, connection):
        """ Handling inserting into 'ersat' table """

        # Receive record from the client in JSON format and insert into 'ersat' table
        awarie = []
        record = connection.recv(2048).decode('utf-8')
        ersat_id = query.insert_ersat(self.db_connection, self.cursor, record)
        print(ersat_id)
        i = 0
        while True:
            awaria = connection.recv(2048).decode('utf-8')
            if awaria == '0':
                break
            else:
                print(awaria)
                awarie.append(awaria)
                i += 1
                connection.send(str.encode('awaria ' + str(i) + ' received'))
        query.insert_awarie(self.db_connection, self.cursor, awarie, ersat_id)
        message = 'No. of awarie received: ' + str(i)
        connection.send(str.encode(message))
        print(message)

    def request_raport(self, connection, records_qty):
        """ Handling query for most recent 1 or 100 record/s from 'pomiary' table and sending as JSON """

        records_json = query.get_reports(self.cursor, records_qty)
        for record in records_json:
            connection.send(bytes(record, encoding='utf-8'))
        connection.send(str.encode('0'))

    def request_archiwum(self, connection):
        """ Handling query for all records of specific station from 'ersat' table and sending as JSON """

        stacja_diagnost = connection.recv(2048).decode('utf-8')
        query.get_ersat(self.cursor, connection, stacja_diagnost)
        connection.send(str.encode('0'))

    def request_przewoznicy(self, connection):
        """ Handling query for all records from 'przewoznicy' table and sending as JSON """

        records_json = query.get_przewoznicy(self.cursor)
        for record in records_json:
            connection.send(bytes(record, encoding='utf-8'))
        connection.send(str.encode('0'))

    def request_raportg(self, connection):
        """ Handling query for total number of records and not null values in columns: 'gh', 'gm', 'ok', 'pm'
            for specific station in table 'pomiary' """

        stacja_diagnost = connection.recv(2048).decode('utf-8')
        data = query.get_raportg_data(self.cursor, stacja_diagnost)
        connection.send(bytes(data, encoding='utf-8'))

    def request_pomiar(self, connection):
        """ Handling inserting received record to 'pomiary' table """

        pomiar_json = connection.recv(2048).decode('utf-8')
        if query.insert_pomiar(self.db_connection, self.cursor, pomiar_json):
            connection.send(str.encode('1'))
        else:
            connection.send(str.encode('0'))

    def request_add_user(self, connection):
        # Receive username
        name = connection.recv(2048).decode('utf-8')
        # Receive password
        surname = connection.recv(2048).decode('utf-8')
        # Receive username
        username = connection.recv(2048).decode('utf-8')
        # Receive password
        password = connection.recv(2048).decode('utf-8')

        if query.insert_user(self.db_connection, self.cursor, name, surname, username, password):
            print('Successfully added new user ' + name + ' ' + surname)
            connection.send(str.encode('1'))


# Try to restore the database (finalized only while initial run)
db_restore.restore()

# Run the actual server function
server = ASDEKServer()
server.run()


