from contextlib import contextmanager
import mysql.connector
from mysql.connector import Error
import logging
class DatabaseHelper:
    def __init__(self, host, port, user, password, database):
        """
        Initialize the MySQLHelper class with database connection details.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = self._connect()

    def _connect(self):
        """
        Establish a connection to the MySQL database.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None

    @contextmanager
    def managed_cursor(self):
        """
        Context manager to provide a managed MySQL cursor.
        Yields:
            cursor: A MySQL cursor.
        """
        self._connect()
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Error as e:
            self.connection.rollback()
            print(f"Error during cursor operation: {e}")
            raise e
        finally:
            cursor.close()

    def execute(self, query):
        """
        Execute a query (INSERT, UPDATE, DELETE).
        :param query: SQL query to execute
        :param params: Parameters for the query
        :return: True if the query was successful, else False
        """
        try:
            with self.managed_cursor() as cursor:
                cursor.execute(query)
                print(query)
                result = cursor.fetchall()
                return result
        except Exception as e:
            logging.debug(f'Error execute query in Database: {query}')
            logging.error(e)
            raise e
        return -1
    
    def executemany(self, query, values):
        """
        Execute a query (INSERT, UPDATE, DELETE).
        :param query: SQL query to execute
        :param params: Parameters for the query
        :return: True if the query was successful, else False
        """
        try:
            with self.managed_cursor() as cursor:
                cursor.executemany(query, values)
                print(query)
                result = cursor.fetchall()
                return result
        except Exception as e:
            logging.debug(f'Error executemany query in Database: {query}')
            logging.error(e)
            raise e
        return -1