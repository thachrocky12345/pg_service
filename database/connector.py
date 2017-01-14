"""
Library for connecting to databases used at Farmobile
"""
from os import environ

from psycopg2 import connect, extras


class MissingEnvironmentError(Exception):
    pass


class PostgresDbConnector(object):
    """
    Connect to a Postgres database using psycopg

    In an effort to obfuscate database connection credentials, this class requires implementation
    of the pgpass and pg_service configuration files on any server that uses it.
    """
    def __init__(self, db_name, user_name, env=None, auto_commit=True):
        """
        Constructor

        :param db_name: string representing the name of the database
        :param user_name: string representing the name of the database user
        """

        if env is None:
            try:
                self.db_environment = environ['ENV']
            except KeyError:
                raise MissingEnvironmentError("No ENV environment variable found")
        else:
            self.db_environment = env

        self.connection_alias = "service={db_name}:{environment} user={user_name}".format(
            user_name=user_name, db_name=db_name, environment=self.db_environment
        )
        self.auto_commit = auto_commit

    def connect(self, cursor_factory=extras.NamedTupleCursor):
        """
        Standard function to connect to a Farmobile database

        :return: A psycopg2 connection object
        """
        db_connection = connect(self.connection_alias, cursor_factory=cursor_factory)
        db_connection.autocommit = self.auto_commit

        return db_connection
