from psycopg2 import OperationalError
from psycopg2.extensions import connection
from unittest import TestCase

from mock import patch

from database.connector import PostgresDbConnector

DEVELOPMENT_ENVIRONMENT = 'dev'
TB_DB = 'decoded'
DB_USER = 'thachbui'


class TestDbConnector(TestCase):
    def test_unsupported_db_name(self):
        with self.assertRaises(ValueError):
            PostgresDbConnector('foo', 'foo')

    def test_connect_auto_commit(self):
        with patch('database.connector.DB_ENVIRONMENT', DEVELOPMENT_ENVIRONMENT):
            db_connector = PostgresDbConnector(TB_DB, DB_USER)
            db_connection = db_connector.connect()

        self.assertIsInstance(db_connection, connection)
        service_parameter = 'service={}:{}'.format(TB_DB, DEVELOPMENT_ENVIRONMENT)
        self.assertIn(service_parameter, db_connector.connection_alias)
        self.assertEquals(True, db_connection.autocommit)

    def test_connect_no_auto_commit(self):
        with patch('database.connector.DB_ENVIRONMENT', DEVELOPMENT_ENVIRONMENT):
            db_connector = PostgresDbConnector(TB_DB, DB_USER, False)
            db_connection = db_connector.connect()

        self.assertIsInstance(db_connection, connection)
        service_parameter = 'service={}:{}'.format(TB_DB, DEVELOPMENT_ENVIRONMENT)
        self.assertIn(service_parameter, db_connector.connection_alias)
        self.assertEquals(False, db_connection.autocommit)

    def test_invalid_user(self):
        with patch('database.connector.DB_ENVIRONMENT', DEVELOPMENT_ENVIRONMENT):
            with self.assertRaises(OperationalError):
                db_connector = PostgresDbConnector(TB_DB, 'foo', False)
                db_connector.connect()
