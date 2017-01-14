"""
Provide a set of methods that will eliminate the boilerplate code needed to make a database call

Example usage:
    from farmobile.database import SqlExecuter
    sql_executer = SqlExecuter(<database connection object>)
    sql_executer.statement = "select column_a from table where column_b = %(column_b)s"
    sql_executer.arguments = dict(column_b='foo')
    sql_executer.get_all_rows()
    for row in sql_executer.result:
        <do something>

"""
from collections import namedtuple
from logging import getLogger
from pandas.io import sql as psql

from pkg_resources import resource_string

from .connector import PostgresDbConnector

# Execution types
FETCH_ONE = 'fetchone'
FETCH_ALL = 'fetchall'
MODIFY = 'modify'

DJANGO_DATABASE_WRAPPER = 'django.db.backends.postgresql_psycopg2.base.DatabaseWrapper'
DJANGO_DEFAULT_CONNECTION_PROXY = 'django.db.DefaultConnectionProxy'

_log = getLogger(__name__)

ExecutionResults = namedtuple('ExecutionResults', ['query_data', 'cursor_description', 'row_count'])


class SqlExecuter(object):
    """
    A set of convenience methods for commonly used database activities.

    This class is intended to reduce database cursor boilerplate code when performing simple
    database access tasks, such as fetching a single row, fetching an entire results set,
    or altering data.

    """
    sql_location = 'sql/{file_name}'

    def __init__(self, db_connection):
        self.db_connection = db_connection
        connection_type = str(type(self.db_connection))
        self.is_django_connection = (
            DJANGO_DATABASE_WRAPPER in connection_type or
            DJANGO_DEFAULT_CONNECTION_PROXY in connection_type
        )

    def get_dataframe(self, sql, args=None):
        _log.debug("""executing cursor to dataframe""")
        if args:
            _log.debug("""sql to be executed: {}""".format(sql%(args)))
        else:
            _log.debug("""sql to be executed: {}""".format(sql))

        return psql.read_sql(sql, con=self.db_connection, params=args)

    def get_sql_from_file(self, module, sql_filename):
        """
        Retrieve a SQL statement from a .sql file in a sql directory

        Assumes that each python package has an "sql" directory containing .sql files that are
        used by the Python modules in said package.

        :param module: the __name__ of the module retrieving the SQL
        :param sql_filename: string representig the .sql file containing the SQL statement
        :return: string representing the SQL to be executed.
        """
        package = module.rpartition('.')[0]
        return resource_string(package, self.sql_location.format(file_name=sql_filename))

    def _execute(self, sql, args, execution_type):
        """
        Execute a select statement and fetch a single row.
        """
        with self.db_connection.cursor() as cursor:
            _log.debug(cursor.mogrify(sql, args))
            cursor.execute(sql, args)
            if execution_type == FETCH_ONE:
                query_data = cursor.fetchone()
            elif execution_type == FETCH_ALL:
                query_data = cursor.fetchall()
            else:
                query_data = None

            # HACK ALERT!!!    HACK ALERT!!!    HACK ALERT!!!
            # While the postgres backend used by django is implemented with psycopg,
            # it does not support cursor factories.  So, until we move completely away from
            # accessing the database via django, this hack will use the connection type to
            # determine if the connection is django-flavored without implementing django.
            if self.is_django_connection and query_data is not None:
                query_data = self.convert_result_to_namedtuple(cursor.description, query_data)

            results = ExecutionResults(
                query_data=query_data,
                row_count=cursor.rowcount,
                cursor_description=cursor.description if execution_type == MODIFY else None
            )

        return results

    @staticmethod
    def convert_result_to_namedtuple(cursor_description, query_data):
        namedtuple_result = namedtuple('Result', [col.name for col in cursor_description])
        if query_data is None:
            converted_result = None
        elif isinstance(query_data, list):
            converted_result = [namedtuple_result(*row) for row in query_data]
        else:
            converted_result = namedtuple_result(*query_data)

        return converted_result

    def fetch_one_row(self, sql, args=None):
        """
        Execute a select statement and fetch a single row.
        """
        return self._execute(sql, args, FETCH_ONE)

    def fetch_all_rows(self, sql, args=None):
        """
        Execute a select statement and fetch all rows
        """
        return self._execute(sql, args, FETCH_ALL)

    def modify_rows(self, sql, args=None):
        """
        Execute an insert, update or delete statement.
        """
        return self._execute(sql, args, MODIFY)

    def fetch_all_server_side(self, cursor_name, sql, args):
        """
        Generator function that executes a server side cursor.

        :param cursor_name: A string representing the name passed to the server side cursor
        :param sql: A string representing the sql statment to be executed
        :param args: A dictionary or sequence representing the arguments passed to the sql statement
        """
        auto_commit_was_on = self.db_connection.autocommit
        self.db_connection.autocommit = False
        with self.db_connection as cxn:
            with cxn.cursor(name=cursor_name) as cursor:
                cursor.arraysize = 4000
                _log.debug(cursor.mogrify(sql, args))
                cursor.execute(sql, args)
                while True:
                    result_set = cursor.fetchmany()
                    if not result_set:
                        break
                    for row in result_set:
                        yield row

        if auto_commit_was_on:
            self.db_connection.autocommit = True

    def copy_to_table_from_file(self, sql, dump_file_path):
        with self.db_connection.cursor() as cursor:
            with open(dump_file_path, 'r') as dump_file:
                cursor.copy_expert(sql, dump_file)


def copy_table_to_file(db_connection, table_name, dump_file_path):
    with db_connection.cursor() as cursor:
        with open(dump_file_path, 'w') as dump_file:
            cursor.copy_to(dump_file, table_name)


def copy_table_from_file(db_connection, table_name, dump_file_path):
        with db_connection.cursor() as cursor:
            with open(dump_file_path, 'r') as dump_file:
                cursor.copy_from(dump_file, table_name)


def connect_to_db(db_name, user_name, env=None, cursor_factory=None, auto_commit=True):
    """
    Convenience function to connect to a database and return a SqlExecuter object.

    :param db_name: string representing the name of the database
    :param user_name: string representing the user connecting to the database
    :param cursor_factory: psycopg2 cursor factory object, default to NamedTupleCursor
    :param auto_commit: boolean indicating if connection will have auto commit turned on
    :return: SqlExecuter object for interacting with database.
    """
    db_connector = PostgresDbConnector(db_name, user_name, env, auto_commit)
    if cursor_factory is None:
        db_connection = db_connector.connect()
    else:
        db_connection = db_connector.connect(cursor_factory=cursor_factory)

    return SqlExecuter(db_connection)
