from mock import create_autospec, Mock

from .sql_executer import SqlExecuter


class DatabaseMocker(object):
    def __init__(self):
        self.db = create_autospec(SqlExecuter)
        self.execution_results = Mock(query_data=None, row_count=None)

    def mock_get_sql_from_file(self, dynamic_sql=None):
        self.db.get_sql_from_file.return_value = dynamic_sql

    def mock_fetch_one_row(self, query_data):
        query_results = Mock(query_data=query_data)
        fetch_one_row = Mock(return_value=query_results)
        self.db.fetch_one_row = fetch_one_row

    def mock_fetch_all_rows(self, query_data):
        query_results = Mock(query_data=query_data)
        fetch_all_rows = Mock(return_value=query_results)
        self.db.fetch_all_rows = fetch_all_rows

    def mock_fetch_all_server_side(self, query_data):
        fetch_all_server_side = Mock(return_value=query_data)
        self.db.fetch_all_server_side = fetch_all_server_side

    def mock_modify_rows(self, row_count=0):
        query_results = Mock(row_count=row_count)
        modify_rows = Mock(return_value=query_results)
        self.db.modify_rows = modify_rows
