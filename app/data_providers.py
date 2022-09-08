import logging
# from os import environ, path, walk
# from sqlite3 import connect

# from forum_app import app_secrets, app_path
# from forum_app.databases import BaseDataProviderInterface

import mysql.connector

class MySqlDataProvider(object):
    """Data provider for MySql"""

    def __init__(self, database_settings):
        self.database_settings = database_settings

    def get_database_connection(self):
        mysql_connection = mysql.connector.connect(
            host=self.database_settings['HOST'],
            port=self.database_settings['PORT'],
            user=self.database_settings['USERNAME'],
            password=self.database_settings['PASSWORD'],
            database=self.database_settings['DATABASE'],
            collation='utf8mb4_unicode_ci',
            charset='utf8mb4'
        )
        return mysql_connection

    def execute_batch(self, sql, data_rows):
        """Execute bulk sql"""
        database_connection = None
        database_cursor = None
        try:
            database_connection = self.get_database_connection()
            database_cursor = database_connection.cursor()
            database_cursor.executemany(sql, data_rows)
            database_connection.commit()
            return (0, None) if database_cursor is None else (database_cursor.rowcount, None)
        except Exception as ex:
            logging.error(ex)
            return (-1, ex)
        finally:
            if database_cursor is not None:
                database_cursor.close()
            if database_connection is not None:
                database_connection.close()

    def fetch_record_set(self, sql, args=None):
        """Return a first result_set in record_sets"""
        database_connection = None
        database_cursor = None
        results = []
        try:
            database_connection = self.get_database_connection()
            database_cursor = database_connection.cursor()
            result_sets = database_cursor.execute(sql, args, multi=True)
            for result_set in result_sets:
                if result_set.with_rows:
                    results.append(result_set.fetchall())
            if len(results) > 0:
                return results[0]
            else:
                return []
        except Exception as ex:
            logging.error(ex)
            return None
        finally:
            if database_cursor is not None:
                database_cursor.close()
            if database_connection is not None:
                database_connection.close()