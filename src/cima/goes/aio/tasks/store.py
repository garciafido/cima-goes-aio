import multiprocessing
import os
import datetime
import apsw
import six

from .singleton import SingletonType


_store_lock = multiprocessing.Lock()


@six.add_metaclass(SingletonType)
class Store(object):
    def __init__(self, database_filepath: str):
        self.database_filepath = database_filepath
        self.connection = None
        self._open_database()
        print('init database')

    def add(self, name: str, detail=''):
        add_sql = f"""INSERT INTO task(name, status, detail, begin)
            VALUES('{name}', 'PENDING', '{detail}', '{datetime.datetime.now().isoformat}')
        """
        with _store_lock:
            self.cursor.execute(add_sql)

    def take(self, detail=''):
        select_sql = """select name from task where status = 'PENDING' limit 1;"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                if not rows:
                    return None
                name = rows[0][0]
                update_sql = f"""update task set status = 'TAKED', detail = '{detail}', begin = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
                cursor.execute(update_sql)
                return name

    def processed(self, name, detail=''):
        select_sql = f"""select name from task where name = '{name}';"""
        update_sql = f"""update task set status = 'PROCESSED', detail = '{detail}', end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                if not rows:
                    raise Exception(f"{name} does not exists")
                cursor.execute(update_sql)
                return name

    def cancelled(self, name, detail):
        select_sql = f"""select name from task where name = '{name}';"""
        update_sql = f"""update task set status = 'CANCELLED', detail = '{detail}'end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                if not rows:
                    raise Exception(f"{name} does not exists")
                cursor.execute(update_sql)
                return name

    def get_status(self, name):
        select_sql = f"""select name, status, begin, end_process, detail from task where name = '{name}';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                if not rows:
                    raise Exception(f"{name} does not exists")
                return {
                    "name": rows[0][0],
                    "status": rows[0][1],
                    "begin": rows[0][2],
                    "end_process": rows[0][3],
                    "detail": rows[0][4]
                }

    def _initialize_database(self):
        if self.connection:
            self.connection.close()
        if os.path.exists(self.database_filepath):
            os.remove(self.database_filepath)
        self.connection = apsw.Connection(self.database_filepath)
        self.cursor = self.connection.cursor()
        blobs_sql = """CREATE TABLE IF NOT EXISTS task (
                name text  PRIMARY KEY,
                status text NOT NULL,
                detail text,
                begin timestamp,
                end_process timestamp
        );"""
        index_sql = """CREATE INDEX IF NOT EXISTS by_status ON task(status)"""
        self.cursor.execute(blobs_sql)
        self.cursor.execute(index_sql)

    def _open_database(self):
        with _store_lock:
            initialize = False
            if not os.path.exists(self.database_filepath):
                initialize = True
            self.connection = apsw.Connection(self.database_filepath)
            self.cursor = self.connection.cursor()
            if initialize:
                self._initialize_database()

    def __del__(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
