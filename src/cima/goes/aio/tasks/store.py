import os
import multiprocessing
import datetime
import apsw

print("Number of cpu : ", multiprocessing.cpu_count())


class Store(object):
    def __init__(self, database_filepath: str):
        self.database_filepath = database_filepath
        self.connection = None
        self._open_database()

    def add(self, name: str, detail=''):
        sql = f"""INSERT INTO task(name, status, detail, begin)
            VALUES('{name}', 'PENDING', '{detail}', '{datetime.datetime.now().isoformat}')
        """
        self.cursor.execute(sql)

    def take(self, detail=''):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = """select name from task where status = 'PENDING' limit 1;"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                return None
            name = rows[0][0]
            update_sql = f"""update task set status = 'TAKED', detail = '{detail}', begin = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
            cursor.execute(update_sql)
            return name

    def processed(self, name, detail=''):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = f"""select name from task where name = '{name}';"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                raise Exception(f"{name} does not exists")
            update_sql = f"""update task set status = 'PROCESSED', detail = '{detail}', end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
            cursor.execute(update_sql)
            return name

    def cancelled(self, name, detail):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = f"""select name from task where name = '{name}';"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                raise Exception(f"{name} does not exists")
            update_sql = f"""update task set status = 'CANCELLED', detail = '{detail}'end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
            cursor.execute(update_sql)
            return name

    def get_status(self, name):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = f"""select name, status, begin, end_process, detail from task where name = '{name}';"""
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

    def initialize_database(self):
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
        initialize = False
        if not os.path.exists(self.database_filepath):
            initialize = True
        self.connection = apsw.Connection(self.database_filepath)
        self.cursor = self.connection.cursor()
        if initialize:
            self.initialize_database()

    def __del__(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
