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

    def add(self, name: str):
        sql = f"""INSERT INTO task(name, status, begin)
            VALUES('{name}', 'PENDING', '{datetime.datetime.now().isoformat}')
        """
        self.cursor.execute(sql)

    def take(self):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = """select name from task where status = 'PENDING' limit 1;"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                return None
            name = rows[0][0]
            update_sql = f"""update task set status = 'TAKED', begin = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
            cursor.execute(update_sql)
            return name

    def processed(self, name):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = f"""select name from task where name = '{name}';"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                raise Exception(f"{name} does not exists")
            update_sql = f"""update task set status = 'PROCESSED', end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
            cursor.execute(update_sql)
            return name

    def get_status(self, name):
        with self.connection:
            cursor = self.connection.cursor()
            select_sql = """select name, status, begin, end_process from task where name = '{name}';"""
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            if not rows:
                raise Exception(f"{name} does not exists")
            return {"name": rows[0][0], "status": rows[0][1], "begin": rows[0][2], "end_process": rows[0][3]}

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


''' 
    def upsert_range(
            key_name: str,
            from_date: datetime.datetime,
            to_date: datetime.datetime,
            hours: List[int]):

        str_hours = ",".join(map(str, hours))
        sql = f"""INSERT INTO range(key_name, from_date, to_date, hours)
            VALUES('{key_name}', '{_isodate(from_date)}', '{_isodate(to_date)}', '{str_hours}')
            ON CONFLICT(key_name) DO UPDATE SET
                key_name = '{key_name}',
                from_date = '{_isodate(from_date)}',
                to_date = '{_isodate(to_date)}',
                hours = '{str_hours}'
            ;"""

        self.conn.execute(sql)
        self.conn.commit()


    def complete(key_name: str, successful_files: int, date: datetime.date, hour: int):
        sql = f"""INSERT INTO completed_hours(range_name, successful_files, hour_completed, date)
                    VALUES('{key_name}', {successful_files}, '{date+datetime.timedelta(hours=hour)}', '{datetime.datetime.now().isoformat()}')
                    ;"""
        self.conn.execute(sql)
        self.conn.commit()


    def peek_next(key_name):
        c = self.conn.cursor()
        sql = f"""SELECT from_date, hours, to_date FROM range WHERE key_name = '{key_name}';"""
        c.execute(sql)
        from_date, hours, to_date = c.fetchall()[0]
        hours = map(int, hours.split(','))
        sql = f"""SELECT max(hour_completed) FROM completed_hours WHERE range_name = '{key_name}';"""
        c.execute(sql)
        last_datetime = c.fetchall()[0][0]
        if last_datetime is None:
            return datetime.date.fromisoformat(from_date), min(hours)
        next_datetime = last_datetime + datetime.timedelta(hours=1)
        while next_datetime.hour not in hours:
            next_datetime = last_datetime + datetime.timedelta(hours=1)
        if next_datetime >= to_date:
            return next_datetime.date(), next_datetime.hours
        return None


    def _isodate(date: datetime.date):
        return date.isoformat()

'''