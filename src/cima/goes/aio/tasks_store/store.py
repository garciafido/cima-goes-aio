import asyncio
import multiprocessing
import os
import datetime
from typing import Callable, List, Awaitable, Union

import apsw
import six
import uvloop

from .singleton import SingletonType
from .commands import Command, BreakCommand
from aiomultiprocess import Pool

_store_lock = multiprocessing.Lock()


@six.add_metaclass(SingletonType)
class Store(object):
    def __init__(self, database_filepath: str):
        self.database_filepath = database_filepath
        self.connection = None
        self._open_database()
        #print('init database')

    def add(self, name: str, detail=''):
        add_sql = f"""INSERT INTO task(name, status, detail, begin)
            VALUES('{name}', 'PENDING', '{detail.replace("'", '''"''')}', '{datetime.datetime.now().isoformat}')
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
                update_sql = f"""update task set status = 'TAKEN', detail = '{detail.replace("'", '''"''')}', begin = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
                cursor.execute(update_sql)
                return name

    def _processed(self, name, detail=''):
        select_sql = f"""select name from task where name = '{name}';"""
        update_sql = f"""update task set status = 'PROCESSED', detail = '{detail.replace("'", '''"''')}', end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                rows = cursor.fetchall()
                if not rows:
                    raise Exception(f"{name} does not exists")
                cursor.execute(update_sql)
                return name

    def _cancelled(self, name, detail):
        select_sql = f"""select name from task where name = '{name}';"""
        update_sql = f"""update task set status = 'CANCELLED', detail = '{detail.replace("'", '''"''')}', end_process = '{datetime.datetime.now().isoformat()}' where name = '{name}';"""
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

    def get_stats(self):
        select_sql = f"""select status, count(name) from task group by status;"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(select_sql)
                return cursor.fetchall()

    def free_taken(self):
        update_sql = f"""update task set status = 'PENDING' where status = 'TAKEN';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(update_sql)

    def free_cancelled(self):
        update_sql = f"""update task set status = 'PENDING' where status = 'CANCELLED';"""
        with _store_lock:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute(update_sql)

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

    async def process(self, process_taks: Callable[[List[str]], Awaitable[None]], pool_size: int, workers_count: int=None):
        async def finish():
            queue.put(BreakCommand())
            while not queue.empty():
                await asyncio.sleep(1)

        queue = self._run_queue()
        files_pools = self._get_pools(workers_count, pool_size, queue)
        if not sum([len(x[0]) for x in files_pools]):
            print([len(x[0]) for x in files_pools])
            await finish()
            return False

        async with Pool(loop_initializer=uvloop.new_event_loop) as pool:
            await pool.starmap(process_taks, files_pools)
        await finish()
        return True

    def put(self, command: Command):
        if isinstance(command, Processed):
            self._processed(*command._args, **command._kwargs)
        elif isinstance(command, Cancelled):
            self._cancelled(*command._args, **command._kwargs)

    def _run_queue(self) -> multiprocessing.Queue:
        m = multiprocessing.Manager()
        queue = m.Queue()
        p = multiprocessing.Process(target=self._worker, args=(queue, self.database_filepath))
        p.start()
        return queue

    def _get_pools(self, workers_count: Union[None, int], files_per_pool: int, queue: multiprocessing.Queue):
        pools = []
        if workers_count is None:
            workers_count = multiprocessing.cpu_count()
        for _ in range(workers_count):
            pool = []
            pools.append((pool, queue))
            for _ in range(files_per_pool):
                url = self.take()
                if url is None:
                    return pools
                pool.append(url)
        return pools

    @staticmethod
    def _worker(queue: multiprocessing.Queue, database_filepath: str):
        with Store(database_filepath) as store:
            while True:
                command = queue.get()
                if isinstance(command, BreakCommand):
                    break
                store.put(command)


class Processed(Command):
    pass


class Cancelled(Command):
    pass


