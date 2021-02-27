#!/usr/bin/env python3
import os
import asyncio
import multiprocessing
import time
import traceback
from typing import List
from cima.goes.aio.gcs import Dataset
from cima.goes.aio.gcs import download_datasets
from cima.goes.aio.tasks_store import Store, Processed, Cancelled
from generate_one_file import save_SA_netcdf


DATABASE_FILEPATH = "periodo_relampago.db"
DOWNLOAD_DIR = "./"
#DOWNLOAD_DIR = "/datoslinus/jruiz/Datos_GOES/SouthAmerica/Relampago/Visible"
BATCH_SIZE_PER_WORKER = 2
PROXY=None
#PROXY="http://proxy.fcen.uba.ar:8080"


async def on_error(task_name: str, e: Exception, queue: multiprocessing.Queue):
    print("CANCELLED:", task_name)
    # print("ERROR:", traceback.print_exc())
    queue.put(Cancelled(task_name, str(e)))


async def on_success(task_name: str, dataset: Dataset, queue: multiprocessing.Queue):
    print(task_name)
    await save_SA_netcdf(dataset, path=os.path.join(DOWNLOAD_DIR, os.path.dirname(task_name)))
    queue.put(Processed(task_name))


async def process_tasks(names: List[str], queue):
    await download_datasets(
        names,
        on_success=lambda x, y: on_success(x, y, queue),
        on_error=lambda x, y: on_error(x, y, queue),
        proxy=PROXY)


async def main():
    store = Store(DATABASE_FILEPATH)
    start_time = time.time()
    print(store.get_stats())
    store.free_taken()
    store.free_cancelled()
    tasks_remain = True
    while tasks_remain:
        print(f"{time.time() - start_time} seconds {store.get_stats()}")
        tasks_remain = await store.process(process_tasks, BATCH_SIZE_PER_WORKER)
    print("async --- %s seconds ---" % (time.time() - start_time))
    print(store.get_stats())

if __name__ == "__main__":
    asyncio.run(main())
