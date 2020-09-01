import asyncio
import multiprocessing
import os
import time
from typing import List
from cima.goes.aio.gcs import Dataset, get_blob
from cima.goes.aio.gcs import download_datasets
from cima.goes.aio.tasks_store import Store, Processed, Cancelled
from cima.goes.products import filename_from_media_link


DATABASE_FILEPATH = "test.db"


async def on_error(task_name: str, e: Exception, queue: multiprocessing.Queue):
    queue.put(Cancelled(task_name, str(e)))


async def on_success(task_name: str, dataset: Dataset, queue: multiprocessing.Queue):
    filename = filename_from_media_link(task_name)
    print(filename)
    #print(get_blob(filename))
    queue.put(Processed(task_name))


async def process_taks(names: List[str], queue):
    await download_datasets(
        names,
        on_success=lambda x, y: on_success(x, y, queue),
        on_error=lambda x, y: on_error(x, y, queue))


async def main():
    store = Store(DATABASE_FILEPATH)
    start_time = time.time()
    await store.process(process_taks, 34)
    print("async --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    asyncio.run(main())
