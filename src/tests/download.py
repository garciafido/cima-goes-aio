import asyncio
import datetime
import multiprocessing
import os
import time
from typing import List

from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import Dataset
from cima.goes.aio.gcs import get_blobs, download_datasets
from cima.goes.aio.tasks import Store, Processed, Cancelled


DATABASE_FILEPATH = "test.db"


async def on_error(filename: str, e: Exception, queue: multiprocessing.Queue):
    queue.put(Cancelled(filename, str(e)))


async def on_success(filename: str, dataset: Dataset, queue: multiprocessing.Queue):
    queue.put(Processed(filename))


def init_store():
    blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW),
                      datetime.date(year=2019, month=11, day=12))
    with Store(DATABASE_FILEPATH) as store:
        for blob in blobs:
            store.add(blob.media_link)


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
    if os.path.exists(DATABASE_FILEPATH):
        os.remove(DATABASE_FILEPATH)
    init_store()
    asyncio.run(main())
