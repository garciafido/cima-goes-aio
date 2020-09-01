import asyncio
import multiprocessing
import os
import time
from typing import List

import uvloop
from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import Blob, Dataset
from cima.goes.aio.gcs import get_blobs, download_datasets
from cima.goes.aio.tasks import Store
from aiomultiprocess import Pool


async def on_error(filename: str, e: Exception):
    print(filename)
    print(e)


async def on_success(filename: str, dataset: Dataset):
    print("async OK: ", filename)


def init_store():
    blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW), 2019, 11, 12)
    with Store("test.db") as store:
        for blob in blobs:
            store.add(blob.media_link)


def get_pools(store: Store, files_per_pool: int):
    pools = []
    for _ in range(multiprocessing.cpu_count()):
        pool = []
        pools.append(pool)
        for _ in range(files_per_pool):
            url = store.take()
            if url is None:
                return pools
            pool.append(url)
    return pools


async def get(pool: List[str]):
    await download_datasets(pool, on_success=on_success, on_error=on_error)


async def main():
    start_time = time.time()
    with Store("test.db") as store:
        files_pools = get_pools(store, 34)
    print([len(x) for x in files_pools])
    async with Pool(loop_initializer=uvloop.new_event_loop) as pool:
        await pool.map(get, files_pools)
    print("async --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    os.remove("test.db")
    init_store()
    asyncio.run(main())
