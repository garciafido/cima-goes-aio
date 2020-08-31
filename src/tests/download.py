import asyncio
import time
from typing import List

from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import Blob, Dataset
from cima.goes.aio.gcs import download_blobs_aio, download_blobs, get_blobs
from cima.goes.aio.tasks import Store


async def on_error_aio(blob: Blob, e: Exception):
    print(blob.name)
    print(e)


async def on_success_aio(blob: Blob, dataset: Dataset):
    pass  # print("async OK: ", blob.name)


def on_error(blob: Blob, e: Exception):
    print(blob.name)
    print(e)


def on_success(blob: Blob, dataset: Dataset):
    pass  # print("OK: ", blob.name)


def try_async(blobs: List[Blob]):
    start_time = time.time()
    asyncio.run(download_blobs_aio(blobs, on_success=on_success_aio, on_error=on_error_aio), debug=False)
    print("async --- %s seconds ---" % (time.time() - start_time))


def try_seq(blobs):
    start_time = time.time()
    download_blobs(blobs, on_success=on_success, on_error=on_error)
    print("--- %s seconds ---" % (time.time() - start_time))

def init_store():
    blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW), 2019, 11, 12)
    with Store("test.db") as store:
        for blob in blobs:
            store.add(blob.name)


def play_store():
    with Store("test.db") as store:
        filename = store.get()
        print(filename)
        filename = store.get()
        print(filename)


if __name__ == "__main__":
    #init_store()
    play_store()
