import asyncio
import io
import netCDF4
import aiohttp
from typing import List, Callable, Awaitable
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage import Blob
from cima.goes.aio.products import GOES_PUBLIC_BUCKET, path_prefix, file_regex_pattern, ANY_MODE
from cima.goes.aio.products import Product, Band, ProductBand, day_path_prefix

MAX_CONCURRENT = 10


def download_from_blob(blob):
    in_memory_file = io.BytesIO()
    blob.download_to_file(in_memory_file)
    in_memory_file.seek(0)
    return in_memory_file.read()


async def download_aio(url: str,
                       session: aiohttp.ClientSession=None,
                       semaphore: asyncio.Semaphore=None) -> bytes:
    async with semaphore:
        async with session.get(url) as resp:
            return await resp.read()


async def download_from_blob_aio(blob: Blob,
                                 session: aiohttp.ClientSession=None,
                                 semaphore: asyncio.Semaphore=None) -> bytes:
    return await download_aio(blob.media_link, session=session, semaphore=semaphore)


def get_dataset(blob: Blob) -> netCDF4.Dataset:
    data = download_from_blob(blob)
    return netCDF4.Dataset("in_memory_file", mode='r', memory=data)


async def get_dataset_aio(blob: Blob,
                          session: aiohttp.ClientSession=None,
                          semaphore: asyncio.Semaphore=None) -> netCDF4.Dataset:
    data = await download_from_blob_aio(blob, session, semaphore=semaphore)
    return netCDF4.Dataset("in_memory_file", mode='r', memory=data)


def download_blobs(blobs: List[Blob],
                   on_success: Callable[[Blob, netCDF4.Dataset], None],
                   on_error: Callable[[Blob, Exception], None]):
    for blob in blobs:
        try:
            on_success(blob, get_dataset(blob))
        except Exception as e:
            on_error(blob, e)


async def download_blobs_aio(blobs: List[Blob],
                             on_success: Callable[[Blob, netCDF4.Dataset], Awaitable[None]],
                             on_error: Callable[[Blob, Exception], Awaitable[None]]):
    async def process(blob, session, semaphore):
        try:
            dataset = await get_dataset_aio(blob, session=session, semaphore=semaphore)
            await on_success(blob, dataset)
        except Exception as e:
            await on_error(blob, e)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for blob in blobs:
            tasks.append(process(blob, session, semaphore))
        await asyncio.gather(*tasks)


def get_blobs(product_band: ProductBand, year: int, month: int, day: int, hour: int=None) -> List[Blob]:
    client = storage.Client(project="<none>", credentials=AnonymousCredentials())
    bucket = client.get_bucket(GOES_PUBLIC_BUCKET)
    if hour is None:
        prefix = day_path_prefix(year=year, month=month, day=day, product=product_band.product)
    else:
        prefix = path_prefix(year=year, month=month, day=day, hour=hour, product=product_band.product)
    pattern = file_regex_pattern(
        band=product_band.band, product=product_band.product, mode=ANY_MODE,
        subproduct=product_band.subproduct)
    blobs = bucket.list_blobs(prefix=prefix)
    if pattern is None:
        return blobs
    return [b for b in blobs if pattern.search(b.name)]


if __name__ == "__main__":
    import time

    async def on_error_aio(blob: Blob, e: Exception):
        print(blob.name)
        print(e)

    async def on_success_aio(blob: Blob, dataset: netCDF4.Dataset):
        pass  # print("async OK: ", blob.name)

    def on_error(blob: Blob, e: Exception):
        print(blob.name)
        print(e)

    def on_success(blob: Blob, dataset: netCDF4.Dataset):
        pass  # print("OK: ", blob.name)

    def try_async(blobs):
        start_time = time.time()
        asyncio.run(download_blobs_aio(blobs, on_success=on_success_aio, on_error=on_error_aio), debug=False)
        print("async --- %s seconds ---" % (time.time() - start_time))

    def try_seq(blobs):
        start_time = time.time()
        download_blobs(blobs, on_success=on_success, on_error=on_error)
        print("--- %s seconds ---" % (time.time() - start_time))

    blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW), 2019, 11, 12)
    try_async(blobs)
    try_seq(blobs)
    try_async(blobs)
    try_seq(blobs)
