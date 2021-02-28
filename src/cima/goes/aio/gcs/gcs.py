import asyncio
import datetime
import io

import netCDF4
import aiohttp
from typing import List, Callable, Awaitable
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage import Blob
from cima.goes.products import GOES_PUBLIC_BUCKET, path_prefix, file_regex_pattern, ANY_MODE
from cima.goes.products import ProductBand


MAX_CONCURRENT = 10


async def download(url: str,
                   session: aiohttp.ClientSession=None,
                   semaphore: asyncio.Semaphore=None,
                   proxy: str=None) -> bytes:
    async with semaphore:
        async with session.get(url, proxy=proxy) as resp:
            return await resp.read()


def get_blob_dataset(blob: Blob) -> netCDF4.Dataset:
    in_memory_file = io.BytesIO()
    blob.download_to_file(in_memory_file)
    in_memory_file.seek(0)
    bytes = in_memory_file.read()
    return netCDF4.Dataset("in_memory_file", mode='r', memory=bytes)


def save_blob(blob: Blob, filename: str):
    blob.download_to_filename(filename)


async def get_dataset(url: str,
                      session: aiohttp.ClientSession=None,
                      semaphore: asyncio.Semaphore=None,
                      proxy: str=None) -> netCDF4.Dataset:
    data = await download(url, session, semaphore=semaphore, proxy=proxy)
    return netCDF4.Dataset("in_memory_file", mode='r', memory=data)


async def download_datasets(names: List[str],
                            on_success: Callable[[str, netCDF4.Dataset], Awaitable[None]],
                            on_error: Callable[[str, Exception], Awaitable[None]],
                            proxy: str=None):
    async def process(name, url, session, semaphore):
        try:
            dataset = await get_dataset(url, session=session, semaphore=semaphore, proxy=proxy)
            try:
                await on_success(name, dataset)
            finally:
                dataset.close()
        except Exception as e:
            await on_error(name, e)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = []
    storage_client = storage.Client(project="<none>", credentials=AnonymousCredentials())
    bucket = storage_client.get_bucket(GOES_PUBLIC_BUCKET)
    timeout = aiohttp.ClientTimeout(total=10*60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for name in names:
            blob = bucket.blob(name)
            url = blob.public_url
            tasks.append(process(name, url, session, semaphore))
        await asyncio.gather(*tasks)


def get_blob(name: str):
    client = storage.Client(project="<none>", credentials=AnonymousCredentials())
    bucket = client.get_bucket(GOES_PUBLIC_BUCKET)
    return bucket.blob(name)


def get_blobs(product_band: ProductBand, date: datetime.date, hour: int=None) -> List[Blob]:
    client = storage.Client(project="<none>", credentials=AnonymousCredentials())
    bucket = client.get_bucket(GOES_PUBLIC_BUCKET)
    prefix = path_prefix(product=product_band.product, year=date.year, month=date.month, day=date.day, hour=hour)
    pattern = file_regex_pattern(
        band=product_band.band, product=product_band.product, mode=ANY_MODE,
        subproduct=product_band.subproduct)
    blobs = bucket.list_blobs(prefix=prefix)
    if pattern is None:
        return blobs
    return [b for b in blobs if pattern.search(b.name)]
