import asyncio
import netCDF4
import aiohttp
from typing import List, Callable, Awaitable
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage import Blob
from cima.goes.products import GOES_PUBLIC_BUCKET, path_prefix, file_regex_pattern, ANY_MODE
from cima.goes.products import ProductBand, day_path_prefix, get_gcs_url


MAX_CONCURRENT = 10


async def download(url: str,
                   session: aiohttp.ClientSession=None,
                   semaphore: asyncio.Semaphore=None) -> bytes:
    async with semaphore:
        async with session.get(url) as resp:
            return await resp.read()


async def get_dataset(url: str,
                          session: aiohttp.ClientSession=None,
                          semaphore: asyncio.Semaphore=None) -> netCDF4.Dataset:
    data = await download(url, session, semaphore=semaphore)
    return netCDF4.Dataset("in_memory_file", mode='r', memory=data)


async def download_datasets(urls: List[str],
                            on_success: Callable[[str, netCDF4.Dataset], Awaitable[None]],
                            on_error: Callable[[str, Exception], Awaitable[None]]):
    async def process(url, session, semaphore):
        try:
            dataset = await get_dataset(url, session=session, semaphore=semaphore)
            await on_success(url, dataset)
        except Exception as e:
            await on_error(url, e)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(process(url, session, semaphore))
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
