from dataclasses import dataclass
from typing import Union, List
from cima.goes.products import Product, Band

from gcloud.aio.storage.blob import Blob

GoesBlob = Union[Blob]


@dataclass
class BandBlobs:
    product: Product
    band: Band
    blobs: List[GoesBlob]
    subproduct: int = None


@dataclass
class GroupedBandBlobs:
    start: str
    blobs: List[BandBlobs]
