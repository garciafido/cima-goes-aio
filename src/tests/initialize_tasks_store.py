import datetime
import os
import time

from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import get_blobs
from cima.goes.aio.tasks_store import Store


DATABASE_FILEPATH = "test.db"


def init_store():
    blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW),
                      datetime.date(year=2019, month=11, day=12))
    with Store(DATABASE_FILEPATH) as store:
        for blob in blobs:
            store.add(blob.media_link)


def main():
    if os.path.exists(DATABASE_FILEPATH):
        os.remove(DATABASE_FILEPATH)
    start_time = time.time()
    init_store()
    print("async --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
