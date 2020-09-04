#!/usr/bin/env python3
import datetime
import os
import time

from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import get_blobs
from cima.goes.aio.tasks_store import Store


DATABASE_FILEPATH = "test.db"


def init_store():
    date=datetime.date(2017, 7, 11)
    to_date1=datetime.date(2017, 11, 30)
    from_date2=datetime.date(2017, 12, 15)
    to_date2=datetime.date.today()
    range = 1
    with Store(DATABASE_FILEPATH) as store:
        while True:
            if range == 1 and date > to_date1:
                date = from_date2
                range = 2
                print("---------------  RANGE 2 --------------")
            elif range == 2 and date > to_date2:
                break
            blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW), date)
            for blob in blobs:
                store.add(blob.name)
            print(date.isoformat())
            date = date + datetime.timedelta(days=1)


def main():
    if os.path.exists(DATABASE_FILEPATH):
        os.remove(DATABASE_FILEPATH)
    start_time = time.time()
    init_store()
    print("async --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
