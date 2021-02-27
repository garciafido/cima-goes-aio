#!/usr/bin/env python3
import datetime
import os
import time

from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import get_blobs
from cima.goes.aio.tasks_store import Store


DATABASE_FILEPATH = "periodo_relampago_ir.db"
all_until_today = False

def init_store():
    if all_until_today:
        date=datetime.date(2017, 7, 11)
        to_date1=datetime.date(2017, 11, 30)
        from_date2=datetime.date(2017, 12, 15)
        to_date2=datetime.date.today()
        range = 1
    else:
        # Agosto 2018 - Abril 2019
        date = datetime.date(2018, 8, 1)
        to_date2 = datetime.date(2019, 4, 30)
        range = 2
    with Store(DATABASE_FILEPATH) as store:
        while True:
            if range == 1 and date > to_date1:
                date = from_date2
                range = 2
                print("---------------  RANGE 2 --------------")
            elif range == 2 and date > to_date2:
                break
            for hour in [8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0]:
                blobs = get_blobs(ProductBand(Product.CMIPF, Band.CLEAN_LONGWAVE_WINDOW), date, hour)
                for blob in blobs:
                    store.add(blob.name)
                print(date.isoformat(), hour)
            date = date + datetime.timedelta(days=1)


def main():
    if os.path.exists(DATABASE_FILEPATH):
        os.remove(DATABASE_FILEPATH)
    start_time = time.time()
    init_store()
    print("async --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
