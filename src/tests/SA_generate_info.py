import datetime
from typing import List, Dict

import netCDF4
import numpy as np
from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import get_blobs, get_blob_dataset
from cima.goes.datasets import get_clipping_info_from_dataset, old_sat_lon, actual_sat_lon, \
    write_clipping_to_info_dataset, get_spatial_resolution
from cima.goes.datasets import LatLonRegion, DatasetClippingInfo, RegionIndexes


SA_region: LatLonRegion = LatLonRegion(
    lat_south=-53.9,
    lat_north=15.7,
    lon_west=-81.4,
    lon_east=-34.7
)


all_clipping_info: Dict[str, Dict[str, Dict[int, DatasetClippingInfo]]] = {}


def generate_clipping_info(product_bands: List[ProductBand]):
    for product_band in product_bands:
        product = product_band.product
        band = product_band.band
        all_clipping_info[product] = {band: {}}

        blob = get_blobs(product_band, datetime.date(year=2017, month=8, day=1), hour=15)[0]
        dataset = get_blob_dataset(blob)
        all_clipping_info[product][band][old_sat_lon] = get_clipping_info_from_dataset(dataset, SA_region)

        blob = get_blobs(product_band, datetime.date(year=2019, month=6, day=1), hour=15)[0]
        dataset = get_blob_dataset(blob)
        all_clipping_info[product][band][actual_sat_lon] = get_clipping_info_from_dataset(dataset, SA_region)


def write_institutional_info_to_dataset(dataset: netCDF4.Dataset):
    dataset.institution = 'Center for Oceanic and Atmospheric Research(CIMA), University of Buenos Aires (UBA) > ARGENTINA'
    dataset.creator_name = "Juan Ruiz and Paola Salio"
    dataset.creator_email = "jruiz@cima.fcen.uba.ar, salio@cima.fcen.uba.ar"
    dataset.geospatial_lat_min = SA_region.lat_south
    dataset.geospatial_lat_max = SA_region.lat_north
    dataset.geospatial_lon_min = SA_region.lon_west
    dataset.geospatial_lon_max = SA_region.lon_east


def save_info_netcdf(product_band: ProductBand) -> None:
    for sat_lon, clipping_info in all_clipping_info[product_band.product][product_band.band].items():
        filename = get_region_data_filename(product_band, clipping_info, sat_lon)
        info_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
        try:
            info_dataset.dataset_name = filename
            write_clipping_to_info_dataset(info_dataset, clipping_info)
            write_institutional_info_to_dataset(info_dataset)
        finally:
            info_dataset.close()


def get_dataset(product_band) -> netCDF4.Dataset:
    blob = get_blobs(product_band, datetime.date(year=2020, month=4, day=20), hour=15)[0]
    return get_blob_dataset(blob)


def get_region_data_filename(product_band: ProductBand, clipping_info: DatasetClippingInfo, sat_lon: int):
    resolution = clipping_info.spatial_resolution.split(" ")[0]
    return f'SA-{product_band.product.name}-{resolution}-{str(int(sat_lon)).replace("-", "").replace(".", "_")}W.nc'


def run():
    product_band = ProductBand(product=Product.CMIPF, band=Band.CLEAN_LONGWAVE_WINDOW)
    generate_clipping_info([product_band])

    save_info_netcdf(product_band)


if __name__ == "__main__":
    run()
