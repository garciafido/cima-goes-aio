import datetime
from typing import Dict, List

import netCDF4
from cima.goes.aio.gcs import get_blobs, get_blob_dataset
from cima.goes.datasets import write_clipping_to_dataset, DatasetClippingInfo, old_sat_lon, \
    get_clipping_info_from_dataset, actual_sat_lon, LatLonRegion
from cima.goes.datasets.clipping import copy_variable, fill_clipped_variable_from_source
from cima.goes.products import ProductBand, Product, Band


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


def save_SA_netcdf(source_dataset: netCDF4.Dataset, clipping_info: DatasetClippingInfo):
    filename = f"SA-{source_dataset.dataset_name}"
    clipped_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
    try:
        clipped_dataset.dataset_name = filename
        write_clipping_to_dataset(clipped_dataset, clipping_info)
        write_institutional_info_to_dataset(clipped_dataset)
        comments = f'Brightness temperature matrix of the cropping area, delimited within ' \
                   f'row_min:{clipped_dataset.row_min} row_max:{clipped_dataset.row_max}; ' \
                   f'col_min:{clipped_dataset.col_min}; col_max:{clipped_dataset.col_min} ' \
                   f'of original matrix size (approximately latitude {clipped_dataset.geospatial_lat_max}°N ' \
                   f'and {-clipped_dataset.geospatial_lat_min}°S; longitude {-clipped_dataset.geospatial_lon_min}°W ' \
                   f'and {-clipped_dataset.geospatial_lon_max}°W.)'
        fill_clipped_variable_from_source(clipped_dataset, source_dataset, comments)
    finally:
        clipped_dataset.close()


def run():
    product_band = ProductBand(product=Product.CMIPF, band=Band.CLEAN_LONGWAVE_WINDOW)
    generate_clipping_info([product_band])
    blob = get_blobs(product_band, datetime.date(year=2017, month=8, day=1), hour=15)[0]
    dataset = get_blob_dataset(blob)
    sat_lon = dataset.variables['goes_imager_projection'].longitude_of_projection_origin
    clipping_info = all_clipping_info[product_band.product][product_band.band][sat_lon]
    save_SA_netcdf(dataset, clipping_info)


if __name__ == "__main__":
    run()
