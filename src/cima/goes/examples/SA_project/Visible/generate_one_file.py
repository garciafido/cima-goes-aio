import asyncio
import datetime
import os
from typing import Dict, Union
import netCDF4
from cima.goes.aio.gcs import get_blobs, get_blob_dataset, save_blob
from cima.goes.datasets import write_clipping_to_dataset, DatasetClippingInfo
from cima.goes.datasets.clipping import fill_clipped_variable_from_source, get_clipping_info_from_info_dataset, \
    old_sat_lon, actual_sat_lon, get_sat_lon
from cima.goes.products import ProductBand, Product, Band


def write_institutional_info_to_dataset(dataset: netCDF4.Dataset, clipping_info: DatasetClippingInfo):
    dataset.institution = 'Center for Oceanic and Atmospheric Research(CIMA), University of Buenos Aires (UBA) > ARGENTINA'
    dataset.creator_name = "Juan Ruiz and Paola Salio"
    dataset.creator_email = "jruiz@cima.fcen.uba.ar, salio@cima.fcen.uba.ar"
    dataset.summary = f'This file contains red data (channel 2) from GOES 16 satellite, ' \
                      f'within the area of South America delimited approximately ' \
                      f'by latitude {-clipping_info.region.lat_north}°N and ' \
                      f'{-clipping_info.region.lat_south}°S; ' \
                      f'longitude {-clipping_info.region.lon_west}°W and ' \
                      f'{-clipping_info.region.lon_east}°W.' \
                      f'To obtain the corresponding Lat-Lon grids, vectors cutting x and y are attached respectively, ' \
                      f'or you can download the file with the grids generated ' \
                      f'"SA-CMIPF-75W" and "SA-CMIPF-89W" in the project root directory'


async def save_SA_netcdf(source_dataset: netCDF4.Dataset, path="./"):
    clipping_info: DatasetClippingInfo = await get_clipping_info(source_dataset, prefix='SA-CMIPF')
    filename = os.path.join(path, f"SA-{source_dataset.dataset_name}")
    if not os.path.exists(path):
        os.makedirs(path)
    clipped_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
    try:
        clipped_dataset.dataset_name = filename
        write_clipping_to_dataset(clipped_dataset, clipping_info)
        write_institutional_info_to_dataset(clipped_dataset, clipping_info)

        comments = f'Visible matrix of the cropping area, delimited within ' \
                   f'row_min:{clipped_dataset.row_min} row_max:{clipped_dataset.row_max}; ' \
                   f'col_min:{clipped_dataset.col_min}; col_max:{clipped_dataset.col_min} ' \
                   f'of original matrix size (approximately latitude {clipped_dataset.geospatial_lat_max}°N ' \
                   f'and {-clipped_dataset.geospatial_lat_min}°S; longitude {-clipped_dataset.geospatial_lon_min}°W ' \
                   f'and {-clipped_dataset.geospatial_lon_max}°W.)'

        fill_clipped_variable_from_source(clipped_dataset, source_dataset, comments)
    finally:
        clipped_dataset.close()


SA_clipping_info: Dict[float, Union[None, DatasetClippingInfo]] = {
    old_sat_lon: None,
    actual_sat_lon: None,
}


def get_info_filename(dataset: netCDF4.Dataset, prefix):
    imager_projection = dataset.variables['goes_imager_projection']
    sat_lon = imager_projection.longitude_of_projection_origin
    clipping_info = SA_clipping_info[sat_lon]
    resolution = dataset.spatial_resolution.split(" ")[0]
    return f'{prefix}-{resolution}-{-int(sat_lon)}W.nc'


async def get_clipping_info(dataset: netCDF4.Dataset, prefix: str) -> DatasetClippingInfo:
    imager_projection = dataset.variables['goes_imager_projection']
    sat_lon = imager_projection.longitude_of_projection_origin
    clipping_info = SA_clipping_info[sat_lon]
    if clipping_info is None:
        filename = get_info_filename(dataset, prefix)
        info_dataset = netCDF4.Dataset(filename)
        SA_clipping_info[sat_lon] = get_clipping_info_from_info_dataset(info_dataset)
    return SA_clipping_info[sat_lon]


async def test_one():
    product_band = ProductBand(product=Product.CMIPF, band=Band.RED)
    blob = get_blobs(product_band, datetime.date(year=2018, month=8, day=1), hour=15)[0]
    # save_blob(blob, f'./{os.path.basename(blob.name)}')
    dataset = get_blob_dataset(blob)
    await save_SA_netcdf(dataset)


if __name__ == "__main__":
    asyncio.run(test_one())
