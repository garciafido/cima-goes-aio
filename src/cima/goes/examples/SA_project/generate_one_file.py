import asyncio
import datetime
import os
import netCDF4
from cima.goes.aio.gcs import get_blobs, get_blob_dataset, save_blob
from cima.goes.datasets import write_clipping_to_dataset, DatasetClippingInfo
from cima.goes.datasets.clipping import fill_clipped_variable_from_source, get_clipping_info_from_info_dataset, \
    old_sat_lon, actual_sat_lon, get_sat_lon, get_clipping_info
from cima.goes.products import ProductBand, Product, Band


def vis_dataset_summary(dataset: netCDF4.Dataset, clipping_info: DatasetClippingInfo):
    dataset.summary = f'This file contains red data (channel 2) from GOES 16 satellite, ' \
                      f'within the area of South America delimited approximately ' \
                      f'by latitude {-clipping_info.region.lat_north}°N and ' \
                      f'{-clipping_info.region.lat_south}°S; ' \
                      f'longitude {-clipping_info.region.lon_west}°W and ' \
                      f'{-clipping_info.region.lon_east}°W.' \
                      f'To obtain the corresponding Lat-Lon grids, vectors cutting x and y are attached respectively, ' \
                      f'or you can download the file with the grids generated ' \
                      f'"SA-CMIPF-0.5km-75W" and "SA-CMIPF-0.5km-89W" in the project root directory'


def ir_dataset_summary(dataset: netCDF4.Dataset, clipping_info: DatasetClippingInfo):
    dataset.summary = f'This file contains the brightness temperature of channel 13 from GOES 16 satellite, ' \
                      f'within the area of South America delimited approximately ' \
                      f'by latitude {-clipping_info.region.lat_north}°N and ' \
                      f'{-clipping_info.region.lat_south}°S; ' \
                      f'longitude {-clipping_info.region.lon_west}°W and ' \
                      f'{-clipping_info.region.lon_east}°W.' \
                      f'To obtain the corresponding Lat-Lon grids, vectors cutting x and y are attached respectively, ' \
                      f'or you can download the file with the grids generated ' \
                      f'"SA-CMIPF-2km-75W" and "SA-CMIPF-2km-89W" in the project root directory'


def write_institutional_info_to_dataset(dataset: netCDF4.Dataset, clipping_info: DatasetClippingInfo):
    dataset.institution = 'Center for Oceanic and Atmospheric Research(CIMA), University of Buenos Aires (UBA) > ARGENTINA'
    dataset.creator_name = "Juan Ruiz and Paola Salio"
    dataset.creator_email = "jruiz@cima.fcen.uba.ar, salio@cima.fcen.uba.ar"


def save_SA_netcdf(source_dataset: netCDF4.Dataset, path="./", matrix_type=''):
    clipping_info: DatasetClippingInfo = get_clipping_info(source_dataset, matrix_type=matrix_type, name_prefix='SA-CMIPF')
    filename = os.path.join(path, f"SA-{source_dataset.dataset_name}")
    if not os.path.exists(path):
        os.makedirs(path)
    clipped_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
    try:
        clipped_dataset.dataset_name = filename
        write_clipping_to_dataset(clipped_dataset, clipping_info)
        write_institutional_info_to_dataset(clipped_dataset, clipping_info)
        if matrix_type == 'IR':
            ir_dataset_summary(clipped_dataset, clipping_info)
            matrix_comment = 'Brightness temperature'
        elif matrix_type == 'VIS':
            vis_dataset_summary(clipped_dataset, clipping_info)
            matrix_comment = 'Visible (Red)'
        else:
            raise Exception(f'Unknown matrix type "{matrix_type}"')

        comments = f'{matrix_comment} of the cropping area, delimited within ' \
                   f'row_min:{clipped_dataset.row_min} row_max:{clipped_dataset.row_max}; ' \
                   f'col_min:{clipped_dataset.col_min}; col_max:{clipped_dataset.col_min} ' \
                   f'of original matrix size (approximately latitude {clipped_dataset.geospatial_lat_max}°N ' \
                   f'and {-clipped_dataset.geospatial_lat_min}°S; longitude {-clipped_dataset.geospatial_lon_min}°W ' \
                   f'and {-clipped_dataset.geospatial_lon_max}°W.)'

        fill_clipped_variable_from_source(clipped_dataset, source_dataset, comments)
    finally:
        clipped_dataset.close()


async def get_vis():
        product_band = ProductBand(product=Product.CMIPF, band=Band.RED)
        blob = get_blobs(product_band, datetime.date(year=2018, month=8, day=1), hour=15)[0]
        # save_blob(blob, f'./{os.path.basename(blob.name)}')
        dataset = get_blob_dataset(blob)
        save_SA_netcdf(dataset, matrix_type='VIS')
        dataset.close()


async def get_ir():
        product_band = ProductBand(product=Product.CMIPF, band=Band.CLEAN_LONGWAVE_WINDOW)
        blob = get_blobs(product_band, datetime.date(year=2018, month=8, day=1), hour=15)[0]
        # save_blob(blob, f'./{os.path.basename(blob.name)}')
        dataset = get_blob_dataset(blob)
        save_SA_netcdf(dataset, matrix_type='IR')
        dataset.close()


async def get_both():
    await get_ir()
    await get_vis()


if __name__ == "__main__":
    asyncio.run(get_both())
