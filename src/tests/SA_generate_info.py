import datetime
from typing import List, Dict

import netCDF4
import numpy as np
from cima.goes.products import ProductBand, Product, Band
from cima.goes.aio.gcs import get_blobs, get_blob_dataset
from cima.goes.datasets import get_clipping_info_from_dataset, old_sat_lon, actual_sat_lon, \
    write_clipping_info_to_dataset, get_spatial_resolution
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


def set_dataset_variables(dataset: netCDF4.Dataset, info_dataset):
    dataset.summary = 'This file contains the brightness temperature of channel 13 from GOES 16 satellite, within the area of South America delimited approximately by latitude 15.7°N and 53.9°S; longitude 81.4°W and 34.7°W. To obtain the corresponding Lat-Lon grids, vectors cutting x and y are attached respectively, or you can download the file with the grids generated "SA-CMIPF-2km-75W" and "SA-CMIPF-2km-89W" in the project root directory'
    write_institutional_info_to_dataset(dataset)

    y_min = info_dataset.row_min
    y_max = info_dataset.row_max
    x_min = info_dataset.col_min
    x_max = info_dataset.col_max

    dataset.row_min = np.short(y_min)
    dataset.row_max = np.short(y_max)
    dataset.col_min = np.short(x_min)
    dataset.col_max = np.short(x_max)

    y_dim = y_max-y_min
    x_dim = x_max-x_min

    dataset.createDimension('cropped_y', y_dim)
    dataset.createDimension('cropped_x', x_dim)
    copy_variable(info_dataset.variables['x'], dataset)
    copy_variable(info_dataset.variables['y'], dataset)


def set_band_info(info_dataset: netCDF4.Dataset, product_band: ProductBand, sat_lon: int):
    product = product_band.product
    band = product_band.band
    base_dataset = base_datasets[product][band][sat_lon]
    product_band = {'product': product_band.product.__doc__, 'band': product_band.band.__doc__}
    dataset_region = get_clipping_info_from_dataset(base_dataset, SA_region)
    dataset_region.spatial_resolution = get_spatial_resolution(base_dataset)
    dataset_region.product_bands = [product_band]
    dataset_region.goes_imager_projection = dataset.variables['goes_imager_projection']
    region_indexes_dict[band_key_as_string(dataset_region.sat_band_key)] = dataset_region


def save_info_netcdf(product_band: ProductBand) -> None:
    for sat_lon, clipping_info in all_clipping_info[product_band.product][product_band.band].items():
        filename = get_region_data_filename(product_band, clipping_info, sat_lon)
        info_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
        try:
            info_dataset.dataset_name = filename
            write_clipping_info_to_dataset(info_dataset, clipping_info)
            write_institutional_info_to_dataset(info_dataset)
        finally:
            info_dataset.close()


def get_dataset(product_band) -> netCDF4.Dataset:
    blob = get_blobs(product_band, datetime.date(year=2020, month=4, day=20), hour=15)[0]
    return get_blob_dataset(blob)


def extract_variables(dataset: netCDF4.Dataset, info_dataset: netCDF4.Dataset, source_dataset: netCDF4.Dataset):
    source_cmi = source_dataset.variables['CMI']
    cmi = dataset.createVariable('CMI', source_cmi.datatype, ('cropped_y', 'cropped_x'))
    cmi_attr = {k: source_cmi.getncattr(k) for k in source_cmi.ncattrs() if k[0] != '_'}
    cmi_attr['comments'] = f'Brightness temperature matrix of the cropping area, delimited within row_min:{info_dataset.row_min} row_max:{info_dataset.row_max}; col_min:{info_dataset.col_min}; col_max:{info_dataset.col_min} of original matrix size (approximately latitude 15.7°N and 53.9°S; longitude 81.4°W and 34.7°W.)'
    cmi.setncatts(cmi_attr)

    dataset.time_coverage_start = source_dataset.time_coverage_start
    dataset.time_coverage_end = source_dataset.time_coverage_end
    copy_variable(source_dataset.variables['goes_imager_projection'], dataset)

    dataset.variables['CMI'][:,:] = source_cmi[info_dataset.row_min:info_dataset.row_max, info_dataset.col_min:info_dataset.col_max]


def get_region_data_filename(product_band: ProductBand, clipping_info: DatasetClippingInfo, sat_lon: int):
    resolution = clipping_info.spatial_resolution.split(" ")[0]
    return f'SA-{product_band.product.name}-{resolution}-{str(int(sat_lon)).replace("-", "").replace(".", "_")}W.nc'


def fill_bands_info(lat_lon_region: LatLonRegion,
                    product_band: ProductBand,
                    year: int, month: int, day: int, hour: int):
    blob = get_blobs(product_band, datetime.date(year=year, month=month, day=day), hour=hour)[0]
    dataset = get_blob_dataset(blob)
    dataset_region = find_dataset_region(dataset, lat_lon_region)
    dataset_region.spatial_resolution = get_spatial_resolution(dataset)
    dataset_region.product_band = product_band
    dataset_region.goes_imager_projection = dataset.variables['goes_imager_projection']
    return dataset_region


def generate_region_data(lat_lon_region: LatLonRegion, product_band: ProductBand):
    region_indexes = {}
    region_indexes[old_sat_lon] = fill_bands_info(lat_lon_region, product_band, 2017, 8, 1, 12)
    region_indexes[actual_sat_lon] = fill_bands_info(lat_lon_region, product_band, 2019, 6, 1, 12)
    return region_indexes


def get_rdata(sat_lon, product_band) -> DatasetClippingInfo:
    filename = get_region_data_filename(product_band, sat_lon)
    info_dataset = netCDF4.Dataset(filename)
    imager_projection = info_dataset.variables['goes_imager_projection']
    sat_lon = imager_projection.longitude_of_projection_origin
    try:
        regionIndexes = RegionIndexes(
            x_min=info_dataset.col_min,
            x_max=info_dataset.col_max,
            y_min=info_dataset.row_min,
            y_max=info_dataset.row_max,
        )

        latLonRegion = LatLonRegion(
            lat_north=info_dataset.geospatial_lat_min,
            lat_south=info_dataset.geospatial_lat_max,
            lon_west=info_dataset.geospatial_lon_min,
            lon_east=info_dataset.geospatial_lon_max,
        )

        return DatasetClippingInfo(
            sat_lon=sat_lon,
            region=latLonRegion,
            indexes=regionIndexes,
            goes_imager_projection=imager_projection,
            lats=info_dataset.variables['lats'][:],
            lons=info_dataset.variables['lons'][:],
            x=info_dataset.variables['x'][:],
            y=info_dataset.variables['y'][:],
        )
    finally:
        info_dataset.close()


def save_SA_netcdf(info_dataset: netCDF4.Dataset, source_dataset: netCDF4.Dataset):
    filename = f'SA-{source_dataset.dataset_name}'
    new_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
    try:
        new_dataset.dataset_name = filename
        set_dataset_variables(new_dataset, info_dataset)
        extract_variables(new_dataset, info_dataset, source_dataset)
    finally:
        new_dataset.close()


def run():
    product_band = ProductBand(product=Product.CMIPF, band=Band.CLEAN_LONGWAVE_WINDOW)
    generate_clipping_info([product_band])

    save_info_netcdf(product_band)

    # dataset = get_dataset(product_band)
    # sat_lon = dataset['goes_imager_projection'].longitude_of_projection_origin
    # filename = get_region_data_filename(product_band, sat_lon)
    # info_dataset = netCDF4.Dataset(filename)
    # try:
    #    save_SA_netcdf(info_dataset, dataset)
    # finally:
    #    info_dataset.close()


if __name__ == "__main__":
    run()
