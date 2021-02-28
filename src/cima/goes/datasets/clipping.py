import multiprocessing
import datetime
from typing import List, Dict, Union
import math

import netCDF4
import pyproj
import numpy as np
from dataclasses import dataclass

from cima.goes.aio.gcs import get_blobs, get_blob_dataset
from cima.goes.products import ProductBand

old_sat_lon = -89.5
actual_sat_lon = -75.0
default_major_order = FORTRAN_ORDER = 'F'

_clipping_info = multiprocessing.Lock()

@dataclass
class LatLonRegion:
    lat_north: float
    lat_south: float
    lon_west: float
    lon_east: float


@dataclass
class RegionIndexes:
    col_min: int = None
    col_max: int = None
    row_min: int = None
    row_max: int = None


@dataclass
class DatasetClippingInfo:
    goes_imager_projection: any
    spatial_resolution: str
    orbital_slot: str
    instrument_type: str
    region: LatLonRegion
    indexes: RegionIndexes
    lats: any
    lons: any
    x: any
    y: any


clipping_info_dict = Dict[float, DatasetClippingInfo]


def get_info_filename(resolution, sat_lon, name_prefix) -> str:
    return f'{name_prefix}-{resolution}-{-int(sat_lon)}W.nc'


def get_info_filename_for_dataset(dataset: netCDF4.Dataset, name_prefix) -> str:
    imager_projection = dataset.variables['goes_imager_projection']
    sat_lon = imager_projection.longitude_of_projection_origin
    resolution = dataset.spatial_resolution.split(" ")[0]
    return get_info_filename(resolution, sat_lon, name_prefix)


clipping_info_cache: Dict[str, Union[None, DatasetClippingInfo]] = {}

def get_clipping_info(dataset: netCDF4.Dataset, name_prefix: str, matrix_type='') -> DatasetClippingInfo:
    global clipping_info_cache
    imager_projection = dataset.variables['goes_imager_projection']
    sat_lon = imager_projection.longitude_of_projection_origin
    clipping_key = f'{matrix_type}-{sat_lon}'
    with _clipping_info:
        clipping_info = clipping_info_cache[clipping_key] if clipping_key in clipping_info_cache else None
        if clipping_info is None:
            filename = get_info_filename_for_dataset(dataset, name_prefix)
            info_dataset = netCDF4.Dataset(filename)
            clipping_info_cache[clipping_key] = get_clipping_info_from_info_dataset(info_dataset)
        return clipping_info_cache[clipping_key]


def generate_info_files(product_bands: List[ProductBand],
                        latLonRegion: LatLonRegion,
                        filename_prefix="./",
                        institution="Center for Oceanic and Atmospheric Research (CIMA), University of Buenos Aires (UBA) > ARGENTINA",
                        creator_name="Juan Ruiz and Paola Salio",
                        creator_email="jruiz@cima.fcen.uba.ar, salio@cima.fcen.uba.ar") -> None:
    filenames = []
    for product_band in product_bands:
        all_clipping_info = _generate_clipping_info(product_band, latLonRegion)
        for sat_lon, clipping_info in all_clipping_info.items():
            resolution = clipping_info.spatial_resolution.split(" ")[0]
            filename = get_info_filename(resolution, sat_lon, filename_prefix)
            filenames.append(filename)
            info_dataset = netCDF4.Dataset(filename, 'w', format='NETCDF4')
            try:
                info_dataset.dataset_name = filename
                write_clipping_to_info_dataset(info_dataset, clipping_info)
                info_dataset.institution = institution
                info_dataset.creator_name = creator_name
                info_dataset.creator_email = creator_email
            finally:
                info_dataset.close()
    return filenames


# def _get_region_data_filename(filename_prefix, product_band: ProductBand, clipping_info: DatasetClippingInfo, sat_lon: float):
#     resolution = clipping_info.spatial_resolution.split(" ")[0]
#     return f'{filename_prefix}{product_band.product.name}-{resolution}-{str(abs(math.trunc(sat_lon))).replace("-", "").replace(".", "_")}W.nc'


def get_sat_lon(dataset: netCDF4.Dataset):
    imager_projection = dataset.variables['goes_imager_projection']
    return imager_projection.longitude_of_projection_origin


def _generate_clipping_info(product_band: ProductBand, latLonRegion: LatLonRegion) -> clipping_info_dict:
    clipping_info: clipping_info_dict = {old_sat_lon: None, actual_sat_lon: None}

    blob = get_blobs(product_band, datetime.date(year=2017, month=8, day=1), hour=15)[0]
    dataset = get_blob_dataset(blob)
    clipping_info[old_sat_lon] = get_clipping_info_from_dataset(dataset, latLonRegion)

    blob = get_blobs(product_band, datetime.date(year=2018, month=8, day=1), hour=15)[0]
    dataset = get_blob_dataset(blob)
    clipping_info[actual_sat_lon] = get_clipping_info_from_dataset(dataset, latLonRegion)

    return clipping_info


def get_clipping_info_from_info_dataset(info_dataset: netCDF4.Dataset):
    latLonRegion = LatLonRegion(
        lat_north=info_dataset.geospatial_lat_max,
        lat_south=info_dataset.geospatial_lat_min,
        lon_west=info_dataset.geospatial_lon_min,
        lon_east=info_dataset.geospatial_lon_max,
    )

    indexes = RegionIndexes(
        col_min=info_dataset.col_min,
        col_max=info_dataset.col_max,
        row_min=info_dataset.row_min,
        row_max=info_dataset.row_max
    )

    return DatasetClippingInfo(
        goes_imager_projection=info_dataset.variables['goes_imager_projection'],
        spatial_resolution=info_dataset.spatial_resolution,
        orbital_slot=info_dataset.orbital_slot,
        instrument_type=info_dataset.instrument_type,
        region=latLonRegion,
        indexes=indexes,
        lats=info_dataset.variables['lats'][:,:],
        lons=info_dataset.variables['lons'][:,:],
        x=info_dataset.variables['x'][:],
        y=info_dataset.variables['y'][:]
    )
    
    
def get_clipping_info_from_dataset(dataset: netCDF4.Dataset, region: LatLonRegion) -> DatasetClippingInfo:
    lats, lons, x, y = get_lats_lons_x_y(dataset)
    indexes = find_indexes(region, lats, lons, default_major_order)
    return DatasetClippingInfo(
        goes_imager_projection=dataset.variables['goes_imager_projection'],
        spatial_resolution=dataset.spatial_resolution,
        orbital_slot=dataset.orbital_slot,
        instrument_type=dataset.instrument_type,
        region=region,
        indexes=indexes,
        lats=lats[indexes.row_min: indexes.row_max, indexes.col_min: indexes.col_max],
        lons=lons[indexes.row_min: indexes.row_max, indexes.col_min: indexes.col_max],
        x=x[indexes.col_min: indexes.col_max],
        y=y[indexes.row_min: indexes.row_max]
    )


def get_spatial_resolution(dataset: netCDF4.Dataset) -> float:
    return float(dataset.spatial_resolution[:dataset.spatial_resolution.find("km")])


def _write_clipping_to_any_dataset(dataset: netCDF4.Dataset, dscd: DatasetClippingInfo):
    copy_variable(dscd.goes_imager_projection, dataset)
    dataset.col_min = np.short(dscd.indexes.col_min)
    dataset.col_max = np.short(dscd.indexes.col_max)
    dataset.row_min = np.short(dscd.indexes.row_min)
    dataset.row_max = np.short(dscd.indexes.row_max)

    dataset.geospatial_lat_min = dscd.region.lat_south
    dataset.geospatial_lat_max = dscd.region.lat_north
    dataset.geospatial_lon_min = dscd.region.lon_west
    dataset.geospatial_lon_max = dscd.region.lon_east

    dataset.spatial_resolution = dscd.spatial_resolution
    dataset.orbital_slot = dscd.orbital_slot
    dataset.instrument_type = dscd.instrument_type

    # create dimensios
    y_dim = dscd.indexes.row_max-dscd.indexes.row_min
    x_dim = dscd.indexes.col_max - dscd.indexes.col_min
    dataset.createDimension('cropped_y', y_dim)
    dataset.createDimension('cropped_x', x_dim)

    # create x
    new_x = dataset.createVariable('x', dscd.x.dtype, ('cropped_x',), zlib=True)
    new_x.standard_name = 'projection_x_coordinate'
    new_x.long_name = 'GOES fixed grid projection x-coordinate'
    new_x.comments = 'Vector x of the cropping area'
    new_x.units = 'rad'
    new_x.axis = 'X'
    new_x[:] = dscd.x[:]

    # create y
    new_y = dataset.createVariable('y', dscd.y.dtype, ('cropped_y',), zlib=True)
    new_y.standard_name = 'projection_y_coordinate'
    new_y.long_name = 'GOES fixed grid projection y-coordinate'
    new_y.comments = 'Vector y of the cropping area'
    new_y.units = 'rad'
    new_y.axis = 'Y'
    new_y[:] = dscd.y[:]


def write_clipping_to_info_dataset(dataset: netCDF4.Dataset, dscd: DatasetClippingInfo):
    _write_clipping_to_any_dataset(dataset, dscd)

    sat_lon = dscd.goes_imager_projection.longitude_of_projection_origin

    if int(sat_lon) == -89:
        dataset.summary = f'This file contains the latitude - longitude grids, corresponding to the period between 07/10/2017 and 11/30/2017, where GOES16 ' \
                          f'was in the position 89.3 degrees west. The grid was cropped within the area of South America delimited approximately ' \
                          f'by latitude {dscd.region.lat_north}°N and {-dscd.region.lat_south}°S; longitude {-dscd.region.lon_west}°W and {-dscd.region.lon_east}°W.'
    elif int(sat_lon) == -75:
        dataset.summary = f'This file contains the latitude - longitude grids, corresponding from 12/14/2017 where GOES-16 ' \
                          f'reached 75.2 degrees west on December 11, 2017 and data flow resumed to users on December 14. ' \
                          f'The grid was cropped within the area of South America delimited approximately ' \
                          f'by latitude {dscd.region.lat_north}°N and {-dscd.region.lat_south}°S; longitude {-dscd.region.lon_west}°W and {-dscd.region.lon_east}°W.'

    # create latitude axis
    new_lats = dataset.createVariable('lats', dscd.lats.dtype, ('cropped_y', 'cropped_x'), zlib=True)
    new_lats.standard_name = 'latitude'
    new_lats.long_name = 'latitude'
    new_lats.units = 'degrees_north'
    new_lats.axis = 'Y'
    new_lats[:,:] = dscd.lats[:,:]

    # create longitude axis
    new_lons = dataset.createVariable('lons', dscd.lons.dtype, ('cropped_y', 'cropped_x'), zlib=True)
    new_lons.standard_name = 'longitude'
    new_lons.long_name = 'longitude'
    new_lons.units = 'degrees_east'
    new_lons.axis = 'X'
    new_lons[:,:] = dscd.lons[:,:]


def write_clipping_to_dataset(dataset: netCDF4.Dataset, dscd: DatasetClippingInfo):
    _write_clipping_to_any_dataset(dataset, dscd)


def fill_clipped_variable_from_source(clipped_dataset: netCDF4.Dataset,
                                      source_dataset: netCDF4.Dataset,
                                      comments: str,
                                      variable_name: str="CMI"):
    source_variable = source_dataset.variables[variable_name]
    cmi = clipped_dataset.createVariable(variable_name, source_variable.datatype, ('cropped_y', 'cropped_x'))
    cmi_attr = {k: source_variable.getncattr(k) for k in source_variable.ncattrs() if k[0] != '_'}
    cmi_attr['comments'] = comments
    cmi.setncatts(cmi_attr)

    clipped_dataset.time_coverage_start = source_dataset.time_coverage_start
    clipped_dataset.time_coverage_end = source_dataset.time_coverage_end
    copy_variable(source_dataset.variables['goes_imager_projection'], clipped_dataset)

    clipped_dataset.variables[variable_name][:, :] = source_variable[
                                             clipped_dataset.row_min:clipped_dataset.row_max,
                                             clipped_dataset.col_min:clipped_dataset.col_max]


def copy_variable(variable, dest_dataset):
    if not variable.name in dest_dataset.variables:
        dest_dataset.createVariable(variable.name, variable.datatype, variable.dimensions)
    dest_dataset[variable.name][:] = variable[:]
    dest_dataset[variable.name].setncatts(variable.__dict__)


def nearest_indexes(lat, lon, lats, lons, major_order):
    # d_lat = lat - lats
    # d_lon = lon - lons
    # distance = d_lat * d_lat + d_lon * d_lon
    distance = np.abs(lats - lat) + np.abs(lons - lon)
    return np.where(distance == distance.min())


def find_indexes(region: LatLonRegion, lats, lons, major_order) -> RegionIndexes:
    lats_aux = np.nan_to_num(lats, posinf=1.7976931348623157e+10, copy=True)
    # lats_aux = lats[:,:]
    # lats_aux[lats_aux == np.inf] = 999
    lons_aux = np.nan_to_num(lons, posinf=1.7976931348623157e+10, copy=True)
    # lons_aux = lons[:,:]
    # lons_aux[lons_aux == np.inf] = 999
    nw_lat, nw_lon = nearest_indexes(region.lat_north, region.lon_west, lats_aux, lons_aux, major_order)
    ne_lat, ne_lon = nearest_indexes(region.lat_north, region.lon_east, lats_aux, lons_aux, major_order)
    sw_lat, sw_lon = nearest_indexes(region.lat_south, region.lon_west, lats_aux, lons_aux, major_order)
    se_lat, se_lon = nearest_indexes(region.lat_south, region.lon_east, lats_aux, lons_aux, major_order)

    print(nw_lat, nw_lon)
    print(ne_lat, ne_lon)
    print(sw_lat, sw_lon)
    print(se_lat, se_lon)

    indexes = RegionIndexes()

    indexes.col_min = int(min(nw_lon, ne_lon, sw_lon, se_lon))
    indexes.col_max = int(max(nw_lon, ne_lon, sw_lon, se_lon))
    indexes.row_min = int(min(nw_lat, ne_lat, sw_lat, se_lat))
    indexes.row_max = int(max(nw_lat, ne_lat, sw_lat, se_lat))

    return indexes


def get_lats_lons_x_y(dataset, indexes: RegionIndexes = None):
    imager_projection = dataset.variables['goes_imager_projection']
    sat_height = imager_projection.perspective_point_height
    sat_lon = imager_projection.longitude_of_projection_origin
    sat_sweep = imager_projection.sweep_angle_axis
    if indexes is None:
        source_x = dataset['x'][:]
        source_y = dataset['y'][:]
    else:
        source_x = dataset['x'][indexes.col_min: indexes.col_max]
        source_y = dataset['y'][indexes.row_min: indexes.row_max]
    x = source_x * sat_height
    y = source_y * sat_height
    XX, YY = np.meshgrid(np.array(x), np.array(y))
    projection = pyproj.Proj(proj='geos', h=sat_height, lon_0=sat_lon, sweep=sat_sweep)
    lons, lats = projection(XX, YY, inverse=True)
    return np.array(lats), np.array(lons), source_x, source_y
