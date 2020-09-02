import netCDF4
import pyproj
import numpy as np
from dataclasses import dataclass


old_sat_lon = -89
actual_sat_lon = -75
default_major_order = FORTRAN_ORDER = 'F'


@dataclass
class LatLonRegion:
    lat_north: float
    lat_south: float
    lon_west: float
    lon_east: float


@dataclass
class RegionIndexes:
    x_min: int = None
    x_max: int = None
    y_min: int = None
    y_max: int = None


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


def copy_variable(variable, dest_dataset):
    dest_dataset.createVariable(variable.name, variable.datatype, variable.dimensions)
    dest_dataset[variable.name][:] = variable[:]
    dest_dataset[variable.name].setncatts(variable.__dict__)


def nearest_indexes(lat, lon, lats, lons, major_order):
    distance = (lat - lats) * (lat - lats) + (lon - lons) * (lon - lons)
    return np.unravel_index(np.argmin(distance), lats.shape, major_order)


def find_indexes(region: LatLonRegion, lats, lons, major_order) -> RegionIndexes:
    x1, y1 = nearest_indexes(region.lat_north, region.lon_west, lats, lons, major_order)
    x2, y2 = nearest_indexes(region.lat_north, region.lon_east, lats, lons, major_order)
    x3, y3 = nearest_indexes(region.lat_south, region.lon_west, lats, lons, major_order)
    x4, y4 = nearest_indexes(region.lat_south, region.lon_east, lats, lons, major_order)

    indexes = RegionIndexes()
    indexes.x_min = int(min(x1, x2, x3, x4))
    indexes.x_max = int(max(x1, x2, x3, x4))
    indexes.y_min = int(min(y1, y2, y3, y4))
    indexes.y_max = int(max(y1, y2, y3, y4))
    return indexes


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
        lats=lats[indexes.y_min: indexes.y_max, indexes.x_min: indexes.x_max],
        lons=lons[indexes.y_min: indexes.y_max, indexes.x_min: indexes.x_max],
        x=x[indexes.x_min: indexes.x_max],
        y=x[indexes.y_min: indexes.y_max]
    )


def get_spatial_resolution(dataset: netCDF4.Dataset) -> float:
    return float(dataset.spatial_resolution[:dataset.spatial_resolution.find("km")])


def write_clipping_info_to_dataset(dataset: netCDF4.Dataset, dscd: DatasetClippingInfo):
    copy_variable(dscd.goes_imager_projection, dataset)
    dataset.col_min = np.short(dscd.indexes.x_min)
    dataset.col_max = np.short(dscd.indexes.x_max)
    dataset.row_min = np.short(dscd.indexes.y_min)
    dataset.row_max = np.short(dscd.indexes.y_max)

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

    dataset.spatial_resolution = dscd.spatial_resolution
    dataset.orbital_slot = dscd.orbital_slot
    dataset.instrument_type = dscd.instrument_type

    # create dimensios
    y_dim = dscd.indexes.y_max-dscd.indexes.y_min
    x_dim = dscd.indexes.x_max-dscd.indexes.x_min
    dataset.createDimension('cropped_y', y_dim)
    dataset.createDimension('cropped_x', x_dim)

    # create latitude axis
    new_lats = dataset.createVariable('lats', dscd.lats.dtype, ('cropped_y', 'cropped_x'), zlib=True)
    new_lats.standard_name = 'latitude'
    new_lats.long_name = 'latitude'
    new_lats.units = 'degrees_north'
    new_lats.axis = 'Y'
    new_lats[:,:] = dscd.lats

    # create longitude axis
    new_lons = dataset.createVariable('lons', dscd.lons.dtype, ('cropped_y', 'cropped_x'), zlib=True)
    new_lons.standard_name = 'longitude'
    new_lons.long_name = 'longitude'
    new_lons.units = 'degrees_east'
    new_lons.axis = 'X'
    new_lons[:,:] = dscd.lons

    # create x
    new_x = dataset.createVariable('x', dscd.x.dtype, ('cropped_x',), zlib=True)
    new_x.standard_name = 'projection_x_coordinate'
    new_x.long_name = 'GOES fixed grid projection x-coordinate'
    new_x.comments = 'Vector x of the cropping area'
    new_x.units = 'rad'
    new_x.axis = 'X'
    new_x[:] = dscd.x

    # create y
    new_y = dataset.createVariable('y', dscd.y.dtype, ('cropped_y',), zlib=True)
    new_y.standard_name = 'projection_y_coordinate'
    new_y.long_name = 'GOES fixed grid projection y-coordinate'
    new_y.comments = 'Vector y of the cropping area'
    new_y.units = 'rad'
    new_y.axis = 'Y'
    new_y[:] = dscd.y


def get_lats_lons_x_y(dataset, indexes: RegionIndexes = None):
    imager_projection = dataset.variables['goes_imager_projection']
    sat_height = imager_projection.perspective_point_height
    sat_lon = imager_projection.longitude_of_projection_origin
    sat_sweep = imager_projection.sweep_angle_axis
    if indexes is None:
        source_x = dataset['x'][:]
        source_y = dataset['y'][:]
    else:
        source_x = dataset['x'][indexes.x_min: indexes.x_max]
        source_y = dataset['y'][indexes.y_min: indexes.y_max]
    x = source_x * sat_height
    y = source_y * sat_height
    XX, YY = np.meshgrid(np.array(x), np.array(y))
    projection = pyproj.Proj(proj='geos', h=sat_height, lon_0=sat_lon, sweep=sat_sweep)
    lons, lats = projection(XX, YY, inverse=True)
    return np.array(lats), np.array(lons), source_x, source_y
