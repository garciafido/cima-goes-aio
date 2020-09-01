import datetime
import os
import urllib
from dataclasses import dataclass
from enum import IntEnum, Enum, unique
import re

# File neme pattern:
# OR_ABI-L2–CMIPF–M3C09_G16_sYYYYJJJHHMMSSs_eYYYYJJJHHMMSSs_cYYYYJJJHHMMSSs.nc
# Where:
# OR: Operational System Real-Time Data
# ABI-L2: Advanced Baseline Imager Level 2+
# CMIPF: Cloud and Moisture Image Product – Full Disk
# M3 / M4: ABI Mode 3 or ABI Mode 4
# C09: Channel Number (Band 9 in this example)
# G16: GOES-16
# sYYYYJJJHHMMSSs: Observation Start
# eYYYYJJJHHMMSSs: Observation End
# cYYYYJJJHHMMSSs: File Creation
#
# http://edc.occ-data.org/goes16/getdata/


@unique
class Product(Enum):
    def __new__(cls, value, doc=None):
        self = object.__new__(cls)  # calling super().__new__(value) here would fail
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    ACMF = 'ABI-L2-ACMF', 'Clear Sky Masks'
    ACHAF = 'ABI-L2-ACHAF', 'Cloud Top Height',
    ACHTF = 'ABI-L2-ACHTF', 'Cloud Top Temperature',
    ACTPF = 'ABI-L2-ACTPF', 'Cloud Top Phase',
    ADPF = 'ABI-L2-ADPF', 'Aerosol Detection (including Smoke and Dust)',
    AODF = 'ABI-L2-AODF', 'Aerosol Optical Depth',
    CMIPF = 'ABI-L2-CMIPF', 'Cloud and Moisture Image Product – Full Disk',
    MCMIPF = 'ABI-L2-MCMIPF', 'ABI-L2-MCMIPF',
    MCMIPC = 'ABI-L2-MCMIPC', 'ABI-L2-MCMIPC',
    CODF = 'ABI-L2-CODF', 'Cloud Optical Depth',
    CPSF = 'ABI-L2-CPSF', 'Cloud Particle Size Distribution',
    DSIF = 'ABI-L2-DSIF', 'Derived Stability Indices',
    DMWF = 'ABI-L2-DMWF', 'Derived Motion Winds',
    FDCF = 'ABI-L2-FDCF', 'Fire / Hot Spot Characterization',
    FSCF = 'ABI-L2-FSCF', 'Snow Cover',
    LSTF = 'ABI-L2-LSTF', 'Land Surface Temperature (Skin)',
    RRQPEF = 'ABI-L2-RRQPEF', 'Rainfall Rate /QPE',
    SSTF = 'ABI-L2-SSTF', 'Sea Surface Temperature (Skin)',
    TPWF = 'ABI-L2-TPWF', 'Total Precipitable Water',
    VAAF = 'ABI-L2-VAAF', 'Volcanic Ash',
    LCFA = 'GLM-L2-LCFA', 'GLM-L2-LCFA'
    # Conus
    CMIPC = 'ABI-L2-CMIPC', 'Cloud & Moisture Imagery. Conus. 5 Minutes'
    RadC = 'ABI-L1b-RadC', 'Radiances. Conus. 5 Minutes'
    # Full disk
    RadF = 'ABI-L1b-RadF', 'Radiances. Full disk. 15 Minutes'
    # Mesoscale
    CMIPM = 'ABI-L2-CMIPM',  'Cloud & Moisture Imagery. Mesoscale. 30-60 seconds'
    MCMIPM = 'ABI-L2-MCMIPM',  'Multi-Band Cloud & Moisture Imagery. Mesoscale.  30-60 seconds'
    RadM = 'ABI-L1b-RadM',  'Radiances. Mesoscale. 30-60 seconds'


@unique
class Band(IntEnum):
    def __new__(cls, value, doc=None):
        self = int.__new__(cls, value)  # calling super().__new__(value) here would fail
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    BLUE = 1, '1: Blue'
    RED = 2, '2: Red'
    VEGGIE = 3, '3: Veggie'
    CIRRUS = 4, '4: Cirrus'
    SNOW_ICE = 5, '5: Snow ice'
    CLOUD_PARTICLE_SIZE = 6, '6: Cloud particle size'
    SHORTWAVE_WINDOW = 7, '7: Shortwave window'
    UPPER_LEVEL_TROPOSPHERIC_WATER_VAPOR = 8, '8: Upper level tropospheric water vapor'
    MID_LEVEL_TROPOSPHERIC_WATER_VAPOR = 9, '9: Mid level tropospheric water vapor'
    LOWER_LEVEL_WATER_VAPOR = 10, '10: Lower level water vapor'
    CLOUD_TOP_PHASE = 11, '11: Cloud top phase'
    OZONE = 12, '12: Ozone'
    CLEAN_LONGWAVE_WINDOW = 13, '13: Clean longwave window'
    IR_LONGWAVE_WINDOW = 14, '14: IR longwave window'
    DIRTY_LONGWAVE_WINDOW = 15, '15: Dirty longwave window'
    CO2_LONGWAVE_INFRARED = 16, '16: CO2 longwave infrared'


@dataclass
class ProductBand:
    product: Product
    band: Band = None
    subproduct: int = None


OR = 'OR' # Operational System Real-Time Data
G16 = 'G16' # GOES-16
ANY_MODE = 'M.'


def get_day_of_year(year, month, day):
    return datetime.datetime(year=year, month=month, day=day).timetuple().tm_yday


def path_prefix(product: Product, year: int, month: int, day: int, hour: int=None):
    day_of_year = get_day_of_year(year, month, day)
    if hour is None:
        return f'{product.value}/{year:04d}/{day_of_year:03d}/'
    else:
        return f'{product.value}/{year:04d}/{day_of_year:03d}/{hour:02d}/'


def file_name(band: Band, product=Product.CMIPF, mode=ANY_MODE, subproduct: int = None):
    subp = subproduct if subproduct is not None else ''
    band_str = f'C{band:02d}' if band is not None else ''
    return f'{OR}_{product.value}{subp}-{mode}{band_str}_{G16}'


def filename_from_media_link(media_link: str):
    return urllib.parse.unquote(os.path.basename(os.path.normpath(media_link)).split('?')[0])


def hour_file_name(hour: int, band: Band, product=Product.CMIPF, mode=ANY_MODE, subproduct: int = None):
    subp = subproduct if subproduct is not None else ''
    band_str = f'C{band:02d}' if band is not None else ''
    return f'{hour:02d}/{OR}_{product.value}{subp}-{mode}{band_str}_{G16}'


def file_regex_pattern(band: Band, product: Product = Product.CMIPF, mode: str = ANY_MODE, subproduct: int = None):
    return re.compile(file_name(band, product, mode, subproduct=subproduct))


def hour_file_regex_pattern(hour: int, band: Band, product: Product = Product.CMIPF, mode: str = ANY_MODE, subproduct: int = None):
    return re.compile(hour_file_name(hour, band, product, mode, subproduct=subproduct))


def slice_obs_start(product=Product.CMIPF, subproduct: int = None):
    prefix_pos = len(path_prefix(year=1111, month=1, day=1, hour=11, product=product)) + len(
        file_name(band=Band.RED, product=product, subproduct=subproduct)) + 2
    return slice(prefix_pos, prefix_pos + len('20183650045364'))


# Browse: https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16
GOES_PUBLIC_BUCKET = 'gcp-public-data-goes-16'


def get_gcs_url(filepath: str):
    return f'https://storage.cloud.google.com/{GOES_PUBLIC_BUCKET}/{filepath}'


def get_browse_url(filepath: str):
    parts = filepath.split('_')
    product = '-'.join(parts[1].split('-')[:-1])
    year = parts[3][1:5]
    day_of_year = parts[3][6:9]
    hour = parts[3][10:12]
    return f'https://storage.cloud.google.com/{GOES_PUBLIC_BUCKET}/{product}/{year}/{day_of_year}/{hour}/{filepath}'
