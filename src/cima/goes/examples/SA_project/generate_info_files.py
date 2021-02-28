import netCDF4
import numpy as np
from cima.goes.products import ProductBand, Product, Band
from cima.goes.datasets import LatLonRegion, generate_info_files, RegionIndexes, get_clipping_info_from_info_dataset
#PROXY=None
PROXY="http://proxy.fcen.uba.ar:8080"


def run():
    visible = ProductBand(product=Product.CMIPF, band=Band.RED)
    ir = ProductBand(product=Product.CMIPF, band=Band.CLEAN_LONGWAVE_WINDOW)
    SA_region = LatLonRegion(
        lat_south=-53.9,
        lat_north=15.7,
        lon_west=-81.4,
        lon_east=-34.7 + 3
    )
    filenames = generate_info_files([visible, ir], SA_region, filename_prefix='SA-CMIPF')
    for filename in filenames:
        info_dataset = netCDF4.Dataset(filename)
        clipping_info = get_clipping_info_from_info_dataset(info_dataset)
        lats = clipping_info.lats
        lons = clipping_info.lons
        print(filename)
        print(f'LAT min: {np.min(lats)} max: {np.nanmax(lats[lats != np.inf])}')
        print(f'LON min: {np.min(lons)} max: {np.nanmax(lons[lons != np.inf])}')

        # ./SA-CMIPF-0.5km-89W.nc
        # LAT min: -59.917279272098725 max: 16.869360262832487
        # LON min: -85.09372970164812 max: -9.76217874418214
        # ./SA-CMIPF-0.5km-75W.nc
        # LAT min: -59.917279272098725 max: 16.349399559336227
        # LON min: -86.30504246561742 max: 2.471384238775894
        # ./SA-CMIPF-2km-89W.nc
        # LAT min: -59.78291949258362 max: 16.881914324858812
        # LON min: -85.09146755883681 max: -10.024768564673654
        # ./SA-CMIPF-2km-75W.nc
        # LAT min: -59.78291949258362 max: 16.346068778979404
        # LON min: -86.29290671299826 max: 2.3402375491461775

        ### with lon_east=-34.7 + 3
        # ./SA-CMIPF-0.5km-89W.nc
        # LAT min: -59.917279272098725 max: 16.998367958187625
        # LON min: -85.09372970164812 max: -9.513564818275833
        # ./SA-CMIPF-0.5km-75W.nc
        # LAT min: -59.917279272098725 max: 16.448415238925538
        # LON min: -86.30504246561742 max: 3.106753710760662
        # ./SA-CMIPF-2km-89W.nc
        # LAT min: -59.78291949258362 max: 17.00932918030755
        # LON min: -85.09146755883681 max: -9.703562895407751
        # ./SA-CMIPF-2km-75W.nc
        # LAT min: -59.78291949258362 max: 16.445470148036414
        # LON min: -86.29290671299826 max: 2.8716513461833535

if __name__ == "__main__":
    run()
