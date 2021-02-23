from cima.goes.products import ProductBand, Product, Band
from cima.goes.datasets import LatLonRegion, generate_info_files


def run():
    product_band = ProductBand(product=Product.CMIPF, band=Band.RED)
    SA_region = LatLonRegion(
        lat_south=-53.9,
        lat_north=15.7,
        lon_west=-81.4,
        lon_east=-34.7
    )
    generate_info_files([product_band], SA_region, filename_prefix='./SA-')


if __name__ == "__main__":
    run()
