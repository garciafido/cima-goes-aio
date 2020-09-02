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
    dataset = get_dataset(product_band)
    sat_lon = dataset['goes_imager_projection'].longitude_of_projection_origin
    filename = get_region_data_filename(product_band, sat_lon)
    info_dataset = netCDF4.Dataset(filename)
    try:
       save_SA_netcdf(info_dataset, dataset)
    finally:
       info_dataset.close()


if __name__ == "__main__":
    run()
