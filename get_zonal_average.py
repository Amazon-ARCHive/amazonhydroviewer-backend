import regionmask
import geopandas as gpd
import xarray as xr
import os
import pandas as pd

# Read gridded data files and find coordinates variable
def get_standard_coordinates(dataset: xr.Dataset, lon_names=None, lat_names=None, time_names=None):
    """
    Retrieve longitude, latitude, and time variables from an xarray dataset,
    accommodating different naming conventions.

    Parameters:
        dataset (xr.Dataset): The xarray dataset to search.
        lon_names (list): List of possible names for longitude (default: common names).
        lat_names (list): List of possible names for latitude (default: common names).
        time_names (list): List of possible names for time (default: common names).

    Returns:
        tuple: Longitude, latitude, and time variables.

    Raises:
        AttributeError: If any of the required variables are not found.
    """

    lon_names = lon_names or ["east_west", "lon", "longitude"]
    lat_names = lat_names or ["north_south", "lat", "latitude"]
    time_names = time_names or ["time"]

    def find_variable(dataset, possible_names):
        for name in possible_names:
            if name in dataset.variables:
                return dataset[name]
        raise AttributeError(f"None of the variable names {possible_names} found in the dataset.")

    # Try to find longitude, latitude, and time variables
    lon = find_variable(dataset, lon_names)
    lat = find_variable(dataset, lat_names)
    time = find_variable(dataset, time_names)

    return lon, lat, time

# Paths
geodataframe_path = "https://raw.githubusercontent.com/blackteacatsu/spring_2024_envs_research_amazon_ldas/main/resources/hybas_sa_lev05_areaofstudy.geojson"

base_dir = os.path.dirname(os.path.abspath(__file__))
nc_files = [
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_Evap_tavg_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_Qair_f_tavg_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_Qs_tavg_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_Rainf_tavg_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilMoist_inst_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilMoist_inst_lvl_1.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilMoist_inst_lvl_2.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilMoist_inst_lvl_3.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilTemp_inst_lvl_0.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilTemp_inst_lvl_1.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilTemp_inst_lvl_2.nc'),
    os.path.join(base_dir, 'get_ldas_raw_forecast', 'ldas_fcst_2024_dec01_SoilTemp_inst_lvl_3.nc')
]
output_dir = "get_zonal_averages_csv"
os.makedirs(output_dir, exist_ok=True)

# Load geodataframe and get all PFAF_IDs
geodataframe = gpd.read_file(geodataframe_path)
pfaf_ids = geodataframe.PFAF_ID.unique()



# For each variable, process and append to per-region CSVs
for nc_file in nc_files:
    ds = xr.open_dataset(nc_file, engine='netcdf4')
    lon, lat, time = get_standard_coordinates(ds)
    variable = list(ds.data_vars)[0]

    for pfaf_id in pfaf_ids:
        aoi = geodataframe[geodataframe.PFAF_ID == pfaf_id]
        if aoi.empty:
            continue
        aoi_mask = regionmask.mask_3D_geopandas(aoi, lon, lat)
        aoi_ds = ds[variable].where(aoi_mask)

        # Zonal stats
        if variable == 'Streamflow_tavg':
            summary = aoi_ds.groupby("time").max(["lat", "lon"])
        else:
            summary = aoi_ds.groupby("time").mean(["lat", "lon"])

        df = summary.to_dataframe().reset_index()
        df = df[['time', variable]]
        df['time'] = df['time'].astype(str)

        out_csv = os.path.join(output_dir, f"pfaf_{pfaf_id}.csv")
        # If file exists, merge on 'time', else create new
        if os.path.exists(out_csv):
            prev = pd.read_csv(out_csv)
            merged = pd.merge(prev, df, on='time', how='outer')
            merged['PFAF_ID'] = pfaf_id
            merged.to_csv(out_csv, index=False)
        else:
            df['PFAF_ID'] = pfaf_id
            df.to_csv(out_csv, index=False)
        print(f"Saved: {out_csv}")
    ds.close()