#!/usr/bin/env python
# coding: utf-8

# # Master Workflow Script
# 
# **Description:** this is the master workflow script, run the following cells to initialize data for visualization.

# ## Step 0 Setup Directories 

# In[1]:


from modules.subsampler import *
import modules.get_zonal_stats as zonal
import modules.get_probabilistic_forecast as prob
import json
from pathlib import Path
from tqdm import tqdm
import os
import gc
import xarray as xr
import regionmask
import geopandas as gpd
import pandas as pd
import traceback
from git import Repo

# In[2]:


# Variable definitions
list_of_variables = {
    'Rainf_tavg': 'Average precipitation', 
    'Qair_f_tavg': 'Specific humidity',
    'Qs_tavg': 'Surface runoff',
    'Evap_tavg': 'Evapotranspiration',
    'Tair_f_tavg': 'Avg. air temperature',
    'SoilMoist_inst': 'Soil moisture',
    'SoilTemp_inst': 'Soil temperature',
    'Streamflow_tavg': 'Stream flow'
}

# Data directory
surface_model_file_path = r"/mnt/vast/prakrut/backup/lis_runs/malaria_amazon/forecast/monthly" # Input location on group server
repo = Repo(os.getcwd())


# Find forecast and hindcast files
try: 
    forecast_file, hindcast_files, f_dt = prob.split_forecast_and_hindcasts(surface_model_file_path)
    initialization_date = f"{f_dt.year}_{f_dt.strftime('%b').lower()}" # create initialization date tag

    print("Found latest forecast file:", forecast_file)
    print("Hindcasts   :", len(hindcast_files), "files")
    print("Forecast initialization date:", initialization_date)

    # Manage working directories/repo:
    
    # Create output directories
    prob_output_dir = Path('./get_ldas_probabilistic_output')
    prob_output_dir.mkdir(exist_ok=True, parents=True)

    # Create output directories for cached .zarr files
    prob_output_cache = prob_output_dir / 'tmp'
    prob_output_cache.mkdir(exist_ok=True, parents=True)
    prob.purge_old_init(prob_output_cache, current_init=initialization_date) # remove tmp files from last month

    # Create output directories for subsampled forecast files
    subsampled_output_dir = prob_output_dir / 'subsampled'
    subsampled_output_dir.mkdir(exist_ok=True, parents=True)
    # prob.purge_old_init(subsampled_output_dir, current_init=initialization_date)

    obs_ = []
    for f in subsampled_output_dir.glob('*'):
        if f.name.endswith('.json'):
            continue
        if initialization_date not in f.name:
            obs_.append(f)
    if len(obs_) > 0:
        repo.index.remove(obs_, r=True) # stage the removal in git

    ###
    ### FIND ZONAL AVG. OF FORECAST
    ###

    # Create output directories for zonal avg. forecast
    zonal_averages_forecast = Path("./get_zonal_averages_forecast")
    zonal_averages_forecast.mkdir(exist_ok=True, parents=True)

    ###
    ### FIND ZONAL CLIMATOLOGY
    ###

    # Create output directories for climatology
    zonal_averages_climatology = Path("./get_zonal_averages_climatology/")
    zonal_averages_climatology.mkdir(exist_ok=True, parents=True)

    # Create output directories for cachaed .nc climatology
    climatology_cache_zarr = zonal_averages_climatology / 'tmp'
    climatology_cache_zarr.mkdir(exist_ok=True, parents=True)
    prob.purge_old_init(climatology_cache_zarr, current_init=initialization_date)

    # Create output directories for zonal avg. [climatology]
    zonal_climatology_tab = zonal_averages_climatology / 'zmean'
    zonal_climatology_tab.mkdir(exist_ok=True, parents=True)

    print(f"\n Output directory: {prob_output_dir}")
    print(f"Subsampled directory: {subsampled_output_dir} \n")

except Exception as e :
    print(f"{type(e).__name__}: {e}")
    traceback.print_exc()

# ## Step 1 Generate Probabilistic Forecast Data Using Hindcast


# ### Main processing loop

# Process each variable
for variable, variable_longname in tqdm(list_of_variables.items()):  # Fixed: .items()
    print(f"\n{'='*60}")
    print(f"{variable_longname} ({variable})")
    print('='*60)

    try:
        # Load data
        print("Loading hindcast data...")
        hindcast = prob.read_trim_hindcast(hindcast_files, variable)
        print(f"  Shape: {hindcast.shape}")

        print("Loading forecast data...")
        forecast = prob.read_trim_forecast(forecast_file, variable)
        print(f"  Shape: {forecast.shape}")

        # Calculate probabilities (convert to percentages)
        print("Calculating tercile probabilities...")
        probs = prob.calculate_probabilities(hindcast, forecast) * 100
        print(f"\nProbability data shape: {probs.shape}")
        print(f"Dimensions: {probs.dims} Categories: {probs.category.values}")
        #print(f"Time steps: {len(probs.time)}")

        # Keep only maximum probability per category
        print("Filtering for maximum probabilities...")
        probs_with_nan = probs.where(probs == probs.max(dim='category'))

        # Output file path base
        output_file = prob_output_cache / f'{initialization_date}_tercile_prob_max_{variable}'
	    
        # Avoid zarr file overwriting error
        if output_file.exists():
            prob.purge_dirct(output_file)

        if variable == 'Streamflow_tavg': # Extract river network
            river_mask_file = Path(f'./static/annual_mean_50cumecs_river_network.nc') # Read precalculated river mask file
            if river_mask_file.exists():
                river_network_ds = xr.open_dataset(river_mask_file)
                river_mask = river_network_ds['mask']
                print(f"\n{'='*60}")
                print("Loaded river mask: \n")
                print(river_mask)
                print(f"\n{'='*60}")
            else: 
                print(f"File not found: {river_mask_file}")
            probs_with_nan = probs_with_nan.where(river_mask)
            # Non-soil variables: save as level 0
            #output_file = f'{file_base}'
            probs_with_nan.to_zarr(output_file, zarr_format = 2)
            print(f"  ✓ Saved → {Path(output_file).name}")

        else:
            # Non-soil variables: save as level 0
            #output_file = f'{file_base}'
            probs_with_nan.to_zarr(output_file, zarr_format = 2)
            print(f"  ✓ Saved → {Path(output_file).name}")

        print(f"\n✓ Completed {variable}")

    except Exception as e:
        print(f"\n✗ ERROR processing {variable}:")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        continue

    finally:
        # Clean up memory
        print("Cleaning up memory...")
        try:
            del hindcast, forecast, probs, probs_with_nan
        except:
            pass
        gc.collect()

print("\n" + "="*60)
print("✓ All variables processed!")
print("="*60)


# ## Step 2 Apply Sub-sampler for Web Use

# ### Setup Directories and boundaries

# In[4]:

# Data bounds for the region
data_bounds = {'lon_min': -81.975, 
               'lon_max': -49.025, 
               'lat_min': -20.975, 
               'lat_max': 5.975}

# Get all probability netCDF files from the cache directory
prob_cache_files = list(prob_output_cache.glob('*_tercile_prob_*'))
print(f"Found {len(prob_cache_files)} files to process\n")

index = {
    "initialization_date": f'{initialization_date}'
}

index_path = subsampled_output_dir / "index.json"
with open(index_path, "w") as f:
    json.dump(index, f, indent=2)

print(f"✓ Wrote index.json → {index_path}")


# ### Sub-sampling loop

# In[5]:
for prob_cache_file in tqdm(prob_cache_files):
    print(f"\n{'='*60}")
    print(f"Processing: {prob_cache_file.name}")
    print('='*60)

    try:
        # Load the tmp data
        ds = xr.open_dataarray(prob_cache_file, engine='zarr')
        ds = ds.load()
        print(f"  Shape: {ds.shape}")
        print(f"  Dims: {ds.dims}")

        # Create subsampler and generate pyramid
        subsampled = HydroViewerSubsampler(ds, resolution=256)
        pyramid, grain_map = subsampled.generate_pyramid(zooms=[4, 5, 6, 7, 8, 9])

        out_dir = save_pyramid_npz(subsampled_output_dir, 
                                   prob_cache_file, 
                                   pyramid, 
                                   grain_map, 
                                   data_bounds)


        print(f"\n  ✓ Saved → {out_dir}")

    except Exception as e:
        print(f"\n  ✗ ERROR: {type(e).__name__}: {e}")
        continue

print(f"\n{'='*60}")
print("✓ All files processed!")
print(f"Output directory: {subsampled_output_dir}")
print('='*60)


# ## Step 3 Get Zonal Statics for boxplot

# In[6]:


# Load geodataframe and get all PFAF_IDs
geodataframe_path = '''
https://raw.githubusercontent.com/blackteacatsu/\
spring_2024_envs_research_amazon_ldas/\
main/resources/hybas_sa_lev05_areaofstudy.geojson
'''

geodataframe = gpd.read_file(geodataframe_path)

pfaf_ids_aoi = geodataframe.PFAF_ID.unique()


# ### Step 3.1 Forecast zonal averages (forecast-specific treatment)

# In[7]:


# Build region mask once from forecast grid
forecast_ds = xr.open_dataset(forecast_file)
lon, lat, time = zonal.get_standard_coordinates(forecast_ds)
#mask_3d = zonal.build_region_mask_3d(geodataframe, lon, lat)

for pfaf_id in tqdm(pfaf_ids_aoi): # Iterate over each region [by PFAF_ID]
    #print(f'Processing PFAF_ID: {pfaf_id}') 
    aoi = geodataframe[geodataframe.PFAF_ID == pfaf_id]

    if aoi.empty:
        continue
    aoi_mask = regionmask.mask_3D_geopandas(aoi, lon, lat) # Create AOI mask

    records_forecast = [] # Initialize records_forecast list
    
    # Iterate over time and ensemble dimensions
    for t in time.values:
        for ens in forecast_ds['ensemble'].values if 'ensemble' in forecast_ds.dims else [None]:
            row = {'time': pd.Timestamp(t).isoformat(), 'ensemble': ens, 'pfaf_id': pfaf_id} # Initialize row with time, ensemble, and PFAF_ID
            for var in list_of_variables.keys(): # Iterate over each variable

                # Check if variable is SoilMoist or SoilTemp
                # then var has more than one depth lvl.
                if var in ['SoilMoist_inst', 'SoilTemp_inst']: 
                    profile_dim = [d for d in forecast_ds[var].dims if 'profile' in d.lower()]
                    if profile_dim:
                        p_dim = profile_dim[0]
                        for level_idx  in range(forecast_ds.sizes[p_dim]):
                            col = f'{var}_lvl_{level_idx}' # Create column name for soil moisture levels
                            data = forecast_ds[var].sel({'time': t, p_dim : level_idx})
                            if 'ensemble' in data.dims and ens is not None:
                                data = data.sel(ensemble=ens)
                            masked = data.where(aoi_mask)
                            row[col] = masked.mean(dim=['lat','lon'], skipna=True).item()
                    else:
                        row[col] = None
                else:
                    if var in forecast_ds.variables:
                        data = forecast_ds[var].sel(time=t)
                        if 'ensemble' in data.dims and ens is not None:
                            data = data.sel(ensemble=ens)
                        masked = data.where(aoi_mask)
                        if var == 'Streamflow_tavg':
                            row[var] = masked.max(dim=['lat','lon'], skipna=True).item()
                        else:
                            row[var] = masked.mean(dim=['lat','lon'], skipna=True).item()
                    else:
                        row[var] = None
            records_forecast.append(row)
    df = pd.DataFrame(records_forecast)
    out_csv = os.path.join(zonal_averages_forecast, f"zonal_forecast_pfaf_{pfaf_id}.csv")
    df.to_csv(out_csv, index=False)
    #print(f"Saved: {out_csv}")


# ### Step 3.2 Hindcast climatology zonal averages (incremental per variable)

# In[ ]:


for variable in tqdm(list_of_variables.keys()):
    climatology = zonal.initialize_climatology(hindcast_files, variable)
    file_base = climatology_cache_zarr / f'deterministic_{initialization_date}_climatology_{variable}'
    if file_base.exists():
        prob.purge_dirct(file_base)
    climatology.to_zarr(file_base, zarr_format = 2)

    print('Saved climatology values for ' + str(list_of_variables.get(variable)) + '!')


# In[ ]:


# Get all climatology zarr files from the cache directory
clim_cache_files = list(climatology_cache_zarr.glob('deterministic_*'))
print(f"Found {len(clim_cache_files)} files to process\n")

climatology_ds = xr.open_mfdataset(clim_cache_files, engine='zarr')
lon, lat, month = zonal.get_standard_coordinates(climatology_ds)


# In[ ]:


for pfaf_id in tqdm(pfaf_ids_aoi): # Iterate over each PFAF_ID
    #print(f'Processing PFAF_ID: {pfaf_id}')
    aoi = geodataframe[geodataframe.PFAF_ID == pfaf_id] 

    if aoi.empty:
        continue
    aoi_mask = regionmask.mask_3D_geopandas(aoi, lon, lat) # Create AOI mask

    records = [] # Initialize records list
    # Iterate over time and ensemble dimensions
    for m in month.values: 
        #for ens in climatology_ds['ensemble'].values if 'ensemble' in climatology_ds.dims else [None]:
        row = {'month': m, #pd.Timestamp(t).isoformat(),
                #'ensemble': ens,
                'pfaf_id': pfaf_id} # Initialize row with time, ensemble, and PFAF_ID
        for var in list_of_variables.keys(): # Iterate over each variable
            # Check if variable is SoilMoist_inst or SoilTemp_inst to handle levels
            if var in ['SoilMoist_inst', 'SoilTemp_inst']: # var has more than one depth lvl.
                profile_dim = [d for d in climatology_ds[var].dims if 'profile' in d.lower()]
                if profile_dim:
                    p_dim = profile_dim[0]
                    for level_idx  in range(climatology_ds.sizes[p_dim]):
                        col = f'{var}_lvl_{level_idx}' # Create column name for soil moisture levels
                        data = climatology_ds[var].sel({'month': m, p_dim : level_idx})
                        if 'ensemble' in data.dims:
                            data = data.mean(dim='ensemble')
                        masked = data.where(aoi_mask)
                        row[col] = masked.mean(dim=['lat','lon'], skipna=True).compute().item()
                else:
                    row[col] = None
            else:
                if var in climatology_ds.variables:
                    data = climatology_ds[var].sel({'month': m})
                    # if 'ensemble' in data.dims and ens is not None:
                    #     data = data.sel(ensemble=ens)
                    if 'ensemble' in data.dims:
                        data = data.mean(dim='ensemble')
                    masked = data.where(aoi_mask)
                    if var == 'Streamflow_tavg':
                        row[var] = masked.max(dim=['lat','lon'], skipna=True).compute().item()
                    else:
                        # if 'ensemble' in data.dims:
                        #     data = data.mean(dim='ensemble')
                        row[var] = masked.mean(dim=['lat','lon'], skipna=True).compute().item()
                else:
                    row[var] = None
        records.append(row)
    df = pd.DataFrame(records)
    out_csv = os.path.join(zonal_climatology_tab, f"zonal_climatology_pfaf_{pfaf_id}.csv")
    df.to_csv(out_csv, index=False)
    #print(f"Saved: {out_csv}")


# ## Step 4 Upload Output to Remote

# In[11]:

repo.index.add(f'{subsampled_output_dir}/{initialization_date}_*') # add subsampled prob anomaly
repo.index.add(f'{subsampled_output_dir}/index.json') # add subsampled prob anomaly
repo.index.add(f'{zonal_averages_forecast}/*.csv') # add forecast zonal avg.
repo.index.add(f'{zonal_climatology_tab}/*.csv') # add zonal avg. climatology
repo.index.commit(f"updated forecast anomaly data - {initialization_date}") # add git commit message

# prob.purge_old_init(subsampled_output_dir, current_init=initialization_date)