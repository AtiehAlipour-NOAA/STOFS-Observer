import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime, timedelta
from typing import List
import s3fs  # Importing the s3fs library for accessing S3 buckets



def read_STOFS_from_s3(bucket_name, key):
    """
    Function to read a STOFS nc files from an S3 bucket.
    
    Parameters:
    - bucket_name: Name of the S3 bucket
    - key: Key/path to the NetCDF file in the bucket
    
    Returns:
    - ds: xarray Dataset containing the NetCDF data
    """
    s3 = s3fs.S3FileSystem(anon=True)
    url = f"s3://{bucket_name}/{key}"
    ds = xr.open_dataset(s3.open(url, 'rb'), drop_variables=['nvel'])
    return ds



def fetch_saved_HRRR_Nowcast_data(filename, modelname, directoryname, directoryname2, bucketname, daterange, stations, steps):
    
    date = daterange[0]
    base_key = f'{modelname}.{date}'
    dataname = f'{filename}.nc'
    if directoryname:
        if directoryname2:
          key = f'{directoryname}/{base_key}/{directoryname2}/{modelname}.{dataname}'
        else: 
          key = f'{directoryname}/{base_key}/{modelname}.{dataname}'
    else:
        if directoryname2:
          key = f'{base_key}/{directoryname2}/{modelname}.{dataname}'
        else:
          key = f'{base_key}/{modelname}.{dataname}'
    try:
        print(key)
        dataset = read_STOFS_from_s3(bucketname, key)
    except Exception as e:
        print(f"Error fetching data from {bucketname}: {e}")
        
    def find_index_closest_data(ds, stations):
        lat_indices = {}
        lon_indices = {}
    
        for y_val, x_val, nos_id in zip(stations['lat'], stations['lon'], stations['nos_id']): 
        
            # Get the nearest indices for latitude and longitude
            lat_idx = np.where((np.abs(ds['lat'][0:1059][0] - float(y_val))) == np.min(np.abs(ds['lat'][0:1059][0] - float(y_val))))
            lon_idx = np.where((np.abs(ds['lon'][0][0:934] - x_val)) == np.min(np.abs(ds['lon'][0][0:934] - x_val)))
       
            # Store indices in dictionaries 
            lat_indices[nos_id] = lat_idx
            lon_indices[nos_id] = lon_idx
        return lat_indices, lon_indices
    lat_indices, lon_indices = find_index_closest_data(dataset, stations)
    
    start_date = datetime.strptime(daterange[0], '%Y%m%d')
    end_date = datetime.strptime(daterange[1], '%Y%m%d') + timedelta(days=1)
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    
    # Initialize a list to store data for the DataFrame
    u_wind_dfs = []
    v_wind_dfs = []
    surface_pressure_dfs = []
    
    for date in dates:
        previous_date = datetime.strptime(date, '%Y%m%d') - timedelta(days=1)
        print(previous_date)
        base_key = f'{modelname}.{previous_date.strftime("%Y%m%d")}'
        dataname = f'{modelname}.{filename}.nc'
        if directoryname:
            if directoryname2:
               key = f'{directoryname}/{base_key}/{directoryname2}/{modelname}.{dataname}'
            else: 
               key = f'{directoryname}/{base_key}/{modelname}.{dataname}'
        else:
            if directoryname2:
              key = f'{base_key}/{directoryname2}/{modelname}.{dataname}'
            else:
              key = f'{base_key}/{modelname}.{dataname}'
                
    
        try:
            nowcast = read_netcdf_from_s3(bucketname, key)

            # Initialize empty DataFrames to store wind and pressure data for this hour
            u_wind_df = []
            v_wind_df = []
            surface_pressure_df = []
            
            for nos_id in  stations['nos_id']:
                    
                    # Extract the forcing data using the index
                    u_wind_values = nowcast.uwind[:steps, lat_indices[int(nos_id)][0][0], lon_indices[int(nos_id)][0][0]]
                    v_wind_values = nowcast.vwind[:steps, lat_indices[int(nos_id)][0][0], lon_indices[int(nos_id)][0][0]]
                    surface_pressure_values = nowcast.prmsl[:steps, lat_indices[int(nos_id)][0][0], lon_indices[int(nos_id)][0][0]]


                    # Append the values to the respective DataFrames as columns with NOS ids as column names
                    u_wind_df.append(u_wind_values)
                    v_wind_df.append(v_wind_values)
                    surface_pressure_df.append(surface_pressure_values)
            
            # Append data for each station to the list
            u_wind_dfs = xr.concat(u_wind_df, dim='time')
            v_wind_dfs = xr.concat(v_wind_df, dim='time')
            surface_pressure_dfs = xr.concat(surface_pressure_df, dim='time')

        
        except Exception as e:
            print(f'Error reading file {key} from S3: {str(e)}')

    

    if u_wind_dfs:
        return u_wind_dfs, v_wind_dfs, surface_pressure_dfs
    else:
        print("No valid data found.")
        return None

# Example usage:
# result = retrieve_wind_forecast(date_range, filtered_wind_stations, bucket_name_3d)
