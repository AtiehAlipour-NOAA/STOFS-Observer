import s3fs  # Importing the s3fs library for accessing S3 buckets
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime, timedelta, timezone


def read_STOFS_from_s3(bucket_name, key):
    """
    Function to read a STOFS station files from an S3 bucket.
    
    Parameters:
    - bucket_name: Name of the S3 bucket
    - key: Key/path to the NetCDF file in the bucket
    
    Returns:
    - ds: xarray Dataset containing the NetCDF data
    """
    s3 = s3fs.S3FileSystem(anon=True)
    url = f"s3://{bucket_name}/{key}"
    ds = xr.open_dataset(s3.open(url, 'rb'))
    return ds


def get_station_nowcast_data(filename, modelname, directoryname, bucketname, daterange, steps, cycles):
    """
    Function to read STOFS Nowcast data from a station file on an S3 bucket.
    
    Parameters:
    - filename (str): The base filename for STOFS data
    - modelname (str): The STOFS model name 
    - directoryname (str): Optional directory name in the S3 bucket
    - bucketname (str): The name of the S3 bucket
    - daterange (list of two str): Start and end dates in 'YYYYMMDD' format
    - steps (int): Number of steps to slice as the nowcast period in each STOFS file
    - cycles (list of str): List of cycles (e.g., ['00', '12'])
    
    Returns:
    - xarray.Dataset: Dataset containing the STOFS Nowcast data
    """

    # Parse the start and end dates from the date range
    start_date = datetime.strptime(daterange[0], '%Y%m%d')
    end_date = datetime.strptime(daterange[1], '%Y%m%d') + timedelta(days=1)  # Include the last day
    
    # Generate a list of dates in the specified range
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y%m%d'))  # Format as YYYYMMDD
        current_date += timedelta(days=1)

    nowcast_all_list = []
    
    for date in dates:
        for cycle in cycles:
            base_key = f'{modelname}.{date}'
            dataname = f't{cycle}z.{filename}.nc'
            if directoryname:
                key = f'{directoryname}/{base_key}/{modelname}.{dataname}'
            else:
                key = f'{base_key}/{modelname}.{dataname}'
                
            try:
                dataset = read_STOFS_from_s3(bucketname, key)

                # Check if dataset exists and has data
                if dataset is not None:
                    nowcast = dataset.isel(time=slice(0, steps))  # First 'steps' time steps (nowcast data)
                    nowcast_all_list.append(nowcast)
            except Exception as e:
                print(f'Error reading file {key} from S3: {str(e)}')

    # Concatenate all nowcast data and filter by date range
    nowcast_all_out_of_range = xr.concat(nowcast_all_list, dim='time')
    nowcast_all = nowcast_all_out_of_range.sel(time=slice(start_date, end_date))  # Filtered dataset

    return nowcast_all


def get_station_data(filename, modelname, directoryname, bucketname, date, cycle):
    """
    Function to read STOFS data for a particular date and cycle from a station file on an S3 bucket.
    
    Parameters:
    - filename (str): The base filename for STOFS data
    - modelname (str): The STOFS model name 
    - directoryname (str): Optional directory name in the S3 bucket
    - bucketname (str): The name of the S3 bucket
    - date (str): date in 'YYYYMMDD' format
    - cycle (str): cycle of the data (e.g.'12')
    
    Returns:
    - xarray.Dataset: Dataset containing the STOFS nowcast+forecast data from one cycle
    """
    

    base_key = f'{modelname}.{date}'
    dataname = f't{cycle}z.{filename}.nc'
    if directoryname:
       key = f'{directoryname}/{base_key}/{modelname}.{dataname}'
    else:
       key = f'{base_key}/{modelname}.{dataname}'
                
    try:
       dataset = read_STOFS_from_s3(bucketname, key)
    except Exception as e:
                print(f'Error reading file {key} from S3: {str(e)}')
    return dataset


