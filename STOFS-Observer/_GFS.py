import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import pygrib
import s3fs
from datetime import datetime, timedelta



def find_index_closest_data(ds, stations):
    lat_indices = {}
    lon_indices = {}
    
    for y_val, x_val, nos_id in zip(stations['lat'], stations['lon'], stations['nos_id']): 
        longitude_adjusted = float(x_val) + 360 if float(x_val) < 0 else float(x_val)
        
        # Get the nearest indices for latitude and longitude
        lat_idx = np.where((np.abs(ds['latitude'][0:4718592:3072] - float(y_val))) == np.min(np.abs(ds['latitude'][0:4718592:3072] -                          float(y_val))))
        lon_idx = np.where((np.abs(ds['longitude'][0:3072] - longitude_adjusted)) == np.min(np.abs(ds['longitude'][0:3072] -                                  longitude_adjusted)))
       
        # Store indices in dictionaries 
        lat_indices[nos_id] = lat_idx 
        lon_indices[nos_id] = lon_idx

    return lat_indices, lon_indices

def fetch_gfs_Nowcast_data(start_date, end_date, cycles, stations, num_time_steps):
    """
    Function to fetch GFS data for specified dates and cycles and return a DataFrame with wind and pressure information.

    Parameters:
    - start_date (str): The start date in 'YYYYMMDD' format.
    - end_date (str): The end date in 'YYYYMMDD' format.
    - cycles (list of str): List of cycles (e.g., ['00', '06', '12', '18']).
    - stations (DataFrame): DataFrame containing station information (lat, lon, nos_id).
    - num_time_steps (int): Number of time steps to retrieve from each cycle.

    Returns:
    - pd.DataFrame: DataFrame containing the time, u_wind, v_wind, and surface pressure.
    """
    
    # Initialize a list to store data for the DataFrame
    u_wind_dfs = pd.DataFrame()
    v_wind_dfs = pd.DataFrame()
    surface_pressure_dfs = pd.DataFrame()

    # Initialize list to store all time information 
    all_times = []


    # Generate date list
    dates = []
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')

    while current_date <= end_date:
        dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    for date in dates:
        print(date)
        for i, cycle in enumerate(cycles):
            for hour in range(0, num_time_steps, 1):  # Loop over specified number of time steps
                # Adjust date for the previous cycle if needed

                key = f'gfs.{date}/{cycle}/atmos/gfs.t{cycle}z.sfluxgrbf{hour:03d}.grib2'
                time = datetime.strptime(date, '%Y%m%d') + timedelta(hours=int(cycle)) + timedelta(hours=hour)

                print(key)    
                url = f"s3://noaa-gfs-bdp-pds/{key}"

                # Fetch the GRIB2 data from S3
                try:
                    s3 = s3fs.S3FileSystem(anon=True)
                    with s3.open(url, 'rb') as f:
                        grib_data = f.read()
                except Exception as e:
                    print(f"Error fetching data from {url}: {e}")
                    continue

                # Define variable names of interest
                variable_names = ['Surface pressure', '10 metre U wind component', '10 metre V wind component']
                data_arrays = {var_name: [] for var_name in variable_names}

                # Read GRIB2 data
                with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp_file:
                    tmp_file.write(grib_data)
                    tmp_file.seek(0)
                    grbs = pygrib.open(tmp_file.name)

                    for grb in grbs:
                        if grb['name'] in variable_names:
                            data_arrays[grb['name']].append(grb.values)

                    grbs.close()


                # Convert data arrays to xarray DataArrays
                pressure_data = xr.DataArray(np.array(data_arrays['Surface pressure']), name='surface_pressure')
                u_wind_data = xr.DataArray(np.array(data_arrays['10 metre U wind component']), name='u_wind')
                v_wind_data = xr.DataArray(np.array(data_arrays['10 metre V wind component']), name='v_wind')

                print(hour)
                # Create an xarray Dataset
                ds = xr.Dataset(
                data_vars={
                'surface_pressure': pressure_data,
                'u_wind': u_wind_data,
                'v_wind': v_wind_data},
                coords={
                'latitude': grb.latitudes,  # Assuming latitudes are the same for all messages
                'longitude': grb.longitudes,},
                attrs={
                'description': 'GRIB Data Example'})


                # Rename the dimensions 'dim_0', 'dim_1', 'dim_2' to 'time', 'y', 'x'
                ds = ds.rename({'dim_0': 'time', 'dim_1': 'y', 'dim_2': 'x'})
                # Initialize empty DataFrames to store wind and pressure data for this hour
                u_wind_df = pd.DataFrame()
                v_wind_df = pd.DataFrame()
                surface_pressure_df = pd.DataFrame()
                   
                # Extract values for  stations
                if not all_times:
                    lat_indices, lon_indices = find_index_closest_data(ds, stations)  
                    print(lat_indices)
             
                for nos_id in  stations['nos_id']:
                    # Extract the forcing data using the index
                    u_wind_value = ds.u_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    v_wind_value = ds.v_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    surface_pressure_value = ds.surface_pressure[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values


                    # Append the values to the respective DataFrames as columns with NOS ids as column names
                    u_wind_df[int(nos_id)] = [np.round(u_wind_value, 2)]
                    v_wind_df[int(nos_id)]= [np.round(v_wind_value, 2)]
                    surface_pressure_df[int(nos_id)] = [np.round(surface_pressure_value, 2)]
            
                # Append data for each station to the list
                u_wind_dfs = pd.concat([u_wind_dfs, u_wind_df], ignore_index=True)
                v_wind_dfs = pd.concat([v_wind_dfs, v_wind_df], ignore_index=True)
                surface_pressure_dfs = pd.concat([surface_pressure_dfs, surface_pressure_df], ignore_index=True)
            
                # Store the time information 
                all_times.append(time) 

    return u_wind_dfs, v_wind_dfs, surface_pressure_dfs, all_times


def fetch_gfs_Forecast_data(date, cycle, stations):
    """
    Function to fetch GFS data for date and cycle used as the STOFS forcing data and return a DataFrame with wind and pressure information.

    Parameters:
    - date (str): The date in 'YYYYMMDD' format.
    - cycle (str): cycle (e.g., ['00', '06', '12', '18']).
    - stations (DataFrame): DataFrame containing station information (lat, lon, nos_id).

    Returns:
    - pd.DataFrames: DataFrame containing the time, u_wind, v_wind, and surface pressure.
    """
    
    # Initialize a list to store data for the DataFrame
    u_wind_dfs = pd.DataFrame()
    v_wind_dfs = pd.DataFrame()
    surface_pressure_dfs = pd.DataFrame()

    # Initialize list to store all time information 
    all_times = []

    # list of cycles in STOFS-2dd-Global
    cycles_2d = ['00', '06', '12', '18']  


    # Generate date list
    for hour in range(0, 121, 1):  # Assuming we want to loop from 0 to 120 hours
            print(hour)
            if hour < 6:
                if cycle == '00':
                        current_date = datetime.strptime(date, '%Y%m%d') 
                        current_date -= timedelta(days=1) # Go back one day 
                        date_one_day_back = current_date.strftime('%Y%m%d')
                        previous_cycle = '18'

                        # Define the filename and the location of the GRIB2 file
                        key = f'gfs.{date_one_day_back}/{previous_cycle}/atmos/gfs.t{previous_cycle}z.sfluxgrbf{hour:03d}.grib2'
                        url = f"s3://noaa-gfs-bdp-pds/{key}"
                        time = datetime.strptime(date_one_day_back, '%Y%m%d') + timedelta(hours=int(previous_cycle)) + timedelta(hours=hour) 
                        print(time)
                else:
                        
                        
                        previous_cycle = cycles_2d[cycles_2d.index(cycle) - 1] 
                         
                        # Define the filename and the location of the GRIB2 file
                        key = f'gfs.{date}/{previous_cycle}/atmos/gfs.t{previous_cycle}z.sfluxgrbf{(hour):03d}.grib2'
                        url = f"s3://noaa-gfs-bdp-pds/{key}"
                        print(url)
                        #time = datetime.strptime(date, '%Y%m%d') + timedelta(hours=int(previous_cycle)) + timedelta(hours=hour) 
                        time = datetime.strptime(date, '%Y%m%d') + timedelta(hours=int(previous_cycle)) + timedelta(hours=hour)
                        print(time)
            else: 
                
                # Define the filename and the location of the GRIB2 file
                key = f'gfs.{date}/{cycle}/atmos/gfs.t{cycle}z.sfluxgrbf{(hour-6):03d}.grib2'
                url = f"s3://noaa-gfs-bdp-pds/{key}"
                print(url)
                time = datetime.strptime(date, '%Y%m%d') + timedelta(hours=int(cycle))+ timedelta(hours=hour-6) 
                print(time)
                

            # Fetch the GRIB2 data from S3
            s3 = s3fs.S3FileSystem(anon=True)
            with s3.open(url, 'rb') as f:
                 grib_data = f.read()


            # Define the variable names of interest
            variable_names = ['Surface pressure', '10 metre U wind component', '10 metre V wind component']


            # Initialize empty arrays to store data
            data_arrays = {var_name: [] for var_name in variable_names}


            # Save the GRIB2 data to a temporary file
            with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp_file:
                 tmp_file.write(grib_data)
                 tmp_file.seek(0)  # Reset file pointer to the beginning
                 # Read the GRIB2 data using pygrib from the temporary file
                 grbs = pygrib.open(tmp_file.name)


                 # Iterate over each message in the GRIB2 file
                 for grb in grbs:
                     # Check if the message corresponds to one of the variables of interest
                 
                     if grb['name'] in variable_names:
                        # Append data to the corresponding array
                        data_arrays[grb['name']].append(grb.values)

                 # Close the GRIB2 file
                 grbs.close()

            # Convert data arrays to xarray DataArrays
            pressdata_arraysure_data = xr.DataArray(np.array(data_arrays['Surface pressure']), name='surface_pressure')
            u_wind_data = xr.DataArray(np.array(data_arrays['10 metre U wind component']), name='u_wind')
            v_wind_data = xr.DataArray(np.array(data_arrays['10 metre V wind component']), name='v_wind')


            # Create an xarray Dataset
            ds = xr.Dataset(
            data_vars={
            'surface_pressure': pressdata_arraysure_data,
            'u_wind': u_wind_data,
            'v_wind': v_wind_data},
             coords={
            'latitude': grb.latitudes,  # Assuming latitudes are the same for all messages
            'longitude': grb.longitudes,},
             attrs={
            'description': 'GRIB Data Example'})


            # Rename the dimensions 'dim_0', 'dim_1', 'dim_2' to 'time', 'y', 'x'
            ds = ds.rename({'dim_0': 'time', 'dim_1': 'y', 'dim_2': 'x'})
    
            # Initialize empty DataFrames to store wind and pressure data for this hour
            u_wind_df = pd.DataFrame()
            v_wind_df = pd.DataFrame()
            surface_pressure_df = pd.DataFrame()
    
            # Extract values for  stations
            if not all_times:
                    lat_indices, lon_indices = find_index_closest_data(ds, stations)  
                    
             
            for nos_id in  stations['nos_id']:
                    
                    # Extract the forcing data using the index
                    u_wind_value = ds.u_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    v_wind_value = ds.v_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    surface_pressure_value = ds.surface_pressure[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values


                    # Append the values to the respective DataFrames as columns with NOS ids as column names
                    u_wind_df[int(nos_id)] = [np.round(u_wind_value, 2)]
                    v_wind_df[int(nos_id)]= [np.round(v_wind_value, 2)]
                    surface_pressure_df[int(nos_id)] = [np.round(surface_pressure_value, 2)]


            u_wind_dfs = pd.concat([u_wind_dfs, u_wind_df], ignore_index=True)
            
            v_wind_dfs = pd.concat([v_wind_dfs, v_wind_df], ignore_index=True)
            surface_pressure_dfs = pd.concat([surface_pressure_dfs, surface_pressure_df], ignore_index=True)
            
            # Store the time information 
            all_times.append(time) 

    # Loop over different files for different hours
    for hour in range(120, 184, 3):  
            print(hour)
            
            # Define the filename and the location of the GRIB2 file for the current hour

            key_current = f'gfs.{date}/{cycle}/atmos/gfs.t{cycle}z.sfluxgrbf{(hour-6):03d}.grib2'
            url_current = f"s3://noaa-gfs-bdp-pds/{key_current}"
             
            print(url_current)
            # Fetch the GRIB2 data from S3 for the current hour
            s3_current = s3fs.S3FileSystem(anon=True)
            with s3_current.open(url_current, 'rb') as f_current:
                 grib_data_current = f_current.read()

            # Define the variable names of interest
            variable_names = ['Surface pressure', '10 metre U wind component', '10 metre V wind component']

            # Initialize empty arrays to store current data
            data_arrays_current = {var_name: [] for var_name in variable_names}
 
            # Save the current GRIB2 data to a temporary file
            with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp_file:
                 tmp_file.write(grib_data_current)
                 tmp_file.seek(0)  # Reset file pointer to the beginning
                 # Read the GRIB2 data using pygrib from the temporary file
                 grbs = pygrib.open(tmp_file.name)

                 # Iterate over each message in the GRIB2 file
                 for grb in grbs:
                     # Check if the message corresponds to one of the variables of interest
                     if grb['name'] in variable_names:
                        # Append data to the corresponding array
                        data_arrays_current[grb['name']].append(grb.values)

                 # Close the GRIB2 file
                 grbs.close()

    
            # Convert data arrays to xarray DataArrays for current data
            pressure_data_current = xr.DataArray(np.array(data_arrays_current['Surface pressure']), name='surface_pressure')
            u_wind_data_current = xr.DataArray(np.array(data_arrays_current['10 metre U wind component']), name='u_wind')
            v_wind_data_current = xr.DataArray(np.array(data_arrays_current['10 metre V wind component']), name='v_wind')

            # Create an xarray Dataset for current data
            ds_current = xr.Dataset(
            data_vars={
               'surface_pressure': pressure_data_current,
               'u_wind': u_wind_data_current,
               'v_wind': v_wind_data_current,},
            coords={
            'latitude': grb.latitudes,  # Assuming latitudes are the same for all messages
            'longitude': grb.longitudes,},  # Assuming longitudes are the same for all messages 
            attrs={ 'description': 'GRIB Data Example', }, )

            # Rename the dimensions 'dim_0', 'dim_1', 'dim_2' to 'time', 'y', 'x'
            ds_current = ds_current.rename({'dim_0': 'time', 'dim_1': 'y', 'dim_2': 'x'})
    
            # Define the filename and the location of the GRIB2 file for the next hour
            hour_next = hour + 3  # Assuming data is available 3-hourly
            key_next = f'gfs.{date}/{cycle}/atmos/gfs.t{cycle}z.sfluxgrbf{(hour_next-6):03d}.grib2'
            url_next = f"s3://noaa-gfs-bdp-pds/{key_next}"
  
            # Fetch the GRIB2 data from S3 for the next hour
            s3_next = s3fs.S3FileSystem(anon=True)
            with s3_next.open(url_next, 'rb') as f_next:
                grib_data_next = f_next.read()

            # Initialize empty arrays to store current data
            data_arrays_next = {var_name: [] for var_name in variable_names}

            # Save the current GRIB2 data to a temporary file
            with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp_file:
                 tmp_file.write(grib_data_next)
                 tmp_file.seek(0)  # Reset file pointer to the beginning
                 # Read the GRIB2 data using pygrib from the temporary file
                 grbs = pygrib.open(tmp_file.name)

                 # Iterate over each message in the GRIB2 file
                 for grb in grbs:
                     # Check if the message corresponds to one of the variables of interest
                     if grb['name'] in variable_names:
                        # Append data to the corresponding array
                        data_arrays_next[grb['name']].append(grb.values)

                 # Close the GRIB2 file
                 grbs.close()


            # Convert data arrays to xarray DataArrays for next file data
            pressure_data_next = xr.DataArray(np.array(data_arrays_next['Surface pressure']), name='surface_pressure')
            u_wind_data_next = xr.DataArray(np.array(data_arrays_next['10 metre U wind component']), name='u_wind')
            v_wind_data_next = xr.DataArray(np.array(data_arrays_next['10 metre V wind component']), name='v_wind')

            # Create an xarray Dataset for next file data
            ds_next = xr.Dataset(
            data_vars={
            'surface_pressure': pressure_data_next,
            'u_wind': u_wind_data_next,
            'v_wind': v_wind_data_next,    },
            coords={
            'latitude': grb.latitudes,  # Assuming latitudes are the same for all messages
            'longitude': grb.longitudes,  # Assuming longitudes are the same for all messages
            },
            attrs={
            'description': 'GRIB Data Example',    },
            )

            # Rename the dimensions 'dim_0', 'dim_1', 'dim_2' to 'time', 'y', 'x'
            ds_next = ds_next.rename({'dim_0': 'time', 'dim_1': 'y', 'dim_2': 'x'})

            # Initialize empty DataFrames to store wind and pressure data for this hour
            for hour_1 in range(1, 4, 1): 
         
                u_wind_df = pd.DataFrame()
                v_wind_df = pd.DataFrame()
                surface_pressure_df = pd.DataFrame()
    
                # Loop over the elements of station_ds['y'] and station_ds['x']
                for nos_id in stations['nos_id']:

                    # Extract the current forcing data using the index 
                    u_wind_value_0 = ds_current.u_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    v_wind_value_0 = ds_current.v_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    surface_pressure_value_0 = ds_current.surface_pressure[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values

                    # Extract the next forcing data using the index 
                    u_wind_value_3 = ds_next.u_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    v_wind_value_3 = ds_next.v_wind[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
                    surface_pressure_value_3 = ds_next.surface_pressure[0, lat_indices[nos_id][0][0], lon_indices[nos_id][0][0]].values
             
                    # Calculate interpolation coefficient
                    u_wind_coeff = (u_wind_value_3 - u_wind_value_0) / 3
                    v_wind_coeff = (v_wind_value_3 - v_wind_value_0) / 3 
                    surface_pressure_coeff = (surface_pressure_value_3 - surface_pressure_value_0) / 3 
                           
                    # Append the values to the respective DataFrames as columns with NOS ids as column names
                    u_wind_df[int(nos_id)] = [np.round(u_wind_value_0, 2)] + hour_1*u_wind_coeff
                    v_wind_df[int(nos_id)]= [np.round(v_wind_value_0, 2)] + hour_1*v_wind_coeff
                    surface_pressure_df[int(nos_id)] = [np.round(surface_pressure_value_0, 2)] + hour_1*surface_pressure_coeff

                u_wind_dfs = pd.concat([u_wind_dfs, u_wind_df], ignore_index=True)
                v_wind_dfs = pd.concat([v_wind_dfs, v_wind_df], ignore_index=True)
                surface_pressure_dfs = pd.concat([surface_pressure_dfs, surface_pressure_df], ignore_index=True)
                time = datetime.strptime(date, '%Y%m%d') + timedelta(hours=int(previous_cycle)) + timedelta(hours=hour)+timedelta(hours=hour_1)
                all_times.append(time) 


    

    return u_wind_dfs, v_wind_dfs, surface_pressure_dfs, all_times


