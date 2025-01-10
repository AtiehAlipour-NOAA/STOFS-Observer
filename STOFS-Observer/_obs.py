import numpy as np
import pandas as pd
from searvey._coops_api import fetch_coops_station

def fetch_coops_multistation_df(st_list, start_time, end_time, product, **kwargs):
    """
    """
    
    result = pd.DataFrame()
    
    for nos_id in st_list:
        
        #print(f"Fetching observation data for NOS ID {nos_id}")
        
        # Wrangle the datetime formats to avoid searvey TimeZone warnings....
        if type(start_time) in [np.ndarray, np.datetime64]:
            start_time = pd.DatetimeIndex([pd.to_datetime(start_time)], tz='utc')[0]
        if type(end_time) in [np.ndarray, np.datetime64]:
            end_time = pd.DatetimeIndex([pd.to_datetime(end_time)], tz='utc')[0]
            
        # Download one station's worth of data.
        station_df = fetch_coops_station(
            station_id=nos_id,
            start_date=start_time,
            end_date=end_time,
            product=product,
            **kwargs
        )
        # Add "station" as a column and convert to multiindex.
        station_df.insert(0, 'station', nos_id)
        station_df = station_df.reset_index().set_index(['station', 'time'])
        
        # Concatenate with other stations.
        try:
            result = pd.concat(
                [result, station_df],
                axis=0,
                join="outer",
                ignore_index=False,
                sort=False
            )
        except:
            print(f"Warning: cannot concatenate station data frame for NOS ID {nos_id}: Skipping.")
            continue
            
    return result