from _STOFS import get_station_nowcast_data
from _STOFS import get_station_data

class StofsRun():
    def __init__(self, bucket, subdirectory, model, output_types, forecast_cycles, nowcast_steps):
        self.bucket = bucket
        self.subdirectory = subdirectory
        self.model = model
        self.output_types = output_types
        self.forecast_cycles = forecast_cycles
        self.nowcast_steps = nowcast_steps
        
    def get_station_nowcast(self, daterange, output_type=None):
        
        # Select output type (variable, bias correction, etc.)
        if not output_type:
            filename = self.output_types["default"]
        else:
            filename = self.output_types[output_type]
            
        ds = get_station_nowcast_data(filename, 
                                      self.model, 
                                      self.subdirectory, 
                                      self.bucket, 
                                      daterange, 
                                      self.nowcast_steps, 
                                      self.forecast_cycles)
        
        # TODO (Jack): add method to extract NOS ID and assign it as a coord?
        # TODO (Jack): maybe convert to pandas data frame here?
        
        return ds
        
    def get_station_forecast(self, first_forecast_date, output_type=None):
        
        # Select output type (variable, bias correction, etc.)
        if not output_type:
            filename = self.output_types["default"]
        else:
            filename = self.output_types[output_type]
            
        # Method for choosing forecast cycle.
        first_hour = first_forecast_date.hour
        cycle_int = max([int(ci) if int(ci) <= first_hour else 0 
                         for ci in self.forecast_cycles])
        # TODO (Jack): This needs some thought for more general cases.
        # E.g., if forecast_cycles are [3,9,15,21], and we have
        # a start time of 0213 UTC, then we would want to select the 
        # 21UTC forecast from the previous day. The current version
        # would fail in such a case.
        
        ds = get_station_data(filename, 
                              self.model, 
                              self.subdirectory, 
                              self.bucket, 
                              first_forecast_date, 
                              "{:02n}".format(cycle_int))
        
        # TODO (Jack): add method to extract NOS ID and assign it as a coord?
        # TODO (Jack): maybe convert to pandas data frame here?
        
        return ds
        