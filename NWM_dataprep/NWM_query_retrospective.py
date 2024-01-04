import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
import fsspec
import argparse

#For creating a flowfile for HAND FIM from NWM Retrospective V2.1 data 


##Function to query S3 bucket--adapted from https://github.com/NOAA-OWP/hydrotools/issues/157#issue-1050233182 -- credit: jarq6c
def get_aws_data(start, end, features):
    """Get data from AWS"""

    # Base URL
    url = r'https://noaa-nwm-retrospective-2-1-zarr-pds.s3.amazonaws.com/chrtout.zarr'
    # url = r'https://noaa-nwm-retrospective-2-1-zarr-pds.s3.amazonaws.com/precip.zarr'

    # Get xarray.dataset
    ds = xr.open_zarr(fsspec.get_mapper(url), consolidated=True)

    # Extract time series data
    ts = ds.streamflow.sel(time=slice(start, end), feature_id=features)

    # Return DataFrame
    return ts.to_dataframe().reset_index()


##Retrieves and returns discharges for each NWM flowline ID. "stat" argument currently hardcoded as maximum, this is for creating a maximum extent map.
def getDischarge(flowId, startDate, endDate, stat):
    arrary=flowId['feature_id'].to_numpy()
    #print(arrary)
    # Get pandas.DataFrame
    NWMdf = get_aws_data(
        start= startDate,
        end= endDate,
        features=arrary
        )
        
    #return NWMdf
    statFlow = NWMdf.groupby('feature_id')['streamflow'].max()
    return statFlow


# startDate = input("start date and time in UTC as <2019-05-24 05:00>")
# endDate = input("end date and time in UTC as <2019-06-09 04:59>")
# hydroTpath = r'.../hydrotable.csv' 
# outPath = r'<flowfilepath>_cms.csv'


## import hydrotable csv file and isolate "feature_id" column, which refers to NWM flowline IDs
df = pd.read_csv(hydroTpath, usecols=['feature_id'])

##function to isolate unique feature_id (NWM flowline ID) values
flowId = df.drop_duplicates()

#call getDischarge
statFlow = getDischarge(flowId, startDate, endDate, 'max')

#rename column from "streamflow" to "discharge" for proper flowfile formatting
statFlow.rename("discharge", inplace= True)

#export Flowfile as csv
statFlow.to_csv(outPath)
