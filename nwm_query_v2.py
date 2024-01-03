import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
#import requests
# from dask.distributed import Client, LocalCluster
import fsspec
import argparse
##

#dependancies in environment for script
#aiohttp requests fsspec yarl xarray pathlib pandas zarr


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


# parser = argparse.ArgumentParser(description='Process params')
# parser.add_argument('startDate')
# #filesnames = os.listdire()

#statistic option 

startDate = "2019-05-24 05:00"
endDate =  "2019-06-09 04:59"
stat = 'max'
hydroTpath = r'/home/geo-linux2/Documents/NWMquery/hydrotable.csv' 
#r'F:\fim\fim_out\ArkRiv203_205\11110205\hydrotable.csv'
outPath = r'/home/geo-linux2/Documents/NWMquery/Arkflowfile_cms.csv'

# startDate = input("start date and time in UTC as <2019-05-24 05:00>")
# endDate = input("end date and time in UTC as <2019-06-09 04:59>")
# hydroTpath = r'/home/geo-linux/Documents/NWMquery/hydrotable.csv' 
# #r'F:\fim\fim_out\ArkRiv203_205\11110205\hydrotable.csv'
# outPath = r'/home/geo-linux/Documents/NWMquery/Arkflowfile_cms.csv'


## import csv file

df = pd.read_csv(hydroTpath, usecols=['feature_id'])

##function to return unique f_id values

#flowId = df['feature_id'].unique()
flowId = df.drop_duplicates()

## convert to series


## pass series to query
   
#main(flowIdu)

statFlow = getDischarge(flowId, startDate, endDate, 'max')

#NWMdf.rename(columns= {'streamflow':'discharge'}, inplace= True)

# summary stat as parameter

#print(maxFlow.dtype)
statFlow.rename("discharge", inplace= True)

#print(maxFlow)
statFlow.to_csv(outPath)


# if __name__ == "__main__":
#     main()





