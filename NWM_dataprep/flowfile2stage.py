import pandas as pd
import numpy as np
import xarray as xr
from numba import types, typed

#adapted from https://github.com/NOAA-OWP/inundation-mapping/blob/dev/tools/inundation.py

#define file paths
hydro_table = ""
flowfile = ""

#define  
def subset_hydroTable_to_forecast(hydroTable, forecast, subset_hucs=None):
    if isinstance(hydroTable, str):
        htable_req_cols = ['HUC', 'feature_id', 'HydroID', 'stage', 'discharge_cms', 'LakeID']
        hydroTable = pd.read_csv(
            hydroTable,
            dtype={
                'HUC': str,
                'feature_id': str,
                'HydroID': str,
                'stage': float,
                'discharge_cms': float,
                'LakeID': int,
                'last_updated': object,
                'submitter': object,
                'obs_source': object,
            },
            low_memory=False,
            usecols=htable_req_cols,
        )
        # huc_error = hydroTable.HUC.unique()
        hydroTable = hydroTable.set_index(['HUC', 'feature_id', 'HydroID'])

    elif isinstance(hydroTable, pd.DataFrame):
        pass  # consider checking for correct dtypes, indices, and columns
    else:
        raise TypeError("Pass path to hydro-table csv or Pandas DataFrame")

    hydroTable = hydroTable[
        hydroTable["LakeID"] == -999
    ]  # Subset hydroTable to include only non-lake catchments.

    # raises error if hydroTable is empty due to all segments being lakes
    #if hydroTable.empty:
       # raise hydroTableHasOnlyLakes("All stream segments in HUC are within lake boundaries.")

    if isinstance(forecast, str):
        try:
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        except UnicodeDecodeError:
            forecast = read_nwm_forecast_file(forecast)

    elif isinstance(forecast, pd.DataFrame):
        pass  # consider checking for dtypes, indices, and columns
    else:
        raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    # susbset hucs if passed
    if subset_hucs is not None:
        if isinstance(subset_hucs, list):
            if len(subset_hucs) == 1:
                try:
                    subset_hucs = open(subset_hucs[0]).read().split('\n')
                except FileNotFoundError:
                    pass
        elif isinstance(subset_hucs, str):
            try:
                subset_hucs = open(subset_hucs).read().split('\n')
            except FileNotFoundError:
                subset_hucs = [subset_hucs]

    if not hydroTable.empty:
        if isinstance(forecast, str):
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        elif isinstance(forecast, pd.DataFrame):
            pass  # consider checking for dtypes, indices, and columns
        else:
            raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

        # susbset hucs if passed
        if subset_hucs is not None:
            if isinstance(subset_hucs, list):
                if len(subset_hucs) == 1:
                    try:
                        subset_hucs = open(subset_hucs[0]).read().split('\n')
                    except FileNotFoundError:
                        pass
            elif isinstance(subset_hucs, str):
                try:
                    subset_hucs = open(subset_hucs).read().split('\n')
                except FileNotFoundError:
                    subset_hucs = [subset_hucs]

            # subsets HUCS
            subset_hucs_orig = subset_hucs.copy()
            subset_hucs = []
            for huc in np.unique(hydroTable.index.get_level_values('HUC')):
                for sh in subset_hucs_orig:
                    if huc.startswith(sh):
                        subset_hucs += [huc]

            hydroTable = hydroTable[np.in1d(hydroTable.index.get_level_values('HUC'), subset_hucs)]

    # join tables
    try:
        hydroTable = hydroTable.join(forecast, on=['feature_id'], how='inner')
    except AttributeError:
         print("FORECAST ERROR")
        #raise NoForecastFound("No forecast value found for the passed feature_ids in the Hydro-Table")

    else:
        # initialize dictionary
        catchmentStagesDict = typed.Dict.empty(types.int32, types.float64)

        # interpolate stages
        for hid, sub_table in hydroTable.groupby(level='HydroID'):
            interpolated_stage = np.interp(
                sub_table.loc[:, 'discharge'].unique(),
                sub_table.loc[:, 'discharge_cms'],
                sub_table.loc[:, 'stage'],
            )

            # add this interpolated stage to catchment stages dict
            h = round(interpolated_stage[0], 4)

            hid = types.int32(hid)
            h = types.float32(h)
            catchmentStagesDict[hid] = h

        # huc set
        hucSet = [str(i) for i in hydroTable.index.get_level_values('HUC').unique().to_list()]

        return (catchmentStagesDict, hucSet)
        #dfCSD = pd.DataFrame.from_dict(catchmentStagesDict)
        #dfCSD = pd.DataFrame.from_dict(catchmentStagesDict, hucSet)
        #dfCSD.to_csv (r'test9.csv', index=False, header=True) 

def read_nwm_forecast_file(forecast_file, rename_headers=True):
    """Reads NWM netcdf comp files and converts to forecast data frame"""

    flows_nc = xr.open_dataset(forecast_file, decode_cf='feature_id', engine='netcdf4')

    flows_df = flows_nc.to_dataframe()
    flows_df = flows_df.reset_index()

    flows_df = flows_df[['streamflow', 'feature_id']]

    if rename_headers:
        flows_df = flows_df.rename(columns={"streamflow": "discharge"})

    convert_dict = {'feature_id': str, 'discharge': float}
    flows_df = flows_df.astype(convert_dict)

    flows_df = flows_df.set_index('feature_id', drop=True)

    flows_df = flows_df.dropna()

    return flows_df
    

def create_src_subset_csv(hydro_table, catchmentStagesDict, src_table):
    src_df = pd.DataFrame.from_dict(catchmentStagesDict, orient='index')
    src_df = src_df.reset_index()
    src_df.columns = ['HydroID', 'stage_inund']
    htable_req_cols = ['HUC', 'feature_id', 'HydroID', 'stage', 'discharge_cms', 'LakeID']
    df_htable = pd.read_csv(
        hydro_table,
        dtype={
            'HydroID': int,
            'HUC': object,
            'branch_id': int,
            'last_updated': object,
            'submitter': object,
            'obs_source': object,
        },
        usecols=htable_req_cols,
    )
    df_htable = df_htable.merge(src_df, how='left', on='HydroID')
    df_htable['find_match'] = (df_htable['stage'] - df_htable['stage_inund']).abs()
    df_htable = df_htable.loc[df_htable.groupby('HydroID')['find_match'].idxmin()].reset_index(drop=True)
    df_htable.to_csv(src_table, index=False)



#write to CSV

catchmentStagesDict, hucSet  = subset_hydroTable_to_forecast(hydroTable, flowfile)

create_src_subset_csv(hydro_table, catchmentStagesDict, '.csv')



