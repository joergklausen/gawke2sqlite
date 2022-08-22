# %%
import os
from datetime import datetime
from datetime import timedelta
import pandas as pd

# %%
def milos2df(fpath: str) -> pd.DataFrame:
    try:
        # fpath = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/001217.LOG"
        # fpath = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/031211.LOG"
        df = pd.DataFrame()
        if ".log" in fpath.lower():
            # read file content into pd.DataFrame
            dte = os.path.basename(fpath).split(".")[0]
            custom_date_parser = lambda x: datetime.strptime(f"{dte} {x}", "%y%m%d %H %M %S")
            
            df = pd.read_csv(fpath, sep=r"\s+", skiprows=1, header=None, na_values=['//', '///', '////', '/////'],
                parse_dates={"dtm": [0, 1, 2]}, date_parser=custom_date_parser, engine="python")

            # correct dtm: round up to full minute, make sure first entry is from previous day
            if df['dtm'][0].second == 59:
                df.loc[:, 'dtm'] = df.loc[:, 'dtm'] + timedelta(seconds=1)
            if df['dtm'][0] > df['dtm'][1]:
                df.loc[0, 'dtm'] = df.loc[0, 'dtm'] - timedelta(days=1)
            df.set_index('dtm', inplace=True)

            # drop last 2 columns
            df = df.iloc[:, :-2]

            # handle different formats
            if len(df.columns) == 11:
                # first column is surface ozone, include as at the end
                df = df.iloc[:, 1:12].merge(df.iloc[:, :1], on='dtm')
            elif len(df.columns) == 10:
                df['itosurs0'] = None

            # rename columns
            df.columns=["CO_raw", "tre200s0", "uor200s0", 
                "tde200s0", "prestas0", "dkl010s0", "fkl010s0", 
                "gor000s0", "ods000so", "dirrad", 'itosurs0']

        if ".csv" in fpath.lower():
            # TODO
            fpath = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/000127.csv"
            # fpath = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/010711.csv"
            # read file content into pd.DataFrame
            dte = os.path.basename(fpath).split(".")[0]
            # custom_date_parser = lambda x: datetime.strptime(f"{dte} {x}", "%y%m%d %H %M %S")
            df = pd.read_csv(fpath, sep=",", skiprows=3, header=None, engine="python")            
            # df = pd.read_csv(fpath, sep=",", skiprows=3, header=None, 
            #     parse_dates={"dtm": [1, 2, 3]}, date_parser=custom_date_parser, engine="python")
            cols=["year","month","day"]
            df['tmp'] = df[[1, 2, 3]].apply(lambda x: ':'.join(x.values.astype(str)), axis="columns")
            
            df['dte'] = dte
            df['dtm'] = pd.to_datetime(df['dte'].merge(df.iloc[:, 1:3], format=custom_date_parser))

            print(fpath)
        return df

    except Exception as err:
        print(err)


def extract_o3(df: pd.DataFrame, index=None, o3=None, aggregate=None) -> pd.DataFrame:
    """extract ozone readings from DataFrame and optionally aggregate.

    Args:
        df (pd.DataFrame): _description_
        index (pd.DataFrame): _description_. Defaults to None.
        o3 (pd.DataFrame): _description_. Defaults to None.
        aggregate (_type_, optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    try:
        if index is None:
            index = "dtm"
        if o3 is None:
            or = "itosurs0"
        
        df.reset_index(inplace=True)
        df = df.loc[:, [index, o3]]
        df.set_index(index, inplace=True)

        if aggregate:
            df.resample("10M").agg("mean")

        return df
    except Exception as err:
        print(err)
