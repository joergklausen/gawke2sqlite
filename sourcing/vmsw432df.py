# -*- coding: utf-8 -*-

# %%
import pandas as pd
from io import BytesIO
import re
import requests
import zipfile
from sourcing import utils

# %%
def extract_vmsw43_file(file: str, stream=None, verbose=True) -> pd.DataFrame:
    """
    Open a file, determine its type from the file name, then extract content into a Pandas dataframe.

    Args:
        file (str): full path to file.
        seconds (bool, optional): Should seconds remain in the timestamps? Defaults to 'False'.
    """
    try:
        msg = 'Extracting file %s.' % file
        if verbose:
            print(msg)
 
        df = pd.DataFrame()
        if bool(re.search('http[s]?://', file)):
            # only way to get rid of the 
            # InsecureRequestWarning: Unverified HTTPS request is being made to host 'hub.meteoswiss.ch'
            # very unfortunate ...
            requests.packages.urllib3.disable_warnings()
            res = requests.get(url=file, proxies={"http": "", "https": ""}, \
                    verify=False)
            if bool(re.search('VMSW43.+zip', file)):
                zf = zipfile.ZipFile(BytesIO(res.content))
                df = pd.read_csv(zf.open(zf.namelist()[0]), skiprows=1, header=1, sep=' ', na_values='/')
            elif bool(re.search('VMSW43.+001', file)):
                df = pd.read_csv(BytesIO(res.content), skiprows=1, header=1, sep=' ', na_values='/')
        else:
            if bool(re.search('VMSW43.+zip', file)):
                zf = zipfile.ZipFile(file)
                df = pd.read_csv(zf.open(zf.namelist()[0]), skiprows=1, header=1, sep=' ', na_values='/')
            else:
                df = pd.read_csv(file, skiprows=1, header=1, sep=' ', na_values='/')
            
        df["dtm"] = pd.to_datetime(df['zzzztttt'], format='%Y%m%d%H%M')
        df['source'] = file
        df.set_index("dtm", inplace=True)

        if not df.empty:
            df = utils.downcast_dataframe(df)
        return df

    except Exception as err:
        print(err)
        return pd.DataFrame()


if __name__ == "__main__":
    pass

# %%
