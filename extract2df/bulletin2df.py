# -*- coding: utf-8 -*-

# %%
# import os
import pandas as pd
import logging
# from io import BytesIO
import re
# import requests
import zipfile
# from jklutils import utils


# %%
def extract_bulletin_file(file: str, pattern: str, log=True) -> pd.DataFrame:
    """
    Open a file, determine its type from the file name, then extract content into a Pandas dataframe.

    Args:
        file (str): full path to file.
        pattern (str): should be one of "VMSW43" or "VRXA00"
        replace (bln): 
    """
    try:
        if log:
            logging.basicConfig(level=logging.INFO, filename="bulletin.log", filemode="a")
        
        msg = f"Extracting file {file}."
        if log:
            logging.info(msg)
 
        df = pd.DataFrame()
        if bool(re.search(f'{pattern}', file)):
            if bool(re.search('.zip', file)):
                zf = zipfile.ZipFile(file)
                file = zf.open(zf.namelist()[0])
        df = pd.read_csv(file, skiprows=1, header=1, sep=' ', na_values='/')
        df["dtm"] = pd.to_datetime(df['zzzztttt'], format='%Y%m%d%H%M')
        df['source'] = file
        df.set_index("dtm", inplace=True)

        if not df.empty:
            for column in df:
                if df[column].dtype == 'float64':
                    df[column] = pd.to_numeric(df[column], downcast='float')
                if df[column].dtype == 'int64':
                    df[column] = pd.to_numeric(df[column], downcast='integer')
        return df

    except Exception as err:
        logging.error(err)
        return pd.DataFrame()

# def extract_bulletin_file(file: str, pattern: str, replace=None, target=None, verbose=True) -> pd.DataFrame:
#     """
#     Open a file, determine its type from the file name, then extract content into a Pandas dataframe.

#     Args:
#         file (str): full URL or path to file.
#         pattern (str): should be one of "VMSW43" or "VRXA00"
#     """
#     try:
#         if target:
#             os.makedirs(target, exist_ok=True)
#             fname = os.path.join(target, os.path.basename(file))
#             if replace:
#                 fname = fname.replace(pattern, replace).replace(".zip", ".001")

#         msg = f"Extracting file {file}."
#         if verbose:
#             print(msg)
 
#         df = pd.DataFrame()
#         if bool(re.search('http[s]?://', file)):
#             # only way to get rid of the 
#             # InsecureRequestWarning: Unverified HTTPS request is being made to host 'hub.meteoswiss.ch'
#             # very unfortunate ...
#             requests.packages.urllib3.disable_warnings()
#             res = requests.get(url=file, proxies={"http": "", "https": ""}, \
#                     verify=False)
#             if bool(re.search(f'{pattern}.+zip', file)):
#                 zf = zipfile.ZipFile(BytesIO(res.content))
#                 bulletin = zf.open(zf.namelist()[0])
#                 df = pd.read_csv(bulletin, skiprows=1, header=1, sep=' ', na_values='/')
#                 if target:
#                     bulletin = zf.open(zf.namelist()[0]).read().decode()
#             elif bool(re.search(f'{pattern}.+001', file)):
#                 bulletin = BytesIO(res.content)
#                 df = pd.read_csv(bulletin, skiprows=1, header=1, sep=' ', na_values='/')
#                 if target:
#                     bulletin = BytesIO(res.content).read().decode()
#         else:
#             if bool(re.search(f'{pattern}.+zip', file)):
#                 zf = zipfile.ZipFile(file)
#                 bulletin = zf.open(zf.namelist()[0])
#                 df = pd.read_csv(bulletin, skiprows=1, header=1, sep=' ', na_values='/')
#                 if target:
#                     bulletin = zf.open(zf.namelist()[0]).read().decode()
#             elif bool(re.search(f'{pattern}.+001', file)):
#                 df = pd.read_csv(file, skiprows=1, header=1, sep=' ', na_values='/')
#                 if target:
#                     with open(file, "r") as fh:
#                         bulletin = fh.read()

#         if target:
#             os.makedirs(target, exist_ok=True)
#             fname = os.path.join(target, os.path.basename(file)).replace(".zip", ".001")
#             if replace:
#                 # replace pattern in bulletin and store bulletin with new name at target
#                 fname = fname.replace(pattern, replace)
#                 with open(os.path.join(target, fname), 'w', encoding='utf8') as fh:
#                     fh.write(bulletin.replace(pattern, replace))
#             print(f"File saves as {fname}")

#         df["dtm"] = pd.to_datetime(df['zzzztttt'], format='%Y%m%d%H%M')
#         df['source'] = file
#         df.set_index("dtm", inplace=True)

#         if not df.empty:
#             df = utils.downcast_dataframe(df)
#         return df

#     except Exception as err:
#         print(err)
#         return pd.DataFrame()


if __name__ == "__main__":
    pass

# %%
