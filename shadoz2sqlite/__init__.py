# -*- coding: utf-8 -*-

# %%
import os
from datetime import datetime
import zipfile
import df2sqlite
import tarfile
from dateutil. relativedelta import relativedelta
import pandas as pd
import requests


# %%
def download_archives(base_url: str, target_dir: str,
    begin_date: datetime, end_date=None, file_type="sfco3") -> list:
    if not end_date:
        end_date = datetime.today()
    if file_type not in ['sfco3', 'V06']:
        raise ValueError(f"file_type '{file_type}' not supported.")
    try:
        flist = []
        dtm = begin_date
        while dtm < end_date:
            if file_type == 'sfco3':
                fname = f"nairobi_{file_type}_{dtm.strftime('%Y%m')}"
                months = 1
            elif file_type == 'V06':
                fname = f"shadoz_nairobi_{dtm.strftime('%Y')}_{file_type}"
                months = 12
            for ext in (".tar", ".zip"):
                url = "".join([base_url, fname, ext])
                print(f"Downloading {url} ... ", end="")
                res = requests.get(url, allow_redirects=True)
                if res.ok:
                    fpath = os.path.join(target_dir, "".join([fname, ext]))
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    print(f"ok.\nSaving to {fpath}")
                    open(fpath, 'wb').write(res.content)
                    flist.append(fpath)
                else:
                    print("failed.")
            dtm = dtm + relativedelta(months=months)
        print("done.")

        return flist

    except Exception as err:
        print(err)

# %%
def extract_archive(fpath: str, include=None) -> list:
    try:
        if not include:
            include = [".txt", ".dat"]
        flist = []
        fname = os.path.basename(fpath)
        print(f"Processing {fname} ...")
        if ".tar" in fpath:
            tar = tarfile.open(fpath)
            for member in tar:
                if member.name[-4:] in include:
                    print(f" - Extracting {member.name} ...")
                    # tar.extractfile(member)
                    tar.extract(member, os.path.dirname(fpath))
                    flist.append(os.path.join(os.path.dirname(fpath), member.name))
            tar.close()
        elif ".zip" in fpath:
            with zipfile.ZipFile(fpath, 'r') as zfh:
                for fname in zfh.namelist():
                    print(fname)
                    if fname[-4:] in include:
                        zfh.extract(fname, os.path.dirname(fpath))
                        flist.append(os.path.join(os.path.dirname(fpath), fname))
        print('done.')
        return flist
    except Exception as err:
        print(err)

# %%
def extract_shadoz_file(fpath: str, remove_file=False) -> pd.DataFrame:
    try:
        # read file and determine number of header rows, file type
        with open(fpath, 'r', encoding='utf') as fh:
            data = fh.readlines()
        header_rows = int(data[0])
        shadoz_data_type = data[4].split(sep=": ")[1]
        if "01" in shadoz_data_type:
            df = pd.read_csv(fpath, sep=r"\s+", header=None,
                skiprows=header_rows, parse_dates=[[0, 1]],
                index_col=[0], engine="python")
            df.index.rename('dtm', inplace=True)
            df.rename(columns={2: "O3_ppb"}, inplace=True)
            df.reset_index()
        elif "06" in shadoz_data_type:
            launch_dtm = f"{data[12].split(sep=': ')[1].split()[0]} \
                {data[13].split(sep=': ')[1].split()[0]}"
            launch_dtm = datetime.strptime(launch_dtm, "%Y%m%d %H:%M:%S")
            names = data[header_rows - 2].split()
            units = data[header_rows - 1].split()
            df = pd.read_csv(fpath, sep=r"\s+", header=None,
                skiprows=header_rows, parse_dates=None,
                na_values=['9000.0', '9000.00', '9000.000', '9000.00000'],
                index_col=None, engine="python")
            # df.columns = [f"{x}_{y}" for x, y in zip(names, units)]
            df.columns = names
            df['dtm'] = launch_dtm + pd.to_timedelta(df['Time'], unit="s")
            df.set_index('dtm', inplace=True)
            df.reset_index()
        else:
            raise ValueError(f"Cannot read file of type {shadoz_data_type}")

        if remove_file:
            os.remove(fpath)

        return df

    except Exception as err:
        print(err)


# %%
ROOT_URL = "https://acd-ext.gsfc.nasa.gov/anonftp/acd"
SOURCE = "shadoz"
STATION = "nairobi"
GAWID = "nrb"
ROOT = os.path.expanduser("~/Documents/data")

# %% Surface ozone data NRB
# download data from SHADOZ repository
FILE_TYPE="sfco3"

begin_date = datetime(2012, 6, 1)
end_date = datetime(2022, 6, 30)
base_url = f"{ROOT_URL}/{SOURCE}/{FILE_TYPE.upper()}/{STATION}/"
target_dir = os.path.join(ROOT, SOURCE, FILE_TYPE)
os.makedirs(target_dir, exist_ok=True)

archives = []
for root, dnames, fnames in os.walk(target_dir):
    for fname in fnames:
        archives.append(os.path.join(root, fname))

if not archives:
    archives = download_archives(base_url, target_dir, \
        begin_date, end_date=end_date, file_type=FILE_TYPE)

# %%
# process tar archives, add data to sqlite db
db = os.path.join(ROOT, f"{GAWID}.sqlite")

for fpath in archives:
    flist = extract_archive(fpath)
    for fh in flist:
        df = extract_shadoz_file(fpath=fh, remove_file=True)
        res = df2sqlite.append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{FILE_TYPE}")
        print(res)

# %% sonde data NRB
# download data from SHADOZ repository
FILE_TYPE="V06"

begin_date = datetime(1998, 1, 1)
end_date = datetime(2022, 6, 30)
base_url = f"{ROOT_URL}/{SOURCE}/{FILE_TYPE.upper()}/{STATION}/"
target_dir = os.path.join(ROOT, SOURCE, FILE_TYPE)
os.makedirs(target_dir, exist_ok=True)

archives = []
for root, dnames, fnames in os.walk(target_dir):
    for fname in fnames:
        archives.append(os.path.join(root, fname))

if not archives:
    archives = download_archives(base_url, target_dir, \
        begin_date, end_date=end_date, file_type=FILE_TYPE)

# %%
# process zip archives, add data to sqlite db
db = os.path.join(ROOT, f"{GAWID}.sqlite")
for fpath in archives:
    flist = extract_archive(fpath)
    for fh in flist:
        df = extract_shadoz_file(fpath=fh, remove_file=True)
        res = df2sqlite.append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{FILE_TYPE}")
        print(res)

# %%
