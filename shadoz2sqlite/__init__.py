# -*- coding: utf-8 -*-

# %%
import os
from datetime import datetime
from dateutil. relativedelta import relativedelta
import sqlite3
import pandas as pd
import requests
import tarfile


# %%
def download_shadoz_tars(base_url: str, target_dir: str, begin_date: datetime, end_date=None, file_type="sfco3") -> list:  
    if not end_date:
        end_date = datetime.today()
    if file_type not in ['sfco3', 'V06']:
        raise ValueError(f"file_type '{file_type}' not supported.")
    try:
        flist = []
        dtm = begin_date
        while dtm < end_date:
            fname = f"nairobi_{file_type}_{dtm.strftime('%Y%m')}.tar"
            url = "".join([base_url, fname])
            print(f"Downloading {url} ... ", end="")
            res = requests.get(url, allow_redirects=True)
            if res.ok:
                fpath = os.path.join(target_dir, fname)
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                print(f"ok.\nSaving to {fpath}")
                open(fpath, 'wb').write(res.content)
                flist.append(fpath)
            else:
                print("failed.")
            dtm = dtm + relativedelta(months=1)
        print("done.")
        
        return flist

    except Exception as err:
        print(err)

# %%
def extract_shadoz_tar(fpath: str, include=[".txt"]) -> list:
    try:
        if ".tar" in fpath:
            flist = []
            fname = os.path.basename(fpath)
            print(f"Processing {fname} ...")
            tar = tarfile.open(fpath)
            # members = tar.getmembers()
            for member in tar:
                if member.name[-4:] in include:
                    print(f" - Extracting {member.name} ...")
                    # tar.extractfile(member)
                    tar.extract(member, os.path.dirname(fpath))
                    flist.append(os.path.join(os.path.dirname(fpath), member.name))
            tar.close()
        print('done.')
        return flist
    except Exception as err:
        print(err)

# %%
def extract_shadoz_sfco3_file(fpath: str, remove_file=False) -> pd.DataFrame:
    try:
        # read file and determine number of header rows, file type
        with open(fpath, 'r', encoding='utf') as fh:
            data = fh.readlines()
        header_rows = int(data[0])
        shadoz_data_type = data[1]
        if "Surface Ozone Measurements" in shadoz_data_type:
            df = pd.read_csv(fpath, sep="\s", header=None, 
                skiprows=header_rows, parse_dates=[[0, 1]], 
                index_col=[0], engine="python")
            df.index.rename('dtm', inplace=True)
            df.rename(columns={2: "O3_ppb"}, inplace=True)
            df.reset_index()

            
            os.remove(fpath)

            return df
        else:
            return pd.DataFrame()

    except Exception as err:
        print(err)


# %%
def append_to_sqlite_db(df: pd.DataFrame, db: str, tbl: str, remove_duplicates=True) -> dict:
    try:
        if df.empty:
            raise ValueError("DataFrame is empty.")

        records_for_insert = len(df)

        con = sqlite3.connect(db)        

        qry_count_records = f"SELECT count({df.index.name}) from {tbl}"
        records_before_insert = con.execute(qry_count_records).fetchone()[0]

        print(f"Inserting {records_for_insert} rows to {db}[{tbl}] ...")
        df.to_sql(name=tbl, con=con, if_exists="append")
        records_after_insert = con.execute(qry_count_records).fetchone()[0]

        if (records_after_insert - records_before_insert) < records_for_insert:
            raise Warning("Could not insert all values.")

        if remove_duplicates:
            group_by = ", ".join(list(df.index.names) + list(df.columns))
            qry = f"DELETE FROM {tbl} WHERE ROWID NOT IN (SELECT min(ROWID) FROM {tbl} GROUP BY {group_by})"
            records_after_deduplication = con.execute(qry_count_records).fetchone()[0]

        res = {"records_inserted": records_for_insert, 
            "duplicate_records": records_after_insert - records_after_deduplication}

        return res
    except Exception as err:
        print(err)


# %%
root_url = "https://acd-ext.gsfc.nasa.gov/anonftp/acd"
source = "shadoz"
station = "nairobi"
file_type="sfco3"

root = os.path.expanduser("~/Documents/git/scratch/data")

target_dir = os.path.join(root, source, file_type)
os.makedirs(target_dir, exist_ok=True)

# %%
# download data from SHADOZ repository
base_url = f"{root_url}/{source}/{file_type.upper()}/{station}/"
begin_date = datetime(2012, 6, 1)
end_date = datetime(2022, 6, 30)

tars = download_shadoz_tars(base_url, target_dir, begin_date, end_date=end_date, file_type=file_type)

# %%
# process tar archives, add data to sqlite db
db = os.path.join(root, "data.sqlite")
for fpath in tars:
    flist = extract_shadoz_tar(fpath)
    for fh in flist:
        df = extract_shadoz_sfco3_file(fpath=fh)
        res = append_to_sqlite_db(df, db, tbl=f"{source}_{file_type}")
        print(res)


# %% 
# SHADOZ sonde data NRB
file_type="V06"


# %%
