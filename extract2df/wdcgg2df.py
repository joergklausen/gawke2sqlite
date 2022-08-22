# -*- coding: utf-8 -*-

# %%
import os
from datetime import datetime
from typing import Tuple
import shutil
import sqlite3
import tarfile
from dateutil. relativedelta import relativedelta
import pandas as pd


# %%
def extract_archive(fpath: str, include=None) -> list:
    try:
        if not include:
            include = [".txt"]
        flist = []
        fname = os.path.basename(fpath)
        print(f"Processing {fname} ...")
        if ".tar" in fpath:
            tar = tarfile.open(fpath)
            for member in tar:
                if member.name[-4:] in include and 'WDCGG' not in member.name:
                    print(f" - Extracting {member.name} ...")
                    # tar.extractfile(member)
                    tar.extract(member, os.path.join(os.path.dirname(fpath), 'tmp'))
                    flist.append(os.path.join(os.path.dirname(fpath), 'tmp', member.name))
            tar.close()
        return flist
    except Exception as err:
        print(err)

# %%
def extract_wdcgg_file(fpath: str, remove_file=False) -> Tuple[str, pd.DataFrame]:
    try:
        # read file and determine number of header rows, file type
        with open(fpath, 'r', encoding='utf') as fh:
            data = fh.readlines()
        try:
            header_lines = int(data[0].split(sep=": ")[1])
        except:
            header_lines = 1

        if header_lines > 1:
            # extract GAWID of station
            index = [i for i, ele in enumerate(data) if 'site_gaw_id' in ele][0]
            gaw_id = data[index].split(sep=": ")[1].split('\n')[0]
            
            # extract species
            index = [i for i, ele in enumerate(data) if 'dataset_parameter_name_1' in ele][0]
            species = data[index].split(sep=": ")[1].split('\n')[0]

            # extract data type
            index = [i for i, ele in enumerate(data) if 'dataset_project' in ele][0]
            data_type = data[index].split(sep=": ")[1].split(sep="-")[1].split('\n')[0]

            # extract data set type
            index = [i for i, ele in enumerate(data) if 'dataset_selection_tag' in ele][0]
            data_set_type = data[index].split(sep=": ")[1].split('\n')[0]
        else:
            # assume insitu data and extract other stuff from filename
            data_type = 'insitu'
            items = os.path.basename(fpath).split(sep="_")
            gaw_id = items[0]
            data_set_type = items[1]
            species = items[2].upper()


        # read actual data
        df = pd.read_csv(fpath, sep=r"\s+", header=None,
                skiprows=header_lines, parse_dates={'dtm': [1, 2, 3, 4, 5, 6]},
                na_values=['-999', '-9', '-99.9', '-999.999'],
                engine="python")
        if header_lines > 1:
            cols = data[header_lines-1].split()[1:]
        else:
            cols = data[header_lines-1].split()
        
        for i in range(6):
            cols.pop(1)
        df['dtm'] = pd.to_datetime(df['dtm'], format="%Y %m %d %H %M %S")
        df.columns = ['dtm'] + cols

        if remove_file:
            os.remove(fpath)

        if 'MET' in species:
            species = 'MET'
            df2 = df[['dtm', 'wind_direction', 'wind_speed', 'relative_humidity', \
                'precipitation_amount', 'air_pressure', 'air_temperature']]
        else:
            df2 = df[['dtm', 'value', 'value_unc', 'QCflag']]
        df2.set_index('dtm', inplace=True)

        description = "_".join([gaw_id, species, data_type, data_set_type])

        return description, df2

    except Exception as err:
        print(err)



# %%
if __name__ == "__main__":
    pass

fpath = "C:/Users/localadmin/Documents/git/scratch/data/wdcgg/txt/WDCGG_20220805041645.tar.gz"
ROOT = os.path.expanduser("~/Documents/data")
SOURCE = "wdcgg"
GAWID = "mkn"
target_dir = os.path.join(ROOT, SOURCE, "txt")
os.makedirs(target_dir, exist_ok=True)
db = os.path.join(ROOT, f"{GAWID}.sqlite")

# %%
# process tar archives, add data to sqlite db
archives = []
for root, dnames, fnames in os.walk(target_dir):
    for fname in fnames:
        archives.append(os.path.join(root, fname))

# %%
for fpath in archives:
    flist = extract_archive(fpath)
    for fh in flist:
        description, df = extract_wdcgg_file(fpath=fh, remove_file=True)
        # res = append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{gaw_id}_{species}_{data_type}")
        res = append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{description}")
        print(res)

# clean up
shutil.rmtree(os.path.join(target_dir, 'tmp'))

# %%
# process header-less files from Empa (received via e-mail from Martin S.)
target_dir = os.path.join(ROOT, "empa")
data_files = []
for root, dnames, fnames in os.walk(target_dir):
    for fname in fnames:
        if 'MKN' in fname:
            data_files.append(os.path.join(root, fname))

# %%
for fh in data_files:
    description, df = extract_wdcgg_file(fpath=fh, remove_file=False)
    # res = append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{gaw_id}_{species}_{data_type}")
    res = append_to_sqlite_db(df, db, tbl=f"{SOURCE}_{description}")
    print(res)
