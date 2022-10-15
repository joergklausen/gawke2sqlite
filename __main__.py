
# %%
import os
import pandas as pd
from extract2df import bulletin2df
from extract2df import dwh2df
from extract2df import ebas2df
from extract2df import milos2df
# from extract2df import utils
from convert import milos2vrxa00
# from df2sqlite import df2sqlite

from jklutils import mchfilebrowser, df2sqlite

# %%
# download data from DWH or load from file
def download_from_dwh():
    ROOT = os.path.expanduser('~/Documents/data')
    STATIONS = {"KEMKN": "mkn", "KENAI": "nrb"}

    for dwh, gawid in STATIONS.items():
        print(dwh)
        df = dwh2df.dwh2df(dwh, path=None)
        res = df2sqlite.df2sqlite(df, db=os.path.join(ROOT, "".join([gawid, ".sqlite"])), tbl=f"dwh_{dwh}")

        df = dwh2df.dwh2df(dwh, path=os.path.join(ROOT, "dwh", ".".join([dwh, ".csv"])))
        res = df2sqlite.df2sqlite(df, db=os.path.join(ROOT, "".join([gawid, ".sqlite"])), tbl=f"dwh_{dwh}")
    print('done.')

# %%
# load from files downloaded from EBAS
def download_from_ebas():
    ROOT = os.path.expanduser('~/Documents/data')
    SOURCE = "ebas"
    GAWID = "mkn"

    for dpath, dnames, fnames in os.walk(os.path.join(ROOT, SOURCE)):
        df = pd.DataFrame()
        long_names = []
        for fname in fnames:
            print(dpath, dnames, fname)
            file = os.path.join(dpath, fname)
            tmp = ebas2df.extract_nasa_ames_file(file=file)
            df = pd.concat([df, tmp['df']])
        if not df.empty:
            # upload to sqlite db
            res = df2sqlite.df2sqlite(df=tmp['mappings'], 
                db=os.path.join(ROOT, GAWID), 
                tbl=f"{SOURCE}_mappings_{os.path.basename(dpath)}")
            # tmp['mappings'].to_sql(name=f"{source}_mappings_{os.path.basename(dpath)}", con=con, if_exists='replace')
        
            res = df2sqlite.df2sqlite(df, db=os.path.join(ROOT, GAWID), tbl=f"{SOURCE}_{os.path.basename(dpath)}")
            # df.to_sql(name=f"{source}_{os.path.basename(dpath)}", con=con, if_exists='replace')

# combine ozone data as DB view
# qry = "DROP VIEW 'V_O3'; CREATE VIEW 'V_O3' AS select dtm, O3_0 as 'O3_ug_m-3', O3_1 as 'sdO3_ug_m-3' from o3_legacy UNION select dtm, O3_0 as 'O3_ug_m-3', O3_2 as 'sdO3_ug_m-3' from o3"

# %%
def process_meteo_bulletins():
    ROOT = os.path.expanduser('~/Documents/data')
    SOURCE = "meteo"
    GAWID = "mkn"
    DB = os.path.join(ROOT, f"{GAWID.lower()}.sqlite")
    OLD_NAMES = "VMSW43"
    NEW_NAMES = "VRXA00"
    TARGET = os.path.join(ROOT, NEW_NAMES)
    BASE_URL = f"https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/{GAWID.upper()}/"
    URLS = [f"{BASE_URL}archive/2021/{SOURCE}", 
        f"{BASE_URL}archive/2022/{SOURCE}", 
        f"{BASE_URL}incoming/{SOURCE}"]

    # Download and rename all existing VMSW43 bulletins from MKN
    for url in URLS:
        # process bulletins with old names
        files = mchfilebrowser.get_urls_from_filebrowser(url=url, pattern=rf">({OLD_NAMES}.+.[zip|001])<")
        msg = 'Downloading and extracting files from %s ...' % url
        print(msg)

        cnt = 1
        for file in files:
            print(f"({cnt}/{len(files)})")
            df = bulletin2df.extract_bulletin_file(file, pattern=OLD_NAMES, replace=NEW_NAMES, target=TARGET)
            res = df2sqlite.df2sqlite(df, db=DB, tbl=f"{SOURCE}")
            cnt += 1

        # process bulletins with new names
        files = mchfilebrowser.get_urls_from_filebrowser(url=url, pattern=rf">({NEW_NAMES}.+.[zip|001])<")
        msg = 'Downloading and extracting files from %s ...' % url
        print(msg)

        cnt = 1
        for file in files:
            print(f"({cnt}/{len(files)})")
            df = bulletin2df.extract_bulletin_file(file, pattern=NEW_NAMES)
            res = df2sqlite.df2sqlite(df, db=DB, tbl=f"{SOURCE}")
            cnt += 1

        print('done.')


# %%
def process_milos_files():
    ROOT = "C:/Users/localadmin/Documents/data"
    DB = os.path.join(ROOT, "mkn.sqlite")
    SOURCE = "milos"
    DWH_STATION_ID = 187
    TARGET = os.path.join(ROOT, "vrxa00")

    # %%
    for root, dirs, files in os.walk(os.path.join(ROOT, SOURCE)):
        # print(f"{root}, {dirs}, {files}")
        for file in files:
            fpath = os.path.join(root, file)
            df = milos2df.milos2df(fpath)

            # extract and aggregate ozone values
            o3 = milos2df.extract_o3(df=df, aggregate="10M")
            df2sqlite.df2sqlite(df, db=DB, tbl=f"{SOURCE}_o3")
            
            df2sqlite.df2sqlite(df, db=DB, tbl=SOURCE)
            milos2vrxa00.df2vrxa00(df, dwh_station_id=DWH_STATION_ID, target=os.path.join(TARGET, f"VRXA00.{file}.001"))
    print("done.")

# %%
# uncomment any of the following and execute script
# download_from_dwh
# download_from_ebas
# process_meteo_bulletins
ROOT = os.path.expanduser('~/Documents/data')
GAWID = 'MKN'
DWH = 'KEMKN'
cfg = dwh2df.get_config(DWH)

df = dwh2df.dwh2df(cfg=cfg, station=DWH, since='202209010000')
res = df2sqlite.df2sqlite(df, db=os.path.join(ROOT, "".join([GAWID, ".sqlite"])), tbl=f"dwh_{DWH}")
