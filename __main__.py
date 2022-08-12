
# %%
import os
import pandas as pd
from sourcing import dwh2df
from sourcing import ebas2df
from sourcing import vmsw432df
from df2sqlite import df2sqlite
from sourcing import utils

# %%
# download data from DWH or load from file
ROOT = os.path.expanduser('~/Documents/data')
STATIONS = {"KEMKN": "mkn", "KENAI": "nrb"}

for dwh, gawid in STATIONS.items():
    print(dwh)
    df = dwh2df.dwh2df(dwh, path=None)
    res = df2sqlite.df2sqlite(df, db=os.path.join(ROOT, gawid), tbl=f"dwh_{dwh}")
print('done.')

# %%
# load from files downloaded from EBAS
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

# # %%
# # extract and load VMSW43 meteo bulletins
# file = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/VMSW43.200001010001.001"
# df = vmsw432df.extract_vmsw43_file(file)
# df.info()

# # %%
# url = "https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/MKN/incoming/meteo/VMSW43.202112052300.001"
# df = vmsw432df.extract_vmsw43_file(file=url)
# df.info()

# # %%
# url = "https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/MKN/incoming/meteo/VMSW43.202205101630.zip"
# df = vmsw432df.extract_vmsw43_file(file=url)
# df.info()

# %%
# process /incoming/meteo
ROOT = os.path.expanduser('~/Documents/data')
SOURCE = "meteo"
GAWID = "mkn"
db = os.path.join(ROOT, f"{GAWID.lower()}.sqlite")
url = f"https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/{GAWID.upper()}/incoming/{SOURCE}"
url = f"https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/{GAWID.upper()}/archive/2021/{SOURCE}"
url = f"https://hub.meteoswiss.ch/filebrowser/pay-data/data/pay/Kenya/{GAWID.upper()}/archive/2022/{SOURCE}"


files = utils.get_urls_from_filebrowser(url=url, pattern=r">(VMSW43.+.[zip|001])<")
msg = 'Downloading and extracting files from %s ...' % url
print(msg)

cnt = 1
for file in files:
    print(f"({cnt}/{len(files)})")
    df = vmsw432df.extract_vmsw43_file(file)
    res = df2sqlite.df2sqlite(df, db=db, tbl=f"{SOURCE}")
    cnt += 1

print('done.')


# %%
