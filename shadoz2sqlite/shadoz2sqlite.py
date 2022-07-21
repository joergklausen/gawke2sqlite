# SHADOZ surface ozone data NRB

# %%
from datetime import datetime
 shadoz2sqlite

# %%
root_url = "https://acd-ext.gsfc.nasa.gov/anonftp/acd/"
source = "shadoz"
file_type="sfco3"
station = "nairobi"

root = os.path.expanduser("~/Documents/git/scratch/data")

target_dir = os.path.join(root, source, file_type)
os.makedirs(target_dir, exist_ok=True)

# %%
# download data from SHADOZ repository
base_url = f"{root_url}/{source}/{file_type}/{station}"
begin_date = datetime(2012, 6, 1)
end_date = datetime(2012, 9, 30)

tars = download_shadoz_tars(base_url, target_dir, begin_date, end_date=end_date, file_type=file_type)

# %%
# process tar archives
db = os.path.join(root, "data.sqlite")
for fpath in tars:
    flist = extract_shadoz_tar(fpath)
    for fh in flist:
        df = extract_shadoz_sfco3_file(fpath=fh)
        res = append_to_sqlite_db(df, db, tbl=f"{source}_{file_type}")
        print(res)


# %% sonde data NRB
root = "https://acd-ext.gsfc.nasa.gov/anonftp/acd/shadoz/V06/nairobi/"
# shadoz_nairobi_1998_V06.zip


# %%
