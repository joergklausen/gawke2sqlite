# %%
from extract2df.bulletin2df import extract_bulletin_file

# %%
df = extract_bulletin_file(file="C:/Users/localadmin/Documents/git/gawke2sqlite/data/meteo/VRXA00.202305120520", pattern="VRXA00")

# %%
df = extract_bulletin_file(file="C:/Users/localadmin/Documents/git/gawke2sqlite/data/meteo/VMSW43.202206270900.zip", pattern="VRXA00")

# %%
