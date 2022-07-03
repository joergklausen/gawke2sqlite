# -*- coding: utf-8 -*-
"""
Fix meteo bulletins with incorrect filename and datetime stamp found on Minix.

Author: joerg.klausen@meteoswiss.ch
"""

# %%
# rename meteo files based on create date
# Background: the Lufft Linuxbox does not set its internal clock correctly after
# re-boot w/o internet. Fortunately, the ftp transfer to the Minix still works and
# its internal clock works pretty well. Thus, the create date of the file on the
# Minix can be used to deduce the correct filename of the files.
# Approach: On the Minix
# cd /mkndaq/data
# dir * -tc > created.txt
import os
import pandas as pd

# read file with create dates and names to Pandas dataframe
LOCAL_PATH = "C:/Users/localadmin/Documents/git/scratch/data/minix/meteo"
df = pd.read_csv(os.path.join(LOCAL_PATH, "created.txt"), header=None, skiprows=10, sep=r"\s+",
    skipfooter=5, engine="python", parse_dates=[[0, 1]],
    infer_datetime_format=True)

# extract old datetimestamp from existing file name
df['old_dtm'] = df[3].str.extract(r'(\d{12})')

# construct new filenames based on create date
df['new_dtm'] = df['0_1'].dt.round("600S").dt.strftime("%Y%m%d%H%M")
df['new_name'] = "VMSW43." + df['new_dtm'] + ".001"

# open each file and replace old datetime stamp with corrected one, save file
NEW_PATH = os.path.join(LOCAL_PATH, "corrected")

for index, row in df.iterrows():
    try:
        with open(os.path.join(LOCAL_PATH, row[3]), 'r', encoding='utf8') as fh:
            data = fh.read()
            data_corrected = data.replace(row['old_dtm'], row['new_dtm'])
        with open(os.path.join(NEW_PATH, row['new_name']), 'w', encoding='utf8') as fh:
            fh.write(data_corrected)
    except Exception as err:
        print(err)
