# -*- coding: utf-8 -*-

# %%
import os
from datetime import datetime
import itertools
import sqlite3
import nappy
import pandas as pd

# %%
# extract name, unit, statistic from long_names
def list2df(illformed_list, sep=',', expected_number_of_items=10):
    expanded_list = []
    for i, x in enumerate(illformed_list):
        try:
            expanded_list.append(x.split(sep)[0:expected_number_of_items])
        except:
            x = x + sep * expected_number_of_items
            expanded_list.append(x.split(sep)[0:expected_number_of_items])

    df = pd.DataFrame(expanded_list)
    df.rename(columns={0: 'variable', 1: 'unit'}, inplace=True)
    df.dropna(how='all', axis=1)
    return df


# %%
def extract_nasa_ames_file(file:str) -> pd.DataFrame:
    try:
        # open file and read data
        fh = nappy.openNAFile(file)
        fh.readData()

        # assign data, substitute missing values for None, convert to Pandas Dataframe
        X = fh.X
        V = fh.V
        na_values = fh.VMISS
        for row in range(len(na_values)):
            V[row] = [None if x == na_values[row] else x for x in V[row]]

        X = pd.DataFrame(X)
        X.columns = fh.XNAME
        V = pd.DataFrame(V).T
        V.columns = fh.VNAME

        df = X.merge(V, left_index=True, right_index=True)
        long_names = list2df(list(df.columns))
        long_names.rename(columns={0: 'long_name', 1: "unit"}, inplace=True)

        # assign short but unique column names
        df.columns = fh.getNADict()['NCOM'][-1].split()
        inc = itertools.count().__next__
        dups = df.columns[df.columns.duplicated()]
        df.rename(columns=lambda x: f"{x}_{inc()}" if x in dups else x, inplace=True)
        short_names = pd.DataFrame(df.columns)
        short_names.rename(columns={0: 'short_name'}, inplace=True)
        mappings = pd.concat([short_names, long_names], axis=1)

        # convert times to datetime
        epoch = datetime.strptime("%s-%s-%s" % tuple(fh.DATE), "%Y-%m-%d")
        df['dtm'] = epoch + pd.to_timedelta(round(df['starttime'] / fh.DX[0]), unit='H')
        df.set_index('dtm')
        res = dict(mappings=mappings, df=df)
        return res
    except Exception as err:
        print(err)
        return dict(mappings=pd.DataFrame(), df=pd.DataFrame())

# %%
# def extract_all_data_in_directory(root: str) -> None:
def main():
    root = os.path.expanduser('~/Documents/git/scratch/data')
    source = "ebas"
    # create sqlite3 connection
    con = sqlite3.connect(os.path.join(root, 'data.sqlite'))

    for dpath, dnames, fnames in os.walk(os.path.join(root, source)):
        df = pd.DataFrame()
        long_names = []
        for fname in fnames:
            if (not 'data.pkl' in fname) and (not 'data.sqlite' in fname):
                print(dpath, dnames, fname)
                file = os.path.join(dpath, fname)
                tmp = extract_nasa_ames_file(file=file)
                df = pd.concat([df, tmp['df']])
        if not df.empty:
            # pickle data and names
            # pd.to_pickle(dict(mappings=tmp['mappings'], df=df), os.path.join(dpath, 'data.pkl'))

            # upload to sqlite db
            tmp['mappings'].to_sql(name=f"{source}_mappings_{os.path.basename(dpath)}", con=con, if_exists='replace')
            df.to_sql(name=f"{source}_{os.path.basename(dpath)}", con=con, if_exists='replace')

    # combine ozone data as DB view
    qry = "CREATE VIEW 'V_O3' AS select dtm, O3_0 as 'O3_ug_m-3', O3_1 as 'sdO3_ug_m-3' from o3_legacy UNION select dtm, O3_0 as 'O3_ug_m-3', O3_2 as 'sdO3_ug_m-3' from o3"
    try:
        with con:
            con.execute(qry)
    except sqlite3.Error:
        print('sqlite3 error received. View not created (probably exists already).')

    # close sqlite3 connection
    con.close()

    print('done.')
    return

# %%
if __name__ == '__main__':
    main()
