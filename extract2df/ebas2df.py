# -*- coding: utf-8 -*-

# %%
import os
from datetime import datetime
import itertools
import nappy
import pandas as pd
from jklutils import utils

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

        df = utils.downcast_dataframe(df)
        res = dict(mappings=mappings, df=df)
        return res

    except Exception as err:
        print(err)
        return dict(mappings=pd.DataFrame(), df=pd.DataFrame())


# %%
if __name__ == '__main__':
    pass
