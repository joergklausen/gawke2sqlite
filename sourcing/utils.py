# -*- coding: utf-8 -*-

import pandas as pd
import re
import requests

# %%
def downcast_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    try:
        dfx = df
        for column in dfx:
            if dfx[column].dtype == 'float64':
                dfx[column]=pd.to_numeric(dfx[column], downcast='float')
            if dfx[column].dtype == 'int64':
                dfx[column]=pd.to_numeric(dfx[column], downcast='integer')
        return dfx
    except Exception as err:
        print(err)
        return df


# %%
def get_urls_from_filebrowser(url: str, pattern=None, verbose=True) -> list:
    try:
        if pattern is None:
            pattern = r">(.+)</a>"
            raise Warning("All urls on page considered. This may yield more than you expect.")
        if verbose:
            msg = f'Fetching urls of files listed under {url} ...'
            print(msg)
        res = requests.get(url=url, proxies={"http": "", "https": ""}, \
                verify=False)
        
        fnames = re.findall(pattern=pattern, string=res.text)
        urls = ["/".join([url, fname]) for fname in fnames]
        return urls
    except Exception as err:
        print(err)


if __name__ == "__main__":
    pass
