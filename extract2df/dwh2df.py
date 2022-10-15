# -*- coding: utf-8 -*-

# %%
import os
import datetime
from io import StringIO
import time

import requests
import pandas as pd

# from extract2df import utils
from jklutils import downcast

# %%
def get_config(station: str):
    if station in ['KENAI', 'NRB']:
        par_short_names = {'prestas0': [1, 'Luftdruck auf Barometerhöhe (QFE); Momentanwert (hPa 90)'],
        'tre200s0':	['Lufttemperatur 2 m über Boden; Momentanwert (°C 91)'],
        'rre150z0': ['Niederschlag; Zehnminutensumme (mm 93)'],
        'fkl010z1': ['Böenspitze (Sekundenböe); Maximum (m/s 101)'],
        'fkl010z0': ['Windgeschwindigkeit skalar; Zehnminutenmittel (m/s 196)'],
        'dkl010z0': ['Windrichtung; Zehnminutenmittel (° 197)'],
        'uor200s0': ['Original Luftfeuchtigkeit 2 m über Boden; Momentanwert (% 321)'],
        'itosurr0': ['Bodenozon; 5 Minutenmittel (ppb 7600)'],
        'ycoml1s0': ['Data linux deliver (y/n 7898)'],
        'ycomirs0': ['iridum data deliver (y/n 7591)']}
        category = 1
        since = '20170330000000'
        # since = '20220330000000'
    if station in ['KEMKN', 'MKN']:
        par_short_names = {'prestas0':	['Luftdruck auf Barometerhöhe (QFE); Momentanwert (hPa 90)'],
        'tre200s0':	['Lufttemperatur 2 m über Boden; Momentanwert (°C 91)'],
        'rre150z0': ['Niederschlag; Zehnminutensumme (mm 93)'],
        'fkl010z1': ['Böenspitze (Sekundenböe); Maximum (m/s 101)'],
        'ua2200s0': ['Vergleichsfeuchtigkeit 2 (% 118)'],
        'ta1200s0': ['Vergleichstemperatur 1; z.T. Wetterhütte (°C 164)'],
        'ta2200s0': ['Vergleichstemperatur 2; u.a. Temp. Hygrometer (°C 165)'],
        'ra1150z0': ['Niederschlagssumme Vergleichsmessung automatisch (mm 178)'],
        'ua1200s0': ['Vergleichsfeuchtigkeit 1; z.T. Wetterhütte (% 181)'],
        'fkl010z0': ['Windgeschwindigkeit skalar; Zehnminutenmittel (m/s 196)'],
        'dkl010z0': 'Windrichtung; Zehnminutenmittel (° 197)',
        'gor000z0': 'Original Globalstrahlung; Zehnminutenmittel (W/m2 285)',
        'uor200s0': 'Original Luftfeuchtigkeit 2 m über Boden; Momentanwert (% 321)',
        'da1010z0': 'Windrichtung; Zehnminutenmittel; Vergleich Instrument 1 (° 757)',
        'fa1010z0': 'Windgeschwindigkeit skalar; Zehnminutenmittel; Vergleich Instrument 1 (m/s 1875)',
        'pa1stas0': 'Vergleichsdruck 1 auf Barometerhöhe (QFE); Momentanwert (hPa 7446)',
        'ycoml1s0': 'Data linux deliver (y/n 7898)',
        'ycomirs0': 'iridum data deliver (y/n 7591)'}
        category = 1
        since = '20160101000000'
    return {'par_short_names': par_short_names,'category': category, 'since': since}



# %%
def jretrieve_data(station, cfg=None, par_short_names=None, category=None, duration=None, since=None, till=None) -> dict:
    """
    Download data from DWH using jretrieve.

    Documentation: http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface/help


    :return: dict
    """
    try:
        # base_url = 'http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface?delimiter=,&placeholder=None'
        base_url = 'http://wlsdepl.meteoswiss.ch:9010/jretrievedwh/surface?delimiter=,&placeholder=None'
        base_url += f"&locationIds={station}"

        urls = []
        df = pd.DataFrame()

        if cfg is None:
            if par_short_names is None:
                raise Exception('"par_short_names" not specified.')
            if category is None:
                raise Exception('"category" not specified.')
            if duration is None:
                if since is None:
                    raise('Either "duration" or "since" must be specified.')
            else:
                if since is None:
                    since = (datetime.datetime.now() - datetime.timedelta(days=duration)).strftime("%Y%m%d%H%M%S")
                if till is None:
                    till = time.strftime("%Y%m%d%H%M%S")
                if since > till:
                    base_url += f"&date={till}-{since}"
            base_url += f"&date={since}-{till}"
            base_url += f"&parameterShortNames={par_short_names}"
            base_url += f"&measCatNr={category}"
            urls.append(base_url)
            series = par_short_names.split(sep=',')
            labels = dict(zip(series, series))
        else:
            if since is None:
                if duration:
                    since = (datetime.datetime.now() - datetime.timedelta(days=duration)).strftime("%Y%m%d%H%M%S")
                else:
                    since = cfg[station]['since']
            if till is None:
                till = time.strftime("%Y%m%d%H%M%S")
            base_url += "&date=%s-%s" % (since, till)

            params = cfg['par_short_names']

            # series = []
            # labels = []
            url = base_url + f"&parameterShortNames={','.join(params.keys())}"
            url += "&measCatNr=%s" % str(cfg['category'])
            urls.append(url)
            # series.append(key)
            # labels.append("%s (%s)" % (params[key]['lbl'], params[key]['var']))
            labels = params

        for url in urls:
            print(f"Calling {url} ...")
            t0 = time.time()
            # res = requests.get(url='https://www.google.com', verify=False)
            # TODO: use proper SSL verification, remove verify0false
            res = requests.get(url=url, proxies={"http": "", "https": ""}, verify=False)
            if res.status_code == 200:
                print(f"Finished downloading in {str(time.time() - t0)} seconds.")
                # return res.text as Pandas dataframe, convert date/time
                if df.empty:
                    df = pd.read_csv(StringIO(res.text), na_values='None', parse_dates=['termin'], index_col=['termin'])
                    df.drop(columns='station', inplace=True)
                    # if drop_null:
                    #     df = df[df.columns[~df.isnull().all()]]
                else:
                    df2 = pd.read_csv(StringIO(res.text), na_values='None', parse_dates=['termin'],
                                                index_col=['termin'])
                    df2.drop(columns='station', inplace=True)
                    df = df.merge(df2, how='outer', left_index=True, right_index=True)
                    # if drop_null:
                    #     df = df[df.columns[~df.isnull().all()]]
            else:
                print(f"Request unsuccessful, returned {res.status_code}.")

        if not df.empty:
            df.index.rename('dtm', inplace=True)
            df.columns = series

            ## downcast data to reduce size of dataframe
            df = downcast.downcast_dataframe(df)

            # make sure df is sorted by date
            df.sort_index(inplace=True)

        return {'data': df, 'labels': labels}

    except Exception as err:
        print(err)


# %%
def climap2df(path: str) -> dict:
    try:
        if ".dat" in path:
            # path ="C:/Users/localadmin/Documents/git/gawke2sqlite/data/KEMKN2020.dat"
            with open(path, 'r', encoding='ascii') as fh:
                data = fh.readlines()
    except Exception as err:
        print(err)


# %%
def dwh2df(cfg, station=None, path=None, labels=None, since=None, till=None) -> dict:
    try:
        if path:
            # try to load data from file
            if ".csv" in path:
                df = pd.read_csv(path, na_values='None', parse_dates=['termin'], index_col=['termin'])
            elif ".dat" in path:
                df = pd.read_csv(path, na_values='None', parse_dates=['termin'], index_col=['termin'])
            df.index.rename('dtm', inplace=True)
            df.drop(columns='station', inplace=True)
            df = downcast.downcast_dataframe(df)    
            X = {'data': df, 'labels': labels}
        elif station:
            if since is None:
                since = cfg['since']
            X = jretrieve_data(station=station, cfg=cfg, par_short_names=",".join(cfg['par_short_names'].keys()), category=cfg['category'], since=since, till=till)
        else:
            raise ValueError("One of 'station' or 'path' must be specified.")
        if labels:
            return X
        else:
            return X['data']

    except Exception as err:
        print(err)

# %%
if __name__ == '__main__':
    pass
