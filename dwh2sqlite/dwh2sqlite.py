# -*- coding: utf-8 -*-

# %%
import os
import datetime
from io import StringIO
import time
import sqlite3
import requests
import pandas as pd


# %%
def get_config(station: str):
    if station == 'KENAI':
        par_short_names = {'prestas0': 'Luftdruck auf Barometerhöhe (QFE); Momentanwert (hPa 90)',
        'tre200s0':	'Lufttemperatur 2 m über Boden; Momentanwert (°C 91)',
        'rre150z0': 'Niederschlag; Zehnminutensumme (mm 93)',
        'fkl010z1': 'Böenspitze (Sekundenböe); Maximum (m/s 101)',
        'fkl010z0': 'Windgeschwindigkeit skalar; Zehnminutenmittel (m/s 196)',
        'dkl010z0': 'Windrichtung; Zehnminutenmittel (° 197)',
        'uor200s0': 'Original Luftfeuchtigkeit 2 m über Boden; Momentanwert (% 321)',
        'itosurr0': 'Bodenozon; 5 Minutenmittel (ppb 7600)',
        'ycoml1s0': 'Data linux deliver (y/n 7898)',
        'ycomirs0': 'iridum data deliver (y/n 7591)'}
        category = 1
        since = '20170330000000'
        # since = '20220330000000'
    if station == 'KEMKN':
        par_short_names = {'prestas0':	'Luftdruck auf Barometerhöhe (QFE); Momentanwert (hPa 90)',
        'tre200s0':	'Lufttemperatur 2 m über Boden; Momentanwert (°C 91)',
        'rre150z0': 'Niederschlag; Zehnminutensumme (mm 93)',
        'fkl010z1': 'Böenspitze (Sekundenböe); Maximum (m/s 101)',
        'ua2200s0': 'Vergleichsfeuchtigkeit 2 (% 118)',
        'ta1200s0': 'Vergleichstemperatur 1; z.T. Wetterhütte (°C 164)',
        'ta2200s0': 'Vergleichstemperatur 2; u.a. Temp. Hygrometer (°C 165)',
        'ra1150z0': 'Niederschlagssumme Vergleichsmessung automatisch (mm 178)',
        'ua1200s0': 'Vergleichsfeuchtigkeit 1; z.T. Wetterhütte (% 181)',
        'fkl010z0': 'Windgeschwindigkeit skalar; Zehnminutenmittel (m/s 196)',
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
def jretrieve_data(station, cfg=None, par_short_names=None, category=None, duration=None, since=None, till=None) -> dict:
    """
    Download data from DWH using jretrieve.

    Documentation: http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface/help


    :return: dict
    """
    try:
        # base_url = 'http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface?delimiter=,&placeholder=None'
        base_url = 'http://wlsdevt.meteoswiss.ch:9010/jretrievedwh/surface?delimiter=,&placeholder=None'
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
                #     raise('Either "duration" or "since" must be specified.')
                # else:
                    since = (datetime.datetime.now() - datetime.timedelta(days=duration)).strftime("%Y%m%d%H%M%S")
                till = time.strftime("%Y%m%d%H%M%S")
                base_url += f"&date={since}-{till}"
                base_url += f"&parameterShortNames={par_short_names}"
                base_url += f"&measCatNr={category}"
                urls.append(base_url)
                series = par_short_names.split(sep=',')
                labels = dict(zip(series, series))
        # else:
        #     if since is None:
        #         if duration:
        #             since = (datetime.datetime.now() - datetime.timedelta(days=duration)).strftime("%Y%m%d%H%M%S")
        #         else:
        #             since = cfg[station]['since']
        #     if till is None:
        #         till = time.strftime("%Y%m%d%H%M%S")
        #     base_url += "&date=%s-%s" % (since, till)

        #     params = cfg[station]['params']

        #     series = []
        #     labels = []
        #     for key in params.keys():
        #         url = base_url + "&parameterShortNames=%s" % params[key]['var']
        #         url += "&measCatNr=%s" % str(params[key]['cat'])
        #         urls.append(url)
        #         series.append(key)
        #         labels.append("%s (%s)" % (params[key]['lbl'], params[key]['var']))
        #     labels = dict(zip(series, labels))

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
            df = downcast_dataframe(df)

            # make sure df is sorted by date
            df.sort_index(inplace=True)

        return {'data': df, 'labels': labels}

    except Exception as err:
        print(err)


# %%
def main():
    root = os.path.expanduser('~/Documents/git/scratch/data')

    stations = ['KEMKN', 'KENAI']
    for station in stations:
        cfg = get_config(station)

        X = jretrieve_data(station=station, par_short_names=",".join(cfg['par_short_names'].keys()), category=cfg['category'], since=cfg['since'])

        if X['data'].empty:
            # try to load data from file
            file = os.path.join(root, "dwh", f"{station}.csv")
            df = pd.read_csv(file, na_values='None', parse_dates=['termin'], index_col=['termin'])
            df.index.rename('dtm', inplace=True)
            df.drop(columns='station', inplace=True)
            df = downcast_dataframe(df)
        else:
            df = X['data']

        if not df.empty:
            # create sqlite3 connection
            con = sqlite3.connect(os.path.join(root, 'data.sqlite'))

            # upload to sqlite db
            # tmp['mappings'].to_sql(name='mappings_%s' % os.path.basename(dpath), con=con, if_exists='replace')
            df.to_sql(name=f"dwh_{station}", con=con, if_exists='replace')

            # close sqlite3 connection
            con.close()

        else:
            print('An empty dataframe was returned.')
    print('done.')

# %%
if __name__ == '__main__':
    main()


# %%
