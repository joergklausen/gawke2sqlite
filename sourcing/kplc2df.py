# -*- coding: utf-8 -*-

# %%
import os
import datetime
import json
import pandas as pd
import matplotlib.pyplot as plt
#from pandas.plotting import register_matplotlib_converters
#register_matplotlib_converters()
import re
import requests
#import xlrd


def download_kplc_smartmeter_load_profile(usr: str, pwd: str, from_date: str, to_date: str, verbose: bool = True) -> json:
    """
    Download KPLC smartmeter data from_date their website.
    
    Scrape KPLC website: login and execute query for load profile data. These
    include voltages and currents for 3 all phases.
    
    Parameters
    ----------
    usr : str
        Account number
    pwd : str
        pwd
    from_date : str
        Beginning of data period, to_date be specified as yyyy-mm-dd
    to_date_date : str
        End of data period, to_date be specified as yyyy-mm-dd
        
    Returns
    -------
    JSON object in native format
    """
    try:
        with requests.session() as session:
        
            # call login page with credentials in payload
            url = "http://41.203.223.137:9090/eup/login!login.do"   
            data = {
                "czyId": usr, 
                "pwd": pwd, 
                "lang": "en_US"
            }
            result = session.post(url, data = data)            
            if verbose:
                print("Login successful!")
            if result.content == b'ok':
                
                # retrieve session cookies
                url = "http://41.203.223.137:9090/eup/login!loginSuccess.do"   
                result = session.get(url)                
                if result.status_code == 200: 
                    
                    # execute query
                    url = "http://41.203.223.137:9090/eup/eup/fhqx/fhqx!query.do?"    
                    data = {
                        "start": "0",
                        "limit": "100000",
                        "hh": usr,
                        "czy": usr,
                        "opp": "0",
                        "cdid": "9",
                        "ksrq": from_date,
                        "jsrq": to_date
                    }
                    result = session.post(url, data = data, headers = dict(referer = url))
                
            if result.status_code == 200: 
                # convert byte object to json
                result = json.loads(result.content.decode())
                
                if verbose:
                        print("{} rows successfully downloaded.".format(result["rows"]))

                result = pd.read_json(json.dumps(result['result']))

                # rename columns to match XLS download feature
                # download mappings of column header names
                url = "http://41.203.223.137:9090/eup/js/locale/eupModule/fhqx/fhqx_en_US.js"                    
                column_headers = session.get(url)
                column_headers = column_headers.content.decode()
                column_headers = re.sub("fhqx_title_|'|\r\n", "", column_headers)
                column_headers = re.split("=|;", column_headers)
                column_headers.pop()                
                column_headers = dict(zip(column_headers[::2], column_headers[1::2]))
                column_headers = {k.upper(): v for k, v in column_headers.items()}

                result.rename(columns=column_headers, inplace=True)
                result['dtm'] = pd.to_datetime(result['Time'], format="%Y-%m-%d %H:%M:%S")
                result.set_index('dtm', inplace=True)
            else:
                print("Download not succesful!!")
                                
        return(result)
                    
    except Exception as err:
        print(err)

def read_kplc_smartmeter_load_profile(file, fix=True, save_csv=True, verbose=True):
    """
    Read KPLC smart meter 'load profile' data and fix time stamps

    MKN powerline has been connected to a smart meter since 17 June 2020.
    Read data downloaded from "http://41.203.223.137:9090/eup/"
    (account: 2097696, pwd: Gawkenya20) and saved as a CSV file.

    Parameters
    ----------
    file : str
        Path to CSV file on disk
        
    fix : bln
        Try to fix erroneous time stamps? Defaults to True

    save_csv : bln
        Should file be saved as a CSV file? default=True
        
    verbose : bln
        Should function return info? default=True

    Returns
    -------
    df: Pandas dataframe
    """
    try:
        df = pd.read_csv(file)
            
        if fix:
            # sort data by 'Total cumulative energy(T1+T2)(kWh)'
            # df.sort_values(by=df.columns[2], ascending=True, inplace=True)
            df['row'] = range(len(df))
            df.sort_values(by='row', ascending=False, inplace=True)

            # assign Time to new column dtm and convert to proper datetime and
            df['dtm'] = df['Time']
            df['dtm'] = pd.to_datetime(df['dtm'], format='%Y-%m-%dT%H:%M:%S')

            # fix erroneous timestamps in data (afternoon times all of by 12 hrs)
            # cf. https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
            # subtract 12 hrs if timestamp jumps 25 hrs from the previous timestamp
#            df['diff1'] = df['dtm'].diff(1)
            df.loc[df['dtm'].diff(1) == pd.to_timedelta(25, unit='h'), 'dtm'] -= pd.to_timedelta(12, unit='h')
            # add 12 h to duplicated time stamps
            df.loc[df['dtm'].duplicated(), 'dtm'] += pd.to_timedelta(12, unit='h')
            # fix the first timestamps
            df.loc[df['dtm'] < pd.datetime(2020,6,18), 'dtm'] += pd.to_timedelta(12, unit='h')

            # Breaks in the time series may lead to 'missing duplicates' for the afternoon timestamps.
            # This can lead to a negative 1st difference. Fix this iteratively
            while(any(df['dtm'].diff(1) < pd.to_timedelta(0, unit='h'))):
                df.loc[df['dtm'].diff(1) < pd.to_timedelta(0, unit='h'), 'dtm'] += -df['dtm'].diff(1) + pd.to_timedelta(1, unit='h')

            df.set_index(df['dtm'], inplace=True)
            df.drop(labels=['row'], axis=1, inplace=True)

        if verbose:
            print(df.describe())

        return(df)

    except Exception as err:
        print(err)


def plot_kplc_smartmeter_load_profile(df, path, figure="load_profile.png", data="load_profile.csv", verbose=True):
    """
    Read KPLC smart meter 'load profile' data and fix time stamps

    MKN powerline has been connected to a smart meter since 17 June 2020.
    Download data from "http://41.203.223.137:9090/eup/login!init.do"
    (account: 2097696, pwd: Gawkenya20) and save as an Excel file.

    Parameters
    ----------
    df : object
        Pandas dataframe

    path : str
        path to folder on disk

    figure : str
        Name of image file with file extension

    data : str
        Name of data file with file extension. At present, only .csv is supported.

    verbose : str
        should function return info? default=True

    Returns
    _______
    nothing
    """
    try:
        # set up 2 plots, ax1, ax2 for voltages, ax3 for currents
        fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, ncols=1, sharex=True)

        # configure ax1
        ax1.set_title('KPLC Powerline Mt. Kenya GAW Station')
        ax1.set_ylabel("Voltage (V)")
#        cols = [20, 21, 22]
        cols = ['A phase voltage(V)', 'B phase voltage(V)', 'C phase voltage(V)']
        ax1.set_ylim(210, 280)
#        ax1.plot(df.iloc[:, cols], label=df.columns[cols], marker=".", linewidth=0.3)
        ax1.plot(df.loc[:, cols], label=cols, marker=".", linewidth=0.3)
#        ax1.legend(df.columns[cols])
        ax1.legend(cols, prop={'size':6}, loc='best')

        # configure ax2
        ax2.set_ylabel("Voltage (V)")
#        cols = [20, 21, 22]
        cols = ['A phase voltage(V)', 'B phase voltage(V)', 'C phase voltage(V)']
        ax2.set_ylim(0, 50)
#        ax2.plot(df.iloc[:, cols], label=df.columns[cols], marker=".", linewidth=0.3)
        ax2.plot(df.loc[:, cols], label=cols, marker=".", linewidth=0.3)
#        ax2.legend(df.columns[cols])
        ax2.legend(cols, prop={'size':6}, loc='upper left')

        # configure ax3
        ax3.set_ylabel("Current (A)")
#        cols = [17, 18, 19]
        cols = ['A phase current(A)', 'B phase current(A)', 'C phase current(A)']
        ax3.set_ylim(0, 15)
#        ax3.plot(df.iloc[:, cols], label=df.columns[cols], marker=".", linewidth=0.3)
        ax3.plot(df.loc[:, cols], label=cols, marker=".", linewidth=0.3)
#        ax3.legend(df.columns[cols])
        ax3.legend(cols, prop={'size':6}, loc='best')

        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        
        os.makedirs(path, exist_ok=True)
            
        plt.savefig(os.path.join(path, figure), dpi=300)

        plt.show()
        if ".csv" in data.lower():
            df.to_csv(os.path.join(path, data))

    except Exception as err:
        print(err)

# %%
def main():
    ROOT = "~/Documents/data/"
    DB = os.path.join(os.path.expanduser(ROOT), "mkn.sqlite")
    TBL = "kplc_smartmeter"
    USR = "2097696"
    PWD = "Gawkenya20"
    to_date = datetime.datetime.today()
    from_date = (to_date - datetime.timedelta(days=1400)).strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    df = download_kplc_smartmeter_load_profile(USR, PWD, from_date, to_date)
    res = df2sqlite.df2sqlite(df, db=DB, tbl=f"{TBL}")

    if not df.empty:
        file = os.path.join(os.path.expanduser(root), "data/kplc/load_profile.csv")
        df.to_csv(file)

        df = read_kplc_smartmeter_load_profile(file, fix=True, verbose=True)

        target = os.path.join(os.path.expanduser(root), "results/kplc")
        plot_kplc_smartmeter_load_profile(df, target)
    
        print(df.tail())
    else:
        print("Could not retrieve any data from source. Exiting ...")
    # cols = (17, 18, 19)
    # ylab = "Phase current (A)"
    # name = os.path.join(root, "results/currents.png")
    # plot_measures(df, cols, name, ylab)
    #
    # folder = "Alarms"
    # df = read_data(root, folder)
    # # df = df.iloc[:,[2, 3, 5, 6, 9]]
    # plot_alarms(df, name='alarms.png')
    #
    # folder = "UPS"
    # cols = [1, 3, 5]
    # ylab = "Voltage (V)"
    # name = 'measures.png'
    # df = read_data(root, folder)
    # plot_measures(df, cols, name, ylab)


if __name__ == "__main__":
    main()