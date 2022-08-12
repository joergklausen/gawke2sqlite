# -*- coding: utf-8 -*-
"""Extract, transform and load files to DB.

Returns:
    nothing
"""
# %%
from io import StringIO, BytesIO
import os
import logging
import re
import datetime
import time
import requests
import pandas as pd
import sqlite3
import zipfile

# %%
class ETLHandler:
    """Extract, transform and load files to DB."""

    @classmethod
    def __init__(self, config, ver='v0.1.0'):
        """Initialize class.

        Args:
            config (dict): Configuration information
            ver (str, optional): Version information. Defaults to 'v0.1.0'.
        """
        try:
            self.config = config
            self.verbose = config['verbose']
            self.index = config['index']
            self.seconds = config['seconds']

            # data paths
            self.incoming = config['incoming']
            self.archive = config['archive']
            self.db = config['database']

            # set up logging
            self.logging = False
            if self.config['logfile']:
                self.logging = True
                self.logger = logging.getLogger(__name__)
                self.logger.info(f"{__file__}.FileHandler {ver} initialized successfully.")
                # set up logging to file
                logging.basicConfig(level=logging.DEBUG,
                                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                    datefmt='%H:%M:%S',
                                    filename=self.config['logfile'],
                                    filemode='a')

        except Exception as err:
            if self.config['logfile']:
                self.logger = logging.getLogger(__name__)
                self.logger.error(f"Error initializing {__file__}. {err}")
            print(err)


    @classmethod
    def archive_file(self, filename, year, src=None, dst=None, overwrite=False):
        """Archive file.

        Args:
            filename (str): filename with file extension (no path)
            src (str, optional): Source path
            dst (str, optional): Destination path
            year (str): Year indicating sub-folder where file should be archived
            overwrite (bool, optional): Should existing files be over-written?. Defaults to False.
        """
        try:
            if src is None:
                src = os.path.join(self.incoming, filename)
            if dst is None:
                dst = os.path.join(self.archive, year, filename)
            if not overwrite:
                if os.path.isfile(dst):
                    basename, ext = os.path.splitext(filename)
                    filename = basename + "-" + time.strftime("%Y%m%d%H%M") + ext
                    dst = os.path.join(self.archive, year, filename)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.replace(src=src, dst=dst)

        except Exception as err:
            msg = "'.archive_file' error: "
            if self.verbose:
                print(msg)
            if self.logging:
                self.logger.error(msg, err)

    @classmethod
    def extract_file(self, file: str, index=None, stream=None):
        """
        Open a file, determine its type from the file name, then extract content into a Pandas dataframe.

        Args:
            file (str): full path to file.
            seconds (bool, optional): Should seconds remain in the timestamps? Defaults to 'False'.
            index (str, optional): The label of the column to set as index of the dataframe. Defaults to None, in which case the value is taken from the configuration.
        """
        try:
            if index is None:
                index = self.index
            msg = 'Extracting file %s.' % file
            if self.verbose:
                print(msg)
            if self.logging:
                self.logger.info(msg)

            df = pd.DataFrame()
            if '.zip' in file:
                try:
                    if stream:
                        zf = zipfile.ZipFile(BytesIO(stream))
                    else:
                        zf = zipfile.ZipFile(file)
                except Exception as err:
                    print('Warning: ', err, 'and will be ignored. Please remove manually.')
                    return pd.DataFrame()

                if 'CFKADS2320' in file:
                    print('not yet implemented')
                    # CREATE TABLE "g2401" (
                    # 	"dtm"	TEXT,
                    # 	"DATE"	TEXT,
                    # 	"TIME"	TEXT,
                    # 	"JULIAN_DAYS"	REAL,
                    # 	"EPOCH_TIME"	REAL,
                    # 	"ALARM_STATUS"	REAL,
                    # 	"INST_STATUS"	REAL,
                    # 	"MPVPosition"	REAL,
                    # 	"solenoid_valves"	REAL,
                    # 	"OutletValve"	REAL,
                    # 	"CavityPressure"	REAL,
                    # 	"CO_sync"	REAL,
                    # 	"CO2_sync"	REAL,
                    # 	"CO2_dry_sync"	REAL,
                    # 	"CH4_sync"	REAL,
                    # 	"CH4_dry_sync"	REAL,
                    # 	"H2O_sync"	REAL,
                    # 	"CavityTemp"	REAL,
                    # 	"DasTemp"	REAL,
                    # 	"EtalonTemp"	REAL,
                    # 	"WarmBoxTemp"	REAL,
                    # 	"source"	TEXT
                    # )

                    #                     df.drop(columns=['FRAC_DAYS_SINCE_JAN1', 'FRAC_HRS_SINCE_JAN1'], inplace=True)
                    #                     df['dtm'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'])
                    #                     df['source'] = file
                    #                     df.set_index(index, inplace=True)
                if 'tei49i' in file:
                    df = pd.read_csv(zf.open(zf.namelist()[0]), sep=' +', engine='python')
                    df[index] = pd.to_datetime(df['pcdate'] + ' ' + df['pctime'])
                    if not self.seconds:
                        df[index] = df[index].apply(lambda x: x.round(freq='T'))
                    df.drop(columns=[df.columns[4], 'hio3'], inplace=True)
                    df['source'] = file
                    df.set_index(self.index, inplace=True)
                    # return df
                elif 'tei49c' in file:
                    # df = pd.read_csv(zf.open(zf.namelist()[0], mode='r'), sep=' ', engine='python', )
                    df = pd.read_csv(file, compression='zip', sep=' +', engine='python')
                    df[index] = pd.to_datetime(df['pcdate'] + ' ' + df['pctime'])
                    if not self.seconds:
                        df[index] = df[index].apply(lambda x: x.round(freq='T'))
                    df.drop(columns=['o3lt'], inplace=True)
                    df['source'] = file
                    df.set_index(self.index, inplace=True)
                    # return df
                elif 'g2401' in file:
                    df = pd.read_csv(zf.open(zf.namelist()[0]), sep=' +', encoding='latin1', engine='python')
                    # columns_to_keep =
                elif 'VMSW43' in file:
                    df = pd.read_csv(zf.open(zf.namelist()[0]), skiprows=1, header=1, sep=' ', na_values='/')
                    df[index] = pd.to_datetime(df['zzzztttt'], format='%Y%m%d%H%M')
                    df['source'] = file
                    df.set_index(index, inplace=True)
            elif 'VMSW43' in file:
                if stream:
                    df = pd.read_csv(BytesIO(stream), skiprows=1, header=1, sep=' ', na_values='/')
                else:
                    df = pd.read_csv(file, skiprows=1, header=1, sep=' ', na_values='/')
                df[index] = pd.to_datetime(df['zzzztttt'], format='%Y%m%d%H%M')
                df['source'] = file
                df.set_index(index, inplace=True)
            return df

        except Exception as err:
            print(err)
            return pd.DataFrame()


    @classmethod
    def load_file(self, tbl: str, df: pd.DataFrame, index=None, remove_duplicates=True) -> None:
        """Append a dataframe to an SQLite3 DB.

        Args:
            tbl (str): Name of DB table
            df (pd.DataFrame): Pandas dataframe with information to load
            index_label (str, optional): Name of dateTime axis. Defaults to None, in which case it is taken from the configuration.
            remove_duplicates (bool, optional): Should duplicate rows in table be removed?. Defaults to True.

        Returns:
            None
        """
        try:
            if index is None:
                index = self.index

            conn = sqlite3.connect(self.db)

            df.to_sql(tbl, conn, if_exists='append', index=True, index_label=index)

            msg = '%s record(s) added to table %s.' % (len(df), tbl)
            if self.verbose:
                print(msg)
            if self.logging:
                self.logger.info(msg)                  

            if remove_duplicates:            
                # collect all field names using pragma, then remove source
                cursor = conn.cursor()
                cursor.execute("pragma table_info(%s)" % tbl)
                res = cursor.fetchall()
                names = [tpl[1] for tpl in res]
                if 'source' in names:
                    names.remove('source')

                # identify duplicates on all fields
                qry = "select count(*) from %s " % tbl
                qry += "group by %s " % ",".join(names)
                qry += "having count(*) > 1"
                cursor.execute(qry)
                res = cursor.fetchall()

                # remove duplicates
                if len(res) > 0:
                    qry = "delete from %s where rowid not in ( " % tbl
                    qry += "select min(rowid) from %s " % tbl
                    qry += "group by %s )" % ",".join(names)
                    cursor.execute(qry)
                    conn.commit()

                    msg = '%s duplicate record(s) removed from table %s.' % (len(res), tbl)
                    if self.verbose:
                        print(msg)
                    if self.logging:
                        self.logger.info(msg)

            conn.close()
            return None

        except Exception as err:
            conn.close()
            print(err)
            if self.logging:
                self.logger.error(f"'.append' error: {err}")
            return err

    @classmethod
    def process_directory(self, path=None, seconds=None, index='dtm'):
        """Loop through entire directory (recursively), extract, load, archive files.

        Args:
            path (str, optional): Path of directory to process. This can also be a URL.
            seconds (bool, optional): Should seconds remain in timestamps of data loaded to DB?. Defaults to None, in which case it is taken from the configuration.
            index (str, optional): _description_. Defaults to 'dtm'.

        Returns:
            _type_: _description_
        """
        try:
            if path is None:
                path = self.incoming
            if seconds is None:
                seconds = self.seconds
            if 'http' in path:
                res = requests.get(url=path, proxies={"http": "", "https": ""}, \
                        verify=False)
                files = re.findall(pattern=">(VMSW43.+.[zip|001])<", string=res.text)
                msg = 'Downloading and extracting files from %s ...' % path
                if self.verbose:
                    print(msg)
                if self.logging:
                    self.logger.info(msg)
                cnt = 0
                for file in files:
                    res = requests.get(url="/".join([path, file]), proxies={"http": "", "https": ""}, verify=False)
                    df = self.extract_file(file, stream=res.content)
                    if not df.empty:
                        # load dataframe to DB
                        tbl = os.path.basename(path)
                        res = self.load_file(tbl, df, index=index)
                        if res is None:
                            cnt += 1
                    msg = '%s of %s files processed successfully.' % (cnt, len(files))
                    if self.verbose:
                        print(msg)
                    if self.logging:
                        self.logger.info(msg)
            else:
                for root, dirs, files in os.walk(path):
                    cnt = 0 
                    msg = 'Extracting files from %s ...' % root
                    if self.verbose:
                        print(msg)
                    if self.logging:
                        self.logger.info(msg)
                    for file in files:
                        file = os.path.join(root, file)
                        df = self.extract_file(file)                
                        if not df.empty:
                            # load dataframe to DB
                            tbl = os.path.basename(root)
                            res = self.load_file(tbl, df, index=index)
                            
                            if res is None:
                                # archive file
                                if self.archive is not None:
                                    if 'meteo' in file:
                                        year = re.findall(r'\.(\d{4})', os.path.basename(file))[0]
                                    else:
                                        year = re.findall(r'-(\d{4})', os.path.basename(file))[0]
                                    os.makedirs(os.path.join(self.archive, os.path.basename(root), year), exist_ok=True)            
                                    dst = os.path.join(self.archive, os.path.basename(root), year, os.path.basename(file))
                                    os.replace(src=file, dst=dst)                                  
                                cnt += 1
                                
                    msg = '%s of %s files processed successfully.' % (cnt, len(files))
                    if self.verbose:
                        print(msg)
                    if self.logging:
                        self.logger.info(msg)
                    return len(files), cnt

        except Exception as err:
            print(err)
            if self.logging:
                self.logger.info(err)


    @classmethod
    def append_sqlite3(self, df: pd.DataFrame, tbl: str, index_label=None):
        """Append a dataframe to an SQLite3 DB.

        Args:
            df (pd.DataFrame): Pandas dataframe holding data to load to DB
            tbl (str): name of DB table
            index_label (str, optional): Name of column holding dateTime stamps. Defaults to None, in which case this is taken from the config file.
        """
        try:
            if index_label is None:
                index_label = self.index

            conn = sqlite3.connect(self.db)

            df.to_sql(tbl, conn, if_exists='append', index=True, index_label=index_label)
            
            msg = '%s records added to table %s.' % (len(df), tbl)
            if self.verbose:
                print(msg)
            if self.logging:
                self.logger.info(msg)                  

            # remove duplicates that were inadvertently added
            # collect all field names using pragma, then remove source
            cursor = conn.cursor()
            cursor.execute("pragma table_info(%s)" % tbl)
            res = cursor.fetchall()
            names = [tpl[1] for tpl in res]
            if 'source' in names:
                names.remove('source')

            # identify duplicates on all fields
            qry = "select count(*) from %s " % tbl
            qry += "group by %s " % ",".join(names)
            qry += "having count(*) > 1"
            cursor.execute(qry)
            res = cursor.fetchall()

            # remove duplicates
            if len(res) > 0:
                qry = "delete from %s where rowid not in ( " % tbl
                qry += "select min(rowid) from %s " % tbl
                qry += "group by %s )" % ",".join(names)
                cursor.execute(qry)
                conn.commit()

                msg = '%s duplicate records removed from table %s.' % (len(res), tbl)
                if self.verbose:
                    print(msg)
                if self.logging:
                    self.logger.info(msg)

            conn.close()
            
        except Exception as err:
            conn.close()
            print(err)
            if self.logging:
                self.logger.error(f"'.append' error: {err}")


    @classmethod
    def jretrieve_dwh(self, station, cfg=None, vars=None, category=None, duration=None, since=None, till=None, drop_null=True) -> dict:
        """
        Download data from DWH using jretrieve.

        Documentation: http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface/help


        :return:
        """
        try:
            base_url = 'http://wlsprod.meteoswiss.ch:9010/jretrievedwh/surface?delimiter=,&placeholder=None'
            base_url += "&locationIds=%s" % station

            urls = []
            df = pd.DataFrame()

            if cfg is None:
                if vars is None:
                    raise ValueError('"vars" not specified.')
                if category is None:
                    raise ValueError('"category" not specified.')
                if duration is None:
                    raise ValueError('"duration" not specified.')
                else:
                    since = (datetime.datetime.now() - datetime.timedelta(days=duration)).strftime("%Y%m%d%H%M%S")
                    till = time.strftime("%Y%m%d%H%M%S")
                    base_url += "&date=%s-%s" % (since, till)
                    base_url += "&parameterShortNames=%s" % vars
                    base_url += "&measCatNr=%s" % category
                urls.append(base_url)
                series = "".split(vars, sep=',')
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
                
                params = cfg[station]['params']

                series = []
                labels = []
                for key in params.keys():
                    url = base_url + "&parameterShortNames=%s" % params[key]['var']
                    url += "&measCatNr=%s" % str(params[key]['cat'])
                    urls.append(url)
                    series.append(key)
                    labels.append("%s (%s)" % (params[key]['lbl'], params[key]['var']))
                labels = dict(zip(series, labels))

            for url in urls:
                print("Calling %s ..." % url)
                t0 = time.time()
                res = requests.get(url=url)
                print("Finished downloading in %s seconds." % str(time.time() - t0))
                if res.status_code == 200:
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

                df.index.rename('dtm', inplace=True)

            df.columns = series

            ## downcast data to reduce size of dataframe
            for column in df:
                if df[column].dtype == 'float64':
                    df[column]=pd.to_numeric(df[column], downcast='float')
                if df[column].dtype == 'int64':
                    df[column]=pd.to_numeric(df[column], downcast='integer')
            
            # make sure df is sorted by date
            df.sort_index(inplace=True)

            return {'data': df, 'labels': labels}

        except Exception as err:
            print(err)


if __name__ == "__main__":
    pass        




