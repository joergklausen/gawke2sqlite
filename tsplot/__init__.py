# %%
import logging
import matplotlib.colors as mc # For the legend
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3

# Another utility for the legend
from matplotlib.cm import ScalarMappable

__version__ = '0.0.1'

LOGGER = logging.getLogger(__name__)

# %%
db = "C:/Users/localadmin/Documents/git/scratch/data/nrb.sqlite"
tbl = "shadoz_V06"
fields = ["Press", "GeopAlt", "O3_ppmv", "O3_DU"]
dtm = ["1998-01-01", "2022-12-31"]

# %%
def load_data(db: str, tbl: str, fields=[], dtm=[]) -> pd.DataFrame:
    try:
        if not fields:
            raise ValueError("fields cannot be an empty list.")
        else:
            fields = ", ".join(fields)
        if len(dtm)==2:
            where = f"WHERE dtm BETWEEN '{dtm[0]}' and '{dtm[1]}'"
        elif len(dtm)==1:
            where = f"WHERE dtm >= '{dtm[0]}'"

        con = sqlite3.connect(db)
        qry = f"SELECT dtm, {fields} FROM {tbl} {where} ORDER BY dtm"
        df = pd.read_sql_query(qry, con, parse_dates = ['dtm'])
        con.close()
        return df

    except Exception as err:
        print(err)


# %%
df = load_data(db, tbl, fields, dtm)
df = df[df['O3_ppmv'] < 25]

# %%
fig, ax = plt.subplots()
# ax.scatter(df['Press'], df['O3_DU'])
# ax.scatter(df['dtm'], np.log10(df['Press']), s=0.1, c=df['O3_DU'], cmap="coolwarm")
im = ax.scatter(df['dtm'], df['GeopAlt'], s=0.1, c=df['O3_ppmv'], cmap="coolwarm")
# ax.invert_yaxis()
ax.set_title("Nairobi ozone (ppmv)")
ax.set_ylabel("Geopotential Height (km)")

fig.subplots_adjust(right=0.8)
cbar_ax = fig.add_axes([0.85, 0.15, 0.02, 0.7])
fig.colorbar(im, cax=cbar_ax)

plt.show()

# %%
# class TSPLOT():
#     """Time series plot object"""

#     def __init__(self, df, title=None, xlab=None, ylab=None):
#         """
#         Initialize a TSPLOT object

#         :param df: pd.DataFrame
#         :param title: title for plot (optional)
#         :param xlab: x-axis label (optional)
#         :param ylab: y-axis label (optional)

#         :returns: tsplot.TSPLOT instance
#         """

#         self.df = df
#         self.title = title
#         self.xlab = xlab
#         self.ylab = ylab

#         if df.empty:
#             raise InvalidDataError('df cannot be empty.')

#         try:

#         except ValueError:
#             raise InvalidDataError('Invalid SHADOZ metadata lines value')

#         LOGGER.debug('Parsing metadata')
#         for metadataline in filelines[1:metadatalines-2]:
#             LOGGER.debug(metadataline)
#             try:
#                 key, value = [v.strip() for v in metadataline.split(': ', 1)]
#                 self.metadata[key] = _get_value_type(key, value)
#             except ValueError as err:
#                 LOGGER.warning(f'Metadata error: {err}')

#         if isinstance(self.metadata['SHADOZ Version'], str):
#             self.version = float(self.metadata['SHADOZ Version'].split()[0])
#         else:
#             self.version = float(self.metadata['SHADOZ Version'])
#         LOGGER.debug('Checking major version')
#         if int(self.version) != int(version):
#             raise InvalidDataError('Invalid SHADOZ version')

#         LOGGER.debug('Parsing data fields')
#         tmp = re.split(r'\s{2,}', filelines[metadatalines-2].strip())
#         self.data_fields = [v.strip() for v in tmp]

#         LOGGER.debug('Parsing data fields units')
#         tmp = re.split(r'\s{2,}', filelines[metadatalines-1].strip())
#         self.data_fields_units = [v.strip() for v in tmp]

#         if len(self.data_fields) != len(self.data_fields_units):
#             raise InvalidDataError(
#                 'Number of fields not equal to number of field units')

#         LOGGER.debug('Parsing data')
#         for dl in filelines[metadatalines:]:
#             data = [_get_value_type('default', v) for v in dl.strip().split()]

#             if len(data) != len(self.data_fields):
#                 raise InvalidDataError(
#                     'Data length not equal to number of fields')

#             self.data.append(data)

#     def write(self):
#         """
#         SHADOZ writer

#         :returns: SHADOZ data as string
#         """

#         lines = []

#         line0 = len(self.metadata.keys()) + 3

#         lines.append(str(line0))

#         mwidth = max(map(len, self.metadata))

#         for key, value in self.metadata.items():
#             if key == 'SHADOZ format data created':
#                 value2 = value.strftime('%d %B, %Y')
#             elif isinstance(value, time):
#                 value2 = value.strftime('%H:%M:%S')
#             elif isinstance(value, datetime) or isinstance(value, date):
#                 value2 = value.strftime('%Y%m%d')
#             else:
#                 value2 = value
#             lines.append('{0: <{width}}: {value}'.format(key, width=mwidth,
#                                                          value=value2))

#         dfl = ' '.join([df.rjust(10) for df in self.data_fields])
#         dfl = dfl.replace('      Time', 'Time')
#         lines.append(dfl)

#         dful = ' '.join([dfu.rjust(10) for dfu in self.data_fields_units])
#         dful = dful.replace('      sec', 'sec')
#         lines.append(dful)

#         for data_ in self.data:
#             dl = ' '.join([repr(d).rjust(10) for d in data_])
#             dl.replace('     ', '')
#             lines.append(dl)

#         return '\n'.join([re.sub('^     ', '', line) for line in lines])

#     def get_data_fields(self):
#         """
#         get a list of data fields and units

#         :returns: list of tuples of data fields and associated units
#         """

#         return list(zip(self.data_fields, self.data_fields_units))

#     def get_data(self, data_field=None, data_field_unit=None,
#                  by_index=None):
#         """
#         get all data from a data field/data field unit

#         :param data_field: data field name
#         :param data_field_unit: data field name unit
#         :param by_index: index of data in table


#         :returns: list of lists of all data (default) or filtered by
#                   field/unit or index
#         """

#         if by_index is not None:
#             return [row[by_index] for row in self.data]

#         if data_field is None and data_field_unit is None:  # return all data
#             return self.data

#         data_field_indexes = \
#             [i for i, x in enumerate(self.data_fields) if x == data_field]

#         if data_field_unit is None:  # find first match
#             return [row[data_field_indexes[0]] for row in self.data]
#         else:
#             data_field_unit_indexes = \
#                 [i for i, x in enumerate(self.data_fields_units)
#                  if x == data_field_unit]

#             data_index = set(data_field_indexes).intersection(
#                 data_field_unit_indexes)

#             if data_index:
#                 data_index2 = list(data_index)[0]
#                 return [row[data_index2] for row in self.data]
#             else:
#                 raise DataAccessError('Data field/unit mismatch')

#     def get_data_index(self, data_field, data_field_unit=None):
#         """
#         Get a data field's index

#         :param data_field: data field name
#         :param data_field_units: data field name unit

#         :returns: index of data field/unit
#         """

#         data_field_indexes = \
#             [i for i, x in enumerate(self.data_fields) if x == data_field]

#         data_field_unit_indexes = \
#             [i for i, x in enumerate(self.data_fields_units)
#              if x == data_field_unit]

#         if data_field_unit is None:
#             return data_field_indexes[0]
#         else:
#             data_index = set(data_field_indexes).intersection(
#                 data_field_unit_indexes)

#             if data_index:
#                 return list(data_index)[0]

#     def __repr__(self):
#         """repr function"""
#         return f'<SHADOZ (filename: {self.filename})>'


# class DataAccessError(Exception):
#     """Exception stub for invalid data access by data field/unit"""
#     pass


# class InvalidDataError(Exception):
#     """Exception stub for format reading errors"""
#     pass


# def load(filename):
#     """
#     Parse SHADOZ data from from file
#     :param filename: filename
#     :returns: pyshadoz.SHADOZ object
#     """

#     with open(filename) as ff:
#         return SHADOZ(ff, filename=filename)


# def loads(strbuf):
#     """
#     Parse SHADOZ data from string
#     :param strbuf: string representation of SHADOZ data
#     :returns: pyshadoz.SHADOZ object
#     """

#     s = StringIO(strbuf)
#     return SHADOZ(s)


# @click.command()
# @click.version_option(version=__version__)
# @click.option('--file', '-f', 'file_',
#               type=click.Path(exists=True, resolve_path=True),
#               help='Path to SHADOZ data file')
# @click.option('--directory', '-d', 'directory',
#               type=click.Path(exists=True, resolve_path=True,
#                               dir_okay=True, file_okay=False),
#               help='Path to directory of SHADOZ data files')
# @click.option('--recursive', '-r', is_flag=True,
#               help='process directory recursively')
# @click.option('--verbose', '-v', is_flag=True, help='verbose mode')
# def shadoz_info(file_, directory, recursive, verbose=False):
#     """parse shadoz data file(s)"""

#     if verbose:
#         ch = logging.StreamHandler(sys.stdout)
#         ch.setLevel(logging.DEBUG)
#         ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
#         LOGGER.addHandler(ch)
#     else:
#         LOGGER.addHandler(logging.NullHandler())

#     if file_ is not None and directory is not None:
#         msg = '--file and --directory are mutually exclusive'
#         raise click.ClickException(msg)

#     if file_ is None and directory is None:
#         msg = 'One of --file or --directory is required'
#         raise click.ClickException(msg)

#     files = []
#     if directory is not None:
#         if recursive:
#             for root, dirs, files_ in os.walk(directory):
#                 for f in files_:
#                     files.append(os.path.join(root, f))
#         else:
#             for files_ in os.listdir(directory):
#                 files.append(os.path.join(directory, files_))
#     elif file_ is not None:
#         files = [file_]

#     for f in files:
#         click.echo(f'Parsing {f}')
#         with open(f) as ff:
#             try:
#                 s = SHADOZ(ff, filename=f)
#                 click.echo(f'SHADOZ file: {s.filename}\n')
#                 click.echo('Metadata:')
#                 for key, value in s.metadata.items():
#                     click.echo(f' {key}: {value}')
#                 click.echo('\nData:')
#                 click.echo(' Number of records: {len(s.data)}')
#                 click.echo(' Attributes:')
#                 for df in s.get_data_fields():
#                     data_field_data = sorted(s.get_data(df[0], df[1]))
#                     click.echo(f'  {df[0]} ({df[1]}): (min={data_field_data[0]}, max={data_field_data[-1]})')  # noqa
#             except InvalidDataError as err:
#                 raise click.ClickException(str(err))
