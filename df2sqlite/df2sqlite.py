# -*- coding: utf-8 -*-

import pandas as pd
import sqlite3


# %%
def df2sqlite(df: pd.DataFrame, db, tbl, if_exists="append", index="dtm", remove_duplicates=True, verbose=True):
    try:
        if df.empty:
            raise ValueError("'df' can't be empty.")

        # create sqlite3 connection
        con = sqlite3.connect(db)

        # upload to sqlite db
        # tmp['mappings'].to_sql(name='mappings_%s' % os.path.basename(dpath), con=con, if_exists='replace')
        df.to_sql(con=con, name=tbl, if_exists=if_exists, index=True, index_label=index)

        msg = '%s record(s) added to table %s.' % (len(df), tbl)
        if verbose:
            print(msg)

        if remove_duplicates:            
            # collect all field names using pragma, then remove source
            cursor = con.cursor()
            cursor.execute(f"pragma table_info({tbl})")
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
                con.commit()

                msg = '%s duplicate record(s) removed from table %s.' % (len(res), tbl)
                if verbose:
                    print(msg)        

    except Exception as err:
        print(err)

# %%
def append_to_sqlite_db(df: pd.DataFrame, db: str, tbl: str, remove_duplicates=True) -> dict:
    try:
        if df.empty:
            raise ValueError("DataFrame is empty.")

        records_for_insert = len(df)

        con = sqlite3.connect(db)

        qry_count_records = f"SELECT count({df.index.name}) from {tbl}"
        try:
            records_before_insert = con.execute(qry_count_records).fetchone()[0]
        except:
            # table does not exist yet
            records_before_insert = 0

        print(f"Inserting {records_for_insert} rows to {db}[{tbl}] ...")
        df.to_sql(name=tbl, con=con, if_exists="append", )
        records_after_insert = con.execute(qry_count_records).fetchone()[0]

        if (records_after_insert - records_before_insert) < records_for_insert:
            raise Warning("Could not insert all values.")

        if remove_duplicates:
            group_by = ", ".join(list(df.index.names) + list(df.columns))
            qry = f"DELETE FROM {tbl} WHERE ROWID NOT IN (SELECT min(ROWID) \
                FROM {tbl} GROUP BY {group_by})"
            con.execute(qry)
            con.commit()
            records_after_deduplication = con.execute(qry_count_records).fetchone()[0]

        con.close()

        res = {"records_inserted": records_for_insert, \
            "duplicate_records": records_after_insert - records_after_deduplication}

        return res
    except Exception as err:
        print(err)


