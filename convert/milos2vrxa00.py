

# %%
def df2vrxa00(df: pd.DataFrame, dwh_station_id: int, target: str):
    try:
        if df.empty:
            print(f"{os.path.basename(target)} cannot be produced with empty DataFrame.")
        else:
            os.makedirs(os.path.dirname(target), exist_ok=True)

            df.reset_index(inplace=True)
            df.rename(columns={'dtm': 'zzzztttt'}, inplace=True)
            df['iii'] = dwh_station_id
        
            df.set_index(['iii', 'zzzztttt'], inplace=True)
            df.reset_index(inplace=True)

            # target = "C:/Users/localadmin/Documents/git/gawke2sqlite/data/VRXA00.001217.001"
            tbl = df.to_csv(sep= " ", na_rep="/", index=False, header=True, line_terminator="\n", date_format="%Y%m%d%H%M%S")
            with open(target, 'w') as fh:
                fh.write(f"001\nVRXA00 LSSW 010001\n\n{tbl}")
            print(f"Bulletin saved as {target}")
    except Exception as err:
        print(err)


