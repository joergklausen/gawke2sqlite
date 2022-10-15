# %%
import os
from jklutils import configparser, jretrievedwh


# %%
cfg = configparser.config("C:/Users/localadmin/Documents/git/gawke2sqlite/dwh.cfg")

station = 'mkn'
dwh_id = cfg[station]['dwh_id']

# %%
for dwh_category, params in cfg[station]['dwh_config'].items():
    print(dwh_category)
    par_short_names = ",".join(params.keys())
    res = jretrievedwh.jretrievedwh(dwh_id=dwh_id, par_short_names=par_short_names, category=dwh_category, duration=1)

# %%
par_short_names = ",".join(cfg[station]['dwh_config'].keys())
# cfg[station]['dwh_config'].values()
# %%
