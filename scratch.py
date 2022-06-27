# -*- coding: utf-8 -*-
"""Sandbox"""

# %%
import matplotlib as plt

import ebas2sqlite

# %%
def plot_df(df, title, yaxs=None, ylabs=None, path=None):
    """Plot dataframe and save files.

    Args:
        df (dataframe): Pandas dataframe
        path (str, optional): Path to results folder. Defaults to None (in which case plots are not saved).
        yaxs (list, optional): list of [lists of] dataframe columns to plot in individual panels
    """
    try:
        print("Plotting dataframe ...")
        if yaxs is None:
            panels = len(df.columns)
            yaxs = df.columns
        else:
            panels = len(yaxs)
        fig, axs = plt.subplots(nrows=panels, ncols=1, sharex=True, figsize=(7, panels*2))
        axs[0].set_title(title)
        for i in range(panels):
            axs[i].plot(df.loc[:, yaxs[i]], label=yaxs[i])
            # axs[i].set_facecolor(None)
            if ylabs is None:        
                axs[i].set_ylabel(yaxs[i])
            else:
                axs[i].set_ylabel(ylabs[i])
            axs[i].legend()
        
        fig.tight_layout()
        plt.show()
        
        if path:
            fig.savefig('%s.svg' % path, transparent=True)
            fig.savefig('%s.png' % path, dpi=300, transparent=True)
            print("Figure saved to '%s'" % path)

    except Exception as err:
        print(err)



# %%
# specify root path
ebas = "~/Documents/git/scratch/data/ebas"

# file = 'C:/Users/localadmin/Documents/git/scratch/data/ebas/aerosol/KE0001G.20150101000000.20160321124934.nephelometer..aerosol.1y.1h.CH02L_Ecotech_Aurora3000_TLL_dry.CH02L_scat_coef.lev2.nas'

df.plot(x='dtm', y='O3_ug_m-3', title='[MKN] Surface Ozone')

# %%
plot_df(df=df, title='[MKN] Surface Ozone')
# fig, axs = plt.subplots(3, 1, figsize=(6.4, 7), constrained_layout=True)
# # common to all three:
# for ax in axs:
#     ax.plot('date', 'adj_close', data=data)
#     # Major ticks every half year, minor ticks every month,
#     ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1, 7)))
#     ax.xaxis.set_minor_locator(mdates.MonthLocator())
#     ax.grid(True)
#     ax.set_ylabel(r'Price [\$]')

# # different formats:
# ax = axs[0]
# ax.set_title('DefaultFormatter', loc='left', y=0.85, x=0.02, fontsize='medium')

# ax = axs[1]
# ax.set_title('ConciseFormatter', loc='left', y=0.85, x=0.02, fontsize='medium')
# ax.xaxis.set_major_formatter(
#     mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))

# ax = axs[2]
# ax.set_title('Manual DateFormatter', loc='left', y=0.85, x=0.02,
#              fontsize='medium')
# # Text in the x axis will be displayed in 'YYYY-mm' format.
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%b'))
# # Rotates and right-aligns the x labels so they don't crowd each other.
# for label in ax.get_xticklabels(which='major'):
#     label.set(rotation=30, horizontalalignment='right')

# plt.show()


# %%
file = "C:/Users/localadmin/Documents/git/scratch/data/ebas/o3/data.pkl"
data = pd.read_pickle(file)
# plot data
data['df'].plot(x='dtm', y='O3_2')
