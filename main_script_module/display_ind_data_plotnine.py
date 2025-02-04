
#=================  Global Parameters  ================================

# main titles for displays
PAGE4_SUPTITLE = "\nOperating Price-Earnings Ratios for " +\
    "the Industries Within the S&P 500"
PAGE5_SUPTITLE = "\nCorrelations among Annual Price-Earnings Ratios \nfor " +\
    "the Industries Within the S&P 500"
PAGE6_SUPTITLE = "\nEach Industry's Share of Total Earnings for the Industries in the S&P 500"

# str: source footnotes for displays
E_DATA_SOURCE = \
    'https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all'
RR_DATA_SOURCE = '10-year TIPS: latest rate for each quarter,' + \
    ' Board of Governors of the Federal Reserve System, ' + \
    '\nMarket Yield on U.S. Treasury Securities at 10-Year' + \
    ' Constant Maturity, Investment Basis, Inflation-Indexed,' +\
    '\nfrom Federal Reserve Bank of St. Louis, FRED [DFII10].'
PAGE4_SOURCE = '\n' + E_DATA_SOURCE + '\n' +\
    "NB: S&P calculates the index of earnings for the S&P 500 " +\
    "differently than earnings for the industries.\n" +\
    "The index of earnings for the S&P 500 usually is more than twice the sum of " +\
    "earnings for the industries. The S&P 500's P/E is not the " +\
    "average of the industries' P/Es."
PAGE5_SOURCE = '\n' + E_DATA_SOURCE

XLABL = 'end of year'

# ================  MAIN ==============================================

def display_ind():
    
    import sys
    import gc

    import polars as pl
    import polars.selectors as cs
    import matplotlib.pyplot as plt
    #import seaborn as sn
    from plotnine import (ggplot, aes, labs, geom_point, 
                          save_as_pdf_pages, theme, themes,
                          element_text)
    import pyarrow
    import pandas
    import scipy
    import numpy as np

    from main_script_module import sp_paths as sp

    #from helper_func_module import display_ind_helper_func as dh
    from helper_func_module import plot_ind_func as pf
    from helper_func_module import helper_func as hp

# FETCH DATA +++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # find latest year for actual data
    # do not use only p/e data, 'real_rate' data, eps data
    ind_df = pl.read_parquet(sp.OUTPUT_IND_ADDR)\
               .drop(cs.matches('Real_Estate'))\
               .sort(by= 'year')
               
    year = pl.Series(
              ind_df.select("year", 'SP500_rep_eps')\
                    .filter(pl.col('SP500_rep_eps')
                            .is_not_null())\
                    .select('year').max()
              ).to_list()[0]
    
    DATE_THIS_PROJECTION = \
        f'actual annual operating earnings through {year}'
    
    # marks years with E, remove reported earnings
    ind_df = ind_df.with_columns(
                        pl.when(pl.col('SP500_rep_eps')
                                .is_not_null())
                          .then(pl.col('year'))
                          .otherwise(pl.col('year') + 'E')
                          .alias('year')
                        )\
                    .select(~cs.matches('_rep_'))
        
    # remove eps, simplify col headings
    op_e_df = ind_df.drop(cs.matches('_eps'))
    op_e_df.columns = [name.split('_op_')[0].replace("_", " ")
                       for name in op_e_df.columns]
    # rep_e_df = ind_df.select(~(cs.matches('_op_')))
    # rep_e_df.columns = [name.split('_op_')[0].replace("_", " ")
    #                     for name in op_e_cor_df.columns]
    
    
# PLOTNINE SCATTERPLOTS WITH JITTER +++++++++++++++++++++++++++++++++
# https://ggplot2.tidyverse.org/reference/position_jitter.html
# https://realpython.com/ggplot-python/
# https://ggplot2.tidyverse.org/reference/ggsave.html
    # PAGE 4
    
    # create tall DF with year, industry, and p/e as columns
    df = op_e_df.select(pl.exclude('real int rate'))\
                .drop(cs.matches('SP500'))\
                .unpivot(index= 'year',
                         variable_name= 'industry',
                         value_name= 'price/earnings')
    
    plot_fig = ggplot(df) \
        + aes(x="year", y="price/earnings", 
            color="industry") \
        + labs(
            x="end of year",
            y="price-earnings ratio",
            color="industries",
            title= f' \n{PAGE4_SUPTITLE}') \
        + geom_point() \
        + theme(plot_title = element_text(ha= 'center'),
                axis_text_x= element_text(rotation= 45, hjust=1)) \
        + themes.themeable.plot_caption(PAGE4_SOURCE)
    
    '''
    
    fig = plt.figure(figsize=(10.5, 8.5), 
                     layout="constrained")
    # plt.tight_layout(pad= 0.5)
    
    fig.suptitle(
        f' \n{PAGE4_SUPTITLE}\n{DATE_THIS_PROJECTION}',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(f'{PAGE4_SOURCE}\n ', fontsize= 8)
    
    ax = fig.subplots()
    
    sn.stripplot(
        df,
        x="year",
        y="price/earnings",
        hue="industry",
        ax=ax,
    )
    
    sn.scatterplot(op_e_df.select(pl.col('year','SP500')),
                   x= 'year',
                   y= 'SP500',
                   label= 'SP500',
                   marker="|", s=4, linewidth=25
)
    # plt.xticks(rotation = 30)
    ax.set_ylim(ymin= -50, ymax= 60)
    ax.set_xlabel(XLABL, fontweight= 'bold')
    ax.set_ylabel(' \nprice-earnings ratio', fontweight= 'bold')
    sn.move_legend(ax, 'lower left')
    # sn.move_legend(ax, 'upper left', bbox_to_anchor= (1, 1))
    '''
    
    print('\n============================')
    print(sp.DISPLAY_4_ADDR)
    print('============================\n')
    save_as_pdf_pages([plot_fig], filename= sp.DISPLAY_4_ADDR)
    
    sys.exit()
    
    del df
    gc.collect()
    
# P/E CORRELATION HEAT MAP ++++++++++++++++++++++++++++++++++++++++++++
# https://seaborn.pydata.org/generated/seaborn.heatmap.html
# https://seaborn.pydata.org/examples/structured_heatmap.html
# https://stackoverflow.com/questions/67879908/lower-triangle-mask-with-seaborn-clustermap
# https://likegeeks.com/seaborn-heatmap-colorbar/
# https://stackoverflow.com/questions/67909597/seaborn-clustermap-colorbar-adjustment
# https://matplotlib.org/stable/api/figure_api.html#matplotlib.figure.Figure.colorbar
# https://matplotlib.org/stable/api/_as_gen/matplotlib.figure.Figure.colorbar.html#matplotlib.figure.Figure.colorbar
# https://www.pythonfixing.com/2021/10/fixed-seaborn-clustermap-colorbar.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.colorbar.html
# https://stackoverflow.com/questions/27988846/how-to-express-classes-on-the-axis-of-a-heatmap-in-seaborn/27992943#27992943

    # PAGE 5
    op_e_cor_df = op_e_df.drop('year')\
                         .filter(pl.col('real int rate').is_not_null())\
                         .to_pandas()
    
    # this creates several axes: row_dendrogram, col_dendrogram, cbar
    cg = sn.clustermap(op_e_cor_df.corr(),
                       #annot=True, fmt=".1f",
                       #cmap= 'Blues',
                       cmap= 'RdYlGn',
                       figsize=(8.5, 11),
                       # cbar_kws= {'shrink': 'left'},
                       )
    
    # cg.ax_col_dendrogram.remove()
    cg.ax_row_dendrogram.remove()
    # cg.ax_cbar.remove()
    
    # make room at the top of the whole fig, not just the plot
    # add suptitle
    cg.figure.subplots_adjust(top=0.87)
    
    cg.figure.suptitle(
        f' \n{PAGE5_SUPTITLE}',
        fontsize=13,
        fontweight='bold')
    # plt.tight_layout(pad= 0.5)
    
    # cg.figure.supxlabel(f'{PAGE4_SOURCE}\n ', fontsize= 8)
    
    # ???
    # cg.figure.subplots_adjust(right=0.7)
    
    # cbar_position shows the relative positions for x and y
    # and the relative sizes of width and height
    # all relative to the size of the ax's dimensions
    x0, y0, w_, h_ = cg.cbar_pos
    # print(cg.cbar_pos)
    cg.ax_cbar.set_position([0.04, 
                             0.4, 
                             0.05, 
                             0.18])
   
    # NB the set_position above moves the cbar, but
    # does not change the cbar_pos values
    # print(cg.cbar_pos)
    
    '''
    # this throws a warning and yields an unexpected result
    cg.ax_cbar.set_yticklabels([-0.5, -0.2, 0.0, 0.2, 0.5, 0.8, 1.0])
    '''
    
    '''
    x0, _y0, _w, _h = g.cbar_pos
    g.ax_cbar.set_position([x0, 0.9, g.ax_row_dendrogram.get_position().width, 0.02])
    g.ax_cbar.set_title('colorbar title')
    g.ax_cbar.tick_params(axis='x', length=10)
    for spine in g.ax_cbar.spines:
        g.ax_cbar.spines[spine].set_color('crimson')
        g.ax_cbar.spines[spine].set_linewidth(2)
    '''
    
    '''
    # to print only upper triangle
    mask = np.tril(np.ones_like(corr))
    values = cg.ax_heatmap.collections[0].get_array().reshape(corr.shape)
    new_values = np.ma.array(values, mask=mask)
    cg.ax_heatmap.collections[0].set_array(new_values)
    '''
    
    print('\n============================')
    print(sp.DISPLAY_5_ADDR)
    print('============================\n')
    cg.savefig(str(sp.DISPLAY_5_ADDR))
    
    del op_e_cor_df
    del cg
    gc.collect()
    
# DISTRIBUTION of E using Matplotlib and Numpy ++++++++++++++++++++++++
# https://seaborn.pydata.org/examples/structured_heatmap.html
# https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_stacked.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.legend.html
# https://matplotlib.org/stable/users/explain/axes/arranging_axes.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.tight_layout.html
# https://matplotlib.org/stable/users/explain/axes/tight_layout_guide.html#sphx-glr-users-explain-axes-tight-layout-guide-py
# https://how2matplotlib.com/how-to-change-order-of-items-in-matplotlib-legend.html
# https://www.geeksforgeeks.org/matplotlib-pyplot-legend-in-python/

    # PAGE 6
    
    fig = plt.figure(figsize=(11, 8.5), 
                     layout="constrained")
    # padding is relative to font size
    # plt.tight_layout(pad= 0.5)
    # one plot
    ax = fig.subplots()
    fig.suptitle(
        f'{PAGE6_SUPTITLE}\n{DATE_THIS_PROJECTION}',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(PAGE4_SOURCE, fontsize= 8)
    
    # remove pe data, simplify column names
    op_e_df = ind_df.drop(cs.matches('_pe'))
    op_e_df.columns = [name.split('_op_')[0].replace("_", " ")
                       for name in op_e_df.columns]
    
    # prepare data
    mat_np = op_e_df.drop('real int rate', 'year', 'SP500').to_numpy()
    mat_np[mat_np < 0] = 0
    
    ind_names = op_e_df.drop('real int rate', 'year', 'SP500')\
                       .columns
    ind_size = mat_np.sum(axis=0).tolist()
    yr_series = pl.Series(op_e_df.select('year')).to_list()
    iterate = sorted(list(zip(ind_names, ind_size)),
                     key= lambda x: x[1],
                     reverse= True)
    ind_names_sorted = [x[0] for x in iterate]
    
    dist_np = op_e_df.select(ind_names_sorted)\
                        .to_numpy()
    dist_np[dist_np < 0] = 0.
    dist_np = (dist_np / 
                  dist_np.sum(axis=1)[:, np.newaxis]).T
    
    width = 0.5
    bottom = np.zeros(len(yr_series))
    # iterate through np array
    # a col of mat_np should contain the data for a bar
    # so mat_np is transposed from yr x industry to ind x yr
    # this loop loads each row (layer) to allocate to each year's bar
    # initialize bottom at zero for all years, then increase
    # after each layer's value for each year
    for idx in range(len(dist_np)):
        p = ax.bar(yr_series, dist_np[idx, :], width, 
                   label= ind_names_sorted[idx], bottom= bottom)
        bottom += dist_np[idx, :]
        
    ax.legend(loc="lower center", reverse= True)
    ax.set_ylabel(' \n')
    
    print('\n============================')
    print(sp.DISPLAY_6_ADDR)
    print('============================\n')
    fig.savefig(str(sp.DISPLAY_6_ADDR))

    return
    