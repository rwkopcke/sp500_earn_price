import polars as pl

def plots_page4(ax, df,
                title= None, 
                ylim = (None, None), 
                xlabl= None,
                ylabl= None):
    '''
        A helper function to show many scatter plots
        x axis is the 1st col of df
        the remaining cols are scatter plots
        the legend names of all plotted series
           are from the col names in df.columns
            before calling this func, remove suffix
            from col names, '_op_...' or '_rep_...'
    '''
    
    # create the title and labels for the plot
    ax.set_title(title, fontweight= 'bold', loc= 'left')
    ax.set_xlabel(xlabl, fontweight= 'bold')
    ax.set_ylabel(ylabl, fontweight= 'bold')
    
    # prepare info for the horizontal axis:
    # df.columns[0] is year
    years = pl.Series(df.select('year')).to_list()
    names = df.columns[1:]
    
    # color_ = ['gray', 'blue', 'red'] by industry
    
    # df.columns[1:0] are the industries
    for name in names[1:]:
        ax.scatter(years, 
                   df.select(name), 
                   marker= 'o', 
                   s= 15,
                   label= name)

    # names[0] is sp500 index
    ax.scatter(years, 
               df.select(names[0]), 
               marker= 's', 
               s= 15,
               color= 'black',
               label= 'S&P 500 Index')
    
    ax.plot(years, 
            df.select(names[0]), 
                linestyle= 'dotted',
                color= 'lightgrey')
    
    print(ylim)
    ax.set_ylim(ylim)

    # the 1st arg gets locs for the labels
    # the 2nd arg specs the labels for these locs             
    ax.set_yticks(ax.get_yticks(), ax.get_yticklabels(), 
                  fontsize= 8)
    
    ax.set_xticks(ax.get_xticks(), years, 
                  rotation= 90, fontsize= 7)
    #ax.yaxis.set_ticks_position('both')
    
    ax0 = ax.twinx()
    ax0.set_ylim(ax.get_ylim())
    ax0.set_yticks(ax.get_yticks(), ax.get_yticklabels(),
                   fontsize= 8)
    ax0.set_ylabel(' ') #creates a space on the right side
    
    # https://matplotlib.org/stable/users/explain/axes/legend_guide.html
    ax.legend(fontsize= 8,
              loc= 'upper left')
    
    '''
    ax.hlines(y=200, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    ax.hlines(y=250, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    ax.hlines(y=150, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    '''
    
    return ax
