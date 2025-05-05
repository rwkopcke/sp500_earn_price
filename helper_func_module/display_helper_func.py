from copy import deepcopy
import gc
import sys

import polars as pl

from helper_func_module import helper_func as hp


def contemp_12m_fwd_proj(df, p_dict, eps, name_proj):
    '''
        add col to df that contains
        projected E over the next 4 quarters
        return df
    '''
    # put 12m fwd projection in new col name_proj
    # for all qtrs in df
    df = df.with_columns(pl.Series(
                    [fwd_12m_ern(eps, p_dict[yrqtr])
                     for yrqtr in df['yr_qtr']])
                         .alias(name_proj))\
           .cast({name_proj: pl.Float32})
    return df


def fwd_12m_ern(name, p_df):
    '''
        calculate "contemporaneous" projection 
        of next 4 qtrs of earnings
        return float
    '''
    # ensure the yr_qtrs are ascending to sum down the rows
    # from the current 'yr_qtr'
    p_df = p_df.sort(by= 'yr_qtr')
    fwd_e = sum((p_df.item(id, name)
                 for id in range(4)))
    del p_df
    gc.collect()
    return fwd_e


def page0_df(df, p_dict, p_dict_columns, name_act):
    '''
        return df with data to be plotted on page 0
        input df has actual earn (history)
        input p_dict has projections
    '''
    
    # create hf with 2cols 
    #   yr_qtr and actual 12m eps
    #   which appears only in the 4th qtr, otherwise null
    hf = df.select(pl.col(name_act),
                   pl.col('yr_qtr'))\
                .filter(pl.col('yr_qtr')
                        .map_batches(hp.is_quarter_4))\
                .join(df,
                      how= 'right',
                      on= 'yr_qtr',
                      coalesce= True)\
                .select(pl.col(name_act),
                        pl.col('yr_qtr'))

    # for each yr_qtr in df (hf), fetch its proj_df from p_dict
    # filter to select 12m proj for future Q4s
    # join with df on yr_qtr

    df = df.sort(by='yr_qtr')
    for idx, yrqtr in enumerate(df['yr_qtr']):
        # target yr_qtr, place in col for filtered pro_df
        
        if yrqtr not in p_dict.keys():
            print('\n========================================================================')
            print('In display_helper_func.page0_df():')
            print(f"{yrqtr} from df['yr_qtr'] is not in p_dict's keys")
            print('=> update_data.py likely failed to write all files properly')
            print('check record_dict.json latest_used_file vs')
            print('prev_used_files and files listed for other record_dict keys')
            print('========================================================================\n')
            sys.exit()
        
        # use only full year projections (in 12-m for Q4) and
        # Q4s for years >= year of current projection date (yrqtr)
        pro_df = p_dict[yrqtr]\
                    .select(p_dict_columns)\
                    .filter(pl.col('yr_qtr')
                            .map_batches(hp.is_quarter_4))\
                    .with_columns(pl.col('yr_qtr')
                                      .map_batches(hp.yrqtr_to_yr)
                                      .alias('year'),
                                  pl.lit(yrqtr).alias('yr_qtr'))\
                    .filter((pl.col('year') >= yrqtr[:4]))
        
        # accumulate rows for the projection DF for each yr_qtr  
        if idx == 0:
            p_df = deepcopy(pro_df)
        else:
            p_df = pl.concat([p_df, pro_df],
                             how= 'vertical')
    
    # pivot years into column names for each yr_qtr
    p_df = p_df.pivot(index= 'yr_qtr',
                      columns= 'year')\
               .sort(by= 'yr_qtr')\
    
    # build DF with data to plot
    p_df = hf.select(['yr_qtr', 
                      name_act])\
             .join(p_df,
                   on= 'yr_qtr',
                   how= 'left',
                   coalesce= True)
    del hf
    del pro_df
    gc.collect()
    return p_df


def  page1_df(df, p_df, eps, ROGQ):
    '''
        return df with data to be plotted on page 1
    '''
    
    # find most recent price from projection df
    df = df.with_columns((pl.col('price') / pl.col(eps))
                            .alias('pe'))\
           .sort(by= 'yr_qtr')
    base_price = \
        df.filter(pl.col(eps).is_not_null())[-1, 'price']

    # build projected df for graph from df and p_df
    p_df = p_df.with_columns(pl.lit(base_price)
                               .alias('fixed_price'))\
               .sort(by= 'yr_qtr')\
               .with_columns(pl.Series(
                                    [base_price * ROGQ**idx
                                     for idx in range(len(p_df))])
                                .alias('incr_price'))\
               .with_columns((pl.col('fixed_price') / pl.col(eps))
                                .alias('fix_proj_p/e'),
                             (pl.col('incr_price') / pl.col(eps))
                                .alias('incr_proj_p/e'))                      
    df = df.join(p_df,
                 on= 'yr_qtr',
                 how= 'full',
                 coalesce= True)\
           .sort(by= 'yr_qtr')\
           .select(['yr_qtr', 'pe',
                    'fix_proj_p/e', 'incr_proj_p/e'])
    return df

def page3_df(df, name_12m_fwd_eps):
    '''
        return df with data to be plotted on page 3
    '''
    
    hf = df.with_columns((pl.col(name_12m_fwd_eps) * 100 /
                          pl.col('price'))
           .alias('earnings / price'))\
           .with_columns((
               pl.col('earnings / price') -
               pl.col('real_int_rate'))
           .alias('equity premium'))\
           .rename({'real_int_rate': '10-year TIPS rate'})\
           .select('yr_qtr', 
                   'earnings / price', 
                   'equity premium',
                   '10-year TIPS rate')\
           .sort(by= 'yr_qtr')
    return hf 
