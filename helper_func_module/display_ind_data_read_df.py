import polars as pl
import polars.selectors as cs

def read(env, fixed):
    # find latest year for actual data
    # do not use only p/e data, 'real_rate' data, eps data
    ind_df = pl.read_parquet(env.OUTPUT_IND_ADDR)\
               .drop(cs.matches('Real_Estate'))\
               .sort(by= 'year')
               
    year = pl.Series(
              ind_df.select("year", 'SP500_rep_eps')\
                    .filter(pl.col('SP500_rep_eps')
                            .is_not_null())\
                    .select('year').max()
              ).to_list()[0]
    
    DATE_THIS_PROJECTION = \
        f'\nactual annual operating earnings through {year}'
    
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
    
    return ind_df, op_e_df, year, DATE_THIS_PROJECTION
