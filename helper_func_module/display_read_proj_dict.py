import sys

import polars as pl
import polars.selectors as cs

def read(record_dict, yr_qtr_set, env):
    proj_df = pl.DataFrame()
    if env.OUTPUT_PROJ_ADDR.exists():
        with env.OUTPUT_PROJ_ADDR.open('r') as f:
            proj_df = pl.read_parquet(source= f)
        
    if not proj_df.is_empty():
        print('\n============================================')
        print(f'Read projection dataframe' )
        print(f'at \n{env.OUTPUT_PROJ_ADDR}')
        print('============================================\n')
        
        proj_dict = dict()
        for k in proj_df.columns:
            proj_dict[k] = proj_df.select(pl.col(k)
                                          .struct.unnest())\
                                  .drop_nulls()\
                                  .cast({cs.float(): pl.Float32,
                                         cs.integer(): pl.Int16})\
                                  .filter(pl.col('yr_qtr')>=k)
        
    else:
        print('\n============================================')
        print(f'No file at \n{env.OUTPUT_PROJ_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
    
    return proj_dict, sorted(set(proj_dict.keys()), reverse= True)
