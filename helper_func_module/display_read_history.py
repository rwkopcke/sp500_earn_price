import sys
import polars as pl

def read(record_dict, env, fixed):
    yr_qtr_set = {item['yr_qtr']
                  for item in record_dict['prev_used_files']}
    
    if env.OUTPUT_HIST_ADDR.exists():
        with env.OUTPUT_HIST_ADDR.open('r') as f:
            data_df = pl.read_parquet(source= f,
                            columns= fixed.HIST_COL_NAMES)
            
        print('\n============================================')
        print(f'Read data history from: \n{env.OUTPUT_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No data history in: \n{env.OUTPUT_HIST_ADDR.name}')
        print(f'at: \n{env.OUTPUT_HIST_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
    return data_df, yr_qtr_set
