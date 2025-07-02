import json
import sys

from helper_func_module import helper_func as hp

def read(env):
    if env.RECORD_DICT_ADDR.exists():
        with env.RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)
        print('\n============================================')
        print(f'Read record_dict from: \n{env.RECORD_DICT_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No record_dict in \n{env.RECORD_DICT_ADDR.name}')
        print(f'at: \n{env.RECORD_DICT_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
    # returns proj's date, a polars series with one date element
    date_this_projn = \
        hp.string_to_date([record_dict['latest_used_file']])
    # returns yr_qrt, a polar series with one str element
    # then, extract the str
    yr_qtr_current_projn = \
        hp.date_to_year_qtr(date_this_projn).to_list()[0]
    # extract the datetime.date 
    date_this_projn = date_this_projn.to_list()[0]
    
    return record_dict, date_this_projn, yr_qtr_current_projn
