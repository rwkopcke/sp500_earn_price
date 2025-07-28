'''This program reads selected data from S&P, sp-500-eps-est.xlsx
        https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
   and from the 10-year TIPS rate from FRED: 
        https://fred.stlouisfed.org/series/DFII10
   It writes these data as polars dataframes to .parquet files
        and writes a record of the files that it has read and writen
        as a dictionary to a .json file
        
   see sp_env.py
        for the addresses of the files within this project are declared
        for advice in resetting paths for debugging and for reinitializing
        the project's output files

   The polars dataframes in input_output
        the latest projections of earnings for the
        S&P500 within each quarter since late 2017. 
   A separate polars dataframe contains
        the actual earnings and the value of the index for each quarter 
        beginning in 1988. This dataframe also contains actual values for 
        operating margins, revenues, book values, dividends, and other 
        actual data reported by S&P, plus actual values for the 10-year TIPS.
   
   The addresses of documents for this project appear in this program's 
   project directory: S&P500_PE/sp500_pe/sp_env.py
'''

#######################  Parameters  ###################################
import sys
import gc

import polars as pl
import polars.selectors as cs

from openpyxl import load_workbook

from main_script_module import sp_env as sp
from helper_func_module import update_record
from helper_func_module import update_write_history_and_industry_files
from helper_func_module import update_proj_hist_files
from helper_func_module import update_write_proj_files
from helper_func_module import update_write_record
from helper_func_module import helper_func as hp
from helper_func_module import read_data_func as rd

from dataclasses import dataclass

@dataclass(frozen= True)
class Fixed_Update_Parameters:
    ARCHIVE_RR_FILE = False

    # data from "ESTIMATES&PEs" wksht
    RR_COL_NAME = 'real_int_rate'
    YR_QTR_NAME = 'yr_qtr'
    PREFIX_OUTPUT_FILE_NAME = 'sp-500-eps-est'
    EXT_OUTPUT_FILE_NAME = '.parquet'

    SHT_EST_NAME = "ESTIMATES&PEs"
    COLUMN_NAMES = ['date', 'price', 'op_eps', 'rep_eps',
                    'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']
    PROJ_COLUMN_NAMES = ['date', 'op_eps', 'rep_eps',
                        'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']

    SHT_QTR_NAME = "QUARTERLY DATA"
    COLUMN_NAMES_QTR = ['date', 'div_ps', 'sales_ps',
                        'bk_val_ps', 'capex_ps', 'divisor']

    SHT_IND_NAME = 'SECTOR EPS'

    # NB ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # all search (row or col) "keys" should be None or lists
    # all column indexes in skip lists below are zero-based ('A' is 0)
    # all specific individual column designations are letters
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    ACTUAL_KEYS = ['ACTUALS', 'Actuals']
    
    SHT_EST_DATE_PARAMS = {
        'date_keys' : ['Date', 'Data as of the close of:'],
        'value_col_1' : 'D',
        'date_key_2' : ACTUAL_KEYS,
        'value_col_2' : 'B',
        'column_names' : COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_HIST_PARAMS = {
        'act_key' : ACTUAL_KEYS,
        'end_key' : None,
        'first_col' : 'A',
        'last_col' : 'J',
        'skip_cols' : [4, 7],
        'column_names' : COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    MARG_KEY = 'QTR'
    SHT_BC_MARG_PARAMS = {
        'row_key': MARG_KEY,
        'first_col': 'A',
        'stop_col_key': None,
        'stop_row_data_offset': 4,
        'yr_qtr_name': YR_QTR_NAME
    }

    SHT_IND_PARAMS = {
        'first_row_op': 6,
        'first_row_rep': 63,
        'num_inds': 12,
        'start_col_key': None,
        'stop_col_key': None
    }

    SHT_QTR_PARAMS = {
        'act_key' : ['END'],
        'end_key' : None,
        'first_col' : 'A',
        'last_col' : 'I',
        'skip_cols' : [1, 2, 7],
        'column_names' : COLUMN_NAMES_QTR,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_DATE_PARAMS = {
        'date_keys' : ['Date', 'Data as of the close of:'],
        'value_col_1' : 'D', 
        'date_key_2' : None, 
        'value_col_2' : None,
        'column_names' : None,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_PARAMS = {
        'act_key' : ['ESTIMATES'],
        'end_key' : ACTUAL_KEYS,
        'first_col' : 'A',
        'last_col' : 'J',
        'skip_cols' : [1, 4, 7],
        'column_names' : PROJ_COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_FRED_PARAMS = {
        'first_row': 12,
        'col_1': 'A',
        'col_2': 'B',
        'yr_qtr_name': YR_QTR_NAME,
        'rr_col_name': RR_COL_NAME
    }


#######################  MAIN Function  ###############################

def update():
    ''' Input files from S&P and Fred may contain new data
        This .py incorporates new data with data collected previously
        and
        Writes the updated DFs to
            sp500_pe_df_actuals.parquet
            sp500_pe_df_estimates.parquet
            sp500_ind_df.parquet
        Records these transactions in
            record_dict.json
    '''
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  set immutable parameters  ++++++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 

    # fetch project immutable params specified in sp_env,py
    env = sp.params
    # fetch update_data (local) immutable params, from class above
    loc_env = Fixed_Update_Parameters()
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  update records for new files to be read  +++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 

    # load record_dict - if record_dist is None, create it
    
    [record_dict, new_files_set, files_to_read_set] = \
         update_record.update(env, loc_env)
    
    # no new data in the input dir => no update necessary => quit
    if not files_to_read_set:
        print('\n============================================')
        print('Dates of input data files are "stale"')
        print('Stop Update and return to menu of actions')
        print('============================================\n')
        return

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  fetch historical aggregate data  +++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
    
    print('\n================================================')
    print(f'Updating historical data from: {record_dict["latest_used_file"]}')
    print(f'in directory: \n{env.INPUT_DIR}')
    print('================================================\n')
    
    ## ACTUAL DATA from existing .parquet file (not yet updated with new data)
    # the rows (qtrs) to be updated are the rows that
    # contain null in the op_eps col => assumes history is not revised
    # put the yr_qtr for rows NOT to be updated in the set rows_no_update
    if env.OUTPUT_HIST_ADDR.exists():
        actual_df = pl.read_parquet(env.OUTPUT_HIST_ADDR)
        
        rows_not_to_update_set = \
            set(pl.Series(actual_df
                          .filter(pl.col('op_eps').is_not_null())
                          .select(pl.col(loc_env.YR_QTR_NAME)))
                  .to_list())
    else:
        rows_not_to_update_set = {}
    
## REAL INTEREST RATES, eoq, from FRED DFII10
    active_workbook = load_workbook(filename= env.INPUT_RR_ADDR,
                                    read_only= True,
                                    data_only= True)
    active_sheet = active_workbook.active
    real_rt_df = rd.fred_reader(active_sheet,
                                **loc_env.SHT_FRED_PARAMS)

## NEW HISTORICAL DATA
    ## WKSHT with new historical values for P and E from new excel file
    latest_file_addr = env.INPUT_DIR / record_dict["latest_used_file"]
    active_workbook = load_workbook(filename= latest_file_addr,
                                    read_only= True,
                                    data_only= True)
    # most recent date and prices
    active_sheet = active_workbook[loc_env.SHT_EST_NAME]

    # add_df, dates and latest prices, beyond historical data
    name_date, add_df = rd.read_sp_date(active_sheet, 
                                        **loc_env.SHT_EST_DATE_PARAMS,
                                        include_prices= True)
    
    # load new historical data
    # omit rows whose yr_qtr appears in the rows_no_update list
    df = rd.sp_loader(active_sheet,
                      rows_not_to_update_set,
                      **loc_env.SHT_HIST_PARAMS)
    
    # if any date is None, halt
    if (name_date is None or
        any([item is None
            for item in add_df['date']])):
        
        print('\n============================================')
        print(f'{latest_file_addr} \nmissing history date')
        print(f'Name_date: {name_date}')
        print(actual_df['date'])
        print('============================================\n')
        sys.exit()
        
    # update add_df with new historical data
    add_df = pl.concat([add_df, df], how= "diagonal")
               
    # include rr in add_df
    add_df = add_df.join(real_rt_df, 
                         how="left", 
                         on=[loc_env.YR_QTR_NAME],
                         coalesce= True)
    
    del real_rt_df
    del df
    gc.collect()
        
## MARGINS add to add_df
    margins_df = rd.margin_loader(active_sheet,
                                  rows_not_to_update_set,
                                  **loc_env.SHT_BC_MARG_PARAMS)
    
    add_df = add_df.join(margins_df, 
                         how="left", 
                         on= loc_env.YR_QTR_NAME,
                         coalesce= True)
    
    del margins_df
    gc.collect()

## QUARTERLY DATA add to add_df
    active_sheet = active_workbook[loc_env.SHT_QTR_NAME]

    # ensure all dtypes (if not string or date-like) are float32
    # some dtype are null when all col entries in short df are null
    qtrly_df = rd.sp_loader(active_sheet,
                            rows_not_to_update_set,
                            **loc_env.SHT_QTR_PARAMS)\
                 .cast({~(cs.temporal() | cs.string()): pl.Float32,
                        cs.datetime(): pl.Date})
    
    add_df = add_df.join(qtrly_df,  
                         how= "left", 
                         on= [loc_env.YR_QTR_NAME],
                         coalesce= True)
    
    del qtrly_df
    gc.collect()
    
## ACTUAL_DF update: remove rows to be updated and concat with add_df
    # align cols of actual_df with add_df
    # ensure rows do not overlap
    
    if env.OUTPUT_HIST_ADDR.exists():
        actual_df = pl.concat([add_df.filter(
                                    ~pl.col(loc_env.YR_QTR_NAME)
                                    .is_in(rows_not_to_update_set)),
                               actual_df.select(add_df.columns)
                                    .filter(pl.col(loc_env.YR_QTR_NAME)
                                    .is_in(rows_not_to_update_set))],
                               how= 'vertical')\
                      .sort(by= loc_env.YR_QTR_NAME)
    else:
        actual_df = add_df.sort(by= loc_env.YR_QTR_NAME)
    
    del add_df
    gc.collect()

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++  fetch historical industry data  ++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # read stored data
    if env.OUTPUT_IND_ADDR.exists():
        ind_df = pl.read_parquet(env.OUTPUT_IND_ADDR)\
                        .sort(by= 'year', descending= True)
                    
        years_no_update = set(pl.Series(ind_df
                                .drop_nulls(subset='SP500_rep_eps')
                                .select(pl.col('year')))
                                .to_list())
    else:
        years_no_update = []
    
    # find new industry data
    active_sheet = active_workbook[loc_env.SHT_IND_NAME]
    add_ind_df = rd.industry_loader(active_sheet,
                                    years_no_update,
                                    **loc_env.SHT_IND_PARAMS)
    # add col with Q4 value of real_int_rate each year from actual_df
    add_ind_df = add_ind_df.join(
                 actual_df.select([loc_env.YR_QTR_NAME, 'real_int_rate'])
                          .filter(pl.col(loc_env.YR_QTR_NAME)
                                    .map_elements(lambda x: x[-1:]=='4',
                                                return_dtype= bool))
                          .with_columns(pl.col(loc_env.YR_QTR_NAME)
                                    .map_elements(lambda x: x[0:4],
                                                return_dtype= str)
                                    .alias('year'))
                          .drop(loc_env.YR_QTR_NAME),
                 on= 'year',
                 how= 'left',
                 coalesce= True)\
            .sort(by= 'year', descending= True)\
            .cast({~cs.string() : pl.Float32})
    
    if env.OUTPUT_IND_ADDR.exists():
        years = pl.Series(add_ind_df['year']).to_list()
        ind_df = pl.concat([add_ind_df,
                            ind_df.filter(~pl.col('year')
                                            .is_in(years))],
                            how= 'vertical')
    else:
        ind_df = add_ind_df.sort(by= 'year', descending= True)
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++ write history, industry files  ++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    actual_df = actual_df.cast({cs.float(): pl.Float32,
                                cs.integer(): pl.Int16})
    ind_df = ind_df.cast({cs.float(): pl.Float32,
                          cs.integer(): pl.Int16})

    update_write_history_and_industry_files.write(actual_df, ind_df, env)
    
    del actual_df
    del ind_df
    gc.collect()
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++ fetch current projections & archive all proj input files  +++++++++
## +++ proj_dict: yr_qtr keys & df of proj as values +++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # fetch history
    proj_dict = update_proj_hist_files.update(env)
    
    # Fetch files_to_read from inputs
    # Update proj_dict with info in files_to_read
    # use proj of earnings for latest input file in each yr_qtr
    # ordinarily a very short set
    failure_to_read_lst = []
    for file in files_to_read_set:
        # echo file name and address to console
        active_workbook = \
            load_workbook(filename=  env.INPUT_DIR / file,
                          read_only= True,
                          data_only= True)
        active_sheet = active_workbook[loc_env.SHT_EST_NAME]
        print(f'\n input file: {file}')    
        
        # projections of earnings
        # read date of projection, no prices or other data
        name_date, _ = \
            rd.read_sp_date(active_sheet,
                            **loc_env.SHT_EST_PROJ_DATE_PARAMS)
        name_date = name_date.date()
        year_quarter = hp.date_to_year_qtr([name_date])[0]
    
        # load projections for the name_date
        proj_date_df = rd.sp_loader(
            active_sheet,[],**loc_env.SHT_EST_PROJ_PARAMS)

        # if any date is None, abort file and continue
        if (name_date is None or
            None in proj_date_df['date']):
            print('\n============================================')
            print('In main(), projections:')
            print(f'Skipped sp-500 {name_date} missing projection date')
            print('============================================\n')
            failure_to_read_lst.append(file)
            continue
        
        # accumulate proj_date_dfs in proj_dict, 
        # key for each proj_date_df is its year_quarter
        
        proj_dict[year_quarter] = proj_date_df.cast(
                                    {cs.float(): pl.Float32,
                                     cs.integer(): pl.Int16})
        
    # Print housekeeping summary for files_to_read
    l = len(files_to_read_set)
    n = len(failure_to_read_lst)
    print('\n====================================================')
    print('Reading input projection files is complete')
    print(f'\t{l - n} new input files read')
    print(f'\tfrom {env.INPUT_DIR}')
    print(f'\t{n} files not read:')
    print(f'\t{failure_to_read_lst}')
    print('====================================================')
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++ write updated proj_dict to parquet file +++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    update_write_proj_files.write(proj_dict, new_files_set, env)
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++ write record ++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    update_write_record.write(record_dict, env)
    
    return