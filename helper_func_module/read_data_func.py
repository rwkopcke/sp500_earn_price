'''
   these are functions used by the update_data and display_data
   scripts
        
   access these values in other modules by
        import sp500_pe.read_data_func as rd
'''
import sys

from openpyxl import load_workbook
import openpyxl.utils.cell as ut_cell
import polars as pl
import polars.selectors as cs

from helper_func_module import helper_func as hp


def read_sp_date(wksht,
                 date_keys, value_col_1, 
                 date_key_2, value_col_2,
                 column_names, yr_qtr_name,
                 include_prices= False):
    '''
        fetch date of excel workbook from s&p
        fetch dates and prices that have occurred after
        the last reported set of financial data if
        include_prices= True
        return date in name_date
        (optional) return df with recent dates and prices
    '''
    
    if date_keys is None:
        print('\n============================================')
        print(f'Date keys are {date_keys} for {wksht}')
        print(f'read_data_func.py: read_sp_date')
        print('============================================\n')
        sys.exit()
    
    # fetch row for latest date and price
    key_row = hp.find_key_row(wksht, 'A', 1, 
                              key_values= date_keys)

    if (key_row == 0):
        print('\n============================================')
        print('In read_data_funct.py read_sp_date, date keys:')
        print(f'Found no {date_keys} in {wksht}')
        print('============================================\n')
        sys.exit()
        
    name_date = hp.dt_str_to_date(
                    wksht[f'{value_col_1}{key_row}'].value)
    
    # return without prices if include_prices is False
    if not include_prices:
        return [name_date, None]
        
    date_lst = []
    price_lst = []
    
    date_lst.append(name_date)
    name_date = name_date.date()  # value to return should be date()
    price_lst.append(wksht[f'{value_col_1}{key_row + 1}'].value)
    
    # fetch next date and price
    key_row = hp.find_key_row(wksht, 'A', key_row, 
                              key_values= date_key_2)
    
    if (key_row == 0):
        print('\n============================================')
        print('In read_data_func.py read_sp_date, for date_key_2:')
        print(f'Found no {date_key_2} in {wksht}')
        print('============================================\n')
        sys.exit()
    
    date_lst.append(hp.dt_str_to_date(
        wksht[f'A{key_row - 2}'].value))
    price_lst.append(wksht[f'{value_col_2}{key_row -2}'].value)
    
    df = pl.DataFrame({
                column_names[0]: date_lst,
                column_names[1]: price_lst},
                schema= {column_names[0]: pl.Date, 
                            column_names[1]: pl.Float32})\
           .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(yr_qtr_name))
    
    return [name_date, df]


def data_block_reader(wksht, start_row, stop_row,
                      first_col, last_col, skip_cols= []):
    """
    This function returns the block of data in a worksheet
    as a list of lists
    skip_cols: num of col, zero-based indexing, from first col
        skip_cols are numbers
    """
    
    rng = wksht[f'{first_col}{start_row}:{last_col}{stop_row}']
    data = [[col_cell.value for c_dx, col_cell in enumerate(row)
                            if c_dx not in skip_cols]  
            for row in rng]
    return data
    

def sp_loader(wksht, rows_no_update,
                act_key, end_key, first_col, last_col,
                skip_cols= [], column_names= [],
                yr_qtr_name= ''):
    '''
        read data from s&p excel workbook sheet
        that contains history for prices and earnings
        and that contains projections of future earnings
        return df
    '''
    
    # fetch data from wksht
    # fix the block of rows and cols that contain the data
    key_row = hp.find_key_row(wksht, 'A', 1, 
                              key_values= act_key)
    
    # first data row to read
    start_row = 1 + key_row
    
    # find the row with terminal key values, the extent of the data
    # move up from this row by amount = # row_no_update (hist data)
    # or 1 (proj data)
    # collect only the rows with new data
    stop_row = hp.find_key_row(wksht, 'A', start_row,
                               key_values= end_key,
                               is_stop_row= True)
    len_ = len(rows_no_update)
    bool_v = len_ > 0
    # adjust stop row for # rows not to update
    # if len_ > 0: subtract len_ from stop_row #
    # if len == 0: subtract 1 from stop_row #
    stop_row -= len_ * bool_v + 1 * (1 - bool_v)  

    if (stop_row < start_row):
        print('\n================================================')
        print('In sp_loader:')
        print('the saved history has more quarters than the')
        print('the history in the new file')
        print('================================================\n')
        sys.exit()
    
    # fetch the data from the block
    # always reads at least one row
    data = data_block_reader(wksht, start_row, stop_row,
                             first_col, last_col, skip_cols)
    
    # row[0]: datetime or str, '%m/%d/%Y' is first 'word' in str
    # iterate over rows to convert all dates to datetime
    # this series is called 'date' in the pl.DF (must be one dtype)
    for row in data:
        row[0] = hp.dt_str_to_date(row[0])
        
    df = pl.DataFrame(data, schema=column_names, orient="row")\
                .cast({cs.float(): pl.Float32,
                       cs.datetime(): pl.Date})\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(yr_qtr_name))
                            
    return df


def margin_loader(wksht, dates_no_update,
                  row_key, first_col, stop_col_key,
                  stop_row_data_offset,
                  yr_qtr_name):
    '''
        read data from s&p excel worksheet
        that contains history for margins
        return df
    '''
    
    # find the rows with dates and data
    # start row contains dates
    start_row = hp.find_key_row(wksht, 'A', 1, key_values= [row_key])
    stop_row = start_row + stop_row_data_offset
    
    # fetch block of data from columns
    first_col = 'A'
    # if not fetching the full history
    if len(dates_no_update) > 0:
        last_col = 'C'
    else:
        # find last col with data, return number -> letter
        # years decrease as cols increase
        # start_col 1-based indexing col_num, default key is None
        last_col_num = hp.find_key_col(wksht, start_row,
                                       start_col= 3) - 1
        last_col = ut_cell.get_column_letter(last_col_num)

    # list of lists for each row, including headers for cols (start_row)
    data =  data_block_reader(wksht, start_row, stop_row,
                              first_col, last_col)
     
    # data_values omits the first row (col headers) from data
    data_values = [row for row in data[1:]]
    
    # omit the * for 2008, take first entry (yr) in list data[0]
    col_names = [str(item).split('*')[0] for item in data[0]]
    
    # build "tall" 2-col DF with 'year_qtr' and 'margin'
    df = pl.DataFrame(data_values, schema= col_names,
                      orient= 'row')\
                .with_columns(pl.col(row_key)
                              .map_elements(lambda x: x.split(' ')[0],
                                            return_dtype= str))\
                .cast({cs.float(): pl.Float32})\
                .unpivot(index= row_key, variable_name='year')
            # index: names of cols to remain cols
            # variable_name: name of col to contain names of cols pivoted
    
    df = df.with_columns(
                pl.struct([row_key, 'year'])\
                    .map_elements(lambda x: 
                                  (f"{x['year']}-{x[row_key]}"),
                                  return_dtype= pl.String)\
                    .alias(yr_qtr_name))\
            .drop(['year', row_key])\
            .rename({'value': 'op_margin'})
            
    return df


def industry_loader(wksht, years_no_update,
                    first_row_op, first_row_rep, num_inds,
                    start_col_key, stop_col_key):
    '''
        read data from s&p excel worksheet
        that contains history for industry data
        return df
    ''' 
    
## +++++ read industries from 1st col
    last_row_op = first_row_op + num_inds + 1
    ind = data_block_reader(wksht, first_row_op + 2, 
                            last_row_op, 'A', 'A')
    # remove parentheticals
    ind = [item[0].split(' (')[0]
           for item in ind]
    # remove 'S&P 500' and replace space with '_'
    ind_name = ['_'.join(item.rstrip().split(' ')[2:])
                for item in ind]
    # set first name to 'SP500'
    ind_name[0] = 'SP500'
                        
## +++++  read data  +++++
    # search the first row of data
    # 2nd block, bracketed by default key None
    # read the last three years of data
    
    # key value is None (col before 1st number to read)
    first_col_num = 1 + hp.find_key_col(wksht, first_row_op + 2,
                                        start_col= 3)
    # skip years not to update
    first_col_num += 2 * len(years_no_update)\
    
    first_col = ut_cell.get_column_letter(first_col_num)
    
    # key_value is None
    last_col_num = -1 + hp.find_key_col(wksht, first_row_op + 2, 
                                        start_col= first_col_num)
    
    last_col = ut_cell.get_column_letter(last_col_num)
   
## op data
    # fetch op e by industry, including row with headings
    # list of lists for each row
    data = data_block_reader(wksht, first_row_op, last_row_op,
                             first_col, last_col)
    
    date_data_names = [f'{name[:4]} {name[-3:]}'
                       for name in data[0]]
    
    columns_pe = [col
                  for col in date_data_names
                  if 'P/E' in col]
    columns_e = [col 
                 for col in date_data_names
                 if 'EPS' in col]
    years = [item[0:4] for item in columns_e]

    # use only rows with data
    gf = pl.DataFrame(data[2:], schema= date_data_names,
                      orient= 'row')\
           .cast({cs.float(): pl.Float32})
    
    # select and pivot to years for rows, data type for cols
    gf_pe = hp.gen_sub_df(gf, ind_name, '_op_pe', 
                          columns_pe, years)
    gf_e  = hp.gen_sub_df(gf, ind_name, '_op_eps', 
                          columns_e, years)
    
    df = gf_e.join(gf_pe,
                   on= 'year',
                   how= 'left',
                   coalesce= True)
    
## rep data
    last_row_rep = first_row_rep + num_inds - 1
    
    ## +++++  read data  +++++
    # fetch rep e by industry
    # list of lists for each row
    # no skips rows or year/type data
    data = data_block_reader(wksht, first_row_rep, last_row_rep,
                             first_col, last_col)

    gf = pl.DataFrame(data, schema= date_data_names,
                      orient= 'row')\
           .cast({cs.float(): pl.Float32})

    # select and pivot to years for rows, data type for cols
    gf_pe = hp.gen_sub_df(gf, ind_name, '_rep_pe', 
                          columns_pe, years)
    gf_e  = hp.gen_sub_df(gf, ind_name, '_rep_eps', 
                          columns_e, years)
    
    df = df.join(gf_e,
                   on= 'year',
                   how= 'left',
                   coalesce= True)\
           .join(gf_pe,
                 on= 'year',
                 how= 'left',
                 coalesce= True)
    
    return df


def fred_reader(wksht, first_row, col_1, col_2,
                yr_qtr_name, rr_col_name):
    '''
        read data from FRED excel worksheet
        that contains history for real interest rates
        return df
    '''

    last_row = wksht.max_row
    data = data_block_reader(wksht, first_row, last_row,
                             col_1, col_2)
    
    if data[-1:][0][0] is None:
        print('\n================================================')
        print('In fred_reader:')
        print(f'"none" appears in the input data')
        print(f'last row is {data[-1]}')
        print('================================================\n')
        sys.exit()
    
    df = pl.DataFrame(data, schema=['date', rr_col_name],
                      orient='row')\
           .with_columns(pl.col('date')
                        .map_batches(hp.date_to_year_qtr)
                        .alias(yr_qtr_name))\
           .group_by(yr_qtr_name)\
           .agg([pl.all().sort_by('date').last()])\
           .sort(by= yr_qtr_name)\
           .drop('date')\
           .cast({cs.datetime(): pl.Date,
                  cs.float(): pl.Float32})
    return df
