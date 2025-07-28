import polars as pl
import json

from helper_func_module import helper_func as hp 


def update(env, loc_env):
    '''
        Inputs are Path() instances
        Returns 1 dict and 3 sets
            record_dict
            new_files_set
            files_to_read_set
            files_to remove_set
        
        Reads record_dict.pkl, writes to backup_record_dict.pkl
        Returns set of names for all new input files
        Returns set of names of input files to read (pertinent data)
        Returns set of names of prev used input files 
            that now contain stale data & to be delisted
    '''
    
    address = env.RECORD_DICT_ADDR
    backup_address = env.BACKUP_RECORD_DICT_ADDR
    input_sp_dir = env.INPUT_DIR
    input_rr_addr = env.INPUT_RR_ADDR
    yr_qtr = loc_env.YR_QTR_NAME
    
    return_empty_objects = [dict(), set(), set()]
    
    # READ: load names of new data files, return if none
    input_sp_files_set = \
        set(str(f.name) 
            for f in input_sp_dir.glob('sp-500-eps*.xlsx'))
    
    # if new input data is not complete, return up chain to main    
    if not input_sp_files_set:
        print('\n============================================')
        print(f'{input_sp_dir} contains no sp input files')
        print(f'Return to menu of actions')
        print('============================================\n')
        return return_empty_objects
    
    if not input_rr_addr.exists():
        print('\n============================================')
        print(f'{input_rr_addr} does not exist')
        print(f'Return to menu of actions')
        print('============================================\n')
        return return_empty_objects
    
    # READ: record_dict
    if address.exists():
        with open(address,'r') as f:
            record_dict = json.load(f)
            
    # use blank dict if necessary
    if ((not address.exists()) | 
        (not record_dict)):
        record_dict = {}
        print('\n============================================')
        print(f'No record_dict.json exists at\n{address}')
        print(f'or record_dict is "falsey"')
        print(f'Initializing record_dict')
        print('============================================\n')
         
        record_dict = {'sources': {'s&p': '',
                                   'tips': ''},
                       'latest_used_file': "",
                       'prev_used_files': [],
                       'prev_files': []}
        
    # WRITE: if prev record_dict exists, write backup
    else:
        with open(backup_address, 'w') as f:
            json.dump(record_dict, f, indent= 4)
        print('\n============================================')
        print(f'Read record_dict from: \n{address}')
        print(f'Wrote record_dict to: \n{backup_address}')
        print('============================================\n')
        
# create set of new files that were not previously seen
    prev_files_set = set(record_dict['prev_files'])
    new_files_set = input_sp_files_set - prev_files_set
    record_dict['prev_files'] = \
        sorted(list(prev_files_set | new_files_set), reverse= True)
    
# if nothing new, return up the chain to main
    if not new_files_set:
        print('\n============================================')
        print(f'No previously unseen files at')
        print(f'{address}')
        print('No data files have been written')
        print('============================================\n')
        return return_empty_objects

# ===========================================================
# NB from here, new_files_set is not empty
#    values for record_dict's keys may be empty
# ===========================================================
   
    # create df with the names and dates of the new_files
    data_df = pl.DataFrame(list(new_files_set), 
                           schema= ["file"],
                           orient= 'row')\
                .with_columns(pl.col('file')
                            .map_batches(hp.string_to_date)
                            .alias('date'))\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(yr_qtr))
    
    # fetch used files list from record_dict
    # prev_used_files_list contains dicts, each with keys
    # 'file', 'date', loc_env.YR_QTR_NAME
    # one dict for each prev_used_file
    prev_used_files_list = record_dict['prev_used_files']
    if not prev_used_files_list:
        used_df = pl.DataFrame()
        prev_used_files_set = set()
        
    else:    
        # col names are the same as the keys for the dicts
        # contained in prev_used_files_list
        used_df = pl.DataFrame(prev_used_files_list)  
        prev_used_files_set = set(
            pl.Series(used_df['file']).to_list())
    
    # new files can update and replace prev files for same year_qtr
    # (prev_used_files has only one file per quarter)
    # find the latest new file for each quarter (agg(all.sort_by.last))
    # in each group, using all cols, sort_by date, select the last row
    # in new_files_list
    
    used_df = pl.concat([used_df.cast({'date': pl.Date}), 
                         data_df.select(used_df.columns)],
                         how= 'vertical')\
                .group_by(yr_qtr)\
                .agg(pl.all().sort_by('date').last())\
                .sort(by= yr_qtr)

    # and files to be used from the new input files
    used_files_set = set(pl.Series(used_df['file']).to_list())
    files_to_read_set = used_files_set - prev_used_files_set

    # update prev_used_files for the new files
    # update_df['update_used_files']: list of dicts
    # cast allows json for saving record_dict
    update_df = used_df.cast({'date': pl.String})\
                       .select(pl.struct(pl.all())
                                 .alias('update_used_files'))
    
    record_dict['prev_used_files'] = sorted(
        update_df['update_used_files'],
        reverse= True,
        key= lambda x: x[yr_qtr])
    
    record_dict['latest_used_file'] = \
        record_dict['prev_used_files'][0]['file']
    
    record_dict['sources']['s&p'] = env.SP_SOURCE
    record_dict['sources']['tips'] = env.REAL_RATE_SOURCE
    
    return [record_dict, new_files_set, files_to_read_set]
    