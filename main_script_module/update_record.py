import sys
import json
import polars as pl

from main_script_module import sp_paths as sp
from helper_func_module import helper_func as hp
from helper_func_module import read_data_func as rd 


def update_record_json():
    if sp.path.RECORD_DICT_ADDR.exists():
        with sp.path.RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)

        print('\n============================================')
        print(f'Read record_dict from: \n{sp.path.RECORD_DICT_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No record dict file found at: \n{sp.path.RECORD_DICT_ADDR}')
        print(f'Initialized record_dict with no entries')
        print('============================================\n')
        record_dict = {'sources': {'s&p': '',
                                   'tips': ''},
                       'latest_used_file': "",
                       'proj_yr_qtrs' : [],
                       'prev_used_files': [],
                       'output_proj_files': [],
                       'prev_files': []}
        
# create list sp input files not previously seen
# and add them to 'prev_files'
    prev_files_set = set(record_dict['prev_files'])
    new_files_set = \
        set(str(f.name) 
            for f in sp.path.INPUT_DIR.glob('sp-500-eps*.xlsx'))
        
    if new_files_set:
        print('\n============================================')
        print(f'No eligible files in {sp.path.INPUT_DIR}')
        print('No data files have been written.')
        print('============================================\n')
        sys.exit()
    
    new_files_set = new_files_set - prev_files_set
    new_files_list = list(new_files_set)
    
    # backup existing record_dict and create new record_dict
    with sp.path.BACKUP_RECORD_DICT_ADDR.open('w') as f:
        json.dump(record_dict, f)
    print('============================================')
    print(f'Wrote record_dict to: \n{sp.path.BACKUP_RECORD_DICT_ADDR}')
    print('============================================\n')

# add the new files to historical record
    # record paths for current sources
    record_dict['sources']['s&p'] = sp.path.SP_SOURCE
    record_dict['sources']['tips'] = sp.path.REAL_RATE_SOURCE
    
    record_dict['latest_used_file'] = max(new_files_list)
    if record_dict['prev_files']:
        record_dict['prev_files'] = \
            list(set(record_dict['prev_files']) | new_files_set)
    else:
        record_dict['prev_files'] = list(new_files_set)
    record_dict['prev_files'].sort(reverse= True)

# find the latest new file for each quarter (agg(sort).last)
    data_df = pl.DataFrame(list(new_files_set), 
                          schema= ["new_files"],
                          orient= 'row')\
                .with_columns(pl.col('new_files')
                            .map_batches(hp.string_to_date)
                            .alias('date'))\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias('yr_qtr'))\
                .group_by('yr_qtr')\
                .agg([pl.all().sort_by('date').last()])\
                .sort(by= 'yr_qtr') 
    
    del new_files_set

# combine with prev_files where new_files has larger date for year_qtr
# (new files can update and replace prev files for same year_qtr)
# new_files has only one file per quarter -- no need for group_by
    seq_ = record_dict['prev_used_files']
    if seq_:
        used_df = pl.DataFrame(pl.Series(values= seq_), 
                               schema= ['used_files'],
                               orient= 'row')\
                    .with_columns(pl.col('used_files')
                            .map_batches(hp.string_to_date)
                            .alias('date'))\
                    .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias('yr_qtr'))
        del seq_
                
    # update used_files, a join with new files
        # 1st filter removes used_df 'yr_qtr' rows that 
        #     are not in data_df (i.e. are not to be updated)
        # 2nd filter keeps only the rows for which data_df
        #     updates used_df (used_df['date']<data_df['date'])
        # the filtered yr_qtr rows are to draw their data from new_df
    # after renaming, ensures that 'date' ref only files with new data
    # proj_to_delete names only files that are null or are superceded
        used_df = used_df.join(data_df,
                               on= 'yr_qtr',
                               how= 'full',
                               coalesce= True)\
                         .filter(pl.col('date_right').is_not_null())\
                         .filter(((pl.col('date').is_null()) | 
                                  (pl.col('date') <
                                   pl.col('date_right'))))\
                         .rename({'used_files' : 'proj_to_delete'})\
                         .drop(['date'])\
                         .rename({'date_right': 'date'})\
                         .sort(by= 'yr_qtr')
                         
        # remove superceded files from lists
        #   prev_used_files (.xlsx) & output_proj_files (.parquet)
        # and their .parquet files from the directory of output files
        #   remove the output .parquet file from output_proj_dir
        files_to_remove_list = \
            pl.Series(used_df.select(pl.col('proj_to_delete'))\
                             .filter(pl.col('proj_to_delete')
                                    .is_not_null()))\
                             .to_list()
                            
        if record_dict['prev_used_files']:
            record_dict['prev_used_files'] = \
                list(set(record_dict['prev_used_files']) - 
                     set(files_to_remove_list))
        record_dict['prev_used_files'].sort(reverse= True)
                            
        for file in files_to_remove_list:
            file_list = file.split(" ", 1)
            proj_file = \
                f'{file_list[0]} {file_list[1]
                                    .replace(' ', '-')
                                    .replace('.xlsx', '.parquet')}'
            if proj_file in record_dict['output_proj_files']:
                record_dict['output_proj_files'].remove(proj_file)
                print('\n============================================')
                print(f'Removed {proj_file} from: record_dict.json')
                print(f'Found file with more recent date for the quarter')
                print('============================================\n')
            else:
                print('\n============================================')
                print(f"WARNING")
                print(f"Cannot remove: \n{proj_file} from: record_dict.json")
                print(f'File name is not in list at key: output_proj_files')
                print('============================================\n')
                
            # also remove the file from the dir/
            address_proj_file = sp.path.OUTPUT_PROJ_DIR / proj_file
            if address_proj_file.exists():
                address_proj_file.unlink()
                print('\n============================================')
                print(f'Removed {proj_file} from: \n{sp.path.OUTPUT_PROJ_DIR}')
                print(f'Found file with more recent date for the quarter')
                print('============================================\n')
            else:
                print('\n============================================')
                print(f"WARNING")
                print(f'{sp.path.OUTPUT_PROJ_DIR} \ndoes not contain {proj_file}.')
                print(f"Cannot remove {proj_file} from: \n{sp.path.OUTPUT_PROJ_DIR}")
                print(f'{address_proj_file} does not exist')
                print('============================================\n') 
                
    # when len(seq_) == 0; all data is from new_data
    else:
        used_df = data_df

    del data_df          
    
    # add dates of projections and year_qtr to record_dict
    # https://www.rhosignal.com/posts/polars-nested-dtypes/   pl.list explanation
    # https://www.codemag.com/Article/2212051/Using-the-Polars-DataFrame-Library
    # pl.show_versions()

# files with new data: files_to_read_list, to be
# returned to update_data.py()
    files_to_read_list = \
        pl.Series(used_df.select('new_files')).to_list()
            
    # add dates of projections and year_qtr to record_dict
    if record_dict['prev_used_files']:
        record_dict['prev_used_files'].extend(files_to_read_list)
    else:
        record_dict['prev_used_files'] = files_to_read_list
    
    record_dict['prev_used_files'].sort(reverse= True)
    record_dict['proj_yr_qtrs'] = sorted(
            hp.date_to_year_qtr(
                hp.string_to_date(record_dict['prev_used_files'])), 
            reverse= True)
    
    return record_dict, files_to_read_list
    