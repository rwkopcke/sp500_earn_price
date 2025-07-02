import polars as pl

def write(proj_dict, new_files_set, env):
    '''
        proj_dict: keys, year_quarter of projection
        env: provides the address for storing the data
        
        Writes the projection data for each year_quarter
        to a single parquet file. 
        proj_dict
            keys are year_quarters
            values are df with projections for future qtrs
        proj_df is the format for saving proj_dict
            cols = proj_df keys
            rows are structs, one struct for each
                future qtr in the projection
            a struct contains projections for various
                measures of E for its quarter

        To recover proj_df
            with proj_address.open('rb') as f:
                proj_df = pl.read_parquet(f)
                
        To recover proj_dict
            proj_dict = proj_df.to_dict(as_series= False)
            
        To recover each proj_date_df
            for k in proj_dict.keys():
                # for year_quarter == k
                proj_date_df = pl.DataFrame(proj_dict[k])
    '''
    
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## ++++ Save updated proj_hist_df +++++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    proj_address = env.OUTPUT_PROJ_ADDR

    # convert proj_dict to df to save with parquet
    # use concat to compensate for diff # of rows in each proj_date_df
    # pads short cols with null
    # creates col for each key (k)
    
    proj_hist_df = \
        pl.concat(
            items= [pl.DataFrame({key: value})
                    for key, value in proj_dict.items()],
            how= "horizontal")
    
    with proj_address.open('wb') as f:
        proj_hist_df.write_parquet(f)
        
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## ++++ Archive all new files +++++++++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # new_files_set is not empty  
    for file in new_files_set:
        address = env.INPUT_DIR / file
        new_address = env.ARCHIVE_DIR / file
        address.rename(new_address)
    print('\n====================================================')
    print('Archived all new input projection files')
    print(f'moved from {env.INPUT_DIR}')
    print(f'to         {env.ARCHIVE_DIR}')
    print('====================================================')
        
    return
