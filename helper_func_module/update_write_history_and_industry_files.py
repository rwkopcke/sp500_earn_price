def write(actual_df, ind_df, env):
         
## +++++ write history file +++++++++++++++++++++++++++++++++++++++++++
    # move any existing hist file in output_dir to backup
    if env.OUTPUT_HIST_ADDR.exists():
        env.OUTPUT_HIST_ADDR.rename(env.BACKUP_HIST_ADDR)
        print('\n============================================')
        print(f'Moved history file from: \n{env.OUTPUT_HIST_ADDR}')
        print(f'to: \n{env.BACKUP_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'Found no history file at: \n{env.OUTPUT_HIST_ADDR}')
        print(f'Wrote no history file to: \n{env.BACKUP_HIST_ADDR}')
        print('============================================\n')
        
    # write actual_df, the historical data, into the output_dir
    with env.OUTPUT_HIST_ADDR.open('w') as f:
        actual_df.write_parquet(f)
    print('\n============================================')
    print(f'Wrote history file to: \n{env.OUTPUT_HIST_ADDR}')
    print('============================================\n')
    
## +++++ write industry file ++++++++++++++++++++++++++++++++++++++++++
    # move any existing industry file in output_dir to backup
    if env.OUTPUT_IND_ADDR.exists():
        env.OUTPUT_IND_ADDR.rename(env.BACKUP_IND_ADDR)
        print('\n============================================')
        print(f'Moved industry file from: \n{env.OUTPUT_IND_ADDR}')
        print(f'to: \n{env.BACKUP_IND_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'Found no industry file at: \n{env.OUTPUT_IND_ADDR}')
        print(f'Wrote no industry file to: \n{env.BACKUP_IND_ADDR}')
        print('============================================\n')
        
    # write ind_df, the industry data, into the output_dir
    with env.OUTPUT_IND_ADDR.open('w') as f:
        ind_df.write_parquet(f)
    print('\n============================================')
    print(f'Wrote industry file to: \n{env.OUTPUT_IND_ADDR}')
    print('============================================\n')
    
    return
