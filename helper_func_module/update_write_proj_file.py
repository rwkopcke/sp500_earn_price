def write_files(record_dict, proj_df,
                year_quarter, name_date, env):
    
    output_file_name = \
        f'{env.PREFIX_OUTPUT_FILE_NAME} {name_date}{env.EXT_OUTPUT_FILE_NAME}'
    record_dict['output_proj_files'].append(output_file_name)
    output_file_address = env.OUTPUT_PROJ_DIR / output_file_name
    
    print(year_quarter)
    print(f'output file: {output_file_name}')
    print(proj_df['yr_qtr'].to_list())
    print()
    
    with output_file_address.open('w') as f:
        proj_df.write_parquet(f)
            
    return
