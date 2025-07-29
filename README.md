# S&P500 earnings yield and 10-year TIPS rate
#### initiated:  2023 08
#### current version:  See pyproject.toml
### Uses current and historical data
- index of stock prices for the S&P500
- operating and reported earnings for the S&P500
- projections of operating and reported earnings
- interest rate on 10-year TIPS
- operating margins for the S&P500
- earnings and prices for the S&P's industries

### update_data.py
- reads new data from .xlsx workbooks in input_dir
- S&P data downloaded from S&P's "weekly" posts
- TIPS data downloaded from FRED database
- writes json and parquet files to output_dir
- archives the workbooks from input_dir

### display_data.py
- reads sp500_pe_df_actuals.parquet in output_dir
- reads the files in output_dir/estimates/
- produces pdf documents in display_dir
- presents quarterly data, 2018 through the present
    - page0: projected versus actual earnings
    - page1: future and historical price-earnings ratios
    - page2: margin and equity premium using trailing earnings
    - page3: equity premium using projected earnings

### display_ind_data.py
- reads sp500_ind_df.parquet in output_dir
- produces pdf documents in display_dir
- presents annual data, 2008 through the present
    - page4: distribution of industries' operating P/Es
    - page5: correlation heatmap for industries' operating P/Es
    - page6: distribution of industries' operating earnings

### sources
- https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
- https://fred.stlouisfed.org/series/DFII10/chart

### project file structure
*/sp500_earn_price % tree
```     
richardkopcke@MacBookPro sp500_earn_price % tree
.
├── __init__.py
├── exe_earn_price.py
├── helper_func_module
│   ├── display_helper_func.py
│   ├── display_ind_data_read_df.py
│   ├── display_read_history.py
│   ├── display_read_proj_dict.py
│   ├── display_read_record_dict.py
│   ├── helper_func.py
│   ├── plot_func.py
│   ├── plot_ind_func.py
│   ├── read_data_func.py
│   ├── update_proj_hist_files.py
│   ├── update_record.py
│   ├── update_write_history_and_industry_files.py
│   ├── update_write_proj_file.py
│   ├── update_write_proj_files.py
│   └── update_write_record.py
├── input_output
│   ├── backup_dir
│   │   ├── backup_ind_df.parquet
│   │   ├── backup_pe_df_actuals.parquet
│   │   ├── backup_pe_estimates_df.parquet
│   │   └── backup_record_dict.json
│   ├── display_dir
│   │   ├── eps_page0.pdf
│   │   ├── eps_page1.pdf
│   │   ├── eps_page2.pdf
│   │   ├── eps_page3.pdf
│   │   ├── eps_page4.pdf
│   │   ├── eps_page5.pdf
│   │   └── eps_page6.pdf
│   ├── input_dir
│   │   ├── DFII10.xlsx
│   │   └── sp-500-eps-est 2025 06 17.xlsx
│   ├── output_dir
│   │   ├── sp500_ind_df.parquet
│   │   ├── sp500_pe_df_actuals.parquet
│   │   └── sp500_pe_df_estimates.parquet
│   └── record_dict.json
├── main_script_module
│   ├── display_data.py
│   ├── display_ind_data.py
│   ├── sp_env.py
│   └── update_data.py
├── pyproject.toml
├── README.md
└── uv.lock

10 directories, 63 files
richardkopcke@MacBookPro sp500_earn_price % 
```
<br>
<br>

## Instructions
0. Set ARCHIVE_DIR in main_script_module/sp_env.py

1. Put new .xlsx from S&P into input_dir
    - https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
    - rename: sp-500-eps-est YYYY MM DD.xlsx
    
2. Put new data from FRED into input_dir
    - https://fred.stlouisfed.org/series/DFII10/chart
    - In DFII10.xlsx, add real interest rates for dates that match SP's new file dates

3. Run (from sp500_earn_price/) uv run exe_earn_price.py

    - action 0: update_data.py
        - reads files in input_dir/
        - moves input files to archive
        - writes the existing .json to backup_dir/
        - writes new .json file to sp500-ep-project/record_dict.json
        - moves sp500_pe_df_actuals.parquet to backup_dir/
        - moves sp500_ind_df.parquet to backup_dir/
        - writes new sp500_pe_df_actuals.parquet to output_dir/
        - writes new sp500_ind_df.parquet to output_dir/
        - writes new sp500_pe_df_estimates.parquet to output_dir/

    - action 1: display_data.py
        - reads record_dict.json
        - reads sp500_pe_df_actuals.parquet file in output_dir/
        - reads sp-500-eps-est yyyy-mm-dd.parquet in estimates/
        - writes .pdf pages to display_dir/

    - action 2: display_ind_data.py
        - reads files in output_dir/
        - reads sp500_ind_df.parquet file in output_dir/
        - writes .pdf pages to display_dir/
<br>
<br>

## Other Information
### sp_env.py
-  Contains absolute address of ARCHIVE_DIR
    - user must specify location of ARCHIVE_DIR which contains input files after they have been read
-  Contains addresses for all files relative to the address of the project file
    - addresses of all folders and files (except the ARCHIVE_DIR) fixed by the location of the sp500_ep_project folder, specified by user
- uses pathlib's Path()

### output_dir/
#### sp-500-eps-est YYYY MM DD.parquet
- polars dataframe with projected earnings
- from sp-500-eps-est YYYY MM DD.xlsx
- uses files with the latest date for each quarter
#### sp500_pe_df_actuals.parquet
- one polars dataframe for all historical data
- updated from new input data
### record_dict.json
- records all data files read and written
- records which files have been used
- maintains date of latest file read
- maintains list of quarters covered by data
<br>
<br>

#### To recreate/reinitialize output files from all archived history
1. see sp_env.py
2. debug
    - ensure that DFII10.xlsx is in input_dir
    - copy last workbook of estimates, giving it a more recent date
    - update DFII10 for this new date
3. reinitialize
    - ensure that DFII10.xlsx in INPUT_DIR has data for all quarters
    - where indicated in sp_env.py uncomment INPUT_DIR = ARCHIVE_DIR
    - after reinitialization, recomment INPUT_DIR = ARCHIVE_DIR
