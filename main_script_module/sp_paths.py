'''
    set global absolute paths for file addresses
        using absolute path to "s&p500_ep_project" directory
    create universal path expressions
        using Path() from pathlib
        
    access these dir and addresses in other modules by
        import sp_paths as sp
            which allows the address for INPUT_DIR to be accessed
            as sp.INPUT_DIR
'''

from pathlib import Path
from dataclasses import dataclass

@dataclass(frozen= True)
class Fixed_locations:
    # source of new data, to be stored in the output file record_dict.json
    # .../sp500_earn_price/input_output/record_dict.json
    SP_SOURCE = \
        "https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all"
    REAL_RATE_SOURCE = "https://fred.stlouisfed.org/series/DFII10"

    # path to dir sp500_earn_price/  -- the project's root
    # Path() produces a "universal path" (for OS or Windows)
    #      allows simple appending for extensions
    #      => no '/' at end of path, provided by format of append
    BASE_DIR = Path.cwd()

    # paths to the project's subdirectories
    INPUT_OUTPUT_DIR = BASE_DIR / "input_output"

    RECORD_DICT_DIR = INPUT_OUTPUT_DIR
    RECORD_DICT_FILE = "record_dict.json"
    RECORD_DICT_ADDR = RECORD_DICT_DIR / RECORD_DICT_FILE

    INPUT_DIR = INPUT_OUTPUT_DIR / "input_dir"
    INPUT_RR_FILE = 'DFII10.xlsx'
    INPUT_RR_ADDR = INPUT_DIR / INPUT_RR_FILE
    # #INPUT_SPRICE_FILE = INPUT_DIR / 'SP500.xlsx'

    ARCHIVE_DIR = \
    Path('/Users/richardkopcke/Dropbox/Stock Analysis/sp_data_archive')
    # #INPUT_DIR = ARCHIVE_DIR
    # this path to be set explicitly by user
    # ...
    # for debugging, from the archived source .xlsx files:
    #   ensure that DFII10.xlsx is in input_dir
    #   move the latest sp input file from archive to input_dir
    #   replace record_dict,json with its backup file from backup_dir
    # ...
    # to reinitialize the project's entire output
    #   ensure that DFII10.xlsx in INPUT_DIR has data for all quarters
    #   after setting ARCHIVE_DIR above, reset INPUT_DIR
    #   INPUT_DIR = ARCHIVE_DIR

    OUTPUT_DIR = INPUT_OUTPUT_DIR / "output_dir"
    OUTPUT_HIST_FILE = 'sp500_pe_df_actuals.parquet'
    OUTPUT_HIST_ADDR = OUTPUT_DIR / OUTPUT_HIST_FILE

    OUTPUT_PROJ_DIR = OUTPUT_DIR / 'estimates'

    OUTPUT_IND_FILE = 'sp500_ind_df.parquet'
    OUTPUT_IND_ADDR = OUTPUT_DIR / OUTPUT_IND_FILE

    BACKUP_DIR = INPUT_OUTPUT_DIR / 'backup_dir'
    BACKUP_HIST_FILE = "backup_pe_df_actuals.parquet"
    BACKUP_HIST_ADDR = BACKUP_DIR / BACKUP_HIST_FILE
    BACKUP_IND_FILE  = "backup_ind_df.parquet"
    BACKUP_IND_ADDR  = BACKUP_DIR / BACKUP_IND_FILE
    BACKUP_RECORD_DICT =  "backup_record_dict.json"
    BACKUP_RECORD_DICT_ADDR = BACKUP_DIR / BACKUP_RECORD_DICT

    DISPLAY_DIR = INPUT_OUTPUT_DIR / "display_dir"
    DISPLAY_0 = 'eps_page0.pdf'
    DISPLAY_1 = 'eps_page1.pdf'
    DISPLAY_2 = 'eps_page2.pdf'
    DISPLAY_3 = 'eps_page3.pdf'
    DISPLAY_4 = 'eps_page4.pdf'
    DISPLAY_5 = 'eps_page5.pdf'
    DISPLAY_6 = 'eps_page6.pdf'
    DISPLAY_0_ADDR = DISPLAY_DIR / DISPLAY_0
    DISPLAY_1_ADDR = DISPLAY_DIR / DISPLAY_1
    DISPLAY_2_ADDR = DISPLAY_DIR / DISPLAY_2
    DISPLAY_3_ADDR = DISPLAY_DIR / DISPLAY_3
    DISPLAY_4_ADDR = DISPLAY_DIR / DISPLAY_4
    DISPLAY_5_ADDR = DISPLAY_DIR / DISPLAY_5
    DISPLAY_6_ADDR = DISPLAY_DIR / DISPLAY_6
    
path = Fixed_locations()