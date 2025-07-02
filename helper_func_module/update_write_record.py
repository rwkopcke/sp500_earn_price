import json

def write(record_dict, env):
    with open(env.RECORD_DICT_ADDR, 'w') as f:
        json.dump(record_dict, f, indent= 4)

    record_dict_prev_used_files = \
        record_dict['prev_used_files'][:4]
    print('\n====================================================')
    print('Saved record_dict to file')
    print(f'{env.RECORD_DICT_ADDR}')
    print(f'\nlatest_used_file: {record_dict['latest_used_file']}\n')
    print(f'prev_files: \n{record_dict['prev_files'][:4]}\n')
    print(f"record_dict['prev_used_files']: \n{record_dict_prev_used_files}\n")
    print('====================================================\n')
    
    return
