def main():
    '''
        Calls the three main scripts that produce this
        project's data and displays
    '''
    
    action_dict = {
        "0": 'Update data from recent S&P and FRED workbooks',
        "1": 'Generate Displays for the S&P500 Index',
        "2": 'Generate Displays for the S&P500 Industries'
    }
    
    while True:
        
        print('\n' * 3)
        for key in action_dict.keys():
            print(f'{key}: {action_dict[key]}')
    
        action = input(
            '\nEnter the key shown above for the intended action: ')
        
        match action:
            case "0":
                from main_script_module import update_data
                update_data.update()
            case "1":
                from main_script_module import display_data
                display_data.display()
            case "2":
                from main_script_module import display_ind_data
                display_ind_data.display_ind()
            case _:
                print(f'{action} is not a valid key')
                
        choice = input(
            '\nTo take another action, type T; otherwise, type F: ')
        if choice not in ['T', 't', 'True', 'Y', 'y', 'yes']:
            break


if __name__ == "__main__":
    main()