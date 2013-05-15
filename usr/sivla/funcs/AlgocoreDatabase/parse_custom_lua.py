import re

f = open('C:/Documents and Settings/sivla/Desktop/Dashboard Day/custom.lua', 'r');
g = open('C:/Documents and Settings/sivla/Desktop/Dashboard Day/dash_files_03.sql', 'w')
x = 'asafaga'
dico = {}
eurosmartx_id = 0
condition_id = 0
action_id = 0
eurosmartx_client_id = 0

def randClient():
    import random
    a = ['MW', 'DFA', 'Citadel', 'AB', 'Amundi' ]
    return a[random.randrange(len(a))]
waiting_for_condition = False
while x:    
    x = f.readline()
    
    function_pattern = r"function (.*)\("
    function_list = re.match(function_pattern, x);
    if function_list:
        current_callback = function_list.group(1)
    
    end_list = re.match(r".*end", x)
    if end_list:
        waiting_for_condition = False
    elif waiting_for_condition:
        strings = x.split(' = ');
        variable = strings[0].split('.')[1]
        value = strings[1].strip()
        action_id = action_id + 1        
        print ('insert into dash_eurosmartx_action (action_id, eurosmartx_id, variable_name, variable_value) values (%d, %d, "%s", \'%s\')' % (action_id, eurosmartx_id, variable, value))
        
    
    if_list = re.match(r".*if (.*) then", x)    
    if if_list:
        waiting_for_condition = True
        for cond in if_list.group(1).split(" and "):
            if '==' in cond:
                strings = cond.split(' == ');
                if 'TargetStrategy' in strings[0]:
                    eurosmartx_id = eurosmartx_id + 1
                    print('insert into dash_eurosmartx (eurosmartx_id, algo_name, callback_type) values (%d, %s, "%s")' % (eurosmartx_id, strings[1], current_callback))
                    # insert smartx strings[1]
                else:
                    variable = strings[0].split('.')[1]
                    value = strings[1]
                    condition_id = condition_id + 1
                    print ( 'insert into dash_eurosmartx_condition (condition_id, eurosmartx_id, relation_type, variable_name, condition_value) values (%d, %d, "==", "%s", \'%s\')' % (condition_id, eurosmartx_id, variable, value  ))
                    
            else:
                if "Client" in cond:
                    eurosmartx_client_id = eurosmartx_client_id + 1
                    print ('insert into dash_eurosmartx_client (eurosmartx_client_id, client_name, eurosmartx_id) values (%d, "%s", %d)' % (eurosmartx_client_id, randClient() ,eurosmartx_id))
    
    
    
        
        
    
    