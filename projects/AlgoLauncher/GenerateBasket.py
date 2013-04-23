'''
Created on 20 mars 2013

@author: gpons
'''

import xlrd

excel_file = './cfg/fix_analytics.xls'
workbook = xlrd.open_workbook(excel_file)
    
def from_FLEXtoDICO(f_name):
    worksheet = workbook.sheet_by_name('Sheet1')
    num_rows = worksheet.nrows
    
    curr_row = 1
    found = 0
    while found == 0 and curr_row < num_rows:
        if str(worksheet.cell_value(curr_row, 0)) == f_name:
            found = str((worksheet.cell_value(curr_row, 1)))
        curr_row += 1
    
    return found

def getConf(l_params):
    conf = {}
    worksheet = workbook.sheet_by_name('Sheet2')
    
    num_rows = worksheet.nrows
    num_cols = worksheet.ncols
    
    for param in l_params:
        found = 0
        
        curr_row = 1
        while found == 0 and curr_row < num_rows:
            
            if str(worksheet.cell_value(curr_row, 0)) == param:
                found = 1
                conf_param = {}
                def_val = str(worksheet.cell_value(curr_row, 1))
                
                curr_col = 2
                case_val = []
                while curr_col < num_cols and worksheet.cell_type(curr_row, curr_col) != 0:
                    case_val.append(str(worksheet.cell_value(curr_row, curr_col)))
                    curr_col += 1
                
                if param != 'OrderQty' and param != 'Price':
                    case_val.append(' ')
                
                conf_param['default'] = def_val
                conf_param['case_vals'] = case_val
                conf[param] = conf_param
            
            curr_row += 1
    return conf

if __name__ == '__main__':
    
    #FIXED PARAMS
    trd_venue = 'XPAR'
    sec_symbol = 'FTE'
    sec_isin = 'FR0000133308'
    side = '1'
    curr = 'EUR'
    
    # INVENTORY STRATEGY AND ASSOCIATED PARAMS
    worksheet = workbook.sheet_by_name('Sheet3')
    
    StratParams = {}
    
    num_rows = worksheet.nrows
    num_cols = worksheet.ncols
    
    curr_row = 1
    while curr_row < num_rows:
        curr_col = 2
        list_params = []
        id_strat = str(int(worksheet.cell_value(curr_row, 0))) + ':' + str(worksheet.cell_value(curr_row, 1))
        
        while curr_col < num_cols:
            if worksheet.cell_value(curr_row, curr_col) == 'Optional' or worksheet.cell_value(curr_row, curr_col) == 'Mandatory':
                list_params.append(str(worksheet.cell_value(0, curr_col)))
            curr_col += 1
            
        StratParams[id_strat] = list_params
        curr_row += 1
    
    for u, v in StratParams.items():
        id_split = u.rsplit(':')
        
        algo_name = id_split[1] 
        
        basket_file = open('./inputs/basket_test_%s.txt' %algo_name, 'w')
        
        str_params = 'StrategyName|ExDestination|Symbol|SecurityID|Side|Currency|'
        dico_params = []
        for params in v:
            d_param = from_FLEXtoDICO(params)
            str_params = str_params + d_param + '|'
            dico_params.append(d_param)
        
        str_params = str_params + '\n'
        basket_file.write(str_params)
        
        p_conf = getConf(dico_params)
        
        for v_param in dico_params:
            
            #params fixes
            case_values = p_conf[v_param]['case_vals']
            i = 0
            while i < len(case_values):
                
                param_str = id_split[0] + '|' + trd_venue + '|' + sec_symbol + '|' + sec_isin + '|' + side + '|' + curr + '|'
                for f_param in dico_params:
                    
                    if f_param != v_param:
                        #on prend la valeur par defaut
                        param_str = param_str + p_conf[f_param]['default'] + '|'
                    else:
                        #on itere sur la liste des cas
                        param_str = param_str + case_values[i] + '|'
                        i += 1
                    
                param_str = param_str + '\n'
                basket_file.write(param_str)

        