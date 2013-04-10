   
    
def main(sec_id):    
    #--------------------------------------------------------------------
    # Import modules
    #--------------------------------------------------------------------
    
    from simep.scenarii.metascenario import MetaScenario
    import math
    from simep.sched import Order
    from simep.funcs.dbtools.securities_tools import SecuritiesTools
    
    
    
    # select characteristics daily
    import sqlite3
    conn = sqlite3.connect('C:/st_repository/simep_databases/trading_daily')
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    txt_side = 'buy'
    
    if txt_side.lower() == 'buy':
        order_side = Order.Buy
    else:
        order_side = Order.Sell
        
    
    date_str_beg = '2010-01-01'
    date_str_end = '2010-01-30'
    # from Matlab get ALL security id and the trading_destination_id
    if isinstance(sec_id, int):
        tab_sec_id = [sec_id]
    elif isinstance(sec_id, list):
        tab_sec_id = sec_id
    else:
        tab_sec_id = [10735 ,10762 ,10779 ,10780 ,10844 ,11874 ,11882 ,11883 ,12072 ,\
                     12179 ,12183 ,12184 ,12185 ,12186 ,12375 ,12780 ,12811 ,12812 ,12815 ,\
                     12818 ,12820 ,12869 ,12972 ,12990 ,13094 ,13144 ,14358 ,15069 ,15070 ,\
                     15391 ,15777 ,15780 ,16127 ,16505 ,16770 ,16786 ,16789 ,16812 ,16816 ,\
                     16864 ,16867 ,16869 ,16919 ,17069 ,38359 ,38481 ,38836 ,39552 ,46123 ,\
                     46326 ,60107 ,63018 ,104894 ,119485 ,154959 ,283812 ,292446 ,363224 ,375542]
        

    trading_destination_id = 13

# pb: 12184 12811 12990 15391 16864 17069 38836 283812 60107
    # set up the index
     
    ref_idx_sec_id = 12058
    ref_idx_td_id  = trading_destination_id
    ref_idx        = SecuritiesTools.get_ric('sqlite', ref_idx_sec_id, trading_destination_id)
    
    global dates_dict, dictUniverse
    dates_dict = {}
    dictUniverse = {}
    
    
    for i in range(0,len(tab_sec_id)):
        security_id  = tab_sec_id[i] 
        ric  = SecuritiesTools.get_ric('sqlite', security_id, trading_destination_id)
        c.execute("select date, volume, nb_deal, open_prc, high_prc, low_prc from characteristics where date between date('%s') and  date('%s') and trading_destination_id = %d and security_id = %d" \
                  % (date_str_beg ,date_str_end, trading_destination_id, security_id))
        for data in c:
            date_str = str(data['date'])
            date_str = date_str[0:4] + date_str[5:7] + date_str[8:10]
            dates_dict[date_str] = {}
            if (order_side == Order.Buy):        
                dates_dict[date_str]['soft_limit'] = 0.0001
                price_thrsh = data['high_prc']
    
            else:
                dates_dict[date_str]['soft_limit'] = 10000.0
                price_thrsh = data['low_prc']
    
                
    #        dates_dict[date_str]['price_limit']       = 1.0/3.0*data['open_prc']+ 2.0/3.0*price_thrsh
            # deactive price limit
            dates_dict[date_str]['price_limit']       = price_thrsh - (2.0*(order_side==Order.Buy) - 1)*0.01  
            dates_dict[date_str]['hist_trade_volume'] = data['volume']
            dates_dict[date_str]['hist_avg_t_size']   = int(math.floor(data['volume']/data['nb_deal']))
            dates_dict[date_str]['buy']               = order_side
            
            # update dictUniverse
            dictUniverse[date_str] = [{'ric' : ric,
                                       'trading_destination_id' : trading_destination_id,
                                       'security_id' : security_id}]
        
        #--------------------------------------------------------------------
        # Metascenario
        #--------------------------------------------------------------------
        
        SimulationSlaveFile = 'C:/st_sim/dev/tests/gui/SimepRunner_ACCP_4_20100106_slave.py'
        InputXMLFile = 'C:/st_sim/simep/st_sim.xml'
        EngineParametersDictionary = {
            'number_of_bt'           : 10,
            'full'                   : False
        }
        def custom_func(date, context, params):
            for (key, val) in dates_dict[date].iteritems():
                params[key] = val
            return params
        dfaFloat001_params = {
            'setup'      : {'name'                   : 'dfaFloat001'},
            'context'    : {'trading_destination_id' : trading_destination_id, 
                            'ric'                    : None},
            'parameters' : {'min_size_2_join'        : 20, 
                            'buy'                    : None, 
                            'ref_idx'                : ref_idx, 
                            'ref_idx_sec_id'         : ref_idx_sec_id,
                            'ref_idx_td_id'          : ref_idx_td_id,
                            'idx_rel_off'            : None, 
                            'one_tick'               : 0.01, 
                            'child_fin'              : 100, 
                            'rel_del'                : 0, 
                            'rnd_fact'               : 20, 
                            'tag_vol_src'            : True,
                            'child_size'             : 20, 
                            'hist_trade_volume'      : None, 
                            'soft_limit'             : None, 
                            'const_max_size'         : 50, 
                            'ats_width'              : 60, 
                            'price_limit'            : None,
                            'o_rel_trig'             : 0, 
                            'hist_avg_t_size'        : None},
            'custom_func' : custom_func }
        
        MyScenario = MetaScenario(InputXMLFile, SimulationSlaveFile)
        MyScenario.SetEngine('ROBModel', EngineParametersDictionary)
        MyScenario.SetUniverse(dictUniverse)
        MyScenario.AddTrader('dfa_float', dfaFloat001_params)
        MyScenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/DFA/%s' % txt_side.lower())
    
    conn.close()


if __name__ == '__main__':
    #sec_id = input('Enter the sec ID you want : ')
    main(None)