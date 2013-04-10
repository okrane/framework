#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization = False, 
                 real_time             = False, 
                 log_mode              = True, 
                 debug_mode            = False, 
                 max_time              = '01:00:00', 
                 rfa_publisher         = {'enable': False, 'single_pipe': True, 'source_hostname': 'padev016'}, 
                 fix_server            = {'reuse_address': True, 'enable': False, 'socket_port': '5003', 'check_latency': False}, 
                 opening_auction       = True, 
                 intraday_auction      = True, 
                 closing_auction       = True, 
                 stop_auction          = True)
M.SetEngine('ROBModel', {'number_of_bt'   : 1, 
                         'full'           : True, 
                         'include_closingauction_trades' : True, 
                         'include_intradayauction_trades' : True, 
                         'include_tradatlast_trades' : False, 
                         'prefixing_qty_bias' : 0.0, 
                         'include_cross_trades' : False, 
                         'include_stopauction_trades' : True, 
                         'include_openingauction_trades' : True, 
                         'include_tradafterhours_trades' : False})
M.SetDates([('20111104', '20111104')])
M.SetStocks([{'trading_venue_names'       : ['LONDON STOCK EXCHANGE'], 
              'ric'                       : 'VOD.L', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('VolatilityObserver', {'context'    : {'trading_venue_names'       : None, 
                                                   'ric'                       : None},
                                   'parameters' : {'num_trades'                : 1000, 
                                                   'start_time'                : 36000000000.0}})
M.Run(output_files_dir='C:\st_repository\simep_scenarii\gui\simep_scenarii\gui', schedule=False, split=False)



