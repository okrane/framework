#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization = True, 
                 real_time             = False, 
                 log_mode              = True, 
                 debug_mode            = False, 
                 max_time              = '01:00:00', 
                 rfa_publisher         = {'enable': False, 'single_pipe': True, 'source_hostname': 'padev016'}, 
                 fix_server            = {'reuse_address': True, 'enable': False, 'socket_port': '5003', 'check_latency': False}, 
                 opening_auction       = False, 
                 intraday_auction      = False, 
                 closing_auction       = False, 
                 stop_auction          = False)
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
M.SetDates(['20111104'])
M.SetStocks([{'trading_venue_names'       : ['CHI-X PARIS', 'NYSE EURONEXT PARIS'], 
              'ric'                       : 'TOTF.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('OrderBookObserver', {'context'    : {'trading_venue_names'       : None, 
                                                  'ric'                       : None},
                                  'parameters' : {'dummy_parameter3'          : 'my_string', 
                                                  'dummy_parameter2'          : 387, 
                                                  'slice_duration'            : 30}})
M.Run(output_files_dir='C:\st_repository\simep_scenarii\gui', schedule=False, split=False)



