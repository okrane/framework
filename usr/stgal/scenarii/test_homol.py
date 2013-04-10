#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization = True, 
                 real_time             = True, 
                 log_mode              = True, 
                 debug_mode            = False, 
                 max_time              = '00:03:00',#TODO : parameter 
                 rfa_publisher         = {'enable': True, 'single_pipe': True, 'source_hostname': 'padev016'},#TODO  : parameter 
                 fix_server            = {'reuse_address': True, 'enable': True, 'socket_port': '5003', 'check_latency': False},#TODO  : parameter 
                 opening_auction       = False,#TODO  : True + parameters start+end times 
                 intraday_auction      = False,#TODO  : True + parameters start+end times
                 closing_auction       = False,#TODO  : True + parameters start+end times
                 stop_auction          = False)#TODO  : True when implemented


#parameters for M.SetEngine depend on the desired engine, cf. st_sim.xml

M.SetEngine('ROBModel', {'number_of_bt'   : 1,#TODO : determined from SBB 
                         'full'           : True, 
                         'include_closingauction_trades' : True, 
                         'include_intradayauction_trades' : True, 
                         'include_tradatlast_trades' : False, 
                         'prefixing_qty_bias' : 0.0, 
                         'include_cross_trades' : False, 
                         'include_stopauction_trades' : True, 
                         'include_openingauction_trades' : True, 
                         'include_tradafterhours_trades' : False})
M.SetDates(['20110118'])

#parameters for M.SetStocks depend on the desired engine, cf. st_sim.xml, because we have to
#include the context parameters of the stock for this engine
M.SetStocks([{'trading_venue_names'       : ['CHI-X DELTA PARIS', 'CHI-X PARIS', 'CHI-X PARIS DARK', 'NYSE EURONEXT PARIS'], 
              'ric'                       : 'ACCP.PA', 
              'data_type'                 : 'TBT2'},
             {'trading_venue_names'       : ['CHI-X DELTA PARIS', 'CHI-X PARIS', 'CHI-X PARIS DARK', 'NYSE EURONEXT PARIS'], 
              'ric'                       : 'BNPP.PA', 
              'data_type'                 : 'TBT2'}])

#M.SetStocksForMFGen([{'trading_venue_names'       : ['CHI-X DELTA PARIS', 'CHI-X PARIS', 'CHI-X PARIS DARK', 'NYSE EURONEXT PARIS'], 
#              'ric'                       : 'ACCP.PA', 
#              'data_type'                 : 'TBT2',
#              'reference_price'           : 20.55
#              },
#             {'trading_venue_names'       : ['CHI-X DELTA PARIS', 'CHI-X PARIS', 'CHI-X PARIS DARK', 'NYSE EURONEXT PARIS'], 
#              'ric'                       : 'BNPP.PA', 
#              'data_type'                 : 'TBT2'}])

M.AddTrader('LobChecker', {'context'    : {'trading_venue_names'       : ['CHI-X DELTA PARIS', 'CHI-X PARIS', 'CHI-X PARIS DARK', 'NYSE EURONEXT PARIS'], 
                                           'ric'                       : 'ACCP.PA'},
                           'parameters' : {}})
M.Run(output_files_dir='C:/st_repository/simep_scenarii/gui', schedule=False, split=False)



