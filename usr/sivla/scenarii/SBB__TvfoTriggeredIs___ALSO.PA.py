#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=False)
M.SetEngine('SBBModel', {'include_dark_trades' : False, 
                         'number_of_bt'   : 10, 
                         'full'           : False, 
                         'include_cross_trades' : False, 
                         'include_intradayauction_trades' : False, 
                         'include_tradatlast_trades' : False, 
                         'include_closingauction_trades' : False, 
                         'include_tradafterhours_trades' : False, 
                         'include_stopauction_trades' : False, 
                         'include_openingauction_trades' : False})
M.SetDates([('20100301', '20100301')])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ALSO.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('TvfoTriggeredIs', {'context'    : {'trading_destination_names' : None, 
                                                'ric'                       : None},
                                'parameters' : {'tactic_max_lifetime'       : '00:30:00:000000', 
                                                'tactic_class'              : 'SlippageStrategysimep', 
                                                'algo_start_time'           : '+03:00:00:000000', 
                                                'total_qty'                 : 1000, 
                                                'full_filename_skeleton'    : 'C:/Results/results', 
                                                'delta_trigger'             : '00:30:00:000000', 
                                                'algo_end_time'             : '+06:30:00:000000', 
                                                'limit_price'               : 1000.0, 
                                                'ltb'                       : '0.59999999999999998, 0.80000000000000004, 0.59999999999999998', 
                                                'arrival_price_offset'      : 0, 
                                                'optimal_qty'               : 500, 
                                                'tactic_module'             : 'usr.dev.weibing.agents.SlippageStrategysimep', 
                                                'lto'                       : '1, 1, 0.80000000000000004', 
                                                'side'                      : 'Order.Buy'}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/weibing/scenarii')



