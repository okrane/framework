#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=True)
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : False})
M.SetDates(['20110302'])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'FTE.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('TvfoTriggeredIs', {'context'    : {'trading_destination_names' : ['ENPA'], 
                                                'ric'                       : 'FTE.PA'},
                                'parameters' : {'tactic_max_lifetime'       : '00:10:00:000000', 
                                                'tactic_class'              : 'Cruiser', 
                                                'companion_module'          : 'usr.dev.st_algo.agents.CruiserCompanion',
                                                'companion_class'           : 'CruiserCompanion',
                                                'algo_start_time'           : '+00:03:00:000000', 
                                                'total_qty'                 : 50000, 
                                                'full_filename_skeleton'    : 'C:/Results/results', 
                                                'delta_trigger'             : '00:10:00:000000', 
                                                'algo_end_time'             : '+06:30:00:000000', 
                                                'limit_price'               : 1000.0, 
                                                'arrival_price_offset'      : 0, 
                                                'optimal_qty'               : 20000, 
                                                'tactic_module'             : 'usr.dev.st_algo.agents.Cruiser', 
                                                'side'                      : 'Order.Buy', 
                                                'lob_indicators'            : False,
                                                'order_indicators'          : False,
                                                'market_indicators'         : False,
                                                'companion_indicators'      : True}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



