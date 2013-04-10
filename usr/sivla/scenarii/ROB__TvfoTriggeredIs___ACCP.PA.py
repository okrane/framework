#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=False, disable_bus=False)
M.SetEngine('SBBModel', {'number_of_bt'   : 10, 
                         'full'           : False})
M.SetDates(['20110301'])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ACCP.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('TvfoTriggeredIs', {'context'    : {'trading_destination_names' : ['ENPA'], 
                                                'ric'                       : 'ACCP.PA'},
                                'parameters' : {'tactic_max_lifetime'       : '00:30:00:000000', 
                                                'tactic_class'              : 'BinaryAccelerator', 
                                                'algo_start_time'           : '+03:00:00:000000', 
                                                'total_qty'                 : 5000, 
                                                'full_filename_skeleton'    : 'C:/Results/results', 
                                                'delta_trigger'             : '00:30:00:000000', 
                                                'algo_end_time'             : '+06:30:00:000000', 
                                                'limit_price'               : 1000.0, 
                                                'arrival_price_offset'      : 0, 
                                                'optimal_qty'               : 1000, 
                                                'tactic_module'             : 'usr.dev.st_algo.agents.BinaryAccelerator', 
                                                'side'                      : 'Order.Buy'}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



