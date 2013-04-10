'''
Created on 2 febr. 2011

@author: benca
'''


from simep.scenarii.metascenario import MetaScenario


M = MetaScenario('C:/st_repository/simep_scenarii/mymakingstrat_simulation.py', True)
M.SetEngine('ROBModel', {'number_of_bt' : 10,
                         'full'         : False})
M.SetDates('20101108')
M.SetStocks([{'trading_destination_id'  : 4,
              'ric'                     : 'GSZ.PA',
              'data_type'               : 'TBT2'}])
M.AddTrader('MyMarketMakingStrategy', {'parameters' : {'slice_timestep'         : '00:05:00:000000', 
                                                       'plot_mode'              : 1,
                                                       'gamma'                  : 1.0,
                                                       'sigma'                  : 0.0017,
                                                       'k'                      : 250,
                                                       'A'                      : 0.49,
                                                       'dq'                     : 130.0,
                                                       'debug'                  : False,
                                                       'show_curve'             : 'pnl',
                                                       'benchmark'              : False,
                                                       'perform_market_making'  : True}})
M.GenerateAndRunSimulations('C:/st_repository/simep_scenarii')

# k = 114.0
# A = 0.42
# gamma = 5