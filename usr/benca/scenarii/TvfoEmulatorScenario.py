#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

engine_params = {
                    'number_of_bt'           : 10,
                    'full'                   : False
}
stock_params = {
                    'date'                   : '20100524',
                    'trading_destination_id' : 4,
                    'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100524.binary',
                    'data_type'              : 'BINARY',
                    'trading_destination_name' : 'ENPA',
                    'security_id'            : 2,
                    'ric'                    : 'ACCP.PA'
}
TvfoEmulator001_params = {
    'setup'      : {'indicators'             : None, 
                    'name'                   : 'TvfoEmulator001'},
    'context'    : {'date'                   : '20100524', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'tactic_max_lifetime'    : '01:00:00:000', 
                    'algo_start_time'        : '+03:00:00:000', 
                    'plot_mode'              : 1, 
                    'algo_end_time'          : '+06:00:00:000', 
                    'limit_price'            : 39.5, 
                    'asked_qty'              : 70000, 
                    'optimal_qty'            : 500, 
                    'side'                   : 'Order.Buy'}
}



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

my_scenario = MetaScenario('C:/st_repository/simep_scenarii/gui\\SimepRunner_ACCP_4_20100524_slave.py')
my_scenario.SetEngine('SBBModel', engine_params)
my_scenario.SetUniverse({'20100524' : [stock_params]})
my_scenario.AddTrader('TvfoEmulator', TvfoEmulator001_params)
my_scenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')



