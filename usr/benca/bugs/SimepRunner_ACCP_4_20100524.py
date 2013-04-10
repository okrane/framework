#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



#--------------------------------------------------------------------
# Define slave file : this is the file which will be generated just before simulation
#--------------------------------------------------------------------

SimulationSlaveFile = 'C:\\st_sim\\dev\\tests\\gui\\SimepRunner_ACCP_4_20100524_slave.py'
InputXMLFile = 'C:/st_sim/simep/st_sim.xml'



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

EngineParametersDictionary = {
                    'number_of_bt'           : 10,
                    'full'                   : False
}
ExchangeParametersDictionary = {
                    'date'                   : '20100524',
                    'trading_destination_id' : 4,
                    'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100524.binary',
                    'trading_destination_name' : 'ENPA',
                    'security_id'            : 2,
                    'ric'                    : 'ACCP.PA'
}
VWAPManager002_params = {
    'setup'      : {'indicators'             : 'avg_exec_time;avg_trade_size_300', 
                    'name'                   : 'VWAPManager002'},
    'context'    : {'date'                   : '20100524', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'max_per'                : 0.20, 
                    'would_level'            : -1.0, 
                    'use_pursuit'            : True, 
                    'aggressive_use_business_time' : True, 
                    'plot_mode'              : 1, 
                    'start_time'             : '07:00:00', 
                    'pursuit_distance'       : 1, 
                    'pursuit_use_constant_size' : True, 
                    'min_per'                : 0.01, 
                    'limit_price'            : -1.0, 
                    'asked_qty'              : 100000, 
                    'end_time'               : '15:30:00', 
                    'pursuit_use_business_time' : True, 
                    'use_wouldlevel'         : True, 
                    'use_aggressive'         : True, 
                    'execution_style'        : 'Aggressive', 
                    'side'                   : 'Order.Sell', 
                    'pursuit_cycle_time'     : 5}
}
VWAPManager001_params = {
    'setup'      : {'indicators'             : 'avg_exec_time;avg_trade_size_300', 
                    'name'                   : 'VWAPManager001'},
    'context'    : {'date'                   : '20100524', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'max_per'                : 0.20, 
                    'would_level'            : -1.0, 
                    'use_pursuit'            : True, 
                    'aggressive_use_business_time' : True, 
                    'plot_mode'              : 1, 
                    'start_time'             : '07:00:00', 
                    'pursuit_distance'       : 1, 
                    'pursuit_use_constant_size' : True, 
                    'min_per'                : 0.01, 
                    'limit_price'            : -1.0, 
                    'asked_qty'              : 100000, 
                    'end_time'               : '15:30:00', 
                    'pursuit_use_business_time' : True, 
                    'use_wouldlevel'         : True, 
                    'use_aggressive'         : True, 
                    'execution_style'        : 'Aggressive', 
                    'side'                   : 'Order.Buy', 
                    'pursuit_cycle_time'     : 4}
}
VWAPManager005_params = {
    'setup'      : {'indicators'             : 'avg_exec_time;avg_trade_size_300', 
                    'name'                   : 'VWAPManager005'},
    'context'    : {'date'                   : '20100524', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'max_per'                : 0.20, 
                    'would_level'            : -1.0, 
                    'use_pursuit'            : True, 
                    'aggressive_use_business_time' : True, 
                    'plot_mode'              : 1, 
                    'start_time'             : '07:00:00', 
                    'pursuit_distance'       : 1, 
                    'pursuit_use_constant_size' : True, 
                    'min_per'                : 0.01, 
                    'limit_price'            : -1.0, 
                    'asked_qty'              : 200000, 
                    'end_time'               : '15:30:00', 
                    'pursuit_use_business_time' : True, 
                    'use_wouldlevel'         : True, 
                    'use_aggressive'         : False, 
                    'execution_style'        : 'Aggressive', 
                    'side'                   : 'Order.Sell', 
                    'pursuit_cycle_time'     : 7}
}
VWAPManager004_params = {
    'setup'      : {'indicators'             : 'avg_exec_time;avg_trade_size_300', 
                    'name'                   : 'VWAPManager004'},
    'context'    : {'date'                   : '20100524', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'max_per'                : 0.20, 
                    'would_level'            : -1.0, 
                    'use_pursuit'            : True, 
                    'aggressive_use_business_time' : True, 
                    'plot_mode'              : 1, 
                    'start_time'             : '07:00:00', 
                    'pursuit_distance'       : 1, 
                    'pursuit_use_constant_size' : True, 
                    'min_per'                : 0.01, 
                    'limit_price'            : -1.0, 
                    'asked_qty'              : 200000, 
                    'end_time'               : '15:30:00', 
                    'pursuit_use_business_time' : True, 
                    'use_wouldlevel'         : True, 
                    'use_aggressive'         : True, 
                    'execution_style'        : 'Aggressive', 
                    'side'                   : 'Order.Buy', 
                    'pursuit_cycle_time'     : 1}
}



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

MyScenario = MetaScenario(InputXMLFile, SimulationSlaveFile)
MyScenario.SetEngine('SBBModel', EngineParametersDictionary)
MyScenario.SetUniverse({'20100524' : [ExchangeParametersDictionary]})
#MyScenario.AddTrader('VWAPManager', VWAPManager002_params)
#MyScenario.AddTrader('VWAPManager', VWAPManager001_params)
MyScenario.AddTrader('VWAPManager', VWAPManager005_params)
MyScenario.AddTrader('VWAPManager', VWAPManager004_params)
MyScenario.GenerateAndRunSimulations('C:\\st_sim\\dev\\tests\\gui')



