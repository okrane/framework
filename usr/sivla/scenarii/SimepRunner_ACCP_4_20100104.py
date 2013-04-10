#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

engine_params = {
                    'number_of_bt'           : 10,
                    'full'                   : True
}
stock_params = {
                    'date'                   : '20100104',
                    'trading_destination_id' : 4,
                    'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100104.binary',
                    'data_type'              : 'BINARY',
                    'trading_destination_name' : 'ENPA',
                    'security_id'            : 2,
                    'ric'                    : 'ACCP.PA'
}
Cycle001_params = {
    'setup'      : {'name'                   : 'Cycle001'},
    'context'    : {'date'                   : '20100104', 
                    'security_id'            : 2, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'ACCP.PA'},
    'parameters' : {'bussinessTime'          : True, 
                    'plot_mode'              : 0, 
                    'd'                      : 1, 
                    'cycle'                  : 10, 
                    'side'                   : 'Order.Buy', 
                    'size'                   : 10000}
}



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

my_scenario = MetaScenario('C:/st_sim/usr/dev/sivla/scenarii\\SimepRunner_ACCP_4_20100104_slave.py')
my_scenario.SetEngine('ROBModel', engine_params)
my_scenario.SetUniverse({'20100104' : [stock_params]})
my_scenario.AddTrader('CyclePlacement', Cycle001_params)
my_scenario.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



