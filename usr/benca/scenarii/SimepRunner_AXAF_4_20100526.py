#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



#--------------------------------------------------------------------
# Define slave file : this is the file which will be generated just before simulation
#--------------------------------------------------------------------

SimulationSlaveFile = 'C:/st_repository/simep_scenarii/gui\\SimepRunner_AXAF_4_20100526_slave.py'
InputXMLFile = 'C:/st_sim/simep/st_sim.xml'



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

EngineParametersDictionary = {
                    'number_of_bt'           : 10,
                    'full'                   : False
}
ExchangeParametersDictionary = {
                    'date'                   : '20100526',
                    'trading_destination_id' : 4,
                    'input_file_name'        : 'C:/Histo/lobTrade_18_4_20100526.binary',
                    'trading_destination_name' : 'ENPA',
                    'security_id'            : 18,
                    'ric'                    : 'AXAF.PA'
}
OrdersReplayer001_params = {
    'setup'      : {'name'                   : 'OrdersReplayer001'},
    'context'    : {'date'                   : '20100526', 
                    'security_id'            : 18, 
                    'trading_destination_id' : 4, 
                    'ric'                    : 'AXAF.PA'},
    'parameters' : {'loading_method'         : 'headed_text_file', 
                    'cmd_filename'           : 'C:/st_sim/dev/usr/benca/data/detail_occ_3_Jn}0026.txt'}
}



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

MyScenario = MetaScenario(InputXMLFile, SimulationSlaveFile)
MyScenario.SetEngine('ROBModel', EngineParametersDictionary)
MyScenario.SetUniverse({'20100526' : [ExchangeParametersDictionary]})
MyScenario.AddTrader('OrdersReplayer', OrdersReplayer001_params)
MyScenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')



