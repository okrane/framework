#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



#--------------------------------------------------------------------
# Define slave file : this is the file which will be generated just before simulation
#--------------------------------------------------------------------

SimulationSlaveFile = 'C:/st_repository/simep_scenarii/gui\\SimepRunner_ABBN_VX_18_2010823_slave.py'
InputXMLFile = 'C:/st_sim/simep/st_sim.xml'



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

EngineParametersDictionary = {
                    'number_of_bt'           : 10,
                    'full'                   : True
}
ExchangeParametersDictionary = {
                    'date'                   : '20100823',
                    'trading_destination_id' : 18,
                    'input_file_name'        : 'Q:/tick_ged',
                    'trading_destination_name' : 'VIRTX',
                    'security_id'            : 80940,
                    'ric'                    : 'ABBN.VX',
                    'data_type'              : 'TBT2'
}
OrdersReplayer001_params = {
    'setup'      : {'name'                   : 'OrdersReplayer001'},
    'context'    : {'date'                   : '20100823', 
                    'security_id'            : 80940, 
                    'trading_destination_id' : 18, 
                    'ric'                    : 'ABBN.VX'},
    'parameters' : {'delta_t'                : '00:00:01:000',
                    'loading_method'         : 'headed_text_file', 
                    'cmd_filename'           : 'C:/st_sim/dev/usr/benca/data/detail_occ_3_modified.txt'}
}



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

MyScenario = MetaScenario(InputXMLFile, SimulationSlaveFile)
MyScenario.SetEngine('ROBModel', EngineParametersDictionary)
MyScenario.SetUniverse({'20100823' : [ExchangeParametersDictionary]})
MyScenario.AddTrader('OrdersReplayer', OrdersReplayer001_params)
MyScenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')



