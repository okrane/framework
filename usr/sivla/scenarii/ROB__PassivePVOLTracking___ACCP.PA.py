#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=False, disable_bus=False)
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : False})
M.SetDates(['20101109'])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ACCP.PA', 
              'data_type'                 : 'BINARY'}])
M.AddTrader('PassivePVOLTracking', {'context'    : {'trading_destination_names' : None, 
                                                    'ric'                       : None},
                                    'parameters' : {'c'                         : 2, 
                                                    'k'                         : 0.2, 
                                                    'side'                      : 'Order.Buy'}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



