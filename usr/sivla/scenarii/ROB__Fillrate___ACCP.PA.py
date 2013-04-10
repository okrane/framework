#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario()
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : True})
M.SetDates(['20100104'])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ACCP.PA', 
              'data_type'                 : 'BINARY'}])
M.AddTrader('Fillrate', {'context'    : {'trading_destination_names' : ['ENPA'], 
                                         'ric'                       : 'ACCP.PA'},
                         'parameters' : {'cycle'                     : 15, 
                                         'd': 0, 
                                         'side': 'Order.Sell'}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



