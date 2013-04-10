#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=True, disable_bus=False)
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : False})
M.SetDates([('20110128', '20110128')])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'FTE.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('AlgoStoAgent', {'context'    : {'trading_destination_names' : None, 
                                             'ric'                       : None},
                             'parameters' : {'q'                         : 1000, 
                                             'cycle'                     : 15, 
                                             'side'                      : 0, 
                                             'delta'                     : 2}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')



