#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=False)
M.SetEngine('ROBModel', {'include_dark_trades' : False, 
                         'number_of_bt'   : 10, 
                         'full'           : True, 
                         'include_cross_trades' : False, 
                         'include_intradayauction_trades' : False, 
                         'include_tradatlast_trades' : False, 
                         'include_closingauction_trades' : False, 
                         'include_tradafterhours_trades' : False, 
                         'include_stopauction_trades' : False, 
                         'include_openingauction_trades' : False})
M.SetDates([('20110301', '20110301')])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'FTE.PA', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('Fillrate', {'context'    : {'trading_destination_names' : None, 
                                         'ric'                       : None},
                         'parameters' : {'cycle'                     : 15, 
                                         'd'                         : 0, 
                                         'side'                      : 0}})
M.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')



