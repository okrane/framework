#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=True, disable_bus=False)
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : True})
M.SetDates([('20090601', '20090709'), 
            ('20090714', '20110406')])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'STM.PA', 
              'data_type'                 : 'TBT2'}, 
             {'trading_destination_names' : ['CEDBOR'], 
              'ric'                       : 'STM.MI', 
              'data_type'                 : 'TBT2'}])
M.AddTrader('StocksComparator', {'context'    : {'trading_destination_names' : ['ENPA'], 
                                                 'ric'                       : 'STM.PA'},
                                 'parameters' : {'security_id2'              : 7153, 
                                                 'ric2'                      : 'STM.MI', 
                                                 'plot_mode'                 : 0, 
                                                 'trading_destination_id2'   : 6}})
M.GenerateAndScheduleSimulations('C:/st_repository/simep_scenarii/gui')



