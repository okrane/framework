#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario


my_scenario = MetaScenario()
my_scenario.SetEngine('ROBModel', {'number_of_bt' : 10,
                                   'full'         : True,
                                   'seed'         : 4})
my_scenario.SetDates([('20090601', '20090709'), ('20090714', '20101231')])
my_scenario.SetStocks([{'data_type'               : 'TBT2', 
                        'ric'                     : 'STM.PA', 
                        'trading_destination_ids' : [4]},
                       {'data_type'               : 'TBT2', 
                        'ric'                     : 'STM.MI', 
                        'trading_destination_ids' : [6]}])
my_scenario.AddTrader('StocksComparator', {'context'    : {'trading_destination_ids' : [4], 
                                                           'ric'                     : 'STM.PA'},
                                           'parameters' : {'trading_destination_id2' : 6, 
                                                           'ric2'                    : 'STM.MI', 
                                                           'security_id2'            : 7153,
                                                           'results_filename'        : 'C:/stockscomparator_res'}})
my_scenario.GenerateAndScheduleSimulations('C:/st_repository/simep_scenarii')



