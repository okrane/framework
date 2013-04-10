'''
Created on 20 janv. 2011

@author: benca
'''

#--------------------------------------------------------------------
# Metascenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario



M = MetaScenario(True)
M.SetEngine('ROBModel',  {'number_of_bt' : 10,
                          'full'         : True})
M.SetDates(['20110118'])
M.SetStocks([{'trading_destination_ids' : [4, 61, 81, 89],
              'ric'                     : 'BNPP.PA',
              'data_type'               : 'TBT2'}])
M.AddTrader('EmptyAgent', {'context'    : {'ric'                     : 'BNPP.PA',
                                           'security_id'             : 26,
                                           'trading_destination_ids' : [4, 61, 81, 89]},
                           'parameters' : {'append_into_pydata' : False,
                                           'update_bus'         : False,
                                           'get_lob_and_bidasks': False}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/benca/scenarii')