from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.models.robmodel import *
from usr.dev.sivla.agents.AlgoStoAgent import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '4FTE.PA', 110)
stock_FTE_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                             'name'                    : 'stock_FTE_PA', 
                             'engine_type'             : 'light', 
                             'allowed_data_types'      : 'BINARY;TBT2'},
                            {'trading_destination_id'  : 4, 
                             'name'                    : 'stock_FTE_PA', 
                             'data_type'               : 'TBT2', 
                             'opening'                 : {4: '08:00:00:000'}, 
                             'input_file_names'        : {4: 'Y:/tick_ged'}, 
                             'trading_destination_names' : ['ENPA'], 
                             'rics'                    : {4: 'FTE.PA'}, 
                             'trading_destination_ids' : [4], 
                             'date'                    : '20110128', 
                             'security_id'             : 110, 
                             'closing'                 : {4: '16:30:00:000'}, 
                             'ric'                     : 'FTE.PA', 
                             'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                            {'number_of_bt'            : 10, 
                             'full'                    : False},
                            trace)
sched.addAgent(stock_FTE_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'trading_destination_id'  : 4, 
                          'name'                    : 'stock_FTE_PA', 
                          'data_type'               : 'TBT2', 
                          'opening'                 : {4: '08:00:00:000'}, 
                          'input_file_names'        : {4: 'Y:/tick_ged'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'rics'                    : {4: 'FTE.PA'}, 
                          'trading_destination_ids' : [4], 
                          'date'                    : '20110128', 
                          'security_id'             : 110, 
                          'closing'                 : {4: '16:30:00:000'}, 
                          'ric'                     : 'FTE.PA', 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
AlgoStoAgent000_FTE_PA = AlgoStoAgent({'class_name'              : 'AlgoStoAgent', 
                                       'counter'                 : 0, 
                                       'name'                    : 'AlgoStoAgent000'},
                                      {'trading_destination_ids' : [4], 
                                       'date'                    : '20110128', 
                                       'security_id'             : 110, 
                                       'trading_destination_names' : None, 
                                       'ric'                     : 'FTE.PA', 
                                       'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/AlgoStoAgent/FTE_PA_004/DAY_20110128/39991a558864decc2f393aca2c37fef0'},
                                      {'q'                       : 1000, 
                                       'cycle'                   : 15, 
                                       'side'                    : 0, 
                                       'delta'                   : 2},
                                      trace)
AlgoStoAgent000_FTE_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(AlgoStoAgent000_FTE_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
SimulationResults = {}
SimulationResults['AlgoStoAgent000_FTE_PA']  = AlgoStoAgent000_FTE_PA.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
AlgoStoAgent000_FTE_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/AlgoStoAgent/FTE_PA_004/DAY_20110128/39991a558864decc2f393aca2c37fef0.mat')
ScenarioDict.insertScenarioParameters(103, 'AlgoStoAgent', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/AlgoStoAgent/FTE_PA_004/DAY_20110128/39991a558864decc2f393aca2c37fef0.mat', 'FTE.PA', 110, [4], '20110128', 'ROBModel', "{'q': 1000, 'cycle': 15, 'side': 0, 'delta': 2}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(AlgoStoAgent000_FTE_PA)
del(stock_FTE_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



