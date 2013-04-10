from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.models.robmodel import *
from usr.dev.sivla.agents.PassivePVOLTracking import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '4ACCP.PA', 2)
stock_ACCP_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_ACCP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {4: '08:00:00:000'}, 
                              'input_file_names'        : {4: 'C:/Histo/lobTrade_2_4_20101109.binary'}, 
                              'trading_destination_names' : ['ENPA'], 
                              'date'                    : '20101109', 
                              'ric'                     : 'ACCP.PA', 
                              'trading_destination_id'  : 4, 
                              'name'                    : 'stock_ACCP_PA', 
                              'rics'                    : {4: 'ACCP.PA'}, 
                              'trading_destination_ids' : [4], 
                              'security_id'             : 2, 
                              'closing'                 : {4: '16:30:00:000'}, 
                              'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : False},
                             trace)
sched.addAgent(stock_ACCP_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'data_type'               : 'BINARY', 
                          'opening'                 : {4: '08:00:00:000'}, 
                          'input_file_names'        : {4: 'C:/Histo/lobTrade_2_4_20101109.binary'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'date'                    : '20101109', 
                          'ric'                     : 'ACCP.PA', 
                          'trading_destination_id'  : 4, 
                          'name'                    : 'stock_ACCP_PA', 
                          'rics'                    : {4: 'ACCP.PA'}, 
                          'trading_destination_ids' : [4], 
                          'security_id'             : 2, 
                          'closing'                 : {4: '16:30:00:000'}, 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
PassivePVOLTracking000_ACCP_PA = PassivePVOLTracking({'class_name'              : 'PassivePVOLTracking', 
                                                      'counter'                 : 0, 
                                                      'name'                    : 'PassivePVOLTracking000'},
                                                     {'trading_destination_ids' : [4], 
                                                      'date'                    : '20101109', 
                                                      'security_id'             : 2, 
                                                      'trading_destination_names' : None, 
                                                      'ric'                     : 'ACCP.PA', 
                                                      'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/PassivePVOLTracking/ACCP_PA_004/DAY_20101109/ff563ba9cd81a407d8ff40987445c4ef'},
                                                     {'c'                       : 2, 
                                                      'k'                       : 0.2, 
                                                      'side'                    : Order.Buy},
                                                     trace)
PassivePVOLTracking000_ACCP_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(PassivePVOLTracking000_ACCP_PA)
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
SimulationResults['PassivePVOLTracking000_ACCP_PA']  = PassivePVOLTracking000_ACCP_PA.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
PassivePVOLTracking000_ACCP_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/PassivePVOLTracking/ACCP_PA_004/DAY_20101109/ff563ba9cd81a407d8ff40987445c4ef.mat')
ScenarioDict.insertScenarioParameters(68, 'PassivePVOLTracking', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/PassivePVOLTracking/ACCP_PA_004/DAY_20101109/ff563ba9cd81a407d8ff40987445c4ef.mat', 'ACCP.PA', 2, [4], '20101109', 'ROBModel', "{'c': 2, 'k': 0.20000000000000001, 'side': 'Order.Buy'}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(PassivePVOLTracking000_ACCP_PA)
del(stock_ACCP_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



