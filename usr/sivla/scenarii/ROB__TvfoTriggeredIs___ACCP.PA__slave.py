from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.agents.tvfo_is_tactic_triggerer import *
from simep.models.robmodel import *



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
                             {'trading_destination_id'  : 4, 
                              'name'                    : 'stock_ACCP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {4: '08:00:00:000'}, 
                              'input_file_names'        : {4: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA'], 
                              'rics'                    : {4: 'ACCP.PA'}, 
                              'trading_destination_ids' : [4], 
                              'date'                    : '20110301', 
                              'security_id'             : 2, 
                              'closing'                 : {4: '16:30:00:000'}, 
                              'ric'                     : 'ACCP.PA', 
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
AnalyticsManager.new_bus({'trading_destination_id'  : 4, 
                          'name'                    : 'stock_ACCP_PA', 
                          'data_type'               : 'TBT2', 
                          'opening'                 : {4: '08:00:00:000'}, 
                          'input_file_names'        : {4: 'Y:/tick_ged'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'rics'                    : {4: 'ACCP.PA'}, 
                          'trading_destination_ids' : [4], 
                          'date'                    : '20110301', 
                          'security_id'             : 2, 
                          'closing'                 : {4: '16:30:00:000'}, 
                          'ric'                     : 'ACCP.PA', 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         ['avg_spread_bp_t_60', 'garman_klass_bp_s_900'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
TvfoTriggeredIs000_ACCP_PA = TvfoTriggeredIs({'class_name'              : 'TvfoTriggeredIs', 
                                              'counter'                 : 0, 
                                              'name'                    : 'TvfoTriggeredIs000_ACCP_PA'},
                                             {'trading_destination_ids' : [4], 
                                              'date'                    : '20110301', 
                                              'security_id'             : 2, 
                                              'trading_destination_names' : ['ENPA'], 
                                              'ric'                     : 'ACCP.PA', 
                                              'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/ACCP_PA_004/DAY_20110301/ca0eb878104279f4fdec3b62b3d28265'},
                                             {'tactic_max_lifetime'     : '00:30:00:000000', 
                                              'tactic_class'            : 'BinaryAccelerator', 
                                              'algo_start_time'         : '+03:00:00:000000', 
                                              'full_filename_skeleton'  : 'C:/Results/results', 
                                              'delta_trigger'           : '00:30:00:000000', 
                                              'algo_end_time'           : '+06:30:00:000000', 
                                              'side'                    : Order.Buy, 
                                              'limit_price'             : 1000.0, 
                                              'arrival_price_offset'    : 0, 
                                              'optimal_qty'             : 1000, 
                                              'total_qty'               : 5000, 
                                              'tactic_module'           : 'usr.dev.st_algo.agents.BinaryAccelerator'},
                                             trace)
TvfoTriggeredIs000_ACCP_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(TvfoTriggeredIs000_ACCP_PA)
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
SimulationResults['TvfoTriggeredIs000_ACCP_PA']  = TvfoTriggeredIs000_ACCP_PA.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
TvfoTriggeredIs000_ACCP_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/ACCP_PA_004/DAY_20110301/ca0eb878104279f4fdec3b62b3d28265.mat')
ScenarioDict.insertScenarioParameters(39, 'TvfoTriggeredIs', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/ACCP_PA_004/DAY_20110301/ca0eb878104279f4fdec3b62b3d28265.mat', 'ACCP.PA', 2, [4], '20110301', 'ROBModel', "{'tactic_max_lifetime': '00:30:00:000000', 'tactic_class': 'BinaryAccelerator', 'algo_start_time': '+03:00:00:000000', 'full_filename_skeleton': 'C:/Results/results', 'delta_trigger': '00:30:00:000000', 'algo_end_time': '+06:30:00:000000', 'side': 'Order.Buy', 'limit_price': 1000.0, 'arrival_price_offset': 0, 'optimal_qty': 1000, 'total_qty': 5000, 'tactic_module': 'usr.dev.st_algo.agents.BinaryAccelerator'}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(TvfoTriggeredIs000_ACCP_PA)
del(stock_ACCP_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



