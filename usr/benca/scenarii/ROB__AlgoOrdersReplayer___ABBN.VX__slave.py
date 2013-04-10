from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.agents.algo_orders_replayer import *
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
sched.addOrderBook('light', '18ABBN.VX', 80940)
stock_ABBN_VX_018 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_ABBN_VX', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {18: '07:01:10:000'}, 
                              'input_file_names'        : {18: 'C:/Histo/lobTrade_80940_18_20100823.binary'}, 
                              'trading_destination_names' : ['VIRTX'], 
                              'date'                    : '20100823', 
                              'ric'                     : 'ABBN.VX', 
                              'trading_destination_id'  : 18, 
                              'name'                    : 'stock_ABBN_VX', 
                              'rics'                    : {18: 'ABBN.VX'}, 
                              'trading_destination_ids' : [18], 
                              'security_id'             : 80940, 
                              'closing'                 : {18: '15:20:00:000'}, 
                              'tick_sizes'              : {18: [(0.0, 0.0001), (50.0, 0.050000000000000003), (100.0, 0.10000000000000001), (500.0, 0.5), (1000.0, 1.0), (5000.0, 5.0), (0.5, 0.00050000000000000001), (1.0, 0.001), (5.0, 0.0050000000000000001), (10.0, 0.01), (10000.0, 10.0)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_ABBN_VX_018)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'data_type'               : 'BINARY', 
                          'opening'                 : {18: '07:01:10:000'}, 
                          'input_file_names'        : {18: 'C:/Histo/lobTrade_80940_18_20100823.binary'}, 
                          'trading_destination_names' : ['VIRTX'], 
                          'date'                    : '20100823', 
                          'ric'                     : 'ABBN.VX', 
                          'trading_destination_id'  : 18, 
                          'name'                    : 'stock_ABBN_VX', 
                          'rics'                    : {18: 'ABBN.VX'}, 
                          'trading_destination_ids' : [18], 
                          'security_id'             : 80940, 
                          'closing'                 : {18: '15:20:00:000'}, 
                          'tick_sizes'              : {18: [(0.0, 0.0001), (50.0, 0.050000000000000003), (100.0, 0.10000000000000001), (500.0, 0.5), (1000.0, 1.0), (5000.0, 5.0), (0.5, 0.00050000000000000001), (1.0, 0.001), (5.0, 0.0050000000000000001), (10.0, 0.01), (10000.0, 10.0)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
AlgoOrdersReplayer000_ABBN_VX = AlgoOrdersReplayer({'class_name'              : 'AlgoOrdersReplayer', 
                                                    'counter'                 : 0, 
                                                    'name'                    : 'AlgoOrdersReplayer000_ABBN_VX'},
                                                   {'trading_destination_ids' : [18], 
                                                    'date'                    : '20100823', 
                                                    'security_id'             : 80940, 
                                                    'trading_destination_names' : ['VIRTX'], 
                                                    'ric'                     : 'ABBN.VX', 
                                                    'output_filename'         : 'C:/st_sim/usr/dev/benca/scenarii/ROB/AlgoOrdersReplayer/ABBN_VX_018/DAY_20100823/f245028930812d2fca60b6adb8056e6e'},
                                                   {'delta_t'                 : '00:00:01:000', 
                                                    'loading_method'          : 'headed_text_file', 
                                                    'cmd_filename'            : 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt', 
                                                    'observer_class'          : 'OrdersReplayerObserver', 
                                                    'observer_module'         : 'simep.subagents.orders_replayer_observer'},
                                                   trace)
AlgoOrdersReplayer000_ABBN_VX.isTvfoAgent(__tvfo_mode__)
sched.addAgent(AlgoOrdersReplayer000_ABBN_VX)
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
SimulationResults['AlgoOrdersReplayer000_ABBN_VX']  = AlgoOrdersReplayer000_ABBN_VX.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
AlgoOrdersReplayer000_ABBN_VX.write_mat_file('C:/st_sim/usr/dev/benca/scenarii/ROB/AlgoOrdersReplayer/ABBN_VX_018/DAY_20100823/f245028930812d2fca60b6adb8056e6e.mat')
ScenarioDict.insertScenarioParameters(702, 'AlgoOrdersReplayer', 'C:/st_sim/usr/dev/benca/scenarii/ROB/AlgoOrdersReplayer/ABBN_VX_018/DAY_20100823/f245028930812d2fca60b6adb8056e6e.mat', 'ABBN.VX', 80940, [18], '20100823', 'ROBModel', "{'delta_t': '00:00:01:000', 'loading_method': 'headed_text_file', 'cmd_filename': 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt', 'observer_class': 'OrdersReplayerObserver', 'observer_module': 'simep.subagents.orders_replayer_observer'}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(AlgoOrdersReplayer000_ABBN_VX)
del(stock_ABBN_VX_018)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



