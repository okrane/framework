from simep import __tvfo_mode__, __release__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
if __release__:
    from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from usr.dev.sivla.agents.LobRecorder import *
from simep.models.robmodel import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'name'                    : 'stock_ACCP_PA', 
                          'data_type'               : 'TBT2', 
                          'opening'                 : {4: '08:00:00:000'}, 
                          'input_file_names'        : {4: 'Y:/tick_ged'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'rics'                    : {4: 'ACCP.PA'}, 
                          'trading_destination_ids' : [4], 
                          'date'                    : '20110118', 
                          'security_id'             : 2, 
                          'closing'                 : {4: '16:30:00:000'}, 
                          'ric'                     : 'ACCP.PA', 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         [])
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
                              'date'                    : '20110118', 
                              'security_id'             : 2, 
                              'closing'                 : {4: '16:30:00:000'}, 
                              'ric'                     : 'ACCP.PA', 
                              'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'include_dark_trades'     : False, 
                              'number_of_bt'            : 10, 
                              'full'                    : True, 
                              'include_intradayauction_trades' : False, 
                              'include_tradatlast_trades' : False, 
                              'include_closingauction_trades' : False, 
                              'include_cross_trades'    : False, 
                              'include_tradafterhours_trades' : False, 
                              'include_stopauction_trades' : False, 
                              'include_openingauction_trades' : False},
                             trace)
sched.addAgent(stock_ACCP_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
LobRecorder000_ACCP_PA = LobRecorder({'class_name'              : 'LobRecorder', 
                                      'counter'                 : 0, 
                                      'name'                    : 'LobRecorder000'},
                                     {'trading_destination_ids' : [4], 
                                      'date'                    : '20110118', 
                                      'security_id'             : 2, 
                                      'trading_destination_names' : None, 
                                      'ric'                     : 'ACCP.PA', 
                                      'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/LobRecorder/ACCP_PA_004/DAY_20110118/2dfb1f9ca7051e95b36db178cffa32c5'},
                                     {'start'                   : '20110118-14:30:00', 
                                      'end'                     : '20110118-15:00:00'},
                                     trace)
LobRecorder000_ACCP_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(LobRecorder000_ACCP_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
LobRecorder000_ACCP_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
if __release__:
    LobRecorder000_ACCP_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/LobRecorder/ACCP_PA_004/DAY_20110118/2dfb1f9ca7051e95b36db178cffa32c5.mat')
    ScenarioDict.insertScenarioParameters(106, 'LobRecorder', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/LobRecorder/ACCP_PA_004/DAY_20110118/2dfb1f9ca7051e95b36db178cffa32c5.mat', 'ACCP.PA', 2, [4], '20110118', 'ROBModel', "{'start': '20110118-14:30:00', 'end': '20110118-15:00:00'}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Post process                    #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(LobRecorder000_ACCP_PA)
del(stock_ACCP_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



