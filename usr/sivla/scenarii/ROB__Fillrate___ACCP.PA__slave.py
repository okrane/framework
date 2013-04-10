from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from usr.dev.sivla.agents.Fillrate import *
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
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {4: '08:00:00:000'}, 
                              'input_file_names'        : {4: 'C:/Histo/lobTrade_2_4_20100104.binary'}, 
                              'trading_destination_names' : ['ENPA'], 
                              'date'                    : '20100104', 
                              'ric'                     : 'ACCP.PA', 
                              'trading_destination_id'  : 4, 
                              'name'                    : 'stock_ACCP_PA', 
                              'rics'                    : {4: 'ACCP.PA'}, 
                              'trading_destination_ids' : [4], 
                              'security_id'             : 2, 
                              'closing'                 : {4: '16:30:00:000'}, 
                              'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
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
                          'input_file_names'        : {4: 'C:/Histo/lobTrade_2_4_20100104.binary'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'date'                    : '20100104', 
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
Fillrate000_ACCP_PA = Fillrate({'class_name'              : 'Fillrate', 
                                'counter'                 : 0, 
                                'name'                    : 'Fillrate000'},
                               {'trading_destination_ids' : [4], 
                                'date'                    : '20100104', 
                                'security_id'             : 2, 
                                'trading_destination_names' : None, 
                                'ric'                     : 'ACCP.PA', 
                                'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/Fillrate/ACCP_PA_004/DAY_20100104/14e2a9c1ee93a53df8a734b5213166f2'},
                               {'cycle'                   : 15, 
                                'side'                    : Order.Sell, 
                                'd'                       : 0},
                               trace)
Fillrate000_ACCP_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(Fillrate000_ACCP_PA)
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
SimulationResults['Fillrate000_ACCP_PA']  = Fillrate000_ACCP_PA.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
Fillrate000_ACCP_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/Fillrate/ACCP_PA_004/DAY_20100104/14e2a9c1ee93a53df8a734b5213166f2.mat')
ScenarioDict.insertScenarioParameters(95, 'Fillrate', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/Fillrate/ACCP_PA_004/DAY_20100104/14e2a9c1ee93a53df8a734b5213166f2.mat', 'ACCP.PA', 2, [4], '20100104', 'ROBModel', "{'cycle': 15, 'side': 'Order.Sell', 'd': 0}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(Fillrate000_ACCP_PA)
del(stock_ACCP_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



